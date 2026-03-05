"""Unit tests for DBTProjectTemplate."""
import pytest
import pandas as pd
import sys
sys.path.insert(0, "/Users/johndoe/projects/dbt-project-template")
from src.main import DBTProjectTemplate


@pytest.fixture
def model_df():
    return pd.DataFrame({
        "model_name": ["stg_projects", "stg_activities", "int_carbon_metrics", "mart_nbs_kpis", "BadModelName", "stg_sites"],
        "layer": ["staging", "staging", "intermediate", "mart", "staging", "staging"],
        "materialization": ["view", "view", "table", "table", "view", "view"],
        "has_tests": [True, False, True, True, False, True],
        "has_description": [True, True, False, True, False, True],
        "depends_on": ["source", "source", "stg_projects", "int_carbon_metrics", "source", "source"],
    })


@pytest.fixture
def dbt():
    return DBTProjectTemplate(config={"project_name": "pur_analytics"})


class TestValidation:
    def test_empty_raises(self, dbt):
        with pytest.raises(ValueError, match="empty"):
            dbt.validate(pd.DataFrame())

    def test_missing_model_name_raises(self, dbt):
        df = pd.DataFrame({"layer": ["staging"], "has_tests": [True]})
        with pytest.raises(ValueError, match="Missing required columns"):
            dbt.validate(df)

    def test_valid_passes(self, dbt, model_df):
        assert dbt.validate(model_df) is True


class TestModelQualityReport:
    def test_returns_expected_keys(self, dbt, model_df):
        result = dbt.model_quality_report(model_df)
        assert "total_models" in result
        assert "test_coverage_pct" in result
        assert "doc_coverage_pct" in result
        assert "naming_violations" in result

    def test_total_models_correct(self, dbt, model_df):
        result = dbt.model_quality_report(model_df)
        assert result["total_models"] == 6

    def test_naming_violation_detected(self, dbt, model_df):
        result = dbt.model_quality_report(model_df)
        assert "BadModelName" in result["naming_violations"]

    def test_snake_case_not_violation(self, dbt, model_df):
        result = dbt.model_quality_report(model_df)
        assert "stg_projects" not in result["naming_violations"]

    def test_untested_models_listed(self, dbt, model_df):
        result = dbt.model_quality_report(model_df)
        assert "stg_activities" in result["untested_models"]
        assert "BadModelName" in result["untested_models"]

    def test_test_coverage_pct_calculation(self, dbt, model_df):
        result = dbt.model_quality_report(model_df)
        # 4 out of 6 models have tests
        assert abs(result["test_coverage_pct"] - 66.67) < 1.0

    def test_layer_distribution(self, dbt, model_df):
        result = dbt.model_quality_report(model_df)
        assert result["models_by_layer"]["staging"] == 4
        assert result["models_by_layer"]["mart"] == 1


class TestGenerateModelYamlStub:
    def test_generates_yaml_string(self, dbt):
        yaml = dbt.generate_model_yaml_stub("stg_projects", ["project_id", "area_ha"])
        assert "stg_projects" in yaml
        assert "project_id" in yaml
        assert "not_null" in yaml

    def test_invalid_model_name_raises(self, dbt):
        with pytest.raises(ValueError, match="snake_case"):
            dbt.generate_model_yaml_stub("BadModel!", ["col1"])

    def test_staging_materialized_as_view(self, dbt):
        yaml = dbt.generate_model_yaml_stub("stg_test", ["id"], layer="staging")
        assert "view" in yaml

    def test_mart_materialized_as_table(self, dbt):
        yaml = dbt.generate_model_yaml_stub("mart_test", ["id"], layer="mart")
        assert "table" in yaml


class TestGenerateModelStub:
    def test_returns_string(self, dbt):
        result = dbt.generate_model_stub("my_model")
        assert isinstance(result, str)

    def test_contains_model_name(self, dbt):
        result = dbt.generate_model_stub("stg_orders")
        assert "stg_orders" in result

    def test_contains_config_block(self, dbt):
        result = dbt.generate_model_stub("int_revenue", materialization="table")
        assert "config(" in result
        assert "materialized='table'" in result

    def test_contains_cte_for_sources(self, dbt):
        result = dbt.generate_model_stub("mart_kpis", source_models=["int_carbon", "int_area"])
        assert "ref('int_carbon')" in result
        assert "ref('int_area')" in result

    def test_incremental_clause(self, dbt):
        result = dbt.generate_model_stub("fact_daily", materialization="incremental")
        assert "is_incremental" in result

    def test_invalid_layer_raises(self, dbt):
        with pytest.raises(ValueError, match="layer must be"):
            dbt.generate_model_stub("bad_model", layer="invalid")

    def test_invalid_materialization_raises(self, dbt):
        with pytest.raises(ValueError, match="materialization must be"):
            dbt.generate_model_stub("bad_model", materialization="stream")


class TestTestCoverageReport:
    def test_returns_dict(self, dbt, model_df):
        result = dbt.test_coverage_report(model_df)
        assert isinstance(result, dict)

    def test_coverage_pct_in_range(self, dbt, model_df):
        result = dbt.test_coverage_report(model_df)
        assert 0 <= result["test_coverage_pct"] <= 100

    def test_untested_models_list(self, dbt, model_df):
        result = dbt.test_coverage_report(model_df)
        assert isinstance(result["untested_models"], list)
        assert "stg_activities" in result["untested_models"]

    def test_layer_breakdown_present(self, dbt, model_df):
        result = dbt.test_coverage_report(model_df)
        assert "layer_breakdown" in result
        assert "staging" in result["layer_breakdown"]

    def test_missing_columns_raises(self, dbt):
        df = pd.DataFrame({"model_name": ["m1", "m2"]})
        with pytest.raises(ValueError, match="has_tests"):
            dbt.test_coverage_report(df)
