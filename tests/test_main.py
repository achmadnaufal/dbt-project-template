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
