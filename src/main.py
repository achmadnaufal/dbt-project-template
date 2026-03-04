"""
dbt project template utilities for NbS and pharma BI data modeling.

Provides tools for dbt project scaffolding, model quality analysis,
source freshness checking, and documentation generation helpers.

Author: github.com/achmadnaufal
"""
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional, Dict, Any, List
import json
import re


class DBTProjectTemplate:
    """
    dbt project template and analysis utilities.

    Helps analytics engineers manage dbt projects: analyze model complexity,
    check source/model naming conventions, generate documentation stubs,
    and report on model test coverage.

    Args:
        config: Optional dict with keys:
            - project_name: dbt project name (default "my_project")
            - max_model_complexity: Warning threshold for model dependency depth (default 5)

    Example:
        >>> dbt = DBTProjectTemplate(config={"project_name": "pur_analytics"})
        >>> df = dbt.load_data("data/models_inventory.csv")
        >>> report = dbt.model_quality_report(df)
        >>> print(report["test_coverage_pct"])
    """

    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.project_name = self.config.get("project_name", "my_project")
        self.max_depth = self.config.get("max_model_complexity", 5)

    def load_data(self, filepath: str) -> pd.DataFrame:
        """
        Load dbt model inventory from CSV or Excel.

        Args:
            filepath: Path to file. Expected columns: model_name, layer,
                      has_tests, has_description, depends_on, materialization.

        Returns:
            DataFrame with model inventory.

        Raises:
            FileNotFoundError: If file does not exist.
        """
        p = Path(filepath)
        if not p.exists():
            raise FileNotFoundError(f"Data file not found: {filepath}")
        if p.suffix in (".xlsx", ".xls"):
            return pd.read_excel(filepath)
        return pd.read_csv(filepath)

    def validate(self, df: pd.DataFrame) -> bool:
        """
        Validate model inventory structure.

        Args:
            df: DataFrame to validate.

        Returns:
            True if valid.

        Raises:
            ValueError: If empty or missing required columns.
        """
        if df.empty:
            raise ValueError("Input DataFrame is empty")
        df_cols = [c.lower().strip().replace(" ", "_") for c in df.columns]
        required = ["model_name"]
        missing = [c for c in required if c not in df_cols]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")
        return True

    def preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize column names and fill missing values."""
        df = df.copy()
        df.dropna(how="all", inplace=True)
        df.columns = [c.lower().strip().replace(" ", "_") for c in df.columns]
        return df

    def model_quality_report(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Generate a dbt model quality and coverage report.

        Analyzes model test coverage, documentation completeness,
        naming convention compliance, and layer distribution.

        Args:
            df: Model inventory DataFrame with model_name and optional:
                has_tests, has_description, layer, materialization.

        Returns:
            Dict with:
                - total_models: Model count
                - test_coverage_pct: % models with at least one test
                - doc_coverage_pct: % models with descriptions
                - naming_violations: Models not following snake_case convention
                - models_by_layer: Count per dbt layer (staging/intermediate/mart)
                - models_by_materialization: Count per materialization type
                - untested_models: List of model names lacking tests
        """
        df = self.preprocess(df)
        total = len(df)

        # Test coverage
        test_cov = None
        untested = []
        if "has_tests" in df.columns:
            tested = df["has_tests"].astype(str).str.lower().isin(["true", "1", "yes"])
            test_cov = round(tested.sum() / total * 100, 2) if total > 0 else 0
            untested = df.loc[~tested, "model_name"].tolist()

        # Doc coverage
        doc_cov = None
        if "has_description" in df.columns:
            documented = df["has_description"].astype(str).str.lower().isin(["true", "1", "yes"])
            doc_cov = round(documented.sum() / total * 100, 2) if total > 0 else 0

        # Naming convention (snake_case, lowercase, no spaces)
        snake_case_re = re.compile(r"^[a-z][a-z0-9_]*$")
        violations = df.loc[
            ~df["model_name"].astype(str).str.match(snake_case_re), "model_name"
        ].tolist()

        # Layer distribution
        by_layer = {}
        if "layer" in df.columns:
            by_layer = df.groupby("layer").size().to_dict()

        # Materialization
        by_mat = {}
        if "materialization" in df.columns:
            by_mat = df.groupby("materialization").size().to_dict()

        return {
            "total_models": total,
            "test_coverage_pct": test_cov,
            "doc_coverage_pct": doc_cov,
            "naming_violations": violations,
            "models_by_layer": by_layer,
            "models_by_materialization": by_mat,
            "untested_models": untested,
        }

    def generate_model_yaml_stub(
        self, model_name: str, columns: List[str], layer: str = "staging"
    ) -> str:
        """
        Generate a dbt YAML documentation stub for a model.

        Args:
            model_name: dbt model name (snake_case).
            columns: List of column names to document.
            layer: dbt layer (staging, intermediate, mart).

        Returns:
            YAML string for the model documentation.

        Raises:
            ValueError: If model_name contains invalid characters.
        """
        if not re.match(r"^[a-z][a-z0-9_]*$", model_name):
            raise ValueError(
                f"Invalid model name '{model_name}'. Use snake_case (lowercase letters, numbers, underscores)."
            )

        yaml_lines = [
            f"version: 2",
            f"",
            f"models:",
            f"  - name: {model_name}",
            f"    description: >",
            f"      TODO: Add description for {model_name} ({layer} layer)",
            f"    config:",
            f"      materialized: {'view' if layer == 'staging' else 'table'}",
            f"    columns:",
        ]
        for col in columns:
            yaml_lines += [
                f"      - name: {col}",
                f"        description: TODO: Describe {col}",
                f"        tests:",
                f"          - not_null",
            ]

        return "\n".join(yaml_lines)

    def analyze(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Run descriptive analysis and return summary metrics."""
        df = self.preprocess(df)
        result = {
            "total_records": len(df),
            "columns": list(df.columns),
            "missing_pct": (df.isnull().sum() / len(df) * 100).round(1).to_dict(),
        }
        numeric_df = df.select_dtypes(include="number")
        if not numeric_df.empty:
            result["summary_stats"] = numeric_df.describe().round(3).to_dict()
            result["totals"] = numeric_df.sum().round(2).to_dict()
            result["means"] = numeric_df.mean().round(3).to_dict()
        return result

    def run(self, filepath: str) -> Dict[str, Any]:
        """Full pipeline: load → validate → analyze."""
        df = self.load_data(filepath)
        self.validate(df)
        return self.analyze(df)

    def to_dataframe(self, result: Dict) -> pd.DataFrame:
        """Convert result dict to flat DataFrame for export."""
        rows = []
        for k, v in result.items():
            if isinstance(v, dict):
                for kk, vv in v.items():
                    rows.append({"metric": f"{k}.{kk}", "value": vv})
            else:
                rows.append({"metric": k, "value": v})
        return pd.DataFrame(rows)
