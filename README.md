# dbt Project Template

dbt project scaffolding and quality analysis for NbS and pharma BI data models.

## Features
- **Model quality report**: test coverage %, documentation coverage %, naming violations
- **YAML stub generator**: auto-generate model documentation with not_null tests
- **Layer analysis**: model distribution across staging/intermediate/mart
- **Naming convention**: snake_case validation for all model names
- **Sample inventory**: NbS analytics dbt project with 11 models

## Quick Start

```python
from src.main import DBTProjectTemplate

dbt = DBTProjectTemplate(config={"project_name": "pur_analytics"})
df = dbt.load_data("sample_data/models_inventory.csv")

report = dbt.model_quality_report(df)
print(f"Test Coverage:  {report['test_coverage_pct']:.1f}%")
print(f"Doc Coverage:   {report['doc_coverage_pct']:.1f}%")
print(f"Naming Issues:  {report['naming_violations']}")
print(f"Untested:       {report['untested_models']}")
print(f"By Layer:       {report['models_by_layer']}")

# Generate YAML stub
yaml = dbt.generate_model_yaml_stub(
    "stg_carbon_events",
    ["event_id", "project_id", "period", "tco2_issued"],
    layer="staging"
)
print(yaml)
```

## Running Tests
```bash
pytest tests/ -v
```
