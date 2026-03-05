# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

## [1.3.0] - 2026-03-05
### Added
- `generate_model_stub()`: generates dbt SQL stub with config block, CTEs, and optional incremental clause
- `test_coverage_report()`: detailed test and documentation coverage report with per-layer breakdown
- 12 new unit tests covering model stub generation and coverage reporting
### Improved
- README updated with generate_model_stub output example and coverage report walkthrough

## [1.2.0] - 2026-03-04
### Added
- `model_quality_report()`: test coverage %, doc coverage %, naming violations, layer distribution
- `generate_model_yaml_stub()`: YAML documentation stub generator with not_null tests
- dbt model inventory sample data for NbS analytics project
- 14 unit tests covering quality report, naming validation, and YAML generation
### Fixed
- `validate()` checks for required model_name column
- Naming violation detection uses regex for proper snake_case validation
## [1.1.0] - 2026-03-02
### Added
- Add cross-project source freshness checks and dynamic macro library
- Improved unit test coverage
- Enhanced documentation with realistic examples
