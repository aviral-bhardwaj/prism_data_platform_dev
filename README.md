# Prism Data Platform

Databricks-based data engineering platform for survey data processing. Ingests data from the Decipher API and processes it through a Bronze / Silver / Gold medallion architecture into analytics-ready Delta tables.

## What it does

- Pulls survey data (responses, datamaps, layouts) from the Decipher REST API into landing volumes
- Runs Delta Live Tables pipelines to load landing data into Bronze Delta tables (SCD Type 2)
- Applies SHA-256-based CDC to build Silver dimension and fact tables
- Produces Gold-layer star schema analytics for the US Hotels domain
- Uploads outputs to SharePoint via Microsoft Graph API

## Quick start

```bash
# from repo root
cd data_platform
pip install -r requirements-dev.txt
pip install pyspark delta-spark requests msal chardet

# deploy to dev (requires Databricks CLI + secrets)
databricks bundle deploy -t dev
databricks bundle run Data_Platform_Main_Job -t dev
```

## Tests

Framework: **pytest** (configured in `data_platform/pytest.ini`). Java 11 is required for the PySpark-based tests.

```bash
cd data_platform

# run everything
python -m pytest tests/ -v

# unit tests only (no Spark needed locally; skipped automatically in CI when no cluster)
python -m pytest tests/main_test.py -v

# integration tests only (spins up a local PySpark + Delta session — no cluster needed)
python -m pytest tests/integration/ -v
```

### Unit tests (`tests/main_test.py`)

| Test | What it covers |
|------|----------------|
| `test_main` | Smoke test — calls `get_taxis(get_spark())` against a live Databricks cluster. Skipped in CI. |

### Integration tests (`tests/integration/`)

These run in CI (GitHub Actions) using a local PySpark session with Delta Lake — no Databricks cluster required.

| File | What it covers |
|------|----------------|
| `test_wave_dimension.py` | `derive_wave_fields` — quarterly, half-year, and annual wave parsing |
| `test_cdc_logic.py` | Index column creation, empty-to-null conversion, SHA-256 hash-based CDC |
| `test_bronze_config.py` | `config.json` structure, SCD Type 2 merge, CSV/JSON ingestion paths |
| `test_silver_transformations.py` | Tag parsing, pivot logic, quality filters, Delta read/write |
| `test_gold_layer.py` | Star schema joins, NPS calculations, aggregations, window functions |
| `test_logging_utils.py` | Log schema validation, Delta MERGE for `IN_PROGRESS`/`SUCCESS`/`FAILED` states |
| `test_sharepoint_utils.py` | `GraphAuth` config, `SharePointClient` headers, folder-path parsing |

### CI

GitHub Actions runs on push/PR to `dev`:

- **lint** — flake8 on `data_platform/src/` and `utils/`
- **unit-tests** — `pytest tests/ --ignore=tests/integration/`
- **integration-tests** — `pytest tests/integration/`
- **build-wheel** — builds the Python wheel
- **deploy-dev** — deploys the Databricks Asset Bundle to `dev` (push to `dev` only)
- **deploy-prod** — disabled (`if: false`); production deploy is manual

## Notes

- Secrets (Decipher API key, SharePoint credentials) live in Azure Key Vault under the `prism-dl-scope` Databricks secret scope.
- Production deployment is intentionally manual — the `deploy-prod` CI job is disabled.
- The `dev` target prefixes all Databricks resources with `[dev <username>]` and pauses schedules.
