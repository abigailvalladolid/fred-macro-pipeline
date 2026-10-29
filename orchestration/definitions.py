from pathlib import Path

from dagster import (
    AssetCheckResult,
    AssetCheckSpec,
    AssetExecutionContext,
    AssetKey,
    AssetSelection,
    AssetSpec,
    Definitions,
    MaterializeResult,
    MetadataValue,
    ScheduleDefinition,
    define_asset_job,
    multi_asset,
)
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from ingestion.ingest_fred import SERIES, run_ingestion

# --- dbt project ---------------------------------------------------------
DBT_PROJECT_DIR = Path(__file__).resolve().parent.parent / "fred_dbt"
fred_dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR)
fred_dbt_project.prepare_if_dev()

# --- Ingestion asset -----------------------------------------------------
# These keys must match the dbt source: source('fred', 'fred_observations')
OBSERVATIONS_KEY = AssetKey(["fred", "fred_observations"])
SERIES_KEY = AssetKey(["fred", "fred_series"])


@multi_asset(
    specs=[
        AssetSpec(OBSERVATIONS_KEY, group_name="ingestion",
                  description="Raw FRED observations, upserted incrementally."),
        AssetSpec(SERIES_KEY, group_name="ingestion",
                  description="Raw FRED series metadata."),
    ],
    check_specs=[
        AssetCheckSpec(
            name="all_series_loaded",
            asset=OBSERVATIONS_KEY,
            blocking=True,
            description="Every configured series loaded at least one row.",
        )
    ],
)
def fred_raw_tables(context: AssetExecutionContext):
    result = run_ingestion()
    rows = result["rows_by_series"]

    yield MaterializeResult(
        asset_key=OBSERVATIONS_KEY,
        metadata={
            "total_rows_upserted": result["total_rows"],
            "rows_by_series": MetadataValue.json(rows),
        },
    )
    yield MaterializeResult(
        asset_key=SERIES_KEY,
        metadata={"series_loaded": len(rows)},
    )

    missing = [s for s in SERIES if rows.get(s, 0) == 0]
    yield AssetCheckResult(
        check_name="all_series_loaded",
        asset_key=OBSERVATIONS_KEY,
        passed=not missing,
        metadata={"missing_or_failed": MetadataValue.json(missing)},
    )


# --- dbt assets ----------------------------------------------------------
@dbt_assets(manifest=fred_dbt_project.manifest_path)
def fred_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


# --- Job, schedule, definitions -----------------------------------------
daily_job = define_asset_job("daily_fred_refresh", selection=AssetSelection.all())

daily_schedule = ScheduleDefinition(
    job=daily_job,
    cron_schedule="0 10 * * *",
    execution_timezone="America/New_York",
)

defs = Definitions(
    assets=[fred_raw_tables, fred_dbt_assets],
    jobs=[daily_job],
    schedules=[daily_schedule],
    resources={"dbt": DbtCliResource(project_dir=fred_dbt_project)},
)