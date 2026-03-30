import faulthandler
import os

faulthandler.enable()

import dagster as dg

from dagster_pipeline.defs.socrata_direct_assets import all_socrata_direct_assets
from dagster_pipeline.defs.federal_direct_assets import all_federal_direct_assets
from dagster_pipeline.defs.name_index_asset import name_index
from dagster_pipeline.defs.resolved_entities_asset import resolved_entities
from dagster_pipeline.defs.flush_sensor import flush_ducklake_sensor
from dagster_pipeline.defs.freshness_sensor import data_freshness_sensor
from dagster_pipeline.defs.foundation_assets import h3_index, phonetic_index, row_fingerprints
from dagster_pipeline.defs.entity_embeddings_asset import entity_name_embeddings
from dagster_pipeline.defs.quality_assets import data_health
from dagster_pipeline.defs.materialized_view_assets import (
    mv_building_hub, mv_acris_deeds, mv_zip_stats, mv_crime_precinct, mv_corp_network,
)
from dagster_pipeline.resources.ducklake import DuckLakeResource

all_assets = [*all_socrata_direct_assets, *all_federal_direct_assets, name_index, resolved_entities,
              h3_index, phonetic_index, row_fingerprints, data_health, entity_name_embeddings,
              mv_building_hub, mv_acris_deeds, mv_zip_stats, mv_crime_precinct, mv_corp_network]

from dagster_pipeline.sources.datasets import (
    STATIC_DATASETS, MONTHLY_DATASETS, SOCRATA_DATASETS,
)

# Build exclusion keys from actual dataset→schema mapping (not cross-product)
_skip_daily = STATIC_DATASETS | MONTHLY_DATASETS
_skip_keys = []
_monthly_keys = []
for schema, datasets in SOCRATA_DATASETS.items():
    for table_name, _dataset_id, _domain in datasets:
        if table_name in _skip_daily:
            _skip_keys.append(dg.AssetKey([schema, table_name]))
        if table_name in MONTHLY_DATASETS:
            _monthly_keys.append(dg.AssetKey([schema, table_name]))

# --- Jobs ---

# Daily: only live/frequently-updated datasets (~67 Socrata + federal API sources)
daily_live_job = dg.define_asset_job(
    name="daily_live",
    selection=(
        dg.AssetSelection.groups("business", "city_government", "education",
                    "environment", "financial", "health", "housing", "public_safety",
                    "recreation", "social_services", "transportation", "federal")
        - dg.AssetSelection.assets(*_skip_keys)
        - dg.AssetSelection.assets(
            dg.AssetKey(["federal", "name_index"]),
            dg.AssetKey(["federal", "resolved_entities"]),
        )
    ),
)

# Monthly: annual/quarterly datasets that don't need daily refresh
monthly_refresh_job = dg.define_asset_job(
    name="monthly_refresh",
    selection=dg.AssetSelection.assets(*_monthly_keys),
)

# Full: everything (initial load, or manual trigger)
all_assets_job = dg.define_asset_job(
    name="all_assets_full",
    selection=dg.AssetSelection.all(),
)

# Entity resolution only
entity_resolution_job = dg.define_asset_job(
    name="entity_resolution",
    selection=dg.AssetSelection.assets(
        dg.AssetKey(["federal", "name_index"]),
        dg.AssetKey(["federal", "resolved_entities"]),
    ),
)

# Foundation: rebuild all foundation indexes
foundation_job = dg.define_asset_job(
    name="foundation_rebuild",
    selection=dg.AssetSelection.groups("foundation"),
)

# Materialized views: rebuild pre-joined analytical views
materialized_views_job = dg.define_asset_job(
    name="materialized_views_rebuild",
    selection=dg.AssetSelection.assets(
        dg.AssetKey(["foundation", "mv_building_hub"]),
        dg.AssetKey(["foundation", "mv_acris_deeds"]),
        dg.AssetKey(["foundation", "mv_zip_stats"]),
        dg.AssetKey(["foundation", "mv_crime_precinct"]),
        dg.AssetKey(["foundation", "mv_corp_network"]),
    ),
)

# Entity embeddings: rebuild Lance vector index
entity_embeddings_job = dg.define_asset_job(
    name="entity_embeddings",
    selection=dg.AssetSelection.assets(
        dg.AssetKey(["foundation", "entity_name_embeddings"]),
    ),
)

# --- Schedules ---
daily_schedule = dg.ScheduleDefinition(
    job=daily_live_job,
    cron_schedule="0 6 * * *",  # 6 AM daily
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

monthly_schedule = dg.ScheduleDefinition(
    job=monthly_refresh_job,
    cron_schedule="0 3 1 * *",  # 3 AM, 1st of month
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

entity_embeddings_schedule = dg.ScheduleDefinition(
    job=entity_embeddings_job,
    cron_schedule="0 12 1 * *",  # noon on 1st of month
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

defs = dg.Definitions(
    assets=all_assets,
    jobs=[daily_live_job, monthly_refresh_job, all_assets_job, entity_resolution_job, foundation_job, materialized_views_job, entity_embeddings_job],
    schedules=[daily_schedule, monthly_schedule, entity_embeddings_schedule],
    sensors=[flush_ducklake_sensor, data_freshness_sensor],
    resources={
        "ducklake": DuckLakeResource(
            catalog_url=os.environ.get(
                "DESTINATION__DUCKLAKE__CREDENTIALS__CATALOG",
                "ducklake:postgres:dbname=ducklake user=dagster password=test host=178.156.228.119 port=5432",
            ),
            s3_endpoint=os.environ.get("S3_ENDPOINT", "178.156.228.119:9000"),
            s3_access_key=os.environ.get(
                "DESTINATION__DUCKLAKE__CREDENTIALS__STORAGE__CREDENTIALS__AWS_ACCESS_KEY_ID",
                "minioadmin",
            ),
            s3_secret_key=os.environ.get(
                "DESTINATION__DUCKLAKE__CREDENTIALS__STORAGE__CREDENTIALS__AWS_SECRET_ACCESS_KEY",
                "minioadmin",
            ),
        ),
    },
    executor=dg.multiprocess_executor.configured({
        "max_concurrent": 4,
        "start_method": {"forkserver": {}},
        "tag_concurrency_limits": [
            {
                "key": "schema",
                "value": {"applyLimitPerUniqueValue": True},
                "limit": 1,
            },
        ],
    }),
)
