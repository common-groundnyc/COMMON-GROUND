import faulthandler
import os

faulthandler.enable()

import dagster as dg

from dagster_pipeline.defs.socrata_direct_assets import all_socrata_direct_assets
from dagster_pipeline.defs.federal_direct_assets import all_federal_direct_assets
from dagster_pipeline.defs.election_assets import election_assets
from dagster_pipeline.defs.name_index_asset import name_index
from dagster_pipeline.defs.resolved_entities_asset import resolved_entities
from dagster_pipeline.defs.flush_sensor import flush_ducklake_sensor
from dagster_pipeline.defs.freshness_sensor import data_freshness_sensor
from dagster_pipeline.defs.foundation_assets import h3_index, phonetic_index, row_fingerprints
from dagster_pipeline.defs.entity_master_asset import entity_master
from dagster_pipeline.defs.quality_assets import data_health
from dagster_pipeline.defs.materialized_view_assets import (
    mv_building_hub, mv_acris_deeds, mv_zip_stats, mv_crime_precinct, mv_corp_network,
    mv_entity_acris, mv_city_averages, mv_pctile_violations, mv_pctile_311,
)
from dagster_pipeline.defs.spatial_views_asset import spatial_views
from dagster_pipeline.defs.address_lookup_asset import address_lookup
from dagster_pipeline.defs.name_tokens_asset import name_tokens
from dagster_pipeline.defs.geo_zip_boundaries_asset import geo_zip_boundaries
from dagster_pipeline.defs.graph_assets import graph_political
from dagster_pipeline.resources.ducklake import DuckLakeResource

all_assets = [*all_socrata_direct_assets, *all_federal_direct_assets, *election_assets, name_index, resolved_entities,
              h3_index, phonetic_index, row_fingerprints, data_health,
              entity_master, address_lookup,
              mv_building_hub, mv_acris_deeds, mv_zip_stats, mv_crime_precinct, mv_corp_network,
              mv_entity_acris, mv_city_averages, mv_pctile_violations, mv_pctile_311,
              name_tokens, spatial_views, geo_zip_boundaries, graph_political]

# --- Jobs (manual trigger only — automation is sensor/schedule-driven) ---

# Federal: 50 assets from BLS, Census, HUD, FEC, EPA, CourtListener, etc.
federal_daily_job = dg.define_asset_job(
    name="federal_daily",
    selection=(
        dg.AssetSelection.groups("federal")
        - dg.AssetSelection.assets(
            dg.AssetKey(["federal", "name_index"]),
            dg.AssetKey(["federal", "resolved_entities"]),
        )
    ),
)

# Entity resolution: name_index + resolved_entities (heavy, manual only)
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

graphs_daily_job = dg.define_asset_job(
    name="graphs_daily",
    selection=dg.AssetSelection.groups("graphs"),
)

graphs_schedule = dg.ScheduleDefinition(
    job=graphs_daily_job,
    cron_schedule="0 7 * * *",  # 7 AM daily — one hour after federal_daily
    default_status=dg.DefaultScheduleStatus.RUNNING,
)


# Full: everything (initial load or recovery)
all_assets_job = dg.define_asset_job(
    name="all_assets_full",
    selection=dg.AssetSelection.all(),
)

# --- Schedules ---

# Federal assets have no Socrata count API — need a schedule.
federal_schedule = dg.ScheduleDefinition(
    job=federal_daily_job,
    cron_schedule="0 6 * * *",  # 6 AM daily
    default_status=dg.DefaultScheduleStatus.RUNNING,
)


# --- Sensors ---
# data_freshness_sensor: hourly, triggers Socrata assets when source > lake (replaces daily/monthly schedules)
# flush_ducklake_sensor: post-run, flushes DuckLake inlined data to parquet
# MVs use AutomationCondition.eager() — evaluated by Dagster's built-in default_automation_condition_sensor

defs = dg.Definitions(
    assets=all_assets,
    jobs=[federal_daily_job, entity_resolution_job, foundation_job, all_assets_job, graphs_daily_job],
    schedules=[federal_schedule, graphs_schedule],
    sensors=[flush_ducklake_sensor, data_freshness_sensor],
    resources={
        "ducklake": DuckLakeResource(
            catalog_url=os.environ.get(
                "DESTINATION__DUCKLAKE__CREDENTIALS__CATALOG",
                "ducklake:postgres:dbname=ducklake user=dagster password=test host=178.156.228.119 port=5432",
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
