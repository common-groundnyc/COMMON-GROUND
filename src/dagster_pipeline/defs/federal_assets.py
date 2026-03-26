"""Federal data assets — BLS, Census, College Scorecard, HUD, FEC, ProPublica,
LittleSis, USAspending, EPA ECHO, NREL, FDA, Urban Institute Education.

Each source gets its own @dlt_assets with independent materialization.
No concurrency pools — DuckLake retry handles write contention.
"""
import dlt
from dagster import AssetExecutionContext, AssetKey, AssetSpec
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData
from dlt.destinations import ducklake

from dagster_pipeline.sources.bls import bls_source
from dagster_pipeline.sources.census import census_source
from dagster_pipeline.sources.college_scorecard import college_scorecard_source
from dagster_pipeline.sources.epa_echo import epa_echo_source
from dagster_pipeline.sources.fda import fda_source
from dagster_pipeline.sources.fec import fec_source
from dagster_pipeline.sources.hud import hud_arcgis_source
from dagster_pipeline.sources.littlesis import littlesis_source
from dagster_pipeline.sources.marriage_index import marriage_index_source
from dagster_pipeline.sources.nrel import nrel_source
from dagster_pipeline.sources.nypd_misconduct import nypd_misconduct_source
from dagster_pipeline.sources.nys_boe import nys_boe_source
from dagster_pipeline.sources.nys_death_index import nys_death_index_source
from dagster_pipeline.sources.oca_housing_court import oca_housing_court_source
from dagster_pipeline.sources.propublica import propublica_source
from dagster_pipeline.sources.urban_education import urban_education_source
from dagster_pipeline.sources.usaspending import usaspending_source
from dagster_pipeline.sources.courtlistener import courtlistener_source
from dagster_pipeline.sources.bulk_csv import bulk_csv_source
from dagster_pipeline.sources.census_zcta import acs_zcta_source

FEDERAL_SCHEMA = "federal"

DUCKLAKE_DESTINATION = ducklake(
    max_identifier_length=63,
    max_column_identifier_length=63,
    loader_parallelism_strategy="parallel",
)


class FederalDltTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data: DltResourceTranslatorData) -> AssetSpec:
        default_spec = super().get_asset_spec(data)
        return default_spec.replace_attributes(
            key=AssetKey([FEDERAL_SCHEMA, data.resource.name]),
            group_name=FEDERAL_SCHEMA,
        )


def _build_federal_assets() -> list:
    assets = []
    translator = FederalDltTranslator()

    # --- BLS: 5 borough unemployment series ---
    for source in bls_source():
        resource_name = list(source.resources.keys())[0]
        pipeline = dlt.pipeline(
            pipeline_name=f"federal__{resource_name}",
            destination=DUCKLAKE_DESTINATION,
            dataset_name=FEDERAL_SCHEMA,
            progress="log",
        )

        @dlt_assets(
            dlt_source=source,
            dlt_pipeline=pipeline,
            name=f"federal__{resource_name}",
            dagster_dlt_translator=translator,
            op_tags={"schema": FEDERAL_SCHEMA},
        )
        def _fn(context: AssetExecutionContext, dlt: DagsterDltResource):
            yield from dlt.run(context=context)

        assets.append(_fn)

    # --- Census ACS ---
    census_src = census_source()
    census_pipeline = dlt.pipeline(
        pipeline_name="federal__acs_demographics",
        destination=DUCKLAKE_DESTINATION,
        dataset_name=FEDERAL_SCHEMA,
        progress="log",
    )

    @dlt_assets(
        dlt_source=census_src,
        dlt_pipeline=census_pipeline,
        name="federal__acs_demographics",
        dagster_dlt_translator=translator,
        op_tags={"schema": FEDERAL_SCHEMA},
    )
    def _census_fn(context: AssetExecutionContext, dlt: DagsterDltResource):
        yield from dlt.run(context=context)

    assets.append(_census_fn)

    # --- College Scorecard ---
    scorecard_src = college_scorecard_source()
    scorecard_pipeline = dlt.pipeline(
        pipeline_name="federal__college_scorecard",
        destination=DUCKLAKE_DESTINATION,
        dataset_name=FEDERAL_SCHEMA,
        progress="log",
    )

    @dlt_assets(
        dlt_source=scorecard_src,
        dlt_pipeline=scorecard_pipeline,
        name="federal__college_scorecard",
        dagster_dlt_translator=translator,
        op_tags={"schema": FEDERAL_SCHEMA},
    )
    def _scorecard_fn(context: AssetExecutionContext, dlt: DagsterDltResource):
        yield from dlt.run(context=context)

    assets.append(_scorecard_fn)

    # --- HUD REST (FMR + Income Limits) ---
    # DISABLED: HUD User API portal is down (403). Re-enable when back.
    # Uses hud_rest_source() with bearer token auth.

    # --- HUD ArcGIS (5 feature services) ---
    hud_arc_src = hud_arcgis_source()
    for resource_name in hud_arc_src.resources:
        source_for_asset = hud_arcgis_source()
        source_for_asset.resources[resource_name].selected = True
        for other in source_for_asset.resources:
            if other != resource_name:
                source_for_asset.resources[other].selected = False

        pipeline = dlt.pipeline(
            pipeline_name=f"federal__{resource_name}",
            destination=DUCKLAKE_DESTINATION,
            dataset_name=FEDERAL_SCHEMA,
            progress="log",
        )

        @dlt_assets(
            dlt_source=source_for_asset,
            dlt_pipeline=pipeline,
            name=f"federal__{resource_name}",
            dagster_dlt_translator=translator,
            op_tags={"schema": FEDERAL_SCHEMA},
        )
        def _hud_arc_fn(context: AssetExecutionContext, dlt: DagsterDltResource):
            yield from dlt.run(context=context)

        assets.append(_hud_arc_fn)

    # --- FEC: NYC campaign contributions (federal) ---
    fec_src = fec_source()
    for resource_name in fec_src.resources:
        source_for_asset = fec_source()
        pipeline = dlt.pipeline(
            pipeline_name=f"federal__{resource_name}",
            destination=DUCKLAKE_DESTINATION,
            dataset_name=FEDERAL_SCHEMA,
            progress="log",
        )

        @dlt_assets(
            dlt_source=source_for_asset,
            dlt_pipeline=pipeline,
            name=f"federal__{resource_name}",
            dagster_dlt_translator=translator,
            op_tags={"schema": FEDERAL_SCHEMA},
        )
        def _fec_fn(context: AssetExecutionContext, dlt: DagsterDltResource):
            yield from dlt.run(context=context)

        assets.append(_fec_fn)

    # --- ProPublica: NY nonprofits (IRS 990) ---
    # All 3 resources together: parent (nonprofits) + children (details, filings).
    # Children resolve {ein} from parent — deselecting parent breaks them.
    pp_src = propublica_source()
    pp_pipeline = dlt.pipeline(
        pipeline_name="federal__propublica",
        destination=DUCKLAKE_DESTINATION,
        dataset_name=FEDERAL_SCHEMA,
        progress="log",
    )

    @dlt_assets(
        dlt_source=pp_src,
        dlt_pipeline=pp_pipeline,
        name="federal__propublica",
        dagster_dlt_translator=translator,
        op_tags={"schema": FEDERAL_SCHEMA},
    )
    def _pp_fn(context: AssetExecutionContext, dlt: DagsterDltResource):
        yield from dlt.run(context=context)

    assets.append(_pp_fn)

    # --- LittleSis: power mapping ---
    ls_src = littlesis_source()
    for resource_name in ls_src.resources:
        source_for_asset = littlesis_source()
        source_for_asset.resources[resource_name].selected = True
        for other in source_for_asset.resources:
            if other != resource_name:
                source_for_asset.resources[other].selected = False

        pipeline = dlt.pipeline(
            pipeline_name=f"federal__{resource_name}",
            destination=DUCKLAKE_DESTINATION,
            dataset_name=FEDERAL_SCHEMA,
            progress="log",
        )

        @dlt_assets(
            dlt_source=source_for_asset,
            dlt_pipeline=pipeline,
            name=f"federal__{resource_name}",
            dagster_dlt_translator=translator,
            op_tags={"schema": FEDERAL_SCHEMA},
        )
        def _ls_fn(context: AssetExecutionContext, dlt: DagsterDltResource):
            yield from dlt.run(context=context)

        assets.append(_ls_fn)

    # --- USAspending: federal contracts/grants to NYC ---
    usa_src = usaspending_source()
    for resource_name in usa_src.resources:
        source_for_asset = usaspending_source()
        source_for_asset.resources[resource_name].selected = True
        for other in source_for_asset.resources:
            if other != resource_name:
                source_for_asset.resources[other].selected = False

        pipeline = dlt.pipeline(
            pipeline_name=f"federal__{resource_name}",
            destination=DUCKLAKE_DESTINATION,
            dataset_name=FEDERAL_SCHEMA,
            progress="log",
        )

        @dlt_assets(
            dlt_source=source_for_asset,
            dlt_pipeline=pipeline,
            name=f"federal__{resource_name}",
            dagster_dlt_translator=translator,
            op_tags={"schema": FEDERAL_SCHEMA},
        )
        def _usa_fn(context: AssetExecutionContext, dlt: DagsterDltResource):
            yield from dlt.run(context=context)

        assets.append(_usa_fn)

    # --- EPA ECHO: environmental compliance (graph-enhancing) ---
    echo_src = epa_echo_source()
    for resource_name in echo_src.resources:
        source_for_asset = epa_echo_source()
        source_for_asset.resources[resource_name].selected = True
        for other in source_for_asset.resources:
            if other != resource_name:
                source_for_asset.resources[other].selected = False

        pipeline = dlt.pipeline(
            pipeline_name=f"federal__{resource_name}",
            destination=DUCKLAKE_DESTINATION,
            dataset_name=FEDERAL_SCHEMA,
            progress="log",
        )

        @dlt_assets(
            dlt_source=source_for_asset,
            dlt_pipeline=pipeline,
            name=f"federal__{resource_name}",
            dagster_dlt_translator=translator,
            op_tags={"schema": FEDERAL_SCHEMA},
        )
        def _echo_fn(context: AssetExecutionContext, dlt: DagsterDltResource):
            yield from dlt.run(context=context)

        assets.append(_echo_fn)

    # --- NREL: alternative fuel stations ---
    nrel_src = nrel_source()
    nrel_pipeline = dlt.pipeline(
        pipeline_name="federal__nrel_alt_fuel_stations",
        destination=DUCKLAKE_DESTINATION,
        dataset_name=FEDERAL_SCHEMA,
        progress="log",
    )

    @dlt_assets(
        dlt_source=nrel_src,
        dlt_pipeline=nrel_pipeline,
        name="federal__nrel_alt_fuel_stations",
        dagster_dlt_translator=translator,
        op_tags={"schema": FEDERAL_SCHEMA},
    )
    def _nrel_fn(context: AssetExecutionContext, dlt: DagsterDltResource):
        yield from dlt.run(context=context)

    assets.append(_nrel_fn)

    # --- FDA: food enforcement actions ---
    fda_src = fda_source()
    for resource_name in fda_src.resources:
        source_for_asset = fda_source()
        pipeline = dlt.pipeline(
            pipeline_name=f"federal__{resource_name}",
            destination=DUCKLAKE_DESTINATION,
            dataset_name=FEDERAL_SCHEMA,
            progress="log",
        )

        @dlt_assets(
            dlt_source=source_for_asset,
            dlt_pipeline=pipeline,
            name=f"federal__{resource_name}",
            dagster_dlt_translator=translator,
            op_tags={"schema": FEDERAL_SCHEMA},
        )
        def _fda_fn(context: AssetExecutionContext, dlt: DagsterDltResource):
            yield from dlt.run(context=context)

        assets.append(_fda_fn)

    # --- Urban Institute Education: NY schools + finance ---
    urban_src = urban_education_source()
    for resource_name in urban_src.resources:
        source_for_asset = urban_education_source()
        source_for_asset.resources[resource_name].selected = True
        for other in source_for_asset.resources:
            if other != resource_name:
                source_for_asset.resources[other].selected = False

        pipeline = dlt.pipeline(
            pipeline_name=f"federal__{resource_name}",
            destination=DUCKLAKE_DESTINATION,
            dataset_name=FEDERAL_SCHEMA,
            progress="log",
        )

        @dlt_assets(
            dlt_source=source_for_asset,
            dlt_pipeline=pipeline,
            name=f"federal__{resource_name}",
            dagster_dlt_translator=translator,
            op_tags={"schema": FEDERAL_SCHEMA},
        )
        def _urban_fn(context: AssetExecutionContext, dlt: DagsterDltResource):
            yield from dlt.run(context=context)

        assets.append(_urban_fn)

    # --- NYC Marriage Index (1950-2017): Reclaim The Records FOIL data ---
    marriage_src = marriage_index_source()
    marriage_pipeline = dlt.pipeline(
        pipeline_name="federal__nyc_marriage_index",
        destination=DUCKLAKE_DESTINATION,
        dataset_name=FEDERAL_SCHEMA,
        progress="log",
    )

    @dlt_assets(
        dlt_source=marriage_src,
        dlt_pipeline=marriage_pipeline,
        name="federal__nyc_marriage_index",
        dagster_dlt_translator=translator,
        op_tags={"schema": FEDERAL_SCHEMA},
    )
    def _marriage_fn(context: AssetExecutionContext, dlt: DagsterDltResource):
        yield from dlt.run(context=context)

    assets.append(_marriage_fn)

    # --- NYPD Misconduct (NYCLU): CCRB complaints with officer names ---
    misconduct_src = nypd_misconduct_source()
    misconduct_pipeline = dlt.pipeline(
        pipeline_name="federal__nypd_ccrb_complaints",
        destination=DUCKLAKE_DESTINATION,
        dataset_name=FEDERAL_SCHEMA,
        progress="log",
    )

    @dlt_assets(
        dlt_source=misconduct_src,
        dlt_pipeline=misconduct_pipeline,
        name="federal__nypd_ccrb_complaints",
        dagster_dlt_translator=translator,
        op_tags={"schema": FEDERAL_SCHEMA},
    )
    def _misconduct_fn(context: AssetExecutionContext, dlt: DagsterDltResource):
        yield from dlt.run(context=context)

    assets.append(_misconduct_fn)

    # --- NYS Death Index (1880-1971): Reclaim The Records transcriptions ---
    death_src = nys_death_index_source()
    death_pipeline = dlt.pipeline(
        pipeline_name="federal__nys_death_index",
        destination=DUCKLAKE_DESTINATION,
        dataset_name=FEDERAL_SCHEMA,
        progress="log",
    )

    @dlt_assets(
        dlt_source=death_src,
        dlt_pipeline=death_pipeline,
        name="federal__nys_death_index",
        dagster_dlt_translator=translator,
        op_tags={"schema": FEDERAL_SCHEMA},
    )
    def _death_fn(context: AssetExecutionContext, dlt: DagsterDltResource):
        yield from dlt.run(context=context)

    assets.append(_death_fn)

    # --- NYS BOE Campaign Finance: state-level contributions ---
    nys_boe_src = nys_boe_source()
    nys_boe_pipeline = dlt.pipeline(
        pipeline_name="federal__nys_campaign_finance",
        destination=DUCKLAKE_DESTINATION,
        dataset_name=FEDERAL_SCHEMA,
        progress="log",
    )

    @dlt_assets(
        dlt_source=nys_boe_src,
        dlt_pipeline=nys_boe_pipeline,
        name="federal__nys_campaign_finance",
        dagster_dlt_translator=translator,
        op_tags={"schema": FEDERAL_SCHEMA},
    )
    def _nys_boe_fn(context: AssetExecutionContext, dlt: DagsterDltResource):
        yield from dlt.run(context=context)

    assets.append(_nys_boe_fn)

    # --- OCA Housing Court: landlord-tenant cases (rolling 5 years) ---
    oca_src = oca_housing_court_source()
    oca_pipeline = dlt.pipeline(
        pipeline_name="federal__oca_housing_court",
        destination=DUCKLAKE_DESTINATION,
        dataset_name=FEDERAL_SCHEMA,
        progress="log",
    )

    @dlt_assets(
        dlt_source=oca_src,
        dlt_pipeline=oca_pipeline,
        name="federal__oca_housing_court",
        dagster_dlt_translator=translator,
        op_tags={"schema": FEDERAL_SCHEMA},
    )
    def _oca_fn(context: AssetExecutionContext, dlt: DagsterDltResource):
        yield from dlt.run(context=context)

    assets.append(_oca_fn)

    # Entity resolution is handled by resolved_entities_asset.py (plain @asset
    # with Splink batch processing), not a dlt_assets. The old dlt-based
    # entity_resolution_source was superseded by the batch approach in Phase 5.

    # --- CourtListener: Federal court data (RECAP, FJC, judges, disclosures) ---
    # rest_api_source returns a DltSource — one asset per resource
    cl_resource_names = [
        "cl_courts", "cl_judges", "cl_positions",
        "cl_financial_disclosures", "cl_debts", "cl_gifts",
        "cl_fjc_cases", "cl_educations", "cl_schools",
        "cl_agreements", "cl_reimbursements", "cl_spousal_income",
        "cl_non_investment_income",
        "cl_nypd_cases_sdny", "cl_nypd_cases_edny",
    ]
    for resource_name in cl_resource_names:
        source_for_asset = courtlistener_source()
        source_for_asset.resources[resource_name].selected = True
        for other in source_for_asset.resources:
            if other != resource_name:
                source_for_asset.resources[other].selected = False

        pipeline = dlt.pipeline(
            pipeline_name=f"federal__{resource_name}",
            destination=DUCKLAKE_DESTINATION,
            dataset_name=FEDERAL_SCHEMA,
            progress="log",
        )

        @dlt_assets(
            dlt_source=source_for_asset,
            dlt_pipeline=pipeline,
            name=f"federal__{resource_name}",
            dagster_dlt_translator=translator,
            op_tags={"schema": FEDERAL_SCHEMA},
        )
        def _cl_fn(context: AssetExecutionContext, dlt: DagsterDltResource):
            yield from dlt.run(context=context)

        assets.append(_cl_fn)

    # --- Bulk CSV: FiveThirtyEight police settlements ---
    bulk_src = bulk_csv_source()
    bulk_pipeline = dlt.pipeline(
        pipeline_name="federal__police_settlements_538",
        destination=DUCKLAKE_DESTINATION,
        dataset_name=FEDERAL_SCHEMA,
        progress="log",
    )

    @dlt_assets(
        dlt_source=bulk_src,
        dlt_pipeline=bulk_pipeline,
        name="federal__police_settlements_538",
        dagster_dlt_translator=translator,
        op_tags={"schema": FEDERAL_SCHEMA},
    )
    def _bulk_fn(context: AssetExecutionContext, dlt: DagsterDltResource):
        yield from dlt.run(context=context)

    assets.append(_bulk_fn)

    # --- ACS 5-Year ZCTA Demographics: population, income, race by ZIP ---
    zcta_src = acs_zcta_source()
    zcta_pipeline = dlt.pipeline(
        pipeline_name="federal__acs_zcta_demographics",
        destination=DUCKLAKE_DESTINATION,
        dataset_name=FEDERAL_SCHEMA,
        progress="log",
    )

    @dlt_assets(
        dlt_source=zcta_src,
        dlt_pipeline=zcta_pipeline,
        name="federal__acs_zcta_demographics",
        dagster_dlt_translator=translator,
        op_tags={"schema": FEDERAL_SCHEMA},
    )
    def _zcta_fn(context: AssetExecutionContext, dlt: DagsterDltResource):
        yield from dlt.run(context=context)

    assets.append(_zcta_fn)

    return assets


all_federal_assets = _build_federal_assets()
