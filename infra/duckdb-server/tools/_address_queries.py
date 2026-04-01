"""SQL queries for address_report — ~50 parallel queries across 10 sections."""


def build_queries(
    bbl: str,
    zipcode: str,
    borough: str,
    precinct: str,
    cd: str,
    boro_code: str,
    address: str = "",
    latitude: float | None = None,
    longitude: float | None = None,
) -> list[tuple[str, str, list | None]]:
    """Return list of (name, sql, params) tuples for the 360 report."""
    queries: list[tuple[str, str, list | None]] = []

    # Block prefix for block-level queries (first 6 digits of BBL = boro + block)
    block_prefix = bbl[:6] if len(bbl) >= 6 else bbl

    # Street name for film/spatial lookups
    street = address.split(",")[0].strip() if address else ""

    # ══════════════════════════════════════════════════════════════════════
    # BUILDING (BBL-level, ~20 queries)
    # ══════════════════════════════════════════════════════════════════════

    queries.append(("building_hpd_violations", """
        SELECT COUNT(*) AS total,
               COUNT(*) FILTER (WHERE currentstatus = 'Open'
                                   OR violationstatus = 'Open') AS open_cnt,
               COUNT(*) FILTER (WHERE class = 'C') AS class_c,
               MAX(novissueddate) AS latest
        FROM lake.housing.hpd_violations
        WHERE REPLACE(CAST(bbl AS VARCHAR), '.00000000', '') = ?
    """, [bbl]))

    queries.append(("building_hpd_complaints", """
        SELECT COUNT(DISTINCT complaint_id) AS total,
               COUNT(DISTINCT complaint_id) FILTER (
                   WHERE complaint_status = 'OPEN') AS open_cnt,
               MODE() WITHIN GROUP (ORDER BY majorcategory) AS top_category
        FROM lake.housing.hpd_complaints
        WHERE REPLACE(CAST(bbl AS VARCHAR), '.00000000', '') = ?
    """, [bbl]))

    queries.append(("building_dob_violations", """
        SELECT COUNT(*) AS total,
               SUM(TRY_CAST(penalty_balance_due AS DOUBLE)) AS penalties
        FROM lake.housing.dob_ecb_violations
        WHERE (boro || LPAD(block::VARCHAR, 5, '0')
                    || LPAD(lot::VARCHAR, 4, '0')) = ?
    """, [bbl]))

    queries.append(("building_fdny", """
        SELECT COUNT(*) AS total
        FROM lake.housing.fdny_violations
        WHERE REPLACE(CAST(bbl AS VARCHAR), '.00000000', '') = ?
    """, [bbl]))

    queries.append(("building_evictions", """
        SELECT COUNT(*) AS total
        FROM lake.housing.evictions
        WHERE REPLACE(CAST(bbl AS VARCHAR), '.00000000', '') = ?
          AND "residential_commercial_ind" = 'Residential'
    """, [bbl]))

    queries.append(("building_sale", """
        SELECT TRY_CAST(m.document_date AS DATE) AS sale_date,
               TRY_CAST(m.document_amt AS DOUBLE) AS price
        FROM lake.housing.acris_master m
        JOIN lake.housing.acris_legals l
          ON m.document_id = l.document_id
        WHERE (l.borough || LPAD(l.block::VARCHAR, 5, '0')
                         || LPAD(l.lot::VARCHAR, 4, '0')) = ?
          AND m.doc_type IN ('DEED', 'DEEDO')
        ORDER BY m.document_date DESC
        LIMIT 1
    """, [bbl]))

    queries.append(("building_energy", """
        SELECT energy_star_score, source_eui_kbtu_ft2,
               total_ghg_emissions_mtco2e
        FROM lake.environment.ll84_energy_2023
        WHERE REPLACE(CAST(bbl AS VARCHAR), '.00000000', '') = ?
        LIMIT 1
    """, [bbl]))

    queries.append(("building_facade", """
        SELECT filing_status
        FROM lake.housing.dob_safety_facades
        WHERE REPLACE(CAST(bbl AS VARCHAR), '.00000000', '') = ?
        ORDER BY last_filing_date DESC NULLS LAST
        LIMIT 1
    """, [bbl]))

    queries.append(("building_boiler", """
        SELECT overall_status
        FROM lake.housing.dob_safety_boiler
        WHERE REPLACE(CAST(bbl AS VARCHAR), '.00000000', '') = ?
        ORDER BY inspection_date DESC NULLS LAST
        LIMIT 1
    """, [bbl]))

    queries.append(("building_aep", """
        SELECT 1 AS on_list
        FROM lake.housing.aep_buildings
        WHERE REPLACE(CAST(bbl AS VARCHAR), '.00000000', '') = ?
        LIMIT 1
    """, [bbl]))

    queries.append(("building_tax_lien", """
        SELECT COUNT(*) AS cnt
        FROM lake.housing.tax_lien_sales
        WHERE REPLACE(CAST(bbl AS VARCHAR), '.00000000', '') = ?
    """, [bbl]))

    queries.append(("building_owner_portfolio", """
        SELECT COUNT(DISTINCT j2.bbl) AS portfolio_size
        FROM lake.housing.hpd_registration_contacts c
        JOIN lake.housing.hpd_jurisdiction j2
          ON c.registrationid = j2.registrationid
        WHERE c.registrationid = (
            SELECT registrationid
            FROM lake.housing.hpd_jurisdiction
            WHERE boroid || LPAD(block::VARCHAR, 5, '0')
                         || LPAD(lot::VARCHAR, 4, '0') = ?
            LIMIT 1
        )
    """, [bbl]))

    queries.append(("building_corps_at_address", """
        SELECT COUNT(*) AS cnt
        FROM lake.business.nys_corporations
        WHERE UPPER(registered_agent_address) LIKE '%' || ? || '%'
        LIMIT 1
    """, [zipcode]))

    queries.append(("building_valuation", """
        SELECT year, curmkttot, curacttot, curtxbtot, owner
        FROM lake.housing.property_valuation
        WHERE (boro || LPAD(block::VARCHAR, 5, '0')
                    || LPAD(lot::VARCHAR, 4, '0')) = ?::VARCHAR
        ORDER BY year DESC
        LIMIT 1
    """, [bbl]))

    queries.append(("building_hpd_litigations", """
        SELECT COUNT(*) AS total
        FROM lake.housing.hpd_litigations
        WHERE REPLACE(CAST(bbl AS VARCHAR), '.00000000', '') = ?
    """, [bbl]))

    queries.append(("building_rent_stab", """
        SELECT uc2022
        FROM lake.housing.ll44_income_rent
        WHERE ucbbl = ?
        LIMIT 1
    """, [bbl]))

    queries.append(("building_housing_court", """
        SELECT COUNT(*) AS total
        FROM lake.housing.oca_housing_court_cases
        WHERE REPLACE(CAST(bbl AS VARCHAR), '.00000000', '') = ?
    """, [bbl]))

    queries.append(("building_permits", """
        SELECT COUNT(*) AS total,
               MAX(TRY_CAST(issuance_date AS DATE)) AS latest
        FROM lake.housing.dob_permit_issuance
        WHERE borough = ? AND block = ? AND lot = ?
        LIMIT 1
    """, [boro_code, bbl[1:6] if len(bbl) >= 6 else "",
          bbl[6:] if len(bbl) >= 6 else ""]))

    # ── BUILDING PERCENTILES ─────────────────────────────────────────────

    queries.append(("pctile_violations", """
        WITH bbl_counts AS (
            SELECT bbl, COUNT(*) AS cnt
            FROM lake.housing.hpd_violations
            GROUP BY bbl
        ),
        ranked AS (
            SELECT bbl, cnt, PERCENT_RANK() OVER (ORDER BY cnt) AS pctile
            FROM bbl_counts
        )
        SELECT pctile FROM ranked
        WHERE REPLACE(CAST(bbl AS VARCHAR), '.00000000', '') = ?
    """, [bbl]))

    queries.append(("pctile_complaints", """
        WITH bbl_counts AS (
            SELECT bbl, COUNT(DISTINCT complaint_id) AS cnt
            FROM lake.housing.hpd_complaints
            GROUP BY bbl
        ),
        ranked AS (
            SELECT bbl, cnt, PERCENT_RANK() OVER (ORDER BY cnt) AS pctile
            FROM bbl_counts
        )
        SELECT pctile FROM ranked
        WHERE REPLACE(CAST(bbl AS VARCHAR), '.00000000', '') = ?
    """, [bbl]))

    queries.append(("pctile_energy", """
        WITH scores AS (
            SELECT bbl,
                   TRY_CAST(energy_star_score AS DOUBLE) AS score
            FROM lake.environment.ll84_energy_2023
            WHERE energy_star_score IS NOT NULL
        ),
        ranked AS (
            SELECT bbl, score, PERCENT_RANK() OVER (ORDER BY score) AS pctile
            FROM scores
        )
        SELECT pctile FROM ranked
        WHERE REPLACE(CAST(bbl AS VARCHAR), '.00000000', '') = ?
    """, [bbl]))

    # ══════════════════════════════════════════════════════════════════════
    # BLOCK (block prefix, ~5 queries)
    # ══════════════════════════════════════════════════════════════════════

    queries.append(("block_buildings", f"""
        SELECT COUNT(*) AS cnt,
               ROUND(AVG(TRY_CAST(numfloors AS INT)))::INT AS avg_floors,
               ROUND(AVG(TRY_CAST(yearbuilt AS INT))
                     FILTER (WHERE TRY_CAST(yearbuilt AS INT) > 1800))::INT AS avg_year,
               SUM(TRY_CAST(unitsres AS INT)) AS total_units
        FROM lake.city_government.pluto
        WHERE borocode || LPAD(block::VARCHAR, 5, '0') LIKE '{block_prefix}%'
    """, None))

    queries.append(("block_trees", """
        SELECT spc_common AS species, COUNT(*) AS cnt
        FROM lake.environment.street_trees
        WHERE zipcode = ? AND spc_common IS NOT NULL
        GROUP BY spc_common
        ORDER BY cnt DESC
        LIMIT 3
    """, [zipcode]))

    queries.append(("block_film", """
        SELECT event_id, parking_held, start_date_time
        FROM lake.city_government.film_permits
        WHERE UPPER(parking_held) LIKE '%' || UPPER(?) || '%'
        ORDER BY start_date_time DESC
        LIMIT 5
    """, [street]))

    queries.append(("block_crashes", """
        SELECT COUNT(*) AS total,
               SUM(TRY_CAST(number_of_persons_injured AS INT)) AS injured,
               SUM(TRY_CAST(number_of_persons_killed AS INT)) AS killed
        FROM lake.public_safety.motor_vehicle_collisions
        WHERE zip_code = ?
          AND TRY_CAST(crash_date AS DATE) >= CURRENT_DATE - INTERVAL '1 year'
        LIMIT 1
    """, [zipcode]))

    queries.append(("block_permits", """
        SELECT job_type, COUNT(*) AS cnt
        FROM lake.housing.dob_permit_issuance
        WHERE borough = ? AND block = ?
          AND TRY_CAST(issuance_date AS DATE) >= CURRENT_DATE - INTERVAL '2 years'
        GROUP BY job_type
        ORDER BY cnt DESC
        LIMIT 5
    """, [boro_code, bbl[1:6] if len(bbl) >= 6 else ""]))

    # ══════════════════════════════════════════════════════════════════════
    # NEIGHBORHOOD (ZIP, ~10 queries)
    # ══════════════════════════════════════════════════════════════════════

    queries.append(("neighborhood_acs", """
        SELECT total_population, median_household_income, median_gross_rent,
               median_home_value,
               ROUND(100.0 * below_poverty / NULLIF(poverty_universe, 0), 1) AS poverty_rate
        FROM lake.federal.acs_zcta_demographics
        WHERE zcta = ?
        LIMIT 1
    """, [zipcode]))

    queries.append(("neighborhood_311_top", """
        SELECT complaint_type, COUNT(*) AS cnt
        FROM lake.social_services.n311_service_requests
        WHERE incident_zip = ?
          AND TRY_CAST(created_date AS DATE) >= CURRENT_DATE - INTERVAL '1 year'
        GROUP BY complaint_type
        ORDER BY cnt DESC
        LIMIT 5
    """, [zipcode]))

    queries.append(("neighborhood_recycling", """
        SELECT diversion_rate_total
        FROM lake.environment.recycling_rates
        WHERE communitydistrict = ?
        LIMIT 1
    """, [cd]))

    queries.append(("neighborhood_broadband", """
        SELECT broadband_adoption_rate
        FROM lake.social_services.broadband_adoption
        WHERE zip_code = ?
        LIMIT 1
    """, [zipcode]))

    queries.append(("pctile_income", """
        WITH zips AS (
            SELECT zcta, median_household_income AS income
            FROM lake.federal.acs_zcta_demographics
            WHERE median_household_income IS NOT NULL
        ),
        ranked AS (
            SELECT zcta, PERCENT_RANK() OVER (ORDER BY income) AS pctile
            FROM zips
        )
        SELECT pctile FROM ranked WHERE zcta = ?
    """, [zipcode]))

    queries.append(("pctile_rent", """
        WITH zips AS (
            SELECT zcta, median_gross_rent AS rent
            FROM lake.federal.acs_zcta_demographics
            WHERE median_gross_rent IS NOT NULL
        ),
        ranked AS (
            SELECT zcta, PERCENT_RANK() OVER (ORDER BY rent) AS pctile
            FROM zips
        )
        SELECT pctile FROM ranked WHERE zcta = ?
    """, [zipcode]))

    queries.append(("pctile_311", """
        WITH zip_counts AS (
            SELECT incident_zip, COUNT(*) AS cnt
            FROM lake.social_services.n311_service_requests
            WHERE TRY_CAST(created_date AS DATE) >= CURRENT_DATE - INTERVAL '1 year'
            GROUP BY incident_zip
        ),
        ranked AS (
            SELECT incident_zip, PERCENT_RANK() OVER (ORDER BY cnt) AS pctile
            FROM zip_counts
        )
        SELECT pctile FROM ranked WHERE incident_zip = ?
    """, [zipcode]))

    # ══════════════════════════════════════════════════════════════════════
    # SAFETY (precinct, ~5 queries)
    # ══════════════════════════════════════════════════════════════════════

    if precinct:
        pct_val = int(precinct) if precinct.isdigit() else 0

        queries.append(("safety_crimes", """
            SELECT COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE law_cat_cd = 'FELONY') AS felonies,
                   COUNT(*) FILTER (
                       WHERE law_cat_cd = 'MISDEMEANOR') AS misdemeanors
            FROM lake.public_safety.nypd_complaints_ytd
            WHERE addr_pct_cd = ?
        """, [pct_val]))

        queries.append(("safety_top_offenses", """
            SELECT ofns_desc, COUNT(*) AS cnt
            FROM lake.public_safety.nypd_complaints_ytd
            WHERE addr_pct_cd = ?
            GROUP BY ofns_desc
            ORDER BY cnt DESC
            LIMIT 5
        """, [pct_val]))

        queries.append(("safety_shootings", """
            SELECT COUNT(*) AS total,
                   COUNT(*) FILTER (
                       WHERE statistical_murder_flag = 'true') AS fatal
            FROM lake.public_safety.shootings
            WHERE precinct = ?
              AND TRY_CAST(occur_date AS DATE)
                  >= CURRENT_DATE - INTERVAL '1 year'
        """, [pct_val]))

        queries.append(("safety_crashes", """
            SELECT COUNT(*) AS total,
                   SUM(TRY_CAST(number_of_persons_injured AS INT)) AS injured,
                   SUM(TRY_CAST(number_of_persons_killed AS INT)) AS killed
            FROM lake.public_safety.motor_vehicle_collisions
            WHERE zip_code = ?
              AND TRY_CAST(crash_date AS DATE)
                  >= CURRENT_DATE - INTERVAL '1 year'
            LIMIT 1
        """, [zipcode]))

        queries.append(("pctile_crime", """
            WITH pct_counts AS (
                SELECT addr_pct_cd, COUNT(*) AS cnt
                FROM lake.public_safety.nypd_complaints_ytd
                GROUP BY addr_pct_cd
            ),
            ranked AS (
                SELECT addr_pct_cd, PERCENT_RANK() OVER (ORDER BY cnt) AS pctile
                FROM pct_counts
            )
            SELECT pctile FROM ranked WHERE addr_pct_cd = ?
        """, [pct_val]))

    # ══════════════════════════════════════════════════════════════════════
    # SCHOOLS (nearest + district, ~3 queries)
    # ══════════════════════════════════════════════════════════════════════

    # DBN format: district(2) + borough_letter(1) + school(3)
    # Borough letter: M=Manhattan, X=Bronx, K=Brooklyn, Q=Queens, R=Staten Island
    boro_letter = {"1": "M", "2": "X", "3": "K", "4": "Q", "5": "R"}.get(boro_code, "")

    queries.append(("school_nearest", """
        SELECT DISTINCT school_name, dbn
        FROM lake.education.quality_reports
        WHERE SUBSTR(dbn, 3, 1) = ?
          AND school_name IS NOT NULL
        ORDER BY school_name
        LIMIT 3
    """, [boro_letter]))

    queries.append(("school_ela", """
        SELECT school_name, mean_scale_score,
               level_3_4_1 AS level_3_4_pct
        FROM lake.education.ela_results
        WHERE report_category = 'School'
          AND grade = 'All Grades'
          AND SUBSTR(geographic_subdivision, 3, 1) = ?
          AND year = (SELECT MAX(year) FROM lake.education.ela_results)
        ORDER BY TRY_CAST(level_3_4_1 AS DOUBLE) DESC NULLS LAST
        LIMIT 3
    """, [boro_letter]))

    queries.append(("school_math", """
        SELECT school_name, mean_scale_score,
               level_3_4_1 AS level_3_4_pct
        FROM lake.education.math_results
        WHERE report_category = 'School'
          AND grade = 'All Grades'
          AND SUBSTR(geographic_subdivision, 3, 1) = ?
          AND year = (SELECT MAX(year) FROM lake.education.math_results)
        ORDER BY TRY_CAST(level_3_4_1 AS DOUBLE) DESC NULLS LAST
        LIMIT 3
    """, [boro_letter]))

    # ══════════════════════════════════════════════════════════════════════
    # HEALTH (ZIP/UHF, ~5 queries)
    # ══════════════════════════════════════════════════════════════════════

    queries.append(("health_cdc", """
        SELECT measure, data_value, short_question_text
        FROM lake.health.cdc_places
        WHERE TRY_CAST(locationid AS VARCHAR) = ?
          AND data_value IS NOT NULL
        ORDER BY measure
        LIMIT 20
    """, [zipcode]))

    queries.append(("health_rats", """
        SELECT COUNT(*) AS inspections,
               COUNT(*) FILTER (WHERE result ILIKE '%active%') AS active_rats
        FROM lake.health.rodent_inspections
        WHERE zip_code = ?
    """, [zipcode]))

    queries.append(("health_covid", """
        SELECT covid_case_rate, covid_death_rate
        FROM lake.health.covid_by_zip
        WHERE modified_zcta = ?
        LIMIT 1
    """, [zipcode]))

    queries.append(("pctile_rats", """
        WITH zip_rats AS (
            SELECT zip_code,
                   COUNT(*) FILTER (WHERE result ILIKE '%active%') AS active
            FROM lake.health.rodent_inspections
            GROUP BY zip_code
        ),
        ranked AS (
            SELECT zip_code, PERCENT_RANK() OVER (ORDER BY active) AS pctile
            FROM zip_rats
        )
        SELECT pctile FROM ranked WHERE zip_code = ?
    """, [zipcode]))

    queries.append(("health_asthma", """
        SELECT data_value
        FROM lake.health.cdc_places
        WHERE TRY_CAST(locationid AS VARCHAR) = ?
          AND measure = 'Current asthma among adults'
        LIMIT 1
    """, [zipcode]))

    # ══════════════════════════════════════════════════════════════════════
    # ENVIRONMENT (~5 queries)
    # ══════════════════════════════════════════════════════════════════════

    queries.append(("env_flood", """
        SELECT f.fshri
        FROM lake.environment.flood_vulnerability f
        JOIN lake.city_government.pluto p
          ON f.geoid = '36' || LPAD(
               CASE p.borocode
                 WHEN '1' THEN '061' WHEN '2' THEN '005'
                 WHEN '3' THEN '047' WHEN '4' THEN '081'
                 WHEN '5' THEN '085' ELSE '000' END, 3, '0')
             || LPAD(CAST(p.tract2010 AS VARCHAR), 6, '0')
        WHERE p.borocode || LPAD(p.block::VARCHAR, 5, '0')
                         || LPAD(p.lot::VARCHAR, 4, '0') = ?
        LIMIT 1
    """, [bbl]))

    queries.append(("env_heat", """
        SELECT hvi
        FROM lake.environment.heat_vulnerability
        WHERE zcta20 = ?
        LIMIT 1
    """, [zipcode]))

    queries.append(("env_air", """
        SELECT name, AVG(TRY_CAST(data_value AS DOUBLE)) AS avg_val
        FROM lake.environment.air_quality
        WHERE geo_type_name = 'CD'
          AND name = 'Fine particles (PM 2.5)'
          AND time_period = (
              SELECT MAX(time_period)
              FROM lake.environment.air_quality
              WHERE geo_type_name = 'CD'
                AND name = 'Fine particles (PM 2.5)'
          )
        GROUP BY name
        LIMIT 1
    """, None))

    queries.append(("env_trees_block", """
        SELECT COUNT(*) AS tree_count
        FROM lake.environment.street_trees
        WHERE zipcode = ?
    """, [zipcode]))

    queries.append(("env_ej", """
        SELECT COUNT(*) AS facilities
        FROM lake.federal.epa_echo_facilities
        WHERE zip_code = ?
    """, [zipcode]))

    # ══════════════════════════════════════════════════════════════════════
    # CIVIC (~3 queries)
    # ══════════════════════════════════════════════════════════════════════

    queries.append(("civic_contracts", """
        SELECT SUM(TRY_CAST(contract_amount AS DOUBLE)) AS total_5yr,
               COUNT(*) AS cnt
        FROM lake.city_government.contract_awards
        WHERE vendor_name IN (
            SELECT DISTINCT vendorname
            FROM lake.city_government.contract_awards
            WHERE vendorname IS NOT NULL
        )
          AND TRY_CAST(start_date AS DATE) >= CURRENT_DATE - INTERVAL '5 years'
        LIMIT 1
    """, None))

    queries.append(("civic_fec", """
        SELECT SUM(TRY_CAST(contribution_receipt_amount AS DOUBLE)) AS total,
               COUNT(*) AS cnt
        FROM lake.federal.fec_contributions
        WHERE contributor_zip LIKE ? || '%'
    """, [zipcode[:5] if zipcode else ""]))

    queries.append(("civic_voter", """
        SELECT COUNT(*) AS registered_voters
        FROM lake.city_government.voter_registrations
        WHERE zip5 = ?
        LIMIT 1
    """, [zipcode]))

    # ══════════════════════════════════════════════════════════════════════
    # SERVICES (~3 queries)
    # ══════════════════════════════════════════════════════════════════════

    if latitude and longitude:
        queries.append(("services_subway", """
            SELECT station_name, line_name AS line,
                   entrance_latitude, entrance_longitude
            FROM lake.transportation.mta_entrances
            WHERE TRY_CAST(entrance_latitude AS DOUBLE) IS NOT NULL
            ORDER BY ABS(TRY_CAST(entrance_latitude AS DOUBLE) - ?)
                   + ABS(TRY_CAST(entrance_longitude AS DOUBLE) - ?)
            LIMIT 3
        """, [latitude, longitude]))

    queries.append(("services_food_pantries", """
        SELECT COUNT(*) AS cnt
        FROM lake.social_services.dycd_program_sites
        WHERE zipcode = ?
          AND program_type ILIKE '%food%'
    """, [zipcode]))

    queries.append(("services_parks", """
        SELECT COUNT(*) AS cnt
        FROM lake.recreation.play_areas
        WHERE zipcode = ?
    """, [zipcode]))

    # ══════════════════════════════════════════════════════════════════════
    # FUN FACTS (~5 queries)
    # ══════════════════════════════════════════════════════════════════════

    queries.append(("fun_baby_names", """
        SELECT nm, gndr, TRY_CAST(cnt AS INT) AS cnt
        FROM lake.recreation.baby_names
        WHERE brth_yr = (SELECT MAX(brth_yr) FROM lake.recreation.baby_names)
        ORDER BY TRY_CAST(cnt AS INT) DESC
        LIMIT 2
    """, None))

    queries.append(("fun_dogs", """
        SELECT COUNT(*) AS cnt
        FROM lake.recreation.canine_waste
        WHERE zipcode = ?
    """, [zipcode]))

    queries.append(("fun_trees", """
        SELECT spc_common AS species, COUNT(*) AS cnt
        FROM lake.environment.street_trees
        WHERE zipcode = ?
          AND spc_common IS NOT NULL
        GROUP BY spc_common
        ORDER BY cnt DESC
        LIMIT 1
    """, [zipcode]))

    queries.append(("fun_film", """
        SELECT COUNT(*) AS shoots_12mo
        FROM lake.city_government.film_permits
        WHERE UPPER(parking_held) LIKE '%' || UPPER(?) || '%'
          AND TRY_CAST(start_date_time AS DATE)
              >= CURRENT_DATE - INTERVAL '1 year'
    """, [street]))

    queries.append(("fun_fishing", """
        SELECT site
        FROM lake.recreation.fishing_sites
        ORDER BY RANDOM()
        LIMIT 1
    """, None))

    queries.append(("fun_corps", """
        SELECT entity_name, formation_date
        FROM lake.business.nys_corporations
        WHERE UPPER(registered_agent_address) LIKE '%' || ? || '%'
        ORDER BY formation_date DESC NULLS LAST
        LIMIT 3
    """, [zipcode]))

    return queries
