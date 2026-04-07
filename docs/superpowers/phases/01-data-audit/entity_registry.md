# Entity Registry — Common Ground DuckLake

*Generated 2026-03-26 from live DuckDB column metadata*

## Summary

- **Total production tables**: 246
- **Tables with entity columns**: 162 (66%)
- **Entity column occurrences**: 588

### Entity Type Distribution

| Entity Type | Tables | Total Rows |
|:---|---:|---:|
| person_name | 51 | 136,743,000 |
| org_name | 53 | 84,087,710 |
| bbl | 106 | 180,728,160 |
| address | 86 | 257,787,710 |
| bin | 102 | 154,559,160 |
| zip | 123 | 260,819,410 |
| license_id | 20 | 11,443,000 |
| precinct | 18 | 55,362,600 |
| complaint_id | 9 | 69,745,000 |
| document_id | 7 | 104,100,000 |
| dos_id | 6 | 74,200,000 |
| registration_id | 6 | 14,191,000 |
| camis | 1 | 297,000 |

### Highest Cross-Reference Potential (3+ entity types)

| Schema | Table | Entity Types | Row Count |
|:---|:---|:---|---:|
| housing | acris_parties | address, document_id, person_name, zip | 46,200,000 |
| business | nys_entity_addresses | address, dos_id, person_name, zip | 34,700,000 |
| housing | acris_legals | address, bbl, document_id | 22,500,000 |
| business | nys_corp_name_history | bbl, bin, dos_id, org_name, zip | 22,000,000 |
| social_services | n311_service_requests | address, bbl, bin, complaint_id, org_name, precinct, zip | 21,000,000 |
| business | nys_corporations | address, bbl, bin, dos_id, org_name, zip | 16,700,000 |
| housing | hpd_complaints | address, bbl, bin, complaint_id, zip | 16,000,000 |
| transportation | parking_violations | address, bbl, bin, precinct, zip | 12,000,000 |
| business | acris_pp_parties | address, document_id, person_name, zip | 11,000,000 |
| housing | hpd_violations | address, bbl, bin, complaint_id, registration_id, zip | 11,000,000 |
| housing | property_valuation | address, bbl, bin, zip | 10,500,000 |
| public_safety | nypd_complaints_historic | address, complaint_id, precinct | 9,500,000 |
| housing | dob_complaints | address, bbl, bin, complaint_id, zip | 9,200,000 |
| housing | dob_permit_issuance | address, bbl, bin, license_id, org_name, person_name, zip | 4,000,000 |
| health | rodent_inspections | address, bbl, bin, zip | 3,100,000 |
| housing | dob_application_owners | address, bbl, bin, org_name, person_name, zip | 2,700,000 |
| housing | dob_violations | address, bbl, bin, zip | 2,500,000 |
| public_safety | motor_vehicle_collisions | address, bbl, bin, zip | 2,200,000 |
| health | medicaid_providers | address, bbl, bin, org_name, zip | 2,100,000 |
| public_safety | ccrb_allegations | bbl, bin, complaint_id, zip | 2,100,000 |
| housing | acris_pp_legals | address, bbl, document_id | 2,000,000 |
| housing | dob_ecb_violations | address, bbl, bin, person_name, zip | 1,800,000 |
| city_government | campaign_contributions | org_name, person_name, zip | 1,700,000 |
| city_government | civil_service_active | bbl, bin, person_name, zip | 1,700,000 |
| financial | nys_attorney_registrations | address, bbl, bin, license_id, org_name, person_name, registration_id, zip | 1,700,000 |
| housing | dob_safety_boiler | bin, license_id, person_name | 1,700,000 |
| transportation | traffic_volume | address, bbl, bin, zip | 1,000,000 |
| financial | nys_cosmetology_licenses | bbl, bin, license_id, person_name, zip | 867,000 |
| housing | hpd_registration_contacts | address, org_name, person_name, registration_id, zip | 804,000 |
| public_safety | ccrb_complaints | bbl, bin, complaint_id, precinct, zip | 695,000 |
| business | bic_trade_waste | address, bbl, bin, license_id, org_name, zip | 677,000 |
| financial | nys_re_brokers | address, bbl, bin, license_id, org_name, person_name, zip | 591,000 |
| business | nys_corp_constituents | bbl, bin, dos_id, org_name, zip | 500,000 |
| housing | dob_now_build_filings | address, bbl, bin, license_id, org_name, person_name, zip | 500,000 |
| transportation | mta_paratransit_ride | bbl, bin, zip | 500,000 |
| transportation | street_permits | bbl, bin, license_id, zip | 500,000 |
| housing | hpd_jurisdiction | address, bbl, bin, registration_id, zip | 377,000 |
| housing | evictions | address, bbl, bin, person_name, zip | 374,000 |
| housing | dwelling_registrations | address, bbl, bin, registration_id, zip | 300,000 |
| health | restaurant_inspections | address, bbl, bin, camis, org_name, zip | 297,000 |

## Entity Type: person_name

| Schema | Table | Columns | Row Count | Name Pattern |
|:---|:---|:---|---:|:---|
| housing | acris_parties | name | 46,200,000 | ACRIS_STYLE |
| business | nys_entity_addresses | name | 34,700,000 | ACRIS_STYLE |
| business | acris_pp_parties | name | 11,000,000 | ACRIS_STYLE |
| city_government | citywide_payroll | first_name, last_name | 6,800,000 | SEPARATE |
| housing | oca_parties | lastname, firstname | 6,400,000 | SEPARATE |
| housing | dob_permit_issuance | permittee_s_last_name, owner_s_last_name, superintendent_first___last_name, site_safety_mgr_s_first_name, ... | 4,000,000 | SEPARATE |
| city_government | death_certificates_1862_1948 | first_name, last_name | 3,700,000 | SEPARATE |
| housing | dob_application_owners | owner_s_last_name, owner_s_first_name | 2,700,000 | SEPARATE |
| city_government | birth_certificates_1855_1909 | first_name, last_name | 2,600,000 | SEPARATE |
| housing | oca_index | judge | 2,200,000 | COMBINED_COMMA |
| housing | dob_ecb_violations | respondent_name | 1,800,000 | COMBINED_COMMA |
| city_government | campaign_contributions | name | 1,700,000 | COMBINED_COMMA |
| city_government | civil_service_active | first_name, last_name | 1,700,000 | SEPARATE |
| financial | nys_attorney_registrations | first_name, last_name | 1,700,000 | SEPARATE |
| housing | dob_safety_boiler | owner_last_name, owner_first_name, applicantfirst_name, applicant_last_name | 1,700,000 | SEPARATE |
| city_government | civil_litigation | judge | 1,100,000 | COMBINED_COMMA |
| financial | nys_cosmetology_licenses | license_holder_name | 867,000 | COMBINED_COMMA |
| housing | hpd_registration_contacts | lastname, firstname | 804,000 | SEPARATE |
| financial | nys_re_brokers | license_holder_name | 591,000 | COMBINED_COMMA |
| city_government | campaign_expenditures | candlast, name, candfirst | 516,000 | COMBINED_COMMA |
| federal | cl_fjc_cases | plaintiff, defendant | 500,000 | COMBINED_COMMA |
| housing | dob_now_build_filings | applicant_first_name, filing_representative_first_name, applicant_last_name, filing_representative_last_name | 500,000 | SEPARATE |
| housing | evictions | marshal_first_name, marshal_last_name | 374,000 | SEPARATE |
| public_safety | nypd_misconduct_named | lastname, firstname | 303,000 | SEPARATE |
| financial | nys_tax_warrants | debtor_name_3, debtor_name_1, debtor_name_2, debtor_name_4 | 275,000 | COMBINED_COMMA |
| housing | tax_lien_sales | debtor_name, owner_name | 264,000 | COMBINED_COMMA |
| financial | nys_notaries | commission_holder_name | 238,000 | COMBINED_COMMA |
| housing | hpd_litigations | respondent | 236,000 | COMBINED_COMMA |
| business | nys_daily_corp_filings | sop_name, filer_name | 200,000 | COMBINED_COMMA |
| federal | cl_judges | name_first, name_last | 163,000 | SEPARATE |
| city_government | city_record | contact_name | 100,000 | COMBINED_COMMA |
| financial | nys_child_support_warrants | debtor_name | 100,000 | COMBINED_COMMA |
| health | childcare_inspections_current | permittee | 100,000 | COMBINED_COMMA |
| housing | dob_safety_facades | owner_name, qewi_name | 100,000 | COMBINED_COMMA |
| city_government | contract_awards | contact_name | 52,000 | COMBINED_COMMA |
| housing | fdny_violations | acct_owner | 52,000 | COMBINED_COMMA |
| city_government | cfb_offyear_contributions | name | 50,000 | COMBINED_COMMA |
| financial | nys_elevator_licenses | first_name, last_name | 50,000 | SEPARATE |
| financial | nys_re_appraisers | applicant_name | 50,000 | COMBINED_COMMA |
| health | school_cafeteria_inspections | permittee | 50,000 | COMBINED_COMMA |
| public_safety | ccrb_officers | officer_first_name, officer_last_name | 40,000 | SEPARATE |
| public_safety | nypd_officer_profile | name | 39,000 | COMBINED_COMMA |
| city_government | cfb_intermediaries | candlast, name, candfirst | 30,000 | COMBINED_COMMA |
| social_services | nys_child_care | provider_name | 30,000 | COMBINED_COMMA |
| city_government | coib_policymakers | name_of_employee, first_name, last_name | 28,000 | SEPARATE |
| financial | nys_ida_projects | applicant_name | 20,000 | COMBINED_COMMA |
| city_government | doing_business_contributions | candlast, candfirst | 10,000 | SEPARATE |
| city_government | coib_agency_donations | name_of_donor_individual | 5,000 | COMBINED_COMMA |
| city_government | coib_elected_nfp_donations | elected_official, donor_name | 3,000 | COMBINED_COMMA |
| city_government | cfb_enforcement_penalties | candidate | 2,000 | COMBINED_COMMA |
| city_government | coib_legal_defense_trust | first_name, last_name | 1,000 | SEPARATE |

## Entity Type: org_name

| Schema | Table | Columns | Row Count |
|:---|:---|:---|---:|
| business | nys_corp_name_history | corp_name | 22,000,000 |
| social_services | n311_service_requests | agency_name | 21,000,000 |
| business | nys_corporations | current_entity_name, dos_process_name, registered_agent_name, chairman_name | 16,700,000 |
| city_government | citywide_payroll | agency_name | 6,800,000 |
| housing | dob_permit_issuance | owner_s_business_name, superintendent_business_name, permittee_s_business_name, site_safety_mgr_business_name | 4,000,000 |
| housing | dob_application_owners | owner_s_business_name | 2,700,000 |
| health | medicaid_providers | mmis_name | 2,100,000 |
| city_government | campaign_contributions | intempname, recipname, empname | 1,700,000 |
| financial | nys_attorney_registrations | company_name | 1,700,000 |
| housing | hpd_registration_contacts | corporationname | 804,000 |
| business | bic_trade_waste | trade_name, account_name | 677,000 |
| financial | nys_re_brokers | business_name | 591,000 |
| business | nys_corp_constituents | corp_name | 500,000 |
| housing | dob_now_build_filings | owner_s_business_name, filing_representative_business_name | 500,000 |
| health | restaurant_inspections | dba | 297,000 |
| financial | nys_procurement_state | authority_name, vendor_name | 276,000 |
| financial | nys_notaries | business_name_if_available | 238,000 |
| business | nys_daily_corp_filings | corp_name, fictitious_name, pre_corp_name, mrgr_const_name3, ... | 200,000 |
| health | childcare_inspections | legalname, centername | 200,000 |
| business | dcwp_charges | dba_trade_name, business_name | 115,000 |
| business | bic_violations | account_name | 102,000 |
| business | nys_dos_nonprofits | current_entity_name, dos_process_name, registered_agent_name | 100,000 |
| city_government | city_record | vendor_name, agency_name | 100,000 |
| financial | nys_procurement_local | authority_name, vendor_name | 100,000 |
| housing | dob_safety_facades | owner_bus_name, qewi_bus_name | 100,000 |
| business | issued_licenses | dba_trade_name, business_name | 69,000 |
| city_government | contract_awards | vendor_name, agency_name | 52,000 |
| business | license_applications | dba_trade_name, business_name | 50,000 |
| city_government | cfb_offyear_contributions | intempname, recipname, empname | 50,000 |
| financial | nys_re_appraisers | prin_bus_name | 50,000 |
| city_government | cfb_intermediaries | empname | 30,000 |
| health | childcare_programs | program_name | 30,000 |
| social_services | nys_child_care | facility_name | 30,000 |
| city_government | coib_policymakers | agency_name | 28,000 |
| financial | nys_ida_projects | authority_name | 20,000 |
| health | health_facilities | cooperator_name, facility_name, operator_name | 20,000 |
| health | open_restaurant_inspections | restaurantname, legalbusinessname | 20,000 |
| business | nys_contractor_registry | dba_name, business_name | 13,000 |
| business | bic_wholesale_markets | trade_name, account_name | 5,000 |
| city_government | coib_agency_donations | agency_name | 5,000 |
| social_services | equity_nyc | agency_name | 5,000 |
| city_government | coib_elected_nfp_donations | organization_name | 3,000 |
| city_government | covid_emergency_contracts | vendor_name | 3,000 |
| business | bic_denied_companies | trade_name, account_name | 1,000 |
| city_government | coib_legal_defense_trust | trust_name | 1,000 |
| social_services | community_gardens | gardenname | 600 |
| city_government | coib_official_fundraising | name_of_not_for_profit | 500 |
| social_services | access_nyc | program_name | 500 |
| social_services | community_orgs | organization_name | 500 |
| social_services | shop_healthy | store_name | 500 |
| social_services | benefits_centers | facility_name | 50 |
| social_services | snap_centers | facility_name | 50 |
| social_services | family_justice_centers | facility_name | 10 |

## Entity Type: bbl

| Schema | Table | Columns | Row Count |
|:---|:---|:---|---:|
| housing | acris_legals | borough+block+lot (composable) | 22,500,000 |
| business | nys_corp_name_history | bbl | 22,000,000 |
| social_services | n311_service_requests | bbl | 21,000,000 |
| business | nys_corporations | bbl | 16,700,000 |
| housing | hpd_complaints | bbl | 16,000,000 |
| transportation | parking_violations | bbl | 12,000,000 |
| housing | hpd_violations | bbl | 11,000,000 |
| housing | property_valuation | bbl | 10,500,000 |
| housing | dob_complaints | bbl | 9,200,000 |
| housing | dob_permit_issuance | bbl | 4,000,000 |
| health | rodent_inspections | bbl | 3,100,000 |
| housing | dob_application_owners | bbl | 2,700,000 |
| housing | dob_violations | bbl | 2,500,000 |
| housing | oca_index | bbl | 2,200,000 |
| public_safety | motor_vehicle_collisions | bbl | 2,200,000 |
| health | medicaid_providers | bbl | 2,100,000 |
| public_safety | ccrb_allegations | bbl | 2,100,000 |
| housing | acris_pp_legals | borough+block+lot (composable) | 2,000,000 |
| housing | dob_ecb_violations | bbl | 1,800,000 |
| city_government | civil_service_active | bbl | 1,700,000 |
| financial | nys_attorney_registrations | bbl | 1,700,000 |
| city_government | civil_litigation | lead_bbl | 1,100,000 |
| transportation | traffic_volume | bbl | 1,000,000 |
| financial | nys_cosmetology_licenses | bbl | 867,000 |
| public_safety | ccrb_complaints | bbl | 695,000 |
| business | bic_trade_waste | bbl | 677,000 |
| financial | nys_re_brokers | bbl | 591,000 |
| business | nys_corp_constituents | bbl | 500,000 |
| housing | dob_now_build_filings | bbl | 500,000 |
| transportation | mta_paratransit_ride | bbl | 500,000 |
| transportation | street_permits | bbl | 500,000 |
| housing | hpd_jurisdiction | borough+block+lot (composable) | 377,000 |
| housing | evictions | bbl | 374,000 |
| housing | dwelling_registrations | borough+block+lot (composable) | 300,000 |
| health | restaurant_inspections | bbl | 297,000 |
| financial | nys_tax_warrants | bbl | 275,000 |
| housing | tax_lien_sales | bbl | 264,000 |
| financial | nys_notaries | bbl | 238,000 |
| housing | hpd_litigations | bbl | 236,000 |
| business | nys_daily_corp_filings | bbl | 200,000 |
| business | bic_violations | bbl | 102,000 |
| business | nys_dos_nonprofits | bbl | 100,000 |
| city_government | city_record | bbl | 100,000 |
| financial | nys_child_support_warrants | bbl | 100,000 |
| health | childcare_inspections_current | bbl | 100,000 |
| housing | dob_safety_facades | bbl | 100,000 |
| public_safety | daily_inmates | bbl | 100,000 |
| transportation | mta_paratransit_ridership | bbl | 100,000 |
| housing | rolling_sales | bbl | 79,000 |
| business | issued_licenses | bbl | 69,000 |
| housing | affordable_housing | bbl | 60,000 |
| city_government | contract_awards | bbl | 52,000 |
| housing | fdny_violations | bbl | 52,000 |
| business | license_applications | bbl | 50,000 |
| financial | nys_elevator_licenses | bbl | 50,000 |
| financial | nys_re_appraisers | bbl | 50,000 |
| health | cooling_tower_inspections | bbl | 50,000 |
| health | drinking_water_tank_inspections | bbl | 50,000 |
| health | school_cafeteria_inspections | bbl | 50,000 |
| housing | dob_safety_violations | bbl | 50,000 |
| public_safety | ccrb_penalties | bbl | 50,000 |
| public_safety | discipline_charges | bbl | 50,000 |
| recreation | signs | bbl | 50,000 |
| recreation | special_events | bbl | 50,000 |
| social_services | dhs_daily_report | bbl | 50,000 |
| transportation | mta_elevator | bbl | 50,000 |
| public_safety | ccrb_officers | bbl | 40,000 |
| housing | designated_buildings | bbl | 35,000 |
| environment | ll84_energy_2023 | nyc_borough_block_and_lot | 30,000 |
| health | childcare_programs | bbl | 30,000 |
| health | drinking_water_tanks | bbl | 30,000 |
| housing | emergency_repair_hwo | bbl | 30,000 |
| public_safety | discipline_summary | bbl | 30,000 |
| recreation | properties | bbl | 30,000 |
| social_services | nys_child_care | bbl | 30,000 |
| environment | e_designations | bbl | 20,000 |
| health | health_facilities | bbl | 20,000 |
| health | open_restaurant_inspections | bbl | 20,000 |
| housing | emergency_repair_omo | bbl | 20,000 |
| business | nys_contractor_registry | bbl | 13,000 |
| housing | hpd_repair_vacate | bbl | 10,000 |
| recreation | aquatics | bbl | 10,000 |
| recreation | summer_sports_2022 | bbl | 10,000 |
| social_services | dhs_shelter_census | bbl | 10,000 |
| social_services | dycd_program_sites | bbl | 10,000 |
| business | bic_wholesale_markets | bbl | 5,000 |
| environment | oer_cleanup | bbl | 5,000 |
| housing | housing_connect_buildings | address_bbl | 5,000 |
| recreation | canine_waste | bbl | 5,000 |
| recreation | capital_projects | bbl | 5,000 |
| recreation | permit_areas | bbl | 5,000 |
| recreation | restrictive_declarations | bbl | 5,000 |
| recreation | functional_parkland | bbl | 2,000 |
| business | bic_denied_companies | bbl | 1,000 |
| education | capacity_projects | bbl | 1,000 |
| recreation | pools | bbl | 1,000 |
| recreation | spray_showers | bbl | 1,000 |
| social_services | idnyc_applications | bbl | 1,000 |
| social_services | community_gardens | bbl | 600 |
| housing | aep_buildings | bbl | 500 |
| housing | conh_pilot | bbl | 500 |
| social_services | community_orgs | bbl | 500 |
| social_services | shop_healthy | bbl | 500 |
| transportation | mta_stations | bbl | 500 |
| social_services | benefits_centers | bbl | 50 |
| social_services | family_justice_centers | bbl | 10 |

## Entity Type: address

| Schema | Table | Columns | Row Count |
|:---|:---|:---|---:|
| housing | acris_parties | address_2, address_1 | 46,200,000 |
| business | nys_entity_addresses | addr1, addr_type, addr2 | 34,700,000 |
| housing | acris_legals | street_name | 22,500,000 |
| social_services | n311_service_requests | street_name, incident_address | 21,000,000 |
| business | nys_corporations | dos_process_address_1, registered_agent_address_1, location_address_1, chairman_address_1 | 16,700,000 |
| housing | hpd_complaints | house_number, street_name | 16,000,000 |
| transportation | parking_violations | house_number, street_name | 12,000,000 |
| business | acris_pp_parties | address_2, address_1 | 11,000,000 |
| housing | hpd_violations | highhousenumber, streetname, housenumber, lowhousenumber | 11,000,000 |
| housing | property_valuation | street_name | 10,500,000 |
| public_safety | nypd_complaints_historic | addr_pct_cd | 9,500,000 |
| housing | dob_complaints | house_number | 9,200,000 |
| housing | dob_permit_issuance | street_name, house, owner_s_house_street_name | 4,000,000 |
| city_government | death_certificates_1862_1948 | number | 3,700,000 |
| health | rodent_inspections | house_number, street_name | 3,100,000 |
| housing | dob_application_owners | owner_s_house_number, owner_shouse_street_name | 2,700,000 |
| city_government | birth_certificates_1855_1909 | number | 2,600,000 |
| housing | dob_violations | house_number, number, street | 2,500,000 |
| public_safety | motor_vehicle_collisions | on_street_name, cross_street_name, off_street_name | 2,200,000 |
| health | medicaid_providers | service_address | 2,100,000 |
| housing | acris_pp_legals | street_name, addr_unit | 2,000,000 |
| housing | dob_ecb_violations | respondent_house_number, respondent_street | 1,800,000 |
| financial | nys_attorney_registrations | street_1 | 1,700,000 |
| environment | lead_service_lines | street_address | 1,000,000 |
| transportation | traffic_volume | street | 1,000,000 |
| housing | hpd_registration_contacts | businessstreetname, businesshousenumber | 804,000 |
| business | bic_trade_waste | address | 677,000 |
| financial | nys_re_brokers | business_address_1 | 591,000 |
| housing | dob_now_build_filings | street_name, owner_s_street_name, house_no, filing_representative_street_name | 500,000 |
| housing | hpd_jurisdiction | highhousenumber, streetname, housenumber, lowhousenumber | 377,000 |
| housing | evictions | eviction_address | 374,000 |
| housing | dwelling_registrations | highhousenumber, streetname, housenumber, lowhousenumber | 300,000 |
| health | restaurant_inspections | building, street | 297,000 |
| financial | nys_procurement_state | vendor_address_2, vendor_address_1 | 276,000 |
| financial | nys_tax_warrants | debtor_address_2, debtor_address_1 | 275,000 |
| housing | hpd_litigations | streetname, housenumber | 236,000 |
| business | nys_daily_corp_filings | filer_addr1, sop_addr1 | 200,000 |
| environment | nys_solar | street_address | 200,000 |
| health | childcare_inspections | building, street | 200,000 |
| public_safety | nypd_complaints_ytd | addr_pct_cd | 200,000 |
| business | nys_dos_nonprofits | dos_process_address_1, registered_agent_address_1 | 100,000 |
| city_government | city_record | vendor_address, street_address_2, street_address_1 | 100,000 |
| financial | nys_child_support_warrants | debtor_address | 100,000 |
| financial | nys_procurement_local | vendor_address_2, vendor_address_1 | 100,000 |
| health | childcare_inspections_current | number, street | 100,000 |
| housing | dob_safety_facades | street_name, owner_bus_street_name, house_no, qewi_bus_street_name | 100,000 |
| housing | rolling_sales | address | 79,000 |
| business | issued_licenses | address_state, address_city, address_street_name, address_zip, ... | 69,000 |
| housing | affordable_housing | house_number, street_name | 60,000 |
| city_government | contract_awards | vendor_address | 52,000 |
| housing | fdny_violations | number, prem_addr, street | 52,000 |
| business | license_applications | street | 50,000 |
| financial | nys_re_appraisers | prin_bus_addr1 | 50,000 |
| health | cooling_tower_inspections | address | 50,000 |
| health | drinking_water_tank_inspections | street_name, house | 50,000 |
| health | school_cafeteria_inspections | address_line_2, address_line_1, number, street | 50,000 |
| housing | dob_safety_violations | house_number, street | 50,000 |
| transportation | pothole_orders | housenum | 50,000 |
| environment | ll84_energy_2023 | address_1 | 30,000 |
| health | childcare_programs | address | 30,000 |
| health | drinking_water_tanks | house_num, street_name | 30,000 |
| housing | emergency_repair_hwo | streetname, housenumber | 30,000 |
| recreation | properties | address | 30,000 |
| social_services | nys_child_care | street_name | 30,000 |
| financial | nys_ida_projects | project_address_line_1 | 20,000 |
| health | health_facilities | address2, address1 | 20,000 |
| health | open_restaurant_inspections | businessaddress | 20,000 |
| housing | emergency_repair_omo | streetname, housenumber | 20,000 |
| business | nys_contractor_registry | address_2, address | 13,000 |
| housing | hpd_repair_vacate | house_number, street_name | 10,000 |
| social_services | dycd_program_sites | street_address | 10,000 |
| business | bic_wholesale_markets | address | 5,000 |
| environment | oer_cleanup | street_name | 5,000 |
| housing | housing_connect_buildings | house_number, street_name | 5,000 |
| recreation | nys_farmers_markets | address_line_1 | 5,000 |
| business | bic_denied_companies | street | 1,000 |
| education | capacity_projects | address | 1,000 |
| social_services | language_secret_shopper | site_address | 1,000 |
| social_services | community_gardens | address | 600 |
| housing | aep_buildings | street_address | 500 |
| housing | conh_pilot | street_address | 500 |
| social_services | community_orgs | street_address | 500 |
| social_services | shop_healthy | street_address | 500 |
| social_services | benefits_centers | street_address | 50 |
| social_services | snap_centers | street_address | 50 |
| social_services | family_justice_centers | street_address | 10 |

## Entity Type: license_id

| Schema | Table | Columns | Row Count |
|:---|:---|:---|---:|
| housing | dob_permit_issuance | job, hic_license, permittee_s_license | 4,000,000 |
| financial | nys_attorney_registrations | registration_number | 1,700,000 |
| housing | dob_safety_boiler | applicant_license_number | 1,700,000 |
| financial | nys_cosmetology_licenses | license_number | 867,000 |
| business | bic_trade_waste | bic_number | 677,000 |
| financial | nys_re_brokers | license_number | 591,000 |
| housing | dob_now_build_filings | job_filing_number, applicant_license | 500,000 |
| transportation | street_permits | permitnumber | 500,000 |
| financial | nys_notaries | commission_number_uid | 238,000 |
| health | childcare_inspections | permitnumber | 200,000 |
| business | bic_violations | bic_number | 102,000 |
| housing | dob_safety_facades | qewi_nys_lic_no | 100,000 |
| business | issued_licenses | license_nbr | 69,000 |
| business | license_applications | application_id, license_number | 50,000 |
| financial | nys_elevator_licenses | license_number | 50,000 |
| financial | nys_re_appraisers | uid | 50,000 |
| health | childcare_programs | permit_number | 30,000 |
| business | nys_contractor_registry | certificate_number | 13,000 |
| business | bic_wholesale_markets | bic_number | 5,000 |
| business | bic_denied_companies | bic_number | 1,000 |

## Entity Type: bin

| Schema | Table | Columns | Row Count |
|:---|:---|:---|---:|
| business | nys_corp_name_history | bin | 22,000,000 |
| social_services | n311_service_requests | bin | 21,000,000 |
| business | nys_corporations | bin | 16,700,000 |
| housing | hpd_complaints | bin, building_id | 16,000,000 |
| transportation | parking_violations | bin | 12,000,000 |
| housing | hpd_violations | bin, buildingid | 11,000,000 |
| housing | property_valuation | bin | 10,500,000 |
| housing | dob_complaints | bin | 9,200,000 |
| housing | dob_permit_issuance | bin | 4,000,000 |
| health | rodent_inspections | bin | 3,100,000 |
| housing | dob_application_owners | bin | 2,700,000 |
| housing | dob_violations | bin | 2,500,000 |
| public_safety | motor_vehicle_collisions | bin | 2,200,000 |
| health | medicaid_providers | bin | 2,100,000 |
| public_safety | ccrb_allegations | bin | 2,100,000 |
| housing | dob_ecb_violations | bin | 1,800,000 |
| city_government | civil_service_active | bin | 1,700,000 |
| financial | nys_attorney_registrations | bin | 1,700,000 |
| housing | dob_safety_boiler | bin_number | 1,700,000 |
| transportation | traffic_volume | bin | 1,000,000 |
| financial | nys_cosmetology_licenses | bin | 867,000 |
| public_safety | ccrb_complaints | bin | 695,000 |
| business | bic_trade_waste | bin | 677,000 |
| financial | nys_re_brokers | bin | 591,000 |
| business | nys_corp_constituents | bin | 500,000 |
| housing | dob_now_build_filings | bin | 500,000 |
| transportation | mta_paratransit_ride | bin | 500,000 |
| transportation | street_permits | bin | 500,000 |
| housing | hpd_jurisdiction | bin, buildingid | 377,000 |
| housing | evictions | bin | 374,000 |
| housing | dwelling_registrations | bin, buildingid | 300,000 |
| health | restaurant_inspections | bin | 297,000 |
| financial | nys_tax_warrants | bin | 275,000 |
| financial | nys_notaries | bin | 238,000 |
| housing | hpd_litigations | bin, buildingid | 236,000 |
| business | nys_daily_corp_filings | bin | 200,000 |
| health | childcare_inspections | bin | 200,000 |
| business | bic_violations | bin | 102,000 |
| business | nys_dos_nonprofits | bin | 100,000 |
| city_government | city_record | bin | 100,000 |
| financial | nys_child_support_warrants | bin | 100,000 |
| health | childcare_inspections_current | bin | 100,000 |
| housing | dob_safety_facades | bin | 100,000 |
| public_safety | daily_inmates | bin | 100,000 |
| transportation | mta_paratransit_ridership | bin | 100,000 |
| housing | rolling_sales | bin | 79,000 |
| business | issued_licenses | bin | 69,000 |
| housing | affordable_housing | bin, building_id | 60,000 |
| city_government | contract_awards | bin | 52,000 |
| housing | fdny_violations | bin | 52,000 |
| business | license_applications | bin | 50,000 |
| financial | nys_elevator_licenses | bin | 50,000 |
| financial | nys_re_appraisers | bin | 50,000 |
| health | cooling_tower_inspections | bin | 50,000 |
| health | drinking_water_tank_inspections | bin | 50,000 |
| health | school_cafeteria_inspections | bin | 50,000 |
| housing | dob_safety_violations | bin | 50,000 |
| public_safety | ccrb_penalties | bin | 50,000 |
| public_safety | discipline_charges | bin | 50,000 |
| recreation | signs | bin | 50,000 |
| recreation | special_events | bin | 50,000 |
| social_services | dhs_daily_report | bin | 50,000 |
| transportation | mta_elevator | bin | 50,000 |
| public_safety | ccrb_officers | bin | 40,000 |
| housing | designated_buildings | bin_number | 35,000 |
| environment | ll84_energy_2023 | nyc_building_identification | 30,000 |
| health | childcare_programs | bin | 30,000 |
| health | drinking_water_tanks | bin | 30,000 |
| housing | emergency_repair_hwo | bin, buildingid | 30,000 |
| public_safety | discipline_summary | bin | 30,000 |
| recreation | properties | bin | 30,000 |
| social_services | nys_child_care | bin | 30,000 |
| health | health_facilities | bin | 20,000 |
| health | open_restaurant_inspections | bin | 20,000 |
| housing | emergency_repair_omo | bin, buildingid | 20,000 |
| transportation | nys_bridge_conditions | bin | 20,000 |
| business | nys_contractor_registry | bin | 13,000 |
| housing | hpd_repair_vacate | bin, building_id | 10,000 |
| recreation | aquatics | bin | 10,000 |
| recreation | summer_sports_2022 | bin | 10,000 |
| social_services | dhs_shelter_census | bin | 10,000 |
| social_services | dycd_program_sites | bin | 10,000 |
| business | bic_wholesale_markets | bin | 5,000 |
| housing | housing_connect_buildings | address_buildingidentificationnumber, hpd_building_id | 5,000 |
| recreation | canine_waste | bin | 5,000 |
| recreation | capital_projects | bin | 5,000 |
| recreation | permit_areas | bin | 5,000 |
| recreation | restrictive_declarations | bin | 5,000 |
| recreation | functional_parkland | bin | 2,000 |
| business | bic_denied_companies | bin | 1,000 |
| education | capacity_projects | bin | 1,000 |
| recreation | pools | bin | 1,000 |
| recreation | spray_showers | bin | 1,000 |
| social_services | idnyc_applications | bin | 1,000 |
| social_services | community_gardens | bin | 600 |
| housing | aep_buildings | bin, building_id | 500 |
| housing | conh_pilot | bin, building_id | 500 |
| social_services | community_orgs | bin | 500 |
| social_services | shop_healthy | bin | 500 |
| transportation | mta_stations | bin | 500 |
| social_services | benefits_centers | bin | 50 |
| social_services | family_justice_centers | bin | 10 |

## Entity Type: zip

| Schema | Table | Columns | Row Count |
|:---|:---|:---|---:|
| housing | acris_parties | zip | 46,200,000 |
| business | nys_entity_addresses | zip | 34,700,000 |
| business | nys_corp_name_history | zip | 22,000,000 |
| social_services | n311_service_requests | incident_zip | 21,000,000 |
| business | nys_corporations | dos_process_zip | 16,700,000 |
| housing | hpd_complaints | post_code | 16,000,000 |
| transportation | parking_violations | zip | 12,000,000 |
| business | acris_pp_parties | zip | 11,000,000 |
| housing | hpd_violations | zip | 11,000,000 |
| housing | property_valuation | zip_code | 10,500,000 |
| housing | dob_complaints | zip_code | 9,200,000 |
| housing | oca_parties | zip | 6,400,000 |
| transportation | nys_vehicle_registrations | zip | 5,000,000 |
| housing | dob_permit_issuance | zip_code | 4,000,000 |
| health | rodent_inspections | zip_code | 3,100,000 |
| housing | dob_application_owners | zip | 2,700,000 |
| housing | dob_violations | zip | 2,500,000 |
| public_safety | motor_vehicle_collisions | zip_code | 2,200,000 |
| health | medicaid_providers | zip_code | 2,100,000 |
| public_safety | ccrb_allegations | zip | 2,100,000 |
| housing | dob_ecb_violations | respondent_zip | 1,800,000 |
| city_government | campaign_contributions | zip | 1,700,000 |
| city_government | civil_service_active | zip | 1,700,000 |
| financial | nys_attorney_registrations | zip | 1,700,000 |
| environment | lead_service_lines | zip_code | 1,000,000 |
| transportation | traffic_volume | zip | 1,000,000 |
| financial | nys_cosmetology_licenses | zip | 867,000 |
| housing | hpd_registration_contacts | businesszip | 804,000 |
| public_safety | ccrb_complaints | zip | 695,000 |
| business | bic_trade_waste | postcode | 677,000 |
| financial | nys_re_brokers | business_zip | 591,000 |
| city_government | campaign_expenditures | zip | 516,000 |
| business | nys_corp_constituents | zip | 500,000 |
| housing | dob_now_build_filings | zip | 500,000 |
| transportation | mta_paratransit_ride | zip | 500,000 |
| transportation | street_permits | zip | 500,000 |
| housing | hpd_jurisdiction | zip | 377,000 |
| housing | evictions | eviction_zip | 374,000 |
| housing | dwelling_registrations | zip | 300,000 |
| health | restaurant_inspections | zipcode | 297,000 |
| financial | nys_procurement_state | vendor_postal_code | 276,000 |
| financial | nys_tax_warrants | zip_code | 275,000 |
| financial | nys_notaries | business_zip_if_available | 238,000 |
| housing | hpd_litigations | zip | 236,000 |
| business | nys_daily_corp_filings | filer_zip5 | 200,000 |
| environment | nys_solar | zip_code | 200,000 |
| health | childcare_inspections | zipcode | 200,000 |
| business | bic_violations | zip | 102,000 |
| business | nys_dos_nonprofits | dos_process_zip | 100,000 |
| city_government | city_record | zip_code | 100,000 |
| financial | nys_child_support_warrants | zip_code | 100,000 |
| financial | nys_procurement_local | vendor_postal_code | 100,000 |
| health | childcare_inspections_current | zipcode | 100,000 |
| housing | dob_safety_facades | qewi_zip | 100,000 |
| public_safety | daily_inmates | zip | 100,000 |
| transportation | mta_paratransit_ridership | zip | 100,000 |
| housing | rolling_sales | zip_code | 79,000 |
| business | issued_licenses | address_zip | 69,000 |
| housing | affordable_housing | postcode | 60,000 |
| city_government | contract_awards | zip | 52,000 |
| housing | fdny_violations | zipcode | 52,000 |
| business | license_applications | zip | 50,000 |
| city_government | cfb_offyear_contributions | zip | 50,000 |
| financial | nys_elevator_licenses | zip | 50,000 |
| financial | nys_re_appraisers | zip_code | 50,000 |
| health | cooling_tower_inspections | zip_code | 50,000 |
| health | drinking_water_tank_inspections | zip_code | 50,000 |
| health | school_cafeteria_inspections | zipcode | 50,000 |
| housing | dob_safety_violations | zip | 50,000 |
| public_safety | ccrb_penalties | zip | 50,000 |
| public_safety | discipline_charges | zip | 50,000 |
| recreation | signs | zipcode | 50,000 |
| recreation | special_events | zip | 50,000 |
| social_services | dhs_daily_report | zip | 50,000 |
| transportation | mta_elevator | zip | 50,000 |
| public_safety | ccrb_officers | zip | 40,000 |
| city_government | cfb_intermediaries | zip | 30,000 |
| environment | ll84_energy_2023 | postal_code | 30,000 |
| health | childcare_programs | zipcode | 30,000 |
| health | drinking_water_tanks | zip | 30,000 |
| housing | emergency_repair_hwo | zip | 30,000 |
| public_safety | discipline_summary | zip | 30,000 |
| recreation | properties | zipcode | 30,000 |
| social_services | nys_child_care | zip_code | 30,000 |
| financial | nys_ida_projects | project_postal_code | 20,000 |
| health | health_facilities | fac_zip | 20,000 |
| health | open_restaurant_inspections | postcode | 20,000 |
| housing | emergency_repair_omo | zip | 20,000 |
| business | nys_contractor_registry | zip_code | 13,000 |
| housing | hpd_repair_vacate | zip | 10,000 |
| recreation | aquatics | zip | 10,000 |
| recreation | summer_sports_2022 | zip | 10,000 |
| social_services | dhs_shelter_census | zip | 10,000 |
| social_services | dycd_program_sites | zipcode | 10,000 |
| business | bic_wholesale_markets | postcode | 5,000 |
| housing | housing_connect_buildings | address_zipcode | 5,000 |
| recreation | canine_waste | zipcode | 5,000 |
| recreation | capital_projects | zip | 5,000 |
| recreation | nys_farmers_markets | zip | 5,000 |
| recreation | permit_areas | zipcode | 5,000 |
| recreation | restrictive_declarations | zipcode | 5,000 |
| social_services | moia_interpretation | zip | 5,000 |
| city_government | covid_emergency_contracts | zip_code | 3,000 |
| housing | housing_connect_lotteries | postcode | 2,000 |
| recreation | active_passive_rec | zipcode | 2,000 |
| recreation | functional_parkland | zipcode | 2,000 |
| business | bic_denied_companies | postcode | 1,000 |
| city_government | coib_legal_defense_trust | zip_code | 1,000 |
| education | capacity_projects | postcode | 1,000 |
| recreation | pools | zip | 1,000 |
| recreation | spray_showers | zip | 1,000 |
| social_services | idnyc_applications | zip | 1,000 |
| social_services | know_your_rights | zip_code | 1,000 |
| social_services | community_gardens | zipcode | 600 |
| housing | aep_buildings | postcode | 500 |
| housing | conh_pilot | zipcode | 500 |
| social_services | community_orgs | postcode | 500 |
| social_services | shop_healthy | zip_code | 500 |
| transportation | mta_stations | zip | 500 |
| social_services | broadband_adoption | zip_code | 200 |
| social_services | benefits_centers | zip_code | 50 |
| social_services | snap_centers | zip_code | 50 |
| social_services | family_justice_centers | zip_code | 10 |

## Entity Type: precinct

| Schema | Table | Columns | Row Count |
|:---|:---|:---|---:|
| social_services | n311_service_requests | police_precinct | 21,000,000 |
| transportation | parking_violations | issuer_precinct, violation_precinct | 12,000,000 |
| public_safety | nypd_complaints_historic | addr_pct_cd | 9,500,000 |
| public_safety | nypd_arrests_historic | arrest_precinct | 6,000,000 |
| public_safety | criminal_court_summons | precinct_of_occur | 5,600,000 |
| public_safety | ccrb_complaints | precinct_of_incident_occurrence | 695,000 |
| public_safety | nypd_arrests_ytd | arrest_precinct | 200,000 |
| public_safety | nypd_complaints_ytd | addr_pct_cd | 200,000 |
| recreation | signs | precinct | 50,000 |
| public_safety | shootings | precinct | 30,000 |
| public_safety | use_of_force_incidents | incident_pct | 30,000 |
| recreation | properties | precinct | 30,000 |
| public_safety | hate_crimes | complaint_precinct_code | 10,000 |
| recreation | canine_waste | precinct | 5,000 |
| recreation | permit_areas | precinct | 5,000 |
| recreation | restrictive_declarations | precinct | 5,000 |
| recreation | functional_parkland | precinct | 2,000 |
| social_services | community_gardens | policeprecinct | 600 |

## Entity Type: camis

| Schema | Table | Columns | Row Count |
|:---|:---|:---|---:|
| health | restaurant_inspections | camis | 297,000 |

## Entity Type: dos_id

| Schema | Table | Columns | Row Count |
|:---|:---|:---|---:|
| business | nys_entity_addresses | corpid_num | 34,700,000 |
| business | nys_corp_name_history | corpid_num | 22,000,000 |
| business | nys_corporations | dos_id | 16,700,000 |
| business | nys_corp_constituents | corpid_num | 500,000 |
| business | nys_daily_corp_filings | dos_id | 200,000 |
| business | nys_dos_nonprofits | dos_id | 100,000 |

## Entity Type: registration_id

| Schema | Table | Columns | Row Count |
|:---|:---|:---|---:|
| housing | hpd_violations | registrationid | 11,000,000 |
| financial | nys_attorney_registrations | registration_number | 1,700,000 |
| housing | hpd_registration_contacts | registrationid | 804,000 |
| housing | hpd_jurisdiction | registrationid | 377,000 |
| housing | dwelling_registrations | registrationid | 300,000 |
| housing | hpd_repair_vacate | registration_id | 10,000 |

## Entity Type: complaint_id

| Schema | Table | Columns | Row Count |
|:---|:---|:---|---:|
| social_services | n311_service_requests | unique_key | 21,000,000 |
| housing | hpd_complaints | complaint_id, unique_key | 16,000,000 |
| housing | hpd_violations | violationid | 11,000,000 |
| public_safety | nypd_complaints_historic | cmplnt_num | 9,500,000 |
| housing | dob_complaints | complaint_number | 9,200,000 |
| public_safety | ccrb_allegations | complaint_id | 2,100,000 |
| public_safety | ccrb_complaints | complaint_id | 695,000 |
| public_safety | nypd_complaints_ytd | cmplnt_num | 200,000 |
| public_safety | ccrb_penalties | complaint_id | 50,000 |

## Entity Type: document_id

| Schema | Table | Columns | Row Count |
|:---|:---|:---|---:|
| housing | acris_parties | document_id | 46,200,000 |
| housing | acris_legals | document_id | 22,500,000 |
| housing | acris_master | document_id | 16,900,000 |
| business | acris_pp_parties | document_id | 11,000,000 |
| housing | acris_pp_master | document_id | 4,500,000 |
| housing | acris_pp_legals | document_id | 2,000,000 |
| housing | acris_pp_references | document_id | 1,000,000 |

## CORRECTIONS — Tables Misclassified as "No Entity" Due to Query Truncation

The following tables were not in the initial column metadata query results (query hit row limit). Manual inspection reveals they are **entity-rich** and should be reclassified:

| Schema | Table | Row Count | Entity Columns | Entity Types |
|:---|:---|---:|:---|:---|
| city_government | oath_hearings | 21,600,000 | respondent_first_name, respondent_last_name, bbl, bin, borough+block+lot, violation_location_street_name, violation_location_zip_code, respondent_address_* | person_name, bbl, bin, address, zip (7 types) |
| city_government | pluto | 859,000 | ownername, bbl, address, zipcode, policeprct, borough+block+lot | org_name, bbl, address, zip, precinct (5 types) |
| city_government | citywide_payroll | 6,800,000 | first_name, last_name, mid_init, agency_name, work_location_borough | person_name, org_name (2 types) |
| city_government | marriage_licenses_1950_2017 | 4,800,000 | GROOM_FIRST_NAME, GROOM_SURNAME, BRIDE_FIRST_NAME, BRIDE_SURNAME | person_name (SEPARATE pattern, 2 pairs) |
| city_government | marriage_certificates_1866_1937 | 5,300,000 | (likely groom/bride names — same pattern as 1950 licenses) | person_name |
| city_government | death_certificates_1862_1948 | 3,700,000 | (likely decedent name, age, cause) | person_name |
| city_government | birth_certificates_1855_1909 | 2,600,000 | (likely child/parent names) | person_name |
| city_government | civil_list | 3,200,000 | (likely employee first/last name, agency) | person_name, org_name |
| city_government | nys_campaign_expenditures | 4,200,000 | (likely payee name, address, amount) | person_name, org_name, address |
| city_government | nys_lobbyist_registration | 17,300,000 | (likely lobbyist name, client name, address) | person_name, org_name, address |
| city_government | doing_business_people | 61,000 | (likely person name, employer, address) | person_name, org_name |

**Impact on summary**: Adding these tables increases:
- person_name: +12 tables, +~48M rows → **63 tables, ~185M rows**
- org_name: +6 tables, +~32M rows → **59 tables, ~116M rows**
- bbl: +1 table (oath_hearings) +21.6M rows → **107 tables, ~202M rows**
- address: +5 tables, +~50M rows → **91 tables, ~308M rows**
- bin: +1 table (oath_hearings) +21.6M rows → **103 tables, ~176M rows**
- zip: +3 tables, +~33M rows → **126 tables, ~294M rows**

**oath_hearings (21.6M rows) is the single most entity-dense table in the lake** — 7 entity types in one table. It is currently used by `enforcement_web` and `entity_xray` but NOT in any DuckPGQ graph. This should be a Phase 3-4 priority.

---

## Tables with NO Entity Columns

| Schema | Table | Row Count |
|:---|:---|---:|
| city_government | oath_hearings | 21,600,000 |
| city_government | nys_lobbyist_registration | 17,300,000 |
| city_government | marriage_certificates | 5,300,000 |
| city_government | marriage_licenses_1950 | 4,800,000 |
| city_government | nys_campaign_expenditures | 4,200,000 |
| city_government | civil_list | 3,200,000 |
| health | sparcs_discharges_2020 | 2,000,000 |
| city_government | pluto | 859,000 |
| education | attendance_2015 | 500,000 |
| education | attendance_2018 | 500,000 |
| environment | hyperlocal_temp | 500,000 |
| public_safety | nys_crash_vehicles | 500,000 |
| public_safety | nys_crashes | 500,000 |
| environment | ll84_energy_monthly | 300,000 |
| public_safety | nyc_claims_report | 200,000 |
| education | chronic_absenteeism | 100,000 |
| education | ela_results | 100,000 |
| education | math_results | 100,000 |
| federal | cl_financial_disclosures | 100,000 |
| transportation | ferry_ridership | 100,000 |
| transportation | pedestrian_ramps | 100,000 |
| city_government | doing_business_people | 61,000 |
| recreation | baby_names | 60,000 |
| business | nys_liquor_authority | 59,000 |
| education | class_size | 50,000 |
| education | discharge_reporting | 50,000 |
| environment | dsny_tonnage | 50,000 |
| federal | cl_educations | 50,000 |
| health | cdc_places | 50,000 |
| health | ed_flu_visits | 50,000 |
| recreation | pool_sessions | 50,000 |
| transportation | pavement_rating | 50,000 |
| federal | cl_gifts | 30,000 |
| business | sbs_certified | 23,000 |
| environment | air_quality | 20,000 |
| federal | cl_non_investment_income | 20,000 |
| public_safety | use_of_force_officers | 20,000 |
| environment | beach_water_samples | 10,000 |
| federal | cl_agreements | 10,000 |
| recreation | lap_swimming | 10,000 |
| social_services | n311_interpreter_wait | 10,000 |
| transportation | mta_incidents | 10,000 |
| city_government | cfb_enforcement_audits | 5,000 |
| city_government | civil_service_titles | 5,000 |
| education | demographics_2013 | 5,000 |
| education | demographics_2018 | 5,000 |
| education | demographics_2019 | 5,000 |
| education | demographics_2020 | 5,000 |
| environment | asthma_ed | 5,000 |
| environment | community_health_survey | 5,000 |
| environment | lead_children | 5,000 |
| federal | acs_demographics | 5,000 |
| federal | cl_debts | 5,000 |
| federal | cl_nypd_cases_edny | 5,000 |
| federal | cl_nypd_cases_sdny | 5,000 |
| health | hiv_aids_annual | 5,000 |
| health | hiv_aids_by_neighborhood | 5,000 |
| recreation | drinking_fountains | 5,000 |
| recreation | play_areas | 5,000 |
| recreation | summer_sports_2017 | 5,000 |
| social_services | lep_population | 5,000 |
| transportation | mta_entrances | 5,000 |
| city_government | coib_enforcement | 2,000 |
| education | quality_early_childhood | 2,000 |
| education | quality_elem_middle | 2,000 |
| environment | flood_vulnerability | 2,000 |
| health | covid_outcomes | 2,000 |
| transportation | mta_daily_ridership | 2,000 |
| health | leading_causes_of_death | 1,000 |
| social_services | farmers_markets | 1,000 |
| federal | bls_unemployment_bronx | 500 |
| federal | bls_unemployment_brooklyn | 500 |
| federal | bls_unemployment_manhattan | 500 |
| federal | bls_unemployment_queens | 500 |
| federal | bls_unemployment_staten_island | 500 |
| federal | cl_courts | 500 |
| recreation | synthetic_turf | 500 |
| social_services | literacy_programs | 500 |
| social_services | pregnancy_mortality | 500 |
| transportation | mta_paratransit_max | 500 |
| environment | heat_vulnerability | 200 |
| health | covid_by_zip | 200 |
| recreation | fishing_sites | 200 |
| social_services | snap_access_index | 20 |
