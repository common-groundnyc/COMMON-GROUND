"""HUD (Housing and Urban Development) — ArcGIS Feature Service config for NY state."""

# ArcGIS Feature Services for HUD geospatial data
ARCGIS_BASE = "https://services.arcgis.com/VTyQ9soqVukalItT/arcgis/rest/services"
ARCGIS_DATASETS = [
    ("hud_lihtc", "LIHTC", 0, "FIPS_ST='36'"),
    ("hud_housing_choice_vouchers", "Housing_Choice_Vouchers_by_Tract", 0, "STATE='36'"),
    ("hud_public_housing_developments", "Public_Housing_Developments", 0, "STD_ST='NY'"),
    ("hud_public_housing_buildings", "Public_Housing_Buildings", 0, "STD_ST='NY'"),
    ("hud_multifamily_properties", "MULTIFAMILY_PROPERTIES_ASSISTED", 0, "PROPERTY_STATE_CODE='NY'"),
]
ARCGIS_PAGE_SIZE = 2000
