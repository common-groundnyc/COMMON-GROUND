import pytest
from dagster_pipeline.defs.address_lookup_asset import parse_adr_row, parse_adr_csv


def test_parse_adr_row_basic():
    """Standard Brooklyn address row."""
    row = {
        "boro": "3",
        "block": "00123",
        "lot": "0045",
        "bin": "3012345",
        "lhnd": "200",
        "hhnd": "210",
        "stname": "ATLANTIC AVENUE             ",
        "b10sc": "3004001010",
        "zipcode": "11217",
        "addrtype": "",
    }
    result = parse_adr_row(row)
    assert result["bbl"] == "3001230045"
    assert result["bin"] == "3012345"
    assert result["house_number"] == 200
    assert result["house_number_high"] == 210
    assert result["street_std"] == "ATLANTIC AVENUE"
    assert result["street_code"] == "3004001010"
    assert result["boro_code"] == "3"
    assert result["zipcode"] == "11217"
    assert result["addr_type"] == ""
    assert result["address_std"] == "200 ATLANTIC AVENUE"


def test_parse_adr_row_vanity():
    """Vanity address (addr_type='V') should be included."""
    row = {
        "boro": "1",
        "block": "00077",
        "lot": "0001",
        "bin": "1001389",
        "lhnd": "1",
        "hhnd": "1",
        "stname": "WALL STREET                 ",
        "b10sc": "1008501010",
        "zipcode": "10005",
        "addrtype": "V",
    }
    result = parse_adr_row(row)
    assert result["bbl"] == "1000770001"
    assert result["addr_type"] == "V"
    assert result["address_std"] == "1 WALL STREET"


def test_parse_adr_row_pseudo_excluded():
    """Pseudo address (addr_type='Q') should return None."""
    row = {
        "boro": "1",
        "block": "00001",
        "lot": "0001",
        "bin": "1000000",
        "lhnd": "     ",
        "hhnd": "     ",
        "stname": "BROADWAY                    ",
        "b10sc": "1000401010",
        "zipcode": "10001",
        "addrtype": "Q",
    }
    result = parse_adr_row(row)
    assert result is None


def test_parse_adr_row_no_house_number():
    """Row with blank house number should return None."""
    row = {
        "boro": "2",
        "block": "00500",
        "lot": "0001",
        "bin": "2000000",
        "lhnd": "     ",
        "hhnd": "     ",
        "stname": "GRAND CONCOURSE             ",
        "b10sc": "2002001010",
        "zipcode": "10451",
        "addrtype": "",
    }
    result = parse_adr_row(row)
    assert result is None


def test_parse_adr_row_nap_excluded():
    """NAP address (addr_type='N') should return None."""
    row = {
        "boro": "4",
        "block": "00100",
        "lot": "0001",
        "bin": "4000001",
        "lhnd": "100",
        "hhnd": "100",
        "stname": "QUEENS BOULEVARD            ",
        "b10sc": "4005001010",
        "zipcode": "11101",
        "addrtype": "N",
    }
    result = parse_adr_row(row)
    assert result is None


def test_parse_adr_csv_filters_and_parses(tmp_path):
    """Integration test: parse a small CSV, verify filtering and output."""
    csv_content = (
        "boro,block,lot,bblscc,bin,lhnd,lhns,lcontpar,lsos,hhnd,hhns,hcontpar,"
        "scboro,sc5,sclgc,stname,addrtype,realb7sc,validlgcs,parity,b10sc,"
        "segid,zipcode\n"
        "1,00077,0001,0,1001389,1,    1,,O,1,    1,,1,00850,10,"
        "WALL STREET                 ,,       ,01  ,E,1008501010,0071780,10005\n"
        "1,00077,0001,0,1001389,68,   68,,O,68,   68,,1,00040,10,"
        "BROADWAY                    ,V,1008501,01  ,E,1000401010,0071780,10005\n"
        "3,00123,0045,0,3012345,,     ,,O,,     ,,3,00400,10,"
        "ATLANTIC AVENUE             ,Q,       ,01  ,E,3004001010,0012345,11217\n"
    )
    csv_file = tmp_path / "bobaadr.txt"
    csv_file.write_text(csv_content)

    out_file = tmp_path / "adr_clean.csv"
    row_count = parse_adr_csv(str(csv_file), str(out_file))

    # Real (1 WALL ST) + Vanity (68 BROADWAY) = 2 rows, pseudo excluded
    assert row_count == 2
    assert out_file.exists()

    import csv as _csv
    with open(out_file, newline="", encoding="utf-8") as f:
        rows = list(_csv.DictReader(f))

    assert len(rows) == 2
    assert rows[0]["address_std"] == "1 WALL STREET"
    assert rows[1]["address_std"] == "68 BROADWAY"
    assert rows[1]["addr_type"] == "V"
