# NSPL + Geographic Code Lookup Pipeline

This starter kit recreates (and tidies) your process to build:
1. A **Geographic Area Lookup** (`Code, Name, AlternateName, Status`).
2. A **Postcode Lookup** built from the **NSPL** (National Statistics Postcode Lookup),
   enriched with readable geographic names via (1).

It also documents how to **update** the tables each time a new NSPL / RGC release drops.

---

## Directory layout

/your-project
  config.yaml
  build_geo_lookups.py
  build_postcode_lookup.py
  run_all.py
  /sql
    create_tables.sql
  /data
    nspl.csv              # NSPL extract (CSV) – you place this here
    rgc_ewni.xlsx         # Registers of Geographic Codes (England/Wales/NI)
    rgc_scotland.xlsx     # Registers of Geographic Codes (Scotland)
  /out
    GeoAreaLookup.csv
    GeoAreaLookup.parquet
    PostcodeLookup.csv
    PostcodeLookup.parquet

> You can use CSV **or** Parquet outputs depending on downstream tooling.

---

## Quick start

1. Place the latest **NSPL** CSV and the two **Register of Geographic Codes** files in `./data/`.
2. Edit `config.yaml` to point at your filenames and confirm column mappings.
3. (Optional) `pip install pandas duckdb pyarrow openpyxl`
4. Run:
   ```bash
   python build_geo_lookups.py
   python build_postcode_lookup.py
   # or
   python run_all.py
