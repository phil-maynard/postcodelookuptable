# build_geo_lookups.py
#
# Build a unified GeoAreaLookup from the EW/NI and Scotland register workbooks.
# Configurable via config.yaml.

import os
import re
import pandas as pd
import yaml


def _normcols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _first_present(df: pd.DataFrame, candidates) -> str:
    """Return the first matching column name, or None."""
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates or []:
        key = str(cand).lower()
        if key in cols:
            return cols[key]
        # relaxed contains
        for col in df.columns:
            if key in col.lower():
                return col
    return None


def _coerce_sheet(df: pd.DataFrame, colmap) -> pd.DataFrame:
    """Standardise to Code, Name, AlternateName, Status."""
    df = _normcols(df.copy())
    out = pd.DataFrame()

    code_col = _first_present(df, colmap.get("code", []))
    name_col = _first_present(df, colmap.get("name", []))
    alt_col = _first_present(df, colmap.get("alternatename", []))
    status_col = _first_present(df, colmap.get("status", []))

    out["Code"] = df[code_col].astype(str).str.strip() if code_col else ""
    out["Name"] = df[name_col].astype(str).str.strip() if name_col else ""
    out["AlternateName"] = (
        df[alt_col].astype(str).str.strip() if alt_col else ""
    )
    out["Status"] = df[status_col].astype(str).str.strip() if status_col else ""

    # Drop blank codes and duplicates
    out = out[out["Code"] != ""].drop_duplicates(subset=["Code"])
    return out


def _load_sheets(path: str, keep_pattern: str):
    xls = pd.ExcelFile(path)
    if keep_pattern:
        rx = re.compile(keep_pattern)
        names = [s for s in xls.sheet_names if rx.match(s.strip())]
    else:
        names = xls.sheet_names
    return [(s, pd.read_excel(path, sheet_name=s)) for s in names]


def build_geo_lookup(cfg_path="config.yaml"):
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    paths = cfg["paths"]
    geo_cfg = cfg["geo_lookup"]

    out_csv = geo_cfg["out_csv"]
    out_parquet = geo_cfg.get("out_parquet")
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)

    colmap = geo_cfg.get("column_map", {})
    pattern = geo_cfg.get("sheet_name_pattern")

    outputs = []

    for sheet, df in _load_sheets(paths["rgc_ewni_xlsx"], pattern):
        print(f"Processing EW/NI sheet: {sheet}")
        outputs.append(_coerce_sheet(df, colmap))

    for sheet, df in _load_sheets(paths["rgc_scot_xlsx"], pattern):
        print(f"Processing Scotland sheet: {sheet}")
        outputs.append(_coerce_sheet(df, colmap))

    out = pd.concat(outputs, ignore_index=True).drop_duplicates(subset=["Code"])
    out.to_csv(out_csv, index=False, encoding="utf-8")
    print(f"Geo lookup written: {out_csv} (rows={len(out)})")

    if out_parquet:
        try:
            out.to_parquet(out_parquet, index=False)
            print(f"Parquet written: {out_parquet}")
        except Exception as e:
            print("Parquet write skipped:", e)

    return out


if __name__ == "__main__":
    build_geo_lookup()
