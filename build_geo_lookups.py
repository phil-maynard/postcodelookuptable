# build_geo_lookups.py
#
# Builds a unified GeoAreaLookup from two workbooks:
#  - England/Wales/Northern Ireland (EW/NI)
#  - Scotland
#
# Config (config.yaml):
# paths:
#   rgc_ewni_xlsx: "./data/rgc_ewni.xlsx"
#   rgc_scot_xlsx: "./data/rgc_scotland.xlsx"
# geo_lookup:
#   out_csv: "./out/GeoAreaLookup.csv"
#   out_parquet: "./out/GeoAreaLookup.parquet"
#   ewni_sheets: []            # optional explicit lists; empty = scan all
#   scotland_sheets: []
#   sheet_name_pattern: "^[A-Z][0-9]{2}"   # optional regex filter (e.g. E08, S12)
#   column_map:
#     code: ["Code", "GEOGCD", "GSS_CODE", "CODE", "LAD24CD", "WD24CD"]
#     name: ["Name", "GEOGNM", "GEOGNMK", "GEOGNAME", "NAME", "LAD24NM", "WD24NM"]
#     alternatename: ["Alt Name", "Alternate Name", "GEOGNMK", "ALTNAME", "LAD24NMW", "WD24NMW"]
#     status: ["Status", "STAT", "GEOGSTAT", "ACTIVE", "STATUS"]
#   per_sheet_overrides:
#     "Local Authorities":
#       code: ["LAD24CD"]
#       name: ["LAD24NM"]
#       alternatename: ["LAD24NMW"]
#       status: ["STATUS"]
#
# Usage:
#   python build_geo_lookups.py

import os
import re
import sys
import traceback
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
import yaml


def _log(msg: str) -> None:
    print(msg, flush=True)


def _normcols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _first_present(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    """
    Return the *actual* column name in df that matches one of the candidate names.
    Exact case-insensitive match first; then relaxed 'contains' match.
    """
    if df is None or df.empty:
        return None
    colmap = {c.lower(): c for c in df.columns}
    # exact match
    for cand in candidates or []:
        key = str(cand).lower()
        if key in colmap:
            return colmap[key]
    # relaxed contains match
    for cand in candidates or []:
        key = str(cand).lower()
        for c in df.columns:
            if key and key in c.lower():
                return c
    return None


def _has_required_columns(df: pd.DataFrame, colmap: Dict[str, List[str]]) -> bool:
    """
    Minimal requirement: a resolvable 'code' column.
    """
    df = _normcols(df.copy())
    return _first_present(df, colmap.get("code", [])) is not None


def _coerce_sheet(df: pd.DataFrame, colmap: Dict[str, List[str]]) -> pd.DataFrame:
    """
    Standardise a sheet to the canonical columns:
      Code, Name, AlternateName, Status
    Missing Name/AlternateName/Status are filled with NA.
    """
    df = _normcols(df.copy())

    code_col = _first_present(df, colmap.get("code", []))
    name_col = _first_present(df, colmap.get("name", []))
    alt_col = _first_present(df, colmap.get("alternatename", []))
    status_col = _first_present(df, colmap.get("status", []))

    out = pd.DataFrame()

    if code_col is None:
        # Caller should have checked with _has_required_columns; fail-safe skip.
        return out

    out["Code"] = df[code_col].astype(str).str.strip()

    if name_col is not None:
        out["Name"] = df[name_col].astype(str).str.strip()
    else:
        out["Name"] = pd.NA

    if alt_col is not None:
        out["AlternateName"] = df[alt_col].astype(str).str.strip()
    else:
        out["AlternateName"] = pd.NA

    if status_col is not None:
        out["Status"] = df[status_col].astype(str).str.strip()
    else:
        out["Status"] = pd.NA

    # Basic cleaning
    out = out.dropna(subset=["Code"])
    out = out[out["Code"].str.len() > 0]
    out = out.drop_duplicates(subset=["Code"])

    return out


def _load_xlsx(path: str) -> Tuple[List[str], Dict[str, pd.DataFrame]]:
    """
    Load all sheets from an Excel workbook.
    """
    xls = pd.ExcelFile(path)
    names = xls.sheet_names
    frames = {s: pd.read_excel(path, sheet_name=s) for s in names}
    return names, frames


def _filter_sheet_names(
    all_names: List[str],
    explicit_list: Optional[List[str]],
    pattern: Optional[str],
) -> List[str]:
    """
    Decide which sheet names to process:
      - if explicit_list provided and non-empty: take intersection (preserve order)
      - else: apply regex pattern (if provided), else keep all
    """
    if explicit_list:
        keep = [n for n in all_names if n in explicit_list]
        return keep
    if pattern:
        rx = re.compile(pattern)
        return [n for n in all_names if rx.match(n.strip() or "")]
    return list(all_names)


def build_geo_lookup(cfg_path: str = "config.yaml") -> pd.DataFrame:
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    paths = cfg["paths"]
    geo_cfg = cfg["geo_lookup"]

    out_csv = geo_cfg["out_csv"]
    out_parquet = geo_cfg.get("out_parquet")

    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    if out_parquet:
        os.makedirs(os.path.dirname(out_parquet), exist_ok=True)

    ewni_path = paths["rgc_ewni_xlsx"]
    scot_path = paths["rgc_scot_xlsx"]

    ewni_sheets = geo_cfg.get("ewni_sheets", []) or []
    scot_sheets = geo_cfg.get("scotland_sheets", []) or []
    name_pattern = geo_cfg.get("sheet_name_pattern")  # e.g. "^[A-Z][0-9]{2}"

    base_colmap = geo_cfg.get("column_map", {}) or {}
    overrides = geo_cfg.get("per_sheet_overrides", {}) or {}

    outputs: List[pd.DataFrame] = []

    # --- EW/NI ---
    _log(f"Reading workbook (EW/NI): {ewni_path}")
    ewni_names, ewni_frames = _load_xlsx(ewni_path)
    ewni_keep = _filter_sheet_names(ewni_names, ewni_sheets, name_pattern)

    for sheet in ewni_keep:
        df = ewni_frames[sheet]
        use_map = overrides.get(sheet, base_colmap)
        if not _has_required_columns(df, use_map):
            _log(f"  - Skipping (no code column): {sheet}")
            continue
        _log(f"  + Processing sheet: {sheet}")
        coerced = _coerce_sheet(df, use_map)
        if not coerced.empty:
            outputs.append(coerced)

    # --- Scotland ---
    _log(f"Reading workbook (Scotland): {scot_path}")
    scot_names, scot_frames = _load_xlsx(scot_path)
    scot_keep = _filter_sheet_names(scot_names, scot_sheets, name_pattern)

    for sheet in scot_keep:
        df = scot_frames[sheet]
        use_map = overrides.get(sheet, base_colmap)
        if not _has_required_columns(df, use_map):
            _log(f"  - Skipping (no code column): {sheet}")
            continue
        _log(f"  + Processing sheet: {sheet}")
        coerced = _coerce_sheet(df, use_map)
        if not coerced.empty:
            outputs.append(coerced)

    if not outputs:
        raise RuntimeError("No qualifying sheets found. Check sheet_name_pattern, ewni_sheets/scotland_sheets, or column_map.")

    out = pd.concat(outputs, ignore_index=True)
    out = out.drop_duplicates(subset=["Code"])

    # Write CSV
    out.to_csv(out_csv, index=False, encoding="utf-8")
    _log(f"Geo lookup written: {out_csv}  (rows={len(out)})")

    # Write Parquet (optional)
    if out_parquet:
        try:
            out.to_parquet(out_parquet, index=False)
            _log(f"Parquet written: {out_parquet}")
        except Exception as e:
            _log(f"Parquet write skipped ({type(e).__name__}: {e})")

    return out


if __name__ == "__main__":
    try:
        build_geo_lookup()
    except Exception as ex:
        _log("ERROR: build failed.")
        traceback.print_exc()
        sys.exit(1)
