# build_geo_lookups.py (verbose)
# Simplified, sheet-pattern driven, with progress output.

import os
import re
import time
import pandas as pd
import yaml


def _normcols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _first_present(df: pd.DataFrame, candidates) -> str | None:
    """Return the first matching column name (exact/contains), else None."""
    if not candidates:
        return None
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        key = str(cand).lower()
        if key in cols:
            return cols[key]
        for col in df.columns:
            if key in col.lower():
                return col
    return None


def _coerce_sheet(df: pd.DataFrame, colmap) -> pd.DataFrame:
    """Standardise to Code, Name, AlternateName, Status. Fill blanks if missing."""
    df = _normcols(df.copy())
    out = pd.DataFrame()

    code_col   = _first_present(df, colmap.get("code", []))
    name_col   = _first_present(df, colmap.get("name", []))
    alt_col    = _first_present(df, colmap.get("alternatename", []))
    status_col = _first_present(df, colmap.get("status", []))

    out["Code"]          = df[code_col].astype(str).str.strip() if code_col else ""
    out["Name"]          = df[name_col].astype(str).str.strip() if name_col else ""
    out["AlternateName"] = df[alt_col].astype(str).str.strip() if alt_col else ""
    out["Status"]        = df[status_col].astype(str).str.strip() if status_col else ""

    # Drop blank codes and duplicates
    out = out[out["Code"] != ""].drop_duplicates(subset=["Code"])
    return out


def _list_matching_sheets(xlsx_path: str, pattern: str | None) -> list[str]:
    xls = pd.ExcelFile(xlsx_path)
    if pattern:
        rx = re.compile(pattern)
        names = [s for s in xls.sheet_names if rx.match(s.strip())]
    else:
        names = xls.sheet_names
    return names


def _read_sheet(xlsx_path: str, sheet_name: str) -> pd.DataFrame:
    # Using openpyxl implicitly; fine for most cases.
    return pd.read_excel(xlsx_path, sheet_name=sheet_name)


def build_geo_lookup(cfg_path: str = "config.yaml") -> pd.DataFrame:
    t0 = time.perf_counter()

    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    paths   = cfg["paths"]
    geo_cfg = cfg["geo_lookup"]

    out_csv     = geo_cfg["out_csv"]
    out_parquet = geo_cfg.get("out_parquet")
    pattern     = geo_cfg.get("sheet_name_pattern")
    colmap      = geo_cfg.get("column_map", {})
    verbose     = bool(geo_cfg.get("verbose", True))  # default noisy to reassure users

    os.makedirs(os.path.dirname(out_csv), exist_ok=True)

    ewni_path = paths["rgc_ewni_xlsx"]
    scot_path = paths["rgc_scot_xlsx"]

    outputs = []

    def process_book(label: str, path: str):
        if verbose:
            print(f"\n==> {label}: {path}")
        matched = _list_matching_sheets(path, pattern)
        if verbose:
            print(f"    Sheets matched by pattern {pattern!r}: {len(matched)}")
            if len(matched) == 0:
                print("    (No sheets matched — check sheet_name_pattern or workbook)")
        for i, s in enumerate(matched, start=1):
            st = time.perf_counter()
            try:
                if verbose:
                    print(f"    [{i}/{len(matched)}] Reading: {s} ...", end="", flush=True)
                df = _read_sheet(path, s)
                coerced = _coerce_sheet(df, colmap)
                outputs.append(coerced)
                if verbose:
                    print(f" {len(coerced):,} rows in {time.perf_counter() - st:0.2f}s")
            except Exception as e:
                # Continue past a bad/reference sheet without killing the run
                print(f"\n    !! Skipping sheet '{s}' due to error: {type(e).__name__}: {e}")

    process_book("EW/NI workbook", ewni_path)
    process_book("Scotland workbook", scot_path)

    if len(outputs) == 0:
        raise RuntimeError("No data produced. Check file paths, sheet pattern, and column_map.")

    combined = pd.concat(outputs, ignore_index=True).drop_duplicates(subset=["Code"])

    combined.to_csv(out_csv, index=False, encoding="utf-8")
    if verbose:
        print(f"\n✅ Geo lookup written: {out_csv}  (rows={len(combined):,})")

    if out_parquet:
        try:
            combined.to_parquet(out_parquet, index=False)
            if verbose:
                print(f"✅ Parquet written: {out_parquet}")
        except Exception as e:
            print(f"Parquet write skipped ({type(e).__name__}: {e})")

    if verbose:
        print(f"Done in {time.perf_counter() - t0:0.2f}s")

    return combined


if __name__ == "__main__":
    build_geo_lookup()
