import os
import re
import pandas as pd
import yaml

def _normcols(df):
    df.columns = [str(c).strip() for c in df.columns]
    return df

def _first_present(df, candidates):
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        for col in cols:
            if col == cand.lower():
                return cols[col]
    for cand in candidates:
        for col in cols:
            if cand.lower() in col:
                return cols[col]
    return None

def _coerce_sheet(df, colmap):
    df = _normcols(df)
    out = pd.DataFrame()
    def pick(key, default=None):
        col = _first_present(df, colmap.get(key, []))
        return df[col] if col is not None else default

    out["Code"] = pick("code").astype(str).str.strip()
    out["Name"] = pick("name", pd.Series([None]*len(df))).astype(str).str.strip()
    alt = pick("alternatename", pd.Series([None]*len(df)))
    out["AlternateName"] = alt.astype(str).str.strip() if alt is not None else None
    stat = pick("status", pd.Series([None]*len(df)))
    out["Status"] = stat.astype(str).str.strip() if stat is not None else None

    out = out.dropna(subset=["Code"]).drop_duplicates(subset=["Code"])
    return out

def _load_xlsx(path, wanted_sheets):
    xls = pd.ExcelFile(path)
    if wanted_sheets:
        use = [s for s in xls.sheet_names if s in wanted_sheets]
    else:
        use = xls.sheet_names
    frames = [pd.read_excel(path, sheet_name=s) for s in use]
    return list(zip(use, frames))

def build_geo_lookup(cfg_path="config.yaml"):
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    paths = cfg["paths"]
    geo_cfg = cfg["geo_lookup"]
    os.makedirs(os.path.dirname(geo_cfg["out_csv"]), exist_ok=True)

    colmap = geo_cfg.get("column_map", {})
    overrides = geo_cfg.get("per_sheet_overrides", {})

    outputs = []

    for sheet, df in _load_xlsx(paths["rgc_ewni_xlsx"], geo_cfg.get("ewni_sheets", [])):
        use_map = overrides.get(sheet, colmap)
        outputs.append(_coerce_sheet(df, use_map))

    for sheet, df in _load_xlsx(paths["rgc_scot_xlsx"], geo_cfg.get("scotland_sheets", [])):
        use_map = overrides.get(sheet, colmap)
        outputs.append(_coerce_sheet(df, use_map))

    out = pd.concat(outputs, ignore_index=True).drop_duplicates(subset=["Code"])
    out.to_csv(geo_cfg["out_csv"], index=False, encoding="utf-8")
    try:
        out.to_parquet(geo_cfg["out_parquet"], index=False)
    except Exception as e:
        print("Parquet write skipped:", e)

    print(f"Geo lookup written: {geo_cfg['out_csv']}  (rows={len(out)})")

if __name__ == "__main__":
    build_geo_lookup()
