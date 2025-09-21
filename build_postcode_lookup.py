import os
import pandas as pd
import yaml

def _read_csv_pandas(path, usecols=None):
    return pd.read_csv(path, usecols=usecols, dtype=str, encoding="utf-8", low_memory=False)

def _read_csv_duckdb(path, usecols=None):
    import duckdb
    con = duckdb.connect()
    cols = "*" if not usecols else ", ".join([f'"{c}"' for c in usecols])
    df = con.execute(f'SELECT {cols} FROM read_csv_auto(?, sample_size=-1, header=True)', [path]).df()
    for c in df.columns:
        df[c] = df[c].astype(str)
    return df

def build_postcode_lookup(cfg_path="config.yaml"):
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    paths = cfg["paths"]
    pcfg = cfg["postcode_lookup"]
    os.makedirs(os.path.dirname(pcfg["out_csv"]), exist_ok=True)

    usecols = pcfg.get("keep_columns", [])
    if pcfg.get("use_duckdb", False):
        df = _read_csv_duckdb(paths["nspl_csv"], usecols=usecols)
    else:
        df = _read_csv_pandas(paths["nspl_csv"], usecols=usecols)

    join_codes = pcfg.get("join_name_for_codes", [])
    if join_codes:
        geo = pd.read_csv(cfg["geo_lookup"]["out_csv"], dtype=str, encoding="utf-8")
        code_to_name = dict(zip(geo["Code"], geo["Name"]))
        for col in join_codes:
            if col in df.columns:
                df[f"{col}_name"] = df[col].map(code_to_name)

    df = df.rename(columns=pcfg.get("rename_final", {}))

    df.to_csv(pcfg["out_csv"], index=False, encoding="utf-8")
    try:
        df.to_parquet(pcfg["out_parquet"], index=False)
    except Exception as e:
        print("Parquet write skipped:", e)

    print(f"Postcode lookup written: {pcfg['out_csv']}  (rows={len(df)})")

if __name__ == "__main__":
    build_postcode_lookup()
