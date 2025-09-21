import subprocess, sys

for script in ("build_geo_lookups.py", "build_postcode_lookup.py"):
    print("Running:", script)
    rc = subprocess.call([sys.executable, script])
    if rc != 0:
        raise SystemExit(rc)
print("All done.")
