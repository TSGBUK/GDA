# Unify filenames, because clearly producing the same format over
# and over again is beyond NESO.

# The data source for this is https://www.neso.energy/data-portal/system-frequency-data

import os
import re
from pathlib import Path

ROOT = next(p for p in Path(__file__).resolve().parents if p.name == "GDA")
DATA_DIR = ROOT / "DataSources" / "NESO" / "Frequency"

# Regex patterns to catch different formats
patterns = [
    re.compile(r"f[\s_-]?(\d{4})[\s_-]?(\d{1,2})", re.IGNORECASE),   # f 2019 1.csv / f-2014-10.csv
    re.compile(r"fnew[\s_-]?(\d{4})[\s_-]?(\d{1,2})", re.IGNORECASE), # fNew 2020 1.csv / fnew-2022-10.csv
]

def unify_filenames():
    for fname in os.listdir(DATA_DIR):
        if not fname.lower().endswith(".csv"):
            continue

        year, month = None, None
        for pat in patterns:
            m = pat.search(fname)
            if m:
                year, month = m.group(1), m.group(2)
                break

        if year and month:
            new_name = f"f-{int(year)}-{int(month)}.csv"  # normalize month to no leading zero
            old_path = DATA_DIR / fname
            new_path = DATA_DIR / new_name
            
            if old_path != new_path:
                print(f"Renaming: {fname} -> {new_name}")
                os.rename(old_path, new_path)
        else:
            print(f"Skipping (no match): {fname}")

if __name__ == "__main__":
    unify_filenames()
