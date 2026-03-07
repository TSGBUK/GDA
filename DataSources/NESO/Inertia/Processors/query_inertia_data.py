import pandas as pd
import pyarrow.dataset as ds
import os
import gc
from pathlib import Path

# --- Constants --------------------------------------------------------------
ROOT = next(p for p in Path(__file__).resolve().parents if p.name == "GDA")
PARQUET_DIR = ROOT / "DataSources" / "NESO" / "Inertia" / "Parquet"
F0_HZ = 50.0               # Nominal system frequency
GRID_CAPACITY_GW = 50.0    # Assumed UK system size


# --- Data Loader ------------------------------------------------------------
def list_years():
    years = []
    for name in os.listdir(PARQUET_DIR):
        if name.startswith("year="):
            years.append(int(name.split("=")[1]))
    return sorted(years)


def load_data(year=None):
    dataset = ds.dataset(PARQUET_DIR, format="parquet", partitioning="hive")
    if year is not None:
        table = dataset.to_table(filter=(ds.field("year") == int(year)))
    else:
        table = dataset.to_table()
    return table.to_pandas()


# --- Conversions ------------------------------------------------------------
def inertia_to_units(gvas):
    """Return inertia expressed in multiple units."""
    if pd.isna(gvas):
        return {"GVAs": None, "MWs": None, "MW_equiv": None, "PctCapacity": None}

    # 1 GVA·s = 1000 MW·s
    mw_s = gvas * 1000.0

    # MW imbalance for 1 Hz/s RoCoF
    mw_equiv = (2 * mw_s / F0_HZ)

    # Fraction of system capacity
    pct_capacity = 100.0 * mw_equiv / (GRID_CAPACITY_GW * 1000)

    return {
        "GVAs": round(gvas, 2),
        "MWs": round(mw_s, 0),
        "MW_equiv": round(mw_equiv, 0),
        "PctCapacity": round(pct_capacity, 2)
    }


# --- Menu ------------------------------------------------------------------
def menu():
    years = list_years()
    if not years:
        print("No parquet data found.")
        return

    while True:
        print("\nMenu (Inertia Analysis):")
        print("1) Average Outturn Inertia (per year, multi-units)")
        print("2) Lowest Outturn Inertia (per year, multi-units)")
        print("3) Highest Outturn Inertia (per year, multi-units)")
        print("4) Market vs Outturn Gap (per year, GVAs only)")
        print("5) Yearly Min / Median / Max Table (multi-units for Outturn)")
        print("6) Exit")

        choice = input("Select: ").strip()
        if choice == "6":
            break

        if choice in {"1", "2", "3", "4", "5"}:
            rows = []
            for year in years:
                df = load_data(year)
                try:
                    if choice == "1":
                        avg_gvas = df["Outturn Inertia"].mean()
                        rows.append({"Year": year, **inertia_to_units(avg_gvas)})

                    elif choice == "2":
                        low_gvas = df["Outturn Inertia"].min()
                        rows.append({"Year": year, **inertia_to_units(low_gvas)})

                    elif choice == "3":
                        high_gvas = df["Outturn Inertia"].max()
                        rows.append({"Year": year, **inertia_to_units(high_gvas)})

                    elif choice == "4":
                        gap = (df["Outturn Inertia"] - df["Market Provided Inertia"]).mean()
                        rows.append({"Year": year, "Avg_Gap_GVAs": round(gap, 2)})

                    elif choice == "5":
                        rows.append({
                            "Year": year,
                            "Min": inertia_to_units(df["Outturn Inertia"].min()),
                            "Median": inertia_to_units(df["Outturn Inertia"].median()),
                            "Max": inertia_to_units(df["Outturn Inertia"].max())
                        })
                finally:
                    del df
                    gc.collect()
            print(pd.DataFrame(rows))
        else:
            print("Invalid choice.")


if __name__ == "__main__":
    menu()
