import pandas as pd
import pyarrow.dataset as ds
import os
import gc
from pathlib import Path

# --- Constants --------------------------------------------------------------
ROOT = next(p for p in Path(__file__).resolve().parents if p.name == "GDA")
PARQUET_DIR = ROOT / "DataSources" / "Weather" / "Parquet"


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


# --- Weather Analysis Functions ---------------------------------------------
def weather_stats(df, column):
    """Calculate basic statistics for a weather column."""
    if df.empty or column not in df.columns:
        return {"Mean": None, "Min": None, "Max": None, "Std": None}

    series = df[column]
    return {
        "Mean": round(series.mean(), 2),
        "Min": round(series.min(), 2),
        "Max": round(series.max(), 2),
        "Std": round(series.std(), 2)
    }


# --- Menu ------------------------------------------------------------------
def menu():
    years = list_years()
    if not years:
        print("No parquet data found.")
        return

    while True:
        print("\nMenu (Weather Analysis):")
        print("1) Temperature Statistics (per year)")
        print("2) Wind Speed Statistics (per year)")
        print("3) Solar Radiation Statistics (per year)")
        print("4) All Weather Statistics (per year)")
        print("5) Monthly Averages (for a specific year)")
        print("6) Exit")

        choice = input("Select: ").strip()
        if choice == "6":
            break

        if choice in {"1", "2", "3", "4", "5"}:
            rows = []
            try:
                if choice in {"1", "2", "3", "4"}:
                    for year in years:
                        df = load_data(year)
                        try:
                            row = {"Year": year}

                            if choice == "1" or choice == "4":
                                row.update({f"Temp_{k}": v for k, v in weather_stats(df, "Temperature_C").items()})

                            if choice == "2" or choice == "4":
                                row.update({f"Wind_{k}": v for k, v in weather_stats(df, "Wind_Speed_100m_kph").items()})

                            if choice == "3" or choice == "4":
                                row.update({f"Solar_{k}": v for k, v in weather_stats(df, "Solar_Radiation_W_m2").items()})

                            rows.append(row)
                        finally:
                            del df
                            gc.collect()

                elif choice == "5":
                    year_input = input("Enter year: ").strip()
                    try:
                        year = int(year_input)
                        if year not in years:
                            print(f"Year {year} not available. Available years: {years}")
                            continue
                    except ValueError:
                        print("Invalid year format.")
                        continue

                    df = load_data(year)
                    try:
                        # Add month column
                        df["Month"] = df["Date"].dt.month

                        # Group by month and calculate averages
                        monthly = df.groupby("Month").agg({
                            "Temperature_C": "mean",
                            "Wind_Speed_100m_kph": "mean",
                            "Solar_Radiation_W_m2": "mean"
                        }).round(2)

                        print(f"\nMonthly averages for {year}:")
                        print(monthly)
                        continue
                    finally:
                        del df
                        gc.collect()

                print(pd.DataFrame(rows))

            except Exception as e:
                print(f"Error: {e}")
        else:
            print("Invalid choice.")


if __name__ == "__main__":
    menu()