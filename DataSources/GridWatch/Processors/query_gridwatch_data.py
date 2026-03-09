import pandas as pd
import pyarrow.dataset as ds
import os
import gc
from pathlib import Path

# --- Constants --------------------------------------------------------------
ROOT = next(p for p in Path(__file__).resolve().parents if p.name == "GDA")
PARQUET_DIR = ROOT / "DataSources" / "GridWatch" / "Parquet"


# --- Data Loader ------------------------------------------------------------
def list_years():
    """List the years available in the parquet dataset."""
    years = set()
    if not os.path.isdir(PARQUET_DIR):
        return []
    for year_dir in Path(PARQUET_DIR).glob("**/year=*"):
        try:
            years.add(int(year_dir.name.split("=", 1)[1]))
        except ValueError:
            continue
    return sorted(years)


def load_data(year=None):
    """Load gridwatch data optionally filtered by year."""
    dataset = ds.dataset(PARQUET_DIR, format="parquet", partitioning="hive")
    if year is not None:
        table = dataset.to_table(filter=(ds.field("year") == int(year)))
    else:
        table = dataset.to_table()
    df = table.to_pandas()
    # ensure timestamp column
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df.set_index("timestamp", inplace=True)
        df.sort_index(inplace=True)
    return df


# --- Statistics helpers -----------------------------------------------------
def basic_stats(series: pd.Series):
    if series.empty:
        return {"Mean": None, "Min": None, "Max": None, "Std": None}
    return {
        "Mean": round(series.mean(), 2),
        "Min": round(series.min(), 2),
        "Max": round(series.max(), 2),
        "Std": round(series.std(), 2)
    }


def pretty_df(df: pd.DataFrame, scale: float = 1.0, suffix: str = "") -> pd.DataFrame:
    """Return a copy of DataFrame with numbers formatted with commas.

    ``scale`` can be used to divide values (e.g. 1e3 to convert MW to GW)
    and ``suffix`` appended to each formatted number.
    The ``Year`` column (if present) is left untouched so it remains
    integer and unsuffixed.
    """
    out = df.copy()
    for col in out.columns:
        if col == "Year":
            continue
        if pd.api.types.is_numeric_dtype(out[col]):
            out[col] = out[col].apply(lambda x: f"{x/scale:,.0f}{suffix}" if pd.notna(x) else "")
    return out


def fuel_totals(df):
    """Return total energy produced by each fuel column (sum over timeframe)."""
    fuels = [c for c in df.columns if c not in ["demand", "frequency"]]
    totals = {}
    for f in fuels:
        if pd.api.types.is_numeric_dtype(df[f]):
            totals[f] = float(df[f].sum())
    return totals


# --- Menu ------------------------------------------------------------------
def menu():
    years = list_years()
    if not years:
        print("No parquet data found for GridWatch.")
        return

    while True:
        print("\nGridWatch Data Analysis:")
        print("1) Demand statistics (per year)")
        print("2) Frequency statistics (per year)")
        print("3) Totals (generation / interconnectors)")
        print("4) View raw sample for a year")
        print("5) Yearly summary (generation/import/consumption)")
        print("6) Exit")

        choice = input("Select: ").strip()
        if choice == "6":
            break

        if choice in {"1", "2", "3", "4", "5"}:
            if choice in {"1", "2"}:
                rows = []
                for year in years:
                    df = load_data(year)
                    try:
                        if choice == "1":
                            rows.append({"Year": year, **basic_stats(df["demand"])})
                        elif choice == "2":
                            rows.append({"Year": year, **basic_stats(df["frequency"])})
                    finally:
                        del df
                        gc.collect()
                print(pd.DataFrame(rows))

            elif choice == "3":
                # submenu for totals
                print("\nTotals sub-menu:")
                print("a) Domestic generation sources")
                print("b) Interconnector flows")
                print("c) All numeric columns (wide)")
                sub = input("Select (a/b/c): ").strip().lower()
                if sub not in {"a", "b", "c"}:
                    print("Invalid option.")
                    continue
                rows = []
                for year in years:
                    df = load_data(year)
                    try:
                        row = {"Year": year}
                        # start with numeric columns excluding metadata
                        cols = [c for c in df.columns
                                if pd.api.types.is_numeric_dtype(df[c])
                                and c not in ["demand", "frequency", "id", "year"]]
                        if sub == "a":
                            # domestic generation = numeric cols without ict or interconnector names
                            fuels = [c for c in cols
                                     if not c.endswith("_ict")
                                     and c not in ["north_south", "scotland_england"]]
                        elif sub == "b":
                            fuels = [c for c in cols
                                     if c.endswith("_ict")
                                     or c in ["north_south", "scotland_england"]]
                        else:  # c: all remaining numeric columns
                            fuels = cols
                        for f in fuels:
                            row[f] = float(df[f].sum())
                        rows.append(row)
                    finally:
                        del df
                        gc.collect()
                df_out = pd.DataFrame(rows)
                # assume columns are in MW‑hours/units; show in GW (divide by 1000)
                print(pretty_df(df_out, scale=1000, suffix=" MW"))

            elif choice == "4":
                yr = input("Enter year to sample: ").strip()
                try:
                    y = int(yr)
                    if y not in years:
                        print(f"Year {y} not available. Available: {years}")
                        continue
                except ValueError:
                    print("Invalid year")
                    continue
                df = load_data(y)
                print(df.head())

            elif choice == "5":
                # yearly summary: total generation, net import, domestic consumption
                rows = []
                for year in years:
                    df = load_data(year)
                    try:
                        # determine time step in hours (use modal delta)
                        dt = df.index.to_series().diff().mode()[0]
                        hours_per = dt.total_seconds() / 3600
                        total_hours = hours_per * len(df)
                        # generation columns (domestic sources)
                        cols = [c for c in df.columns
                                if pd.api.types.is_numeric_dtype(df[c])
                                and c not in ["demand", "frequency", "id", "year"]
                                and not c.endswith("_ict")
                                and c not in ["north_south", "scotland_england"]]
                        avg_gen_MW = df[cols].mean().sum()
                        energy_gen_TWh = avg_gen_MW * total_hours / 1e6
                        avg_gen_TW = avg_gen_MW / 1000
                        # net imports (interconnectors)
                        ic_cols = [c for c in df.columns
                                   if pd.api.types.is_numeric_dtype(df[c])
                                   and (c.endswith("_ict")
                                        or c in ["north_south", "scotland_england"]) ]
                        avg_net_MW = df[ic_cols].mean().sum()
                        energy_net_TWh = avg_net_MW * total_hours / 1e6
                        avg_net_TW = avg_net_MW / 1000
                        # domestic consumption = demand average
                        avg_demand_MW = df["demand"].mean()
                        energy_dem_TWh = avg_demand_MW * total_hours / 1e6
                        avg_dem_TW = avg_demand_MW / 1000
                        rows.append({
                            "Year": year,
                            "Gen (TWh)": round(energy_gen_TWh, 2),
                            "Gen (TW avg)": round(avg_gen_TW, 3),
                            "Import (TWh)": round(energy_net_TWh, 2),
                            "Import (TW avg)": round(avg_net_TW, 3),
                            "Demand (TWh)": round(energy_dem_TWh, 2),
                            "Demand (TW avg)": round(avg_dem_TW, 3),
                        })
                    finally:
                        del df
                        gc.collect()
                print(pd.DataFrame(rows))
        else:
            print("Invalid choice.")


if __name__ == "__main__":
    menu()
