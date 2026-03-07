import pandas as pd
import pyarrow.dataset as ds
import os
import gc
from pathlib import Path

# --- Constants --------------------------------------------------------------
ROOT = next(p for p in Path(__file__).resolve().parents if p.name == "GDA")
PARQUET_DIR = ROOT / "DataSources" / "NESO" / "BalancingServices" / "Parquet"

# --- Data Loader ------------------------------------------------------------
def list_years():
    years = []
    for name in os.listdir(PARQUET_DIR):
        if name.startswith("year="):
            try:
                years.append(int(name.split("=")[1]))
            except ValueError:
                pass
    return sorted(years)


def load_data(year=None):
    dataset = ds.dataset(PARQUET_DIR, format="parquet", partitioning="hive")
    if year is not None:
        table = dataset.to_table(filter=(ds.field("year") == int(year)))
    else:
        table = dataset.to_table()
    df = table.to_pandas()
    # ensure datetime
    if "DatetimeUTC" in df.columns:
        df["DatetimeUTC"] = pd.to_datetime(df["DatetimeUTC"], utc=True)
    return df

# --- Helpers ---------------------------------------------------------------
def cost_columns(df: pd.DataFrame):
    """Return list of numeric cost columns present in the dataframe."""
    return [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])
            and c not in ["SETT_DATE", "SETT_PERIOD", "DatetimeUTC", "year"]]


def pretty_costs(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of *df* with numeric cost columns formatted as currency strings.

    Columns named Year/Month/Periods/Hours are left numeric for sorting purposes.
    """
    out = df.copy()
    for col in out.columns:
        if col in ["Year", "Month", "Periods", "Hours"]:
            continue
        if pd.api.types.is_numeric_dtype(out[col]):
            out[col] = out[col].apply(lambda x: f"£{x:,.0f}" if pd.notna(x) else "")
    return out

# --- Menu ------------------------------------------------------------------
def menu():
    years = list_years()
    if not years:
        print("No parquet data found for Balancing Services.")
        return

    while True:
        print("\nMenu (Balancing Services costs):")
        print("1) Yearly total cost (per category)")
        print("2) Yearly average cost (per category)")
        print("3) Monthly totals for single year")
        print("4) Annual overview (counts & totals, assumes 30‑min settles)")
        print("5) Burden per house (input meter count)")
        print("6) Exit")
        choice = input("Select: ").strip()
        if choice == "6":
            break

        if choice in {"1", "2", "4"}:
            # option 4 is the overview which also computes period counts
            rows = []
            for year in years:
                df = load_data(year)
                try:
                    cols = cost_columns(df)
                    row = {"Year": year}
                    if choice == "1":
                        sums = df[cols].sum()
                        row.update({c: float(sums[c]) for c in cols})
                    elif choice == "2":
                        means = df[cols].mean()
                        row.update({c: float(means[c]) for c in cols})
                    else:  # overview
                        sums = df[cols].sum()
                        row.update({c: float(sums[c]) for c in cols})
                        row["Periods"] = len(df)
                        # each settlement period represents half an hour
                        row["Hours"] = row["Periods"] * 0.5
                        # add a total-cost column for the year
                        total_cost = float(sums.sum())
                        row["YearTotal"] = total_cost
                        # average cost per period and per hour
                        row["CostPerPeriod"] = total_cost / row["Periods"] if row["Periods"] else 0
                        row["CostPerHour"] = total_cost / row["Hours"] if row["Hours"] else 0
                    rows.append(row)
                finally:
                    del df
                    gc.collect()
            df_out = pd.DataFrame(rows)
            # add a total row when showing overview
            if choice == "4" and not df_out.empty:
                total = {c: df_out[c].sum() for c in df_out.columns if c != "Year"}
                total["Year"] = "Total"
                # compute average cost per period/hour for total
                if total.get("Periods"):
                    total_cost = total.get("YearTotal", 0)
                    total["CostPerPeriod"] = total_cost / total["Periods"]
                if total.get("Hours"):
                    total_cost = total.get("YearTotal", 0)
                    total["CostPerHour"] = total_cost / total["Hours"]
                df_out = pd.concat([df_out, pd.DataFrame([total])], ignore_index=True)
            print(pretty_costs(df_out))

        elif choice == "5":
            # burden per house
            meters_in = input("Enter number of meters (houses): ").strip()
            try:
                meters = float(meters_in)
                if meters <= 0:
                    raise ValueError
            except ValueError:
                print("Invalid meter count")
                continue
            rows = []
            for year in years:
                df = load_data(year)
                try:
                    cols = cost_columns(df)
                    sums = df[cols].sum() / meters  # per‑house
                    row = {"Year": year}
                    # individual cost columns per house
                    row.update({c: float(sums[c]) for c in cols})
                    year_total = float(sums.sum())
                    row["YearTotal"] = year_total
                    periods = len(df)
                    hours = periods * 0.5
                    row["Periods"] = periods
                    row["Hours"] = hours
                    row["CostPerPeriod"] = (year_total / periods) if periods else 0
                    row["CostPerHour"] = (year_total / hours) if hours else 0
                    rows.append(row)
                finally:
                    del df
                    gc.collect()
            df_out = pd.DataFrame(rows)
            print(pretty_costs(df_out))

        elif choice == "3":
            yr = input("Enter year: ").strip()
            try:
                y = int(yr)
                if y not in years:
                    print(f"Year {y} not available. Available: {years}")
                    continue
            except ValueError:
                print("Invalid year")
                continue
            df = load_data(y)
            try:
                df["Month"] = pd.to_datetime(df["SETT_DATE"], dayfirst=True).dt.month
                cols = cost_columns(df)
                monthly = df.groupby("Month")[cols].sum().round(2)
                print(f"\nMonthly totals for {y}:")
                print(pretty_costs(monthly.reset_index()))
            finally:
                del df
                gc.collect()
        else:
            print("Invalid choice.")


if __name__ == "__main__":
    menu()