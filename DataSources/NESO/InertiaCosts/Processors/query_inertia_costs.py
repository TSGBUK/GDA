import gc
import os
from pathlib import Path

import pandas as pd
import pyarrow.dataset as ds


ROOT = next(p for p in Path(__file__).resolve().parents if p.name == "GDA")
PARQUET_DIR = ROOT / "DataSources" / "NESO" / "InertiaCosts" / "Parquet"


def list_years():
    years = []
    if not os.path.isdir(PARQUET_DIR):
        return years
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
    if "DatetimeUTC" in df.columns:
        df["DatetimeUTC"] = pd.to_datetime(df["DatetimeUTC"], utc=True)
    return df


def pretty_money(value):
    if pd.isna(value):
        return ""
    return f"£{float(value):,.2f}"


def menu():
    years = list_years()
    if not years:
        print("No parquet data found for InertiaCosts.")
        return

    while True:
        print("\nMenu (InertiaCosts):")
        print("1) Yearly summary (avg/min/max/total, incl. non-zero days)")
        print("2) Monthly averages for a year")
        print("3) Top N highest-cost days")
        print("4) Zero-cost day count by year")
        print("5) Exit")

        choice = input("Select: ").strip()
        if choice == "5":
            break

        if choice == "1":
            rows = []
            for year in years:
                df = load_data(year)
                try:
                    costs = df["Cost_per_GVAs"]
                    non_zero = costs[costs > 0]
                    rows.append(
                        {
                            "Year": year,
                            "Days": int(len(costs)),
                            "NonZeroDays": int((costs > 0).sum()),
                            "ZeroDays": int((costs == 0).sum()),
                            "Avg": costs.mean(),
                            "Avg_NonZero": non_zero.mean() if not non_zero.empty else 0.0,
                            "Min": costs.min(),
                            "Max": costs.max(),
                            "Total": costs.sum(),
                        }
                    )
                finally:
                    del df
                    gc.collect()

            out = pd.DataFrame(rows)
            for col in ["Avg", "Avg_NonZero", "Min", "Max", "Total"]:
                out[col] = out[col].apply(pretty_money)
            print(out)

        elif choice == "2":
            year_in = input(f"Enter year ({years[0]}-{years[-1]}): ").strip()
            try:
                year = int(year_in)
            except ValueError:
                print("Invalid year.")
                continue
            if year not in years:
                print(f"Year {year} not available. Available: {years}")
                continue

            df = load_data(year)
            try:
                df["Month"] = df["DatetimeUTC"].dt.month
                monthly = (
                    df.groupby("Month", as_index=False)["Cost_per_GVAs"]
                    .agg(["mean", "min", "max", "sum", "count"])
                    .reset_index()
                )
                monthly.rename(
                    columns={
                        "mean": "Avg",
                        "min": "Min",
                        "max": "Max",
                        "sum": "Total",
                        "count": "Days",
                    },
                    inplace=True,
                )
                for col in ["Avg", "Min", "Max", "Total"]:
                    monthly[col] = monthly[col].apply(pretty_money)
                print(monthly)
            finally:
                del df
                gc.collect()

        elif choice == "3":
            n_in = input("Top how many days? (default 10): ").strip()
            if not n_in:
                n = 10
            else:
                try:
                    n = max(1, int(n_in))
                except ValueError:
                    print("Invalid number.")
                    continue

            df = load_data()
            try:
                top = (
                    df[["Settlement Date", "DatetimeUTC", "Cost_per_GVAs", "year"]]
                    .sort_values("Cost_per_GVAs", ascending=False)
                    .head(n)
                    .copy()
                )
                top["Cost_per_GVAs"] = top["Cost_per_GVAs"].apply(pretty_money)
                print(top)
            finally:
                del df
                gc.collect()

        elif choice == "4":
            rows = []
            for year in years:
                df = load_data(year)
                try:
                    costs = df["Cost_per_GVAs"]
                    rows.append(
                        {
                            "Year": year,
                            "Days": int(len(costs)),
                            "ZeroDays": int((costs == 0).sum()),
                            "ZeroPct": round(100.0 * (costs == 0).sum() / len(costs), 2) if len(costs) else 0,
                        }
                    )
                finally:
                    del df
                    gc.collect()
            print(pd.DataFrame(rows))

        else:
            print("Invalid choice.")


if __name__ == "__main__":
    menu()
