import pandas as pd
import pyarrow.dataset as ds
import os
import gc
from pathlib import Path

# --- Constants --------------------------------------------------------------
ROOT = next(p for p in Path(__file__).resolve().parents if p.name == "GDA")
PARQUET_DIR = ROOT / "DataSources" / "NESO" / "DemandData" / "Parquet"

# --- schema metadata copied from converter for reference and usage in
# validation/printing. keys are the column names as they appear in the CSV and
# parquet files. each value is a dict containing a human description plus the
# usual unit string (empty if not applicable).
COLUMN_SCHEMA = {
    "SETTLEMENT_DATE": {"description": "Settlement Date", "unit": "ISO 8601"},
    "SETTLEMENT_PERIOD": {"description": "Settlement Period", "unit": ""},
    "ND": {"description": "National Demand", "unit": "MW"},
    "TSD": {"description": "Transmission System Demand", "unit": "MW"},
    "ENGLAND_WALES_DEMAND": {"description": "England & Wales Demand", "unit": "MW"},
    "EMBEDDED_WIND_GENERATION": {"description": "Estimated Embedded Wind Generation", "unit": "MW"},
    "EMBEDDED_WIND_CAPACITY": {"description": "Embedded Wind Capacity", "unit": "MW"},
    "EMBEDDED_SOLAR_GENERATION": {"description": "Estimated Embedded Solar Generation", "unit": "MW"},
    "EMBEDDED_SOLAR_CAPACITY": {"description": "Embedded Solar Capacity", "unit": "MW"},
    "NON_BM_STOR": {"description": "Non-Balancing Mechanism Short-Term Operating Reserve", "unit": "MW"},
    "PUMP_STORAGE_PUMPING": {"description": "Pump Storage Pumping", "unit": "MW"},
    "SCOTTISH_TRANSFER": {"description": "Scottish Transfer Volume", "unit": "MW"},
    "IFA_FLOW": {"description": "IFA Interconnector Flow", "unit": "MW"},
    "IFA2_FLOW": {"description": "IFA2 Interconnector Flow", "unit": "MW"},
    "BRITNED_FLOW": {"description": "BritNed Interconnector Flow", "unit": "MW"},
    "MOYLE_FLOW": {"description": "Moyle Interconnector Flow", "unit": "MW"},
    "EAST_WEST_FLOW": {"description": "East West Interconnector Flow", "unit": "MW"},
    "NEMO_FLOW": {"description": "Nemo Interconnector Flow", "unit": "MW"},
    "NSL_FLOW": {"description": "North Sea Link Interconnector Flow", "unit": "MW"},
    "ELECLINK_FLOW": {"description": "ElecLink Interconnector Flow", "unit": "MW"},
    "VIKING_FLOW": {"description": "Viking Interconnector Flow", "unit": "MW"},
    "GREENLINK_FLOW": {"description": "Greenlink Interconnector Flow", "unit": "MW"},
}



# --- Data Loader ------------------------------------------------------------
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
        df.set_index("DatetimeUTC", inplace=True)
    return df

# --- Helpers ---------------------------------------------------------------
def print_schema():
    """Print the column names along with description and unit."""
    for col, info in COLUMN_SCHEMA.items():
        desc = info.get("description")
        unit = info.get("unit")
        if unit:
            print(f"{col}: {desc} [{unit}]")
        else:
            print(f"{col}: {desc}")
    print()


def demand_stats(df: pd.DataFrame, cols=None):
    """Return mean/min/max/std for specified columns (defaults to ND/TSD)."""
    if cols is None:
        cols = [c for c in df.columns if c in ["ND", "TSD"]]
    stats = {}
    for c in cols:
        if c in df.columns:
            series = df[c].dropna()
            meanval = series.mean()
            stats[f"{c}_mean"] = round(meanval, 2) if pd.notna(meanval) else None
            minval = series.min()
            stats[f"{c}_min"] = round(minval, 2) if pd.notna(minval) else None
            maxval = series.max()
            stats[f"{c}_max"] = round(maxval, 2) if pd.notna(maxval) else None
            stdval = series.std()
            stats[f"{c}_std"] = round(stdval, 2) if pd.notna(stdval) else None
    return stats


import re

def _base_column(col: str) -> str:
    """Return the underlying base name for derived statistic columns.

    e.g. "ND_mean" -> "ND" so we can look up the unit against the raw field.
    """
    m = re.match(r"(.+?)_(?:mean|min|max|std)$", col)
    return m.group(1) if m else col


def unit_for(col: str) -> str:
    """Return the unit string for a column (empty if none)."""
    base = _base_column(col)
    info = COLUMN_SCHEMA.get(base)
    return info["unit"] if info else ""


def rename_with_units(df: pd.DataFrame) -> pd.DataFrame:
    """Append unit suffixes to column names when known."""
    renames = {}
    for col in df.columns:
        unit = unit_for(col)
        if unit:
            renames[col] = f"{col} ({unit})"
    return df.rename(columns=renames)


def apply_units_and_scaling(df: pd.DataFrame) -> pd.DataFrame:
    """Scale numeric columns based on magnitude and rename with updated units.

    For MW values, automatically convert to GW/TW etc. based on the maximum
    absolute value in each column. The resulting DataFrame has its column
    headers modified to reflect the new unit.
    """
    out = df.copy()
    for col in list(out.columns):
        base = col.split(' ')[0]
        unit = unit_for(base)
        if unit == "MW" and pd.api.types.is_numeric_dtype(out[col]):
            vals = pd.to_numeric(out[col], errors="coerce").abs().dropna()
            if not vals.empty:
                maxval = vals.max()
                if maxval >= 1e6:
                    scale, newunit = 1e6, "TW"
                elif maxval >= 1e3:
                    scale, newunit = 1e3, "GW"
                else:
                    scale, newunit = 1, "MW"
                if scale != 1:
                    out[col] = out[col] / scale
                # rename header to include new unit
                out = out.rename(columns={col: f"{base} ({newunit})"})
        else:
            # for non-MW columns or non-numeric we still apply basic naming
            if unit:
                out = out.rename(columns={col: f"{base} ({unit})"})
    return out


def _unit_from_header(col: str) -> str:
    m = re.search(r"\(([^)]+)\)", col)
    return m.group(1) if m else unit_for(col.split(' ')[0])


def pretty_numbers(df: pd.DataFrame) -> pd.DataFrame:
    """Format numeric columns with commas and include units detected from header."""
    out = df.copy()
    for col in out.columns:
        if col in ["Year", "Month"]:
            continue
        if pd.api.types.is_numeric_dtype(out[col]):
            unit = _unit_from_header(col)
            out[col] = out[col].apply(
                lambda x: f"{x:,.0f} {unit}" if pd.notna(x) and unit else (f"{x:,.0f}" if pd.notna(x) else "")
            )
    return out

# --- Menu ------------------------------------------------------------------
def menu():
    years = list_years()
    if not years:
        print("No parquet data found for Demand Data.")
        return

    while True:
        print("\nMenu (Demand Data):")
        print("0) Show column schema and descriptions")
        print("1) Yearly ND/TSD statistics")
        print("2) Yearly totals (all numeric columns)")
        print("3) Monthly averages for a year")
        print("4) Exit")
        choice = input("Select: ").strip()
        if choice == "4":
            break
        if choice == "0":
            print_schema()
            continue

        if choice == "1":
            rows = []
            for year in years:
                df = load_data(year)
                try:
                    stats = demand_stats(df)
                    rows.append({"Year": year, **stats})
                finally:
                    del df
                    gc.collect()
            df_out = pd.DataFrame(rows)
            df_out = apply_units_and_scaling(df_out)
            print(pretty_numbers(df_out))

        elif choice == "2":
            rows = []
            for year in years:
                df = load_data(year)
                try:
                    cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
                    sums = df[cols].sum()
                    row = {"Year": year}
                    row.update({c: float(sums[c]) for c in cols})
                    rows.append(row)
                finally:
                    del df
                    gc.collect()
            df_out = pd.DataFrame(rows)
            df_out = apply_units_and_scaling(df_out)
            print(pretty_numbers(df_out))

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
                df["Month"] = df.index.month
                cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
                monthly = df.groupby("Month")[cols].mean().round(2)
                monthly = apply_units_and_scaling(monthly)
                print(f"\nMonthly averages for {y}:")
                # monthly already indexed by Month, just format and display
                print(pretty_numbers(monthly))
            finally:
                del df
                gc.collect()
        else:
            print("Invalid choice.")


if __name__ == "__main__":
    menu()