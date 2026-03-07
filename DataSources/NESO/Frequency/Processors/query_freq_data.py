import pandas as pd
import pyarrow.dataset as ds
import os
from datetime import timedelta
import gc
from pathlib import Path

# --- Constants --------------------------------------------------------------
ROOT = next(p for p in Path(__file__).resolve().parents if p.name == "GDA")
PARQUET_DIR = ROOT / "DataSources" / "NESO" / "Frequency" / "Parquet"
F0_HZ = 50.0               # Nominal system frequency
GRID_CAPACITY_GW = 50.0    # Approximate system capacity
GVA_ESTIMATE_GVA = 150.0   # Ballpark system inertia estimate


# --- Data Loader ------------------------------------------------------------
def list_years():
    """Find available years in the parquet dataset (Hive partitioned)."""
    years = []
    for name in os.listdir(PARQUET_DIR):
        if name.startswith("year="):
            years.append(int(name.split("=")[1]))
    return sorted(years)


def load_data(year=None):
    """Load a single year's parquet data using pyarrow.dataset."""
    dataset = ds.dataset(PARQUET_DIR, format="parquet", partitioning="hive")
    if year is not None:
        table = dataset.to_table(filter=(ds.field("year") == int(year)))
    else:
        table = dataset.to_table()
    return table.to_pandas()


# --- Core physics helpers (with correct units) ------------------------------
def delta_p_from_rocof(inertia_gvas: float, rocof_obs_hz_per_s: float,
                       f0_hz: float = F0_HZ) -> float:
    Ek_MWs = inertia_gvas * 1000.0  # convert GVA·s → MW·s
    return (2.0 * Ek_MWs / f0_hz) * rocof_obs_hz_per_s  # MW


def inertia_required_gvas(delta_p_mw: float, rocof_limit_hz_per_s: float,
                          f0_hz: float = F0_HZ) -> float:
    Ek_req_MWs = (delta_p_mw * f0_hz) / (2.0 * rocof_limit_hz_per_s)  # MW·s
    return Ek_req_MWs / 1000.0  # convert back to GVA·s


# --- Stats ------------------------------------------------------------------
def get_frequency_average(df): return df["Value"].mean()
def get_frequency_low(df): return df["Value"].min()
def get_frequency_high(df): return df["Value"].max()


# --- Excursions -------------------------------------------------------------
def count_excursions(df, low=49.95, high=50.05, mode="under",
                     start_hysteresis_s=1, end_hysteresis_s=3, min_duration_s=2):
    val = df["Value"]
    t = df["Date"]

    if mode == "under":
        beyond = val < low
        recovered = val >= low
    else:  # "over"
        beyond = val > high
        recovered = val <= high

    in_event = False
    start_idx = None
    below_run = 0
    recov_run = 0
    events = 0
    total_sec = 0

    for i in range(len(val)):
        if beyond.iat[i]:
            below_run += 1
            recov_run = 0
            if not in_event and below_run >= start_hysteresis_s:
                in_event = True
                start_idx = i - start_hysteresis_s + 1
        else:
            below_run = 0
            if in_event and recovered.iat[i]:
                recov_run += 1
                if recov_run >= end_hysteresis_s:
                    end_idx = i - end_hysteresis_s + 1
                    dur = int((t.iloc[end_idx] - t.iloc[start_idx]).total_seconds())
                    if dur >= min_duration_s:
                        events += 1
                        total_sec += dur
                    in_event = False
                    start_idx = None
                    recov_run = 0
            else:
                recov_run = 0

    if in_event and start_idx is not None:
        end_idx = len(val) - 1
        dur = int((t.iloc[end_idx] - t.iloc[start_idx]).total_seconds())
        if dur >= min_duration_s:
            events += 1
            total_sec += dur

    return events, total_sec


# --- Event classification ---------------------------------------------------
def classify_event(dp_mw: float) -> str:
    if dp_mw < 500:
        return "Balancing / minor fluctuation"
    elif dp_mw < 1500:
        return "Medium event (possible gen trip or wind block)"
    else:
        return "Major event (likely generator or interconnector loss)"


# --- Duration helpers ------------------------------------------------------
def _time_deltas(df: pd.DataFrame) -> pd.Series:
    """Compute interval lengths between successive samples (seconds)."""
    return df["Date"].diff().dt.total_seconds().fillna(0)


def duration_below(df: pd.DataFrame, threshold: float) -> float:
    """Total seconds where frequency is strictly below *threshold*."""
    deltas = _time_deltas(df)
    mask = df["Value"] < threshold
    return float(deltas[mask].sum())


def duration_above(df: pd.DataFrame, threshold: float) -> float:
    """Total seconds where frequency is strictly above *threshold*."""
    deltas = _time_deltas(df)
    mask = df["Value"] > threshold
    return float(deltas[mask].sum())


def duration_between(df: pd.DataFrame, low: float, high: float) -> float:
    """Seconds where frequency is between low and high inclusive."""
    deltas = _time_deltas(df)
    mask = (df["Value"] >= low) & (df["Value"] <= high)
    return float(deltas[mask].sum())


# --- RoCoF clustering -------------------------------------------------------
def rocof_event_clusters(df, delta=0.125, gap_s=10, min_duration_s=0,
                         inertia_gvas=GVA_ESTIMATE_GVA, f0_hz=F0_HZ):
    dF = df["Value"].diff()
    cond = dF.abs() > delta
    events = df.loc[cond, ["Date"]].copy()
    events["RoCoF"] = dF[cond].values
    events = events.sort_values("Date").reset_index(drop=True)

    if events.empty:
        return pd.DataFrame(columns=[
            "Start", "End", "PeakRoCoF_Hzps", "CurrentInertia_GVAs",
            "dP_est_MW", "dP_est_GW", "I_required_GVAs", "ExtraInertia_GVAs",
            "EventType"
        ])

    clusters = []
    start_idx = 0
    for i in range(1, len(events)):
        if (events.loc[i, "Date"] - events.loc[i - 1, "Date"]) > timedelta(seconds=gap_s):
            seg = events.iloc[start_idx:i]
            if not seg.empty:
                clusters.append(analyze_segment(seg, inertia_gvas, delta, f0_hz, min_duration_s))
            start_idx = i

    seg = events.iloc[start_idx:]
    if not seg.empty:
        clusters.append(analyze_segment(seg, inertia_gvas, delta, f0_hz, min_duration_s))

    clusters = [c for c in clusters if c is not None]
    return pd.DataFrame(clusters, columns=[
        "Start", "End", "PeakRoCoF_Hzps", "CurrentInertia_GVAs",
        "dP_est_MW", "dP_est_GW", "I_required_GVAs", "ExtraInertia_GVAs",
        "EventType"
    ])


def analyze_segment(seg, inertia_gvas, delta, f0_hz, min_duration_s):
    duration = (seg["Date"].iloc[-1] - seg["Date"].iloc[0]).total_seconds() + 1
    if duration < min_duration_s:
        return None

    peak = seg["RoCoF"].abs().max()
    start, end = seg["Date"].iloc[0], seg["Date"].iloc[-1]

    dp_est_mw = delta_p_from_rocof(inertia_gvas, peak, f0_hz)
    I_req = inertia_required_gvas(dp_est_mw, delta, f0_hz)
    extra = I_req - inertia_gvas

    return (start, end, float(peak), inertia_gvas,
            float(dp_est_mw), float(dp_est_mw) / 1000.0,
            float(I_req), float(extra), classify_event(dp_est_mw))


# --- Menu ------------------------------------------------------------------
def menu():
    years = list_years()
    if not years:
        print("No parquet data found.")
        return

    while True:
        print("\nMenu:")
        print("1) Frequency Average (all years)")
        print("2) Frequency Low (all years) + Est. Shortfall (MW)")
        print("3) Frequency High (all years) + Est. Surplus (MW)")
        print("4) Undergen Events (<49.95 Hz excursions)")
        print("5) Overgen Events (>50.05 Hz excursions)")
        print("6) RoCoF Events Count (Δ > 0.125 Hz/s)")
        print("7) RoCoF Inertia Analysis (clustered)")
        print("8) Frequency Volatility (σ over 1s/10s windows)")
        print("9) Frequency Recovery Times (after excursions)")
        print("10) Yearly Nadir/Zenith Statistics (post-event)")
        print("11) Inertia Trend Analysis (min/median/max by year)")
        print("12) Time-in-band statistics (<49.95, <49.9, <49.8, <49.5, <49, >50, on-target)")
        print("13) Exit")

        choice = input("Select: ").strip()
        if choice == "13":
            break
        # --- Options 1–6 now return tables with safe cleanup ---
        if choice in {"1", "2", "3", "4", "5", "6"}:
            rows = []
            for year in years:
                df = load_data(year)
                try:
                    if choice == "1":
                        rows.append({"Year": year, "Average_Hz": get_frequency_average(df)})
                    elif choice == "2":
                        fmin = get_frequency_low(df)
                        delta_f = F0_HZ - fmin
                        shortfall_mw = (delta_f / F0_HZ) * (GRID_CAPACITY_GW * 1000)
                        rows.append({
                            "Year": year,
                            "Lowest_Hz": round(fmin, 3),
                            "Est_Shortfall_MW": round(shortfall_mw, 0)
                        })
                    elif choice == "3":
                        fmax = get_frequency_high(df)
                        delta_f = fmax - F0_HZ
                        surplus_mw = (delta_f / F0_HZ) * (GRID_CAPACITY_GW * 1000)
                        rows.append({
                            "Year": year,
                            "Highest_Hz": round(fmax, 3),
                            "Est_Surplus_MW": round(surplus_mw, 0)
                        })

                    elif choice == "4":
                        print(" Year  Events   Total_s  Avg_s")
                        for year in years:
                            df = load_data(year)
                            try:
                                events, dur = count_excursions(df, low=49.95, mode="under")
                                avg = dur / events if events else 0
                                row = {
                                    "Year": year,
                                    "Events": events,
                                    "Total_s": dur,
                                    "Avg_s": round(avg, 1)
                                }
                                # Print row immediately in aligned format
                                print(f"{row['Year']:5d} {row['Events']:7d} {row['Total_s']:9d} {row['Avg_s']:6.1f}")
                            finally:
                                del df
                                gc.collect()

                    
                    elif choice == "5":
                        events, dur = count_excursions(df, high=50.05, mode="over")
                        avg = dur / events if events else 0
                        rows.append({"Year": year, "Events": events, "Total_s": dur, "Avg_s": round(avg, 1)})
                    elif choice == "6":
                        dF = df["Value"].diff()
                        count = int((dF.abs() > 0.125).sum())
                        rows.append({"Year": year, "RoCoF_Count": count})
                finally:
                    # Always clean up df safely
                    del df
                    gc.collect()

            print(pd.DataFrame(rows))

        elif choice == "7":
            inertia = float(input(f"Enter assumed current inertia (GVA·s) [{GVA_ESTIMATE_GVA}]: ") or GVA_ESTIMATE_GVA)
            for year in years:
                print(f"\n--- Year {year} ---")
                df = load_data(year)
                try:
                    clusters = rocof_event_clusters(df, delta=0.125, inertia_gvas=inertia)
                    if clusters.empty:
                        print("No RoCoF events found.")
                    else:
                        print(clusters.to_string(index=False))
                finally:
                    del df
                    gc.collect()
        elif choice == "8":
            # Volatility
            for year in years:
                df = load_data(year)
                try:
                    sigma_1s = df["Value"].std()
                    sigma_10s = df["Value"].rolling(10).std().mean()
                    print(f"Year {year}: σ1s={sigma_1s:.5f} Hz, σ10s={sigma_10s:.5f} Hz")
                finally:
                    del df
                    gc.collect()

        elif choice == "9":
            # Recovery times after excursions
            print(" Year  Events   Avg_Recovery_s")
            for year in years:
                df = load_data(year)
                try:
                    events, dur = count_excursions(df, low=49.95, mode="under")
                    # (here we’d implement a real recovery time calc per event)
                    avg_recovery = (dur / events) if events else 0
                    print(f"{year:5d} {events:7d} {avg_recovery:15.1f}")
                finally:
                    del df
                    gc.collect()

        elif choice == "10":
            # Nadir (lowest point) and zenith (highest point) per year
            rows = []
            for year in years:
                df = load_data(year)
                try:
                    nadir = df["Value"].min()
                    zenith = df["Value"].max()
                    rows.append({"Year": year, "Nadir_Hz": nadir, "Zenith_Hz": zenith})
                finally:
                    del df
                    gc.collect()
            print(pd.DataFrame(rows))

        elif choice == "11":
            # Inertia trends (approx, using RoCoF distributions)
            rows = []
            for year in years:
                df = load_data(year)
                try:
                    dF = df["Value"].diff()
                    rocof = dF.abs()
                    if len(rocof.dropna()) == 0:
                        continue
                    # Inertia estimate is very rough — assume typical ΔP of 1 GW
                    dp = 1000
                    i_est = inertia_required_gvas(dp, rocof.quantile(0.95))
                    rows.append({"Year": year, "Est_Inertia_GVAs": round(i_est, 1)})
                finally:
                    del df
                    gc.collect()
            print(pd.DataFrame(rows))

        elif choice == "12":
            # time in various bands per year
            thresholds = [49.95, 49.9, 49.8, 49.5, 49.0]
            rows = []
            for year in years:
                df = load_data(year)
                try:
                    row = {"Year": year}
                    for thr in thresholds:
                        row[f"Under_{thr}"] = duration_below(df, thr)
                    row["Over_50"] = duration_above(df, 50.0)
                    row["On_Target"] = duration_between(df, 49.95, 50.0)
                    rows.append(row)
                finally:
                    del df
                    gc.collect()
            print(pd.DataFrame(rows))
        else:
            print("Invalid choice.")


if __name__ == "__main__":
    menu()
