"""Machine learning helpers for UK grid data.

This module provides utility functions to load and preprocess the various
datasets that live in the `Data` workspace.  It is intended to be the
starting point for developing models that identify grid events, explore
fuel mix behaviour, and incorporate weather features.

Available data sources:
  * Historical generation: df_fuel_ckan.csv (half-hourly values)
    * Weather observations: parquet files under DataSources/Weather/Parquet
    * Frequency measurements: parquet files under DataSources/NESO/Frequency/Parquet
    * Inertia data: parquet files under DataSources/NESO/Inertia/Parquet
    * Demand series: parquet under DataSources/NESO/DemandData/Parquet
    * Balancing services costs: parquet under DataSources/NESO/BalancingServices/Parquet
    * GridWatch snapshots: parquet under DataSources/GridWatch/Parquet

Typical usage:

    from MachineLearning import ml_pipeline
    fuel = ml_pipeline.load_generation()
    weather = ml_pipeline.load_weather(years=[2020,2021])
    freq = ml_pipeline.load_frequency(years=2021)
    # optional datasets can be loaded and merged too:
    demand = ml_pipeline.load_demand(years=2021)
    bs_costs = ml_pipeline.load_balancing(years=[2020,2021])
    merged = ml_pipeline.merge_all(fuel, weather, freq,
                                   demand=demand, balancing=bs_costs)

Functions in this module are lightweight wrappers around pandas/pyarrow
load methods and include a few helpers for feature engineering and event
labelling.
"""

import os
from typing import List, Optional

import pandas as pd
import pyarrow.dataset as ds

# optional GPU libraries
try:
    import cudf
    import dask_cudf
    GPU_AVAILABLE = True
except ImportError:
    cudf = None  # type: ignore
    dask_cudf = None  # type: ignore
    GPU_AVAILABLE = False

# base data folder, allow override via environment variable for portability
BASE = os.environ.get("TSGB_DATA_PATH",
                      os.path.normpath(os.path.join(os.path.dirname(__file__), "..")))

# individual dataset locations (most reside in hive-partitioned parquet folders)
GEN_CSV = os.path.join(BASE, "DataSources", "NESO", "HistoricalGenerationData", "df_fuel_ckan.csv")
WEATHER_PARQUET = os.path.join(BASE, "DataSources", "Weather", "Parquet")
FREQ_PARQUET = os.path.join(BASE, "DataSources", "NESO", "Frequency", "Parquet")
INERTIA_PARQUET = os.path.join(BASE, "DataSources", "NESO", "Inertia", "Parquet")
DEMAND_PARQUET = os.path.join(BASE, "DataSources", "NESO", "DemandData", "Parquet")
BALANCING_PARQUET = os.path.join(BASE, "DataSources", "NESO", "BalancingServices", "Parquet")
GRIDWATCH_PARQUET = os.path.join(BASE, "DataSources", "GridWatch", "Parquet")


def _load_parquet(folder: str,
                  years: Optional[object] = None,
                  use_gpu: bool = False) -> pd.DataFrame:
    """Load parquet using pyarrow then convert to pandas or cudf.

    The ``years`` argument may be an integer, a list/tuple of integers, or
    ``None`` (meaning all data).  Hive partitions are inspected in the
    directory so only the requested year(s) are read when possible.
    If ``use_gpu`` is True and RAPIDS is available the returned object will
    be a ``cudf.DataFrame`` (or ``dask_cudf`` when very large)."""
    # normalise years argument to a list of ints for convenience
    year_list: List[int] = []
    if years is not None:
        if isinstance(years, (list, tuple, set)):
            year_list = [int(y) for y in years]
        else:
            year_list = [int(years)]

    # simple hive-partition filter by polling directory names if needed
    paths = []
    if year_list:
        for name in os.listdir(folder):
            if name.startswith("year="):
                try:
                    y = int(name.split("=")[1])
                except ValueError:
                    continue
                if y in year_list:
                    paths.append(os.path.join(folder, name))
    if not paths:
        paths = [folder]

    if use_gpu and GPU_AVAILABLE:
        # read with cudf or dask_cudf if size is huge
        # cudf can read a directory of parquet files directly
        try:
            # attempt single-GPU read
            df = cudf.read_parquet(paths)
        except Exception:
            # fallback to dask for large datasets
            df = dask_cudf.read_parquet(paths)
        return df
    else:
        ds_obj = ds.dataset(folder, format="parquet", partitioning="hive")
        if year_list:
            if len(year_list) == 1:
                filt = (ds.field("year") == year_list[0])
            else:
                filt = ds.field("year").isin(year_list)
            table = ds_obj.to_table(filter=filt)
        else:
            table = ds_obj.to_table()
        df = table.to_pandas()
        return df


def load_generation(start_date: Optional[str] = None,
                    end_date: Optional[str] = None,
                    cols: Optional[List[str]] = None,
                    use_gpu: bool = False) -> pd.DataFrame:
    """Load historical generation CSV, optionally filtering by date or columns.

    The returned DataFrame has a datetime index ('DATETIME').
    """
    if use_gpu and GPU_AVAILABLE:
        df = cudf.read_csv(GEN_CSV, parse_dates=["DATETIME"])
    else:
        df = pd.read_csv(GEN_CSV, parse_dates=["DATETIME"])
    df.set_index("DATETIME", inplace=True)
    if start_date:
        df = df[df.index >= pd.Timestamp(start_date)]
    if end_date:
        df = df[df.index <= pd.Timestamp(end_date)]
    if cols:
        df = df[cols]
    return df


def load_weather(years: Optional[object] = None,
                 use_gpu: bool = False) -> pd.DataFrame:
    """Load weather parquet data.

    ``years`` may be a single int, a list of ints, or ``None`` to load
    all available partitions.  ``use_gpu`` will return a cudf/dask_cudf
    DataFrame when RAPIDS is available.
    """
    df = _load_parquet(WEATHER_PARQUET, years=years, use_gpu=use_gpu)
    # ensure datetime column and sorted index
    if "Date" in df.columns:
        if use_gpu and GPU_AVAILABLE:
            df["Date"] = cudf.to_datetime(df["Date"])
            df = df.set_index("Date").sort_index()
        else:
            df["Date"] = pd.to_datetime(df["Date"])
            df.set_index("Date", inplace=True)
            df.sort_index(inplace=True)
    return df


def load_frequency(years: Optional[object] = None,
                   use_gpu: bool = False) -> pd.DataFrame:
    """Load system frequency measurements.

    ``years`` behaves the same as in :func:`load_weather`. The returned
    DataFrame is indexed by the ``Date`` column if present.
    """
    df = _load_parquet(FREQ_PARQUET, years=years, use_gpu=use_gpu)
    if "Date" in df.columns:
        if use_gpu and GPU_AVAILABLE:
            df["Date"] = cudf.to_datetime(df["Date"])
            df = df.set_index("Date").sort_index()
        else:
            df["Date"] = pd.to_datetime(df["Date"])
            df.set_index("Date", inplace=True)
            df.sort_index(inplace=True)
    return df


def load_inertia(years: Optional[object] = None,
                 use_gpu: bool = False) -> pd.DataFrame:
    """Load inertia dataset (market and outturn values).

    ``years`` follows the same semantics as in :func:`load_weather`.
    The resulting DataFrame uses ``DatetimeUTC`` as its index when present.
    """
    df = _load_parquet(INERTIA_PARQUET, years=years, use_gpu=use_gpu)
    if "DatetimeUTC" in df.columns:
        if use_gpu and GPU_AVAILABLE:
            df["DatetimeUTC"] = cudf.to_datetime(df["DatetimeUTC"])
            df = df.set_index("DatetimeUTC").sort_index()
        else:
            df["DatetimeUTC"] = pd.to_datetime(df["DatetimeUTC"])
            df.set_index("DatetimeUTC", inplace=True)
            df.sort_index(inplace=True)
    return df


# additional loaders --------------------------------------------------------

def load_demand(years: Optional[object] = None,
                use_gpu: bool = False) -> pd.DataFrame:
    """Load demand dataset (settlement period figures).

    ``years`` may be an int, list of ints, or None; see :func:`_load_parquet`.
    Index is converted to ``DatetimeUTC`` if the column exists.
    """
    df = _load_parquet(DEMAND_PARQUET, years=years, use_gpu=use_gpu)
    if "DatetimeUTC" in df.columns:
        if use_gpu and GPU_AVAILABLE:
            df["DatetimeUTC"] = cudf.to_datetime(df["DatetimeUTC"], utc=True)
            df = df.set_index("DatetimeUTC").sort_index()
        else:
            df["DatetimeUTC"] = pd.to_datetime(df["DatetimeUTC"], utc=True)
            df.set_index("DatetimeUTC", inplace=True)
            df.sort_index(inplace=True)
    return df


def load_balancing(years: Optional[object] = None,
                   use_gpu: bool = False) -> pd.DataFrame:
    """Load balancing services cost data.

    ``years`` uses the same semantics as :func:`load_weather`.  The
    returned frame has ``DatetimeUTC`` column unchanged (some analyses
    work with the raw settlement date/periods).
    """
    df = _load_parquet(BALANCING_PARQUET, years=years, use_gpu=use_gpu)
    # note: don't change index automatically, allow caller to work with
    # settlement dates/periods if needed
    return df


def load_gridwatch(years: Optional[object] = None,
                   use_gpu: bool = False) -> pd.DataFrame:
    """Load GridWatch snapshot dataset, optionally restricting to years.

    The parquet partitions contain a ``timestamp`` column which is
    converted to a UTC index.
    """
    df = _load_parquet(GRIDWATCH_PARQUET, years=years, use_gpu=use_gpu)
    if "timestamp" in df.columns:
        if use_gpu and GPU_AVAILABLE:
            df["timestamp"] = cudf.to_datetime(df["timestamp"], utc=True)
            df = df.set_index("timestamp").sort_index()
        else:
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
            df.set_index("timestamp", inplace=True)
            df.sort_index(inplace=True)
    return df


def merge_all(gen: pd.DataFrame,
              weather: pd.DataFrame,
              freq: pd.DataFrame,
              inertia: Optional[pd.DataFrame] = None,
              demand: Optional[pd.DataFrame] = None,
              balancing: Optional[pd.DataFrame] = None,
              gridwatch: Optional[pd.DataFrame] = None,
              how: str = "inner",
              use_gpu: bool = False) -> pd.DataFrame:
    """Join the various data sources together on a common timestamp index.

    The first three arguments (``gen``/``weather``/``freq``) are
    required; the others are optional.  Any DataFrame supplied will be
    concatenated along the column axis using ``pandas.concat`` (or the
    cudf/dask-cudf equivalent when ``use_gpu``).

    - gen: generation data (half‑hourly)
    - weather: observation data (hourly)
    - freq: frequency readings (second/quarter‑second)
    - inertia: optional inertia values (hourly)
    - demand: optional demand time series (settlement periods)
    - balancing: optional balancing services costs
    - gridwatch: optional GridWatch snapshots

    The caller should resample outside this function if a particular
    temporal resolution is required.
    """
    dfs = [gen, weather, freq]
    for optional in (inertia, demand, balancing, gridwatch):
        if optional is not None:
            dfs.append(optional)
    if use_gpu and GPU_AVAILABLE:
        # cudf.concat/dask_cudf.concat is used instead of pandas
        try:
            merged = cudf.concat(dfs, axis=1, join=how)
        except Exception:
            merged = dask_cudf.concat(dfs, axis=1, join=how)
    else:
        merged = pd.concat(dfs, axis=1, join=how)
    return merged


def add_datetime_features(df: pd.DataFrame) -> pd.DataFrame:
    """Create hour, day, month and season columns from the index."""
    out = df.copy()
    idx = out.index
    out["hour"] = idx.hour
    out["dayofweek"] = idx.dayofweek
    out["month"] = idx.month
    out["season"] = ((out["month"] % 12) + 3) // 3
    return out


def label_frequency_events(df: pd.DataFrame, low: float = 49.95,
                            high: float = 50.05) -> pd.DataFrame:
    """Add a binary column `freq_event` when frequency leaves [low,high]."""
    out = df.copy()
    if "Value" not in out.columns:
        return out
    out["freq_event"] = ((out["Value"] < low) | (out["Value"] > high)).astype(int)
    return out
