from __future__ import annotations

import asyncio
import json
import re
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from fastapi import FastAPI, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

ROOT = Path(__file__).resolve().parents[2]
APP_DIR = Path(__file__).resolve().parent
REPLY_DIR = ROOT / 'Applications' / 'RoCoF-Reply'

FREQUENCY_PARQUET = ROOT / 'DataSources' / 'NESO' / 'Frequency' / 'Parquet'
DEMAND_PARQUET = ROOT / 'DataSources' / 'NESO' / 'DemandData' / 'Parquet'
INERTIA_PARQUET = ROOT / 'DataSources' / 'NESO' / 'Inertia' / 'Parquet'
GEN_PARQUET = ROOT / 'DataSources' / 'NESO' / 'HistoricalGenerationData' / 'Parquet'
WEATHER_PARQUET = ROOT / 'DataSources' / 'Weather' / 'Parquet'
NONBM_INSTR_PARQUET = ROOT / 'DataSources' / 'NESO' / 'NonBM_AncillaryServiceDispatchPlatformInstructions' / 'Parquet'
NONBM_WINDOW_PRICE_PARQUET = ROOT / 'DataSources' / 'NESO' / 'NonBM_AncillaryServiceDispatchPlatformWindowPrices' / 'Parquet'
BSAD_AGG_PARQUET = ROOT / 'DataSources' / 'NESO' / 'BSAD_AggregatedData' / 'Parquet'
BSAD_DISS_PARQUET = ROOT / 'DataSources' / 'NESO' / 'BSAD_DissAggregatedData' / 'Parquet'
BSAD_FWD_PARQUET = ROOT / 'DataSources' / 'NESO' / 'BSAD_ForwardContracts' / 'Parquet'
TRANSMISSION_LOSSES_PARQUET = ROOT / 'DataSources' / 'NESO' / 'TransmissionLosses' / 'Parquet'
ORPS_REACTIVE_POWER_PARQUET = ROOT / 'DataSources' / 'NESO' / 'ORPS_ReactivePowerService' / 'Parquet'
NATIONAL_GRID_PARQUET = ROOT / 'DataSources' / 'NationalGrid' / 'Parquet'

FUEL_COLS = [
    'GAS', 'COAL', 'NUCLEAR', 'WIND', 'WIND_EMB', 'HYDRO',
    'IMPORTS', 'BIOMASS', 'OTHER', 'SOLAR', 'STORAGE',
]
INTERCONNECTOR_COLS = [
    'IFA_FLOW', 'IFA2_FLOW', 'BRITNED_FLOW', 'MOYLE_FLOW', 'EAST_WEST_FLOW',
    'NEMO_FLOW', 'NSL_FLOW', 'ELECLINK_FLOW', 'VIKING_FLOW', 'GREENLINK_FLOW',
]

FREQ_FILE_PATTERN = re.compile(r'f-(\d{4})-(\d{1,2})\.parquet$', re.IGNORECASE)
TIMESTAMP_COLUMN_CANDIDATES = ['timestamp_utc', 'Timestamp', 'ValueDate', 'time', 'Date', 'DatetimeUTC']
TIMESTAMP_COLUMN_SET = set(TIMESTAMP_COLUMN_CANDIDATES)

# TEMP TEST FALLBACK
# Set this to False (or comment out fallback calls in _snapshot_master_sites)
# once the merged National Grid sources always provide aligned site rows.
ENABLE_NG_MASTER_TEST_FALLBACK = True
SITE_TABLE_CACHE_MAX_BYTES = 128 * 1024 * 1024

_site_table_cache: OrderedDict[str, tuple[int, dict]] = OrderedDict()
_site_table_cache_bytes = 0


@dataclass
class AuxFrameTable:
    ts_ns: np.ndarray
    records: List[dict]


@dataclass
class AuxSnapshotTable:
    ts_ns: np.ndarray
    records: List[dict]
    columns: List[str]
    label: str


@dataclass
class StreamContext:
    generation: AuxFrameTable
    demand: AuxFrameTable
    inertia: AuxFrameTable
    weather: AuxFrameTable
    transmission_losses: AuxFrameTable
    transmission_losses_series: dict
    table_nonbm_instructions: AuxSnapshotTable
    table_nonbm_window_prices: AuxSnapshotTable
    table_bsad_aggregated: AuxSnapshotTable
    table_bsad_dissaggregated: AuxSnapshotTable
    table_bsad_forward: AuxSnapshotTable
    table_obp_source_1: AuxSnapshotTable
    table_obp_source_2: AuxSnapshotTable
    table_orps_reactive_power: AuxSnapshotTable
    table_transmission_losses_monthly: AuxSnapshotTable
    table_transmission_losses_financial_year: AuxSnapshotTable
    table_ng_live_primary_sites: Dict[str, AuxSnapshotTable]
    table_ng_live_gsp_sites: Dict[str, AuxSnapshotTable]
    table_ng_bsp_sites: Dict[str, AuxSnapshotTable]
    cursor_ng_live_primary_sites: Dict[str, int]
    cursor_ng_live_gsp_sites: Dict[str, int]
    cursor_ng_bsp_sites: Dict[str, int]
    ng_set_period_ns: Dict[str, int]
    ng_set_next_update_ns: Dict[str, int]


@dataclass
class StreamRuntime:
    fps: int = 1


app = FastAPI(title='RoCoF-App Stream', version='1.0')
app.mount('/static', StaticFiles(directory=APP_DIR), name='static')

_context: Optional[StreamContext] = None
_context_year: Optional[int] = None


def _to_utc_series(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors='coerce', utc=True)


def _site_table_cache_clear() -> None:
    global _site_table_cache_bytes
    _site_table_cache.clear()
    _site_table_cache_bytes = 0


def _site_table_cache_get(key: str) -> Optional[dict]:
    cached = _site_table_cache.get(key)
    if cached is None:
        return None
    size, payload = cached
    _site_table_cache.move_to_end(key)
    return payload


def _site_table_cache_put(key: str, payload: dict) -> None:
    global _site_table_cache_bytes
    try:
        size = len(json.dumps(payload, separators=(',', ':'), ensure_ascii=False).encode('utf-8'))
    except Exception:
        return

    if size > SITE_TABLE_CACHE_MAX_BYTES:
        return

    existing = _site_table_cache.pop(key, None)
    if existing is not None:
        _site_table_cache_bytes -= existing[0]

    _site_table_cache[key] = (size, payload)
    _site_table_cache_bytes += size

    while _site_table_cache_bytes > SITE_TABLE_CACHE_MAX_BYTES and _site_table_cache:
        _, (evicted_size, _) = _site_table_cache.popitem(last=False)
        _site_table_cache_bytes -= evicted_size


def _bucket_period_seconds(sample_seconds: float) -> int:
    if not np.isfinite(sample_seconds) or sample_seconds <= 0:
        return 1800
    if sample_seconds <= 45 * 60:
        return 30 * 60
    if sample_seconds <= 90 * 60:
        return 60 * 60
    if sample_seconds <= 6 * 3600:
        return 6 * 3600
    if sample_seconds <= 12 * 3600:
        return 12 * 3600
    return 24 * 3600


def _estimate_set_period_ns(table_by_site: Dict[str, AuxSnapshotTable], default_seconds: int = 1800) -> int:
    intervals: List[float] = []
    site_items = list(table_by_site.items())[:12]
    for _, table in site_items:
        if table.ts_ns.size < 2:
            continue
        ts = table.ts_ns
        step_count = min(20, int(ts.size) - 1)
        if step_count <= 0:
            continue
        diffs = np.diff(ts[: step_count + 1]) / 1_000_000_000.0
        for diff in diffs:
            if np.isfinite(diff) and diff > 0:
                intervals.append(float(diff))

    if not intervals:
        return int(default_seconds * 1_000_000_000)

    median_seconds = float(np.median(np.array(intervals, dtype=np.float64)))
    return int(_bucket_period_seconds(median_seconds) * 1_000_000_000)


def _epoch_ns(series: pd.Series) -> np.ndarray:
    s = _to_utc_series(series).dropna()
    s = s.dt.tz_convert('UTC').dt.tz_localize(None).astype('datetime64[ns]')
    return s.astype('int64').to_numpy()


def _load_generation() -> AuxFrameTable:
    files = sorted(GEN_PARQUET.rglob('*.parquet'))
    if not files:
        return AuxFrameTable(ts_ns=np.array([], dtype=np.int64), records=[])

    df = pd.read_parquet(files[0], columns=['DATETIME', 'GENERATION', *FUEL_COLS])
    df['DATETIME'] = _to_utc_series(df['DATETIME'])
    df = df.dropna(subset=['DATETIME']).sort_values('DATETIME').reset_index(drop=True)

    records: List[dict] = []
    for row in df.itertuples(index=False):
        record = {
            'total_generation_mw': float(getattr(row, 'GENERATION', np.nan)),
        }
        for fuel in FUEL_COLS:
            record[fuel] = float(getattr(row, fuel, np.nan))
        records.append(record)

    ts_ns = _epoch_ns(df['DATETIME'])
    return AuxFrameTable(ts_ns=ts_ns, records=records)


def _partition_year(file_path: Path) -> Optional[int]:
    for part in file_path.parts:
        if part.startswith('year='):
            try:
                return int(part.split('=', 1)[1])
            except Exception:
                return None
    return None


def _filter_files_by_year_window(files: List[Path], start_ts: pd.Timestamp, window_years: int = 1) -> List[Path]:
    lo = int(start_ts.year) - window_years
    hi = int(start_ts.year) + window_years
    filtered: List[Path] = []
    for file in files:
        year = _partition_year(file)
        if year is None or (lo <= year <= hi):
            filtered.append(file)
    return filtered or files


def _load_demand(start_ts: pd.Timestamp) -> AuxFrameTable:
    files = sorted(DEMAND_PARQUET.rglob('*.parquet'))
    if not files:
        return AuxFrameTable(ts_ns=np.array([], dtype=np.int64), records=[])

    files = _filter_files_by_year_window(files, start_ts=start_ts, window_years=1)

    frames = []
    required_cols = ['DatetimeUTC', 'ND', 'TSD', *INTERCONNECTOR_COLS]
    for file in files:
        available_cols = set(pq.ParquetFile(file).schema.names)
        read_cols = [col for col in required_cols if col in available_cols]
        if 'DatetimeUTC' not in read_cols:
            continue

        file_df = pd.read_parquet(file, columns=read_cols)
        for missing_col in required_cols:
            if missing_col not in file_df.columns:
                file_df[missing_col] = np.nan
        file_df = file_df[required_cols]
        frames.append(file_df)

    if not frames:
        return AuxFrameTable(ts_ns=np.array([], dtype=np.int64), records=[])

    df = pd.concat(frames, ignore_index=True)
    df['DatetimeUTC'] = _to_utc_series(df['DatetimeUTC'])
    df = df.dropna(subset=['DatetimeUTC']).sort_values('DatetimeUTC').reset_index(drop=True)

    records = []
    for row in df.itertuples(index=False):
        nd = _safe_float(getattr(row, 'ND', np.nan))
        tsd = _safe_float(getattr(row, 'TSD', np.nan))
        demand = nd if np.isfinite(nd) else tsd
        record = {'estimated_demand_mw': demand}
        for col in INTERCONNECTOR_COLS:
            record[col] = _safe_float(getattr(row, col, np.nan), default=np.nan)
        net_flow = float(np.nansum([record[c] for c in INTERCONNECTOR_COLS]))
        record['NET_INTERCONNECTOR_FLOW'] = net_flow
        records.append(record)

    ts_ns = _epoch_ns(df['DatetimeUTC'])
    return AuxFrameTable(ts_ns=ts_ns, records=records)


def _load_inertia(start_ts: pd.Timestamp) -> AuxFrameTable:
    files = sorted(INERTIA_PARQUET.rglob('*.parquet'))
    if not files:
        return AuxFrameTable(ts_ns=np.array([], dtype=np.int64), records=[])

    files = _filter_files_by_year_window(files, start_ts=start_ts, window_years=1)

    frames = []
    for file in files:
        frames.append(pd.read_parquet(file, columns=['DatetimeUTC', 'Outturn Inertia']))
    df = pd.concat(frames, ignore_index=True)
    df['DatetimeUTC'] = _to_utc_series(df['DatetimeUTC'])
    df = df.dropna(subset=['DatetimeUTC']).sort_values('DatetimeUTC').reset_index(drop=True)

    records = [{'outturn_inertia': float(v)} for v in df['Outturn Inertia'].to_numpy()]
    ts_ns = _epoch_ns(df['DatetimeUTC'])
    return AuxFrameTable(ts_ns=ts_ns, records=records)


def _load_weather(start_ts: pd.Timestamp) -> AuxFrameTable:
    files = sorted(WEATHER_PARQUET.rglob('*.parquet'))
    if not files:
        return AuxFrameTable(ts_ns=np.array([], dtype=np.int64), records=[])

    files = _filter_files_by_year_window(files, start_ts=start_ts, window_years=1)

    frames = []
    required_cols = ['Date', 'Temperature_C', 'Wind_Speed_100m_kph', 'Solar_Radiation_W_m2']
    for file in files:
        available_cols = set(pq.ParquetFile(file).schema.names)
        read_cols = [col for col in required_cols if col in available_cols]
        if 'Date' not in read_cols:
            continue

        file_df = pd.read_parquet(file, columns=read_cols)
        for missing_col in required_cols:
            if missing_col not in file_df.columns:
                file_df[missing_col] = np.nan
        file_df = file_df[required_cols]
        frames.append(file_df)

    if not frames:
        return AuxFrameTable(ts_ns=np.array([], dtype=np.int64), records=[])

    df = pd.concat(frames, ignore_index=True)
    df['Date'] = _to_utc_series(df['Date'])
    df = df.dropna(subset=['Date']).sort_values('Date').reset_index(drop=True)

    records = []
    for row in df.itertuples(index=False):
        record = {
            'temperature_c': _safe_float(getattr(row, 'Temperature_C', np.nan)),
            'wind_speed_100m_kph': _safe_float(getattr(row, 'Wind_Speed_100m_kph', np.nan)),
            'solar_radiation_w_m2': _safe_float(getattr(row, 'Solar_Radiation_W_m2', np.nan)),
        }
        records.append(record)

    ts_ns = _epoch_ns(df['Date'])
    return AuxFrameTable(ts_ns=ts_ns, records=records)


def _parse_month_label_to_utc(month_label: object) -> pd.Timestamp:
    if month_label is None:
        return pd.NaT
    text = str(month_label).strip()
    if not text:
        return pd.NaT
    parsed = pd.to_datetime(text, format='%b-%y', errors='coerce', utc=True)
    if pd.notna(parsed):
        return parsed
    return pd.to_datetime(text, errors='coerce', utc=True)


def _load_transmission_losses(start_ts: pd.Timestamp) -> tuple[AuxFrameTable, dict]:
    files = sorted(TRANSMISSION_LOSSES_PARQUET.rglob('*.parquet'))
    if not files:
        return AuxFrameTable(ts_ns=np.array([], dtype=np.int64), records=[]), {
            'timestamps': [],
            'gb_totals': [],
            'nget': [],
            'spt': [],
            'shetl': [],
        }

    monthly_files = [f for f in files if 'monthly' in f.name.lower()]
    fy_files = [f for f in files if 'financial' in f.name.lower()]

    monthly_df = pd.DataFrame()
    if monthly_files:
        monthly_df = pd.concat([pd.read_parquet(f) for f in monthly_files], ignore_index=True)

    fy_df = pd.DataFrame()
    if fy_files:
        fy_df = pd.concat([pd.read_parquet(f) for f in fy_files], ignore_index=True)

    if monthly_df.empty:
        return AuxFrameTable(ts_ns=np.array([], dtype=np.int64), records=[]), {
            'timestamps': [],
            'gb_totals': [],
            'nget': [],
            'spt': [],
            'shetl': [],
        }

    monthly_df['MonthTs'] = monthly_df.get('Month', pd.Series(dtype='object')).apply(_parse_month_label_to_utc)
    monthly_df = monthly_df.dropna(subset=['MonthTs']).sort_values('MonthTs').reset_index(drop=True)
    if monthly_df.empty:
        return AuxFrameTable(ts_ns=np.array([], dtype=np.int64), records=[]), {
            'timestamps': [],
            'gb_totals': [],
            'nget': [],
            'spt': [],
            'shetl': [],
        }

    fy_lookup: Dict[str, dict] = {}
    if not fy_df.empty:
        for row in fy_df.itertuples(index=False):
            fy_key = str(getattr(row, 'Financial Year', '') or '').strip()
            if not fy_key:
                continue
            fy_lookup[fy_key] = {
                'sum_nget': _safe_float(getattr(row, 'Sum of NGET', np.nan), default=np.nan),
                'sum_spt': _safe_float(getattr(row, 'Sum of SPT', np.nan), default=np.nan),
                'sum_shetl': _safe_float(getattr(row, 'Sum of SHETL', np.nan), default=np.nan),
                'sum_gb_totals': _safe_float(getattr(row, 'Sum of GB totals', np.nan), default=np.nan),
            }

    records: List[dict] = []
    for row in monthly_df.itertuples(index=False):
        fy_key = str(getattr(row, 'Financial Year', '') or '').strip()
        fy_record = fy_lookup.get(fy_key, {})
        records.append(
            {
                'transmission_financial_year': fy_key,
                'transmission_month': str(getattr(row, 'Month', '') or '').strip(),
                'transmission_nget': _safe_float(getattr(row, 'NGET', np.nan), default=np.nan),
                'transmission_spt': _safe_float(getattr(row, 'SPT', np.nan), default=np.nan),
                'transmission_shetl': _safe_float(getattr(row, 'SHETL', np.nan), default=np.nan),
                'transmission_gb_totals': _safe_float(getattr(row, 'GB totals', np.nan), default=np.nan),
                'transmission_sum_nget': _safe_float(fy_record.get('sum_nget', np.nan), default=np.nan),
                'transmission_sum_spt': _safe_float(fy_record.get('sum_spt', np.nan), default=np.nan),
                'transmission_sum_shetl': _safe_float(fy_record.get('sum_shetl', np.nan), default=np.nan),
                'transmission_sum_gb_totals': _safe_float(fy_record.get('sum_gb_totals', np.nan), default=np.nan),
            }
        )

    ts_ns = _epoch_ns(monthly_df['MonthTs'])
    series = {
        'timestamps': [pd.Timestamp(ts).isoformat() for ts in monthly_df['MonthTs'].to_list()],
        'gb_totals': [
            _safe_float(v, default=np.nan) for v in monthly_df.get('GB totals', pd.Series(dtype='float64')).to_numpy()
        ],
        'nget': [
            _safe_float(v, default=np.nan) for v in monthly_df.get('NGET', pd.Series(dtype='float64')).to_numpy()
        ],
        'spt': [
            _safe_float(v, default=np.nan) for v in monthly_df.get('SPT', pd.Series(dtype='float64')).to_numpy()
        ],
        'shetl': [
            _safe_float(v, default=np.nan) for v in monthly_df.get('SHETL', pd.Series(dtype='float64')).to_numpy()
        ],
    }

    return AuxFrameTable(ts_ns=ts_ns, records=records), series


def _load_transmission_losses_monthly_table() -> AuxSnapshotTable:
    files = sorted(TRANSMISSION_LOSSES_PARQUET.rglob('*monthly*.parquet'))
    if not files:
        return AuxSnapshotTable(ts_ns=np.array([], dtype=np.int64), records=[], columns=[], label='Transmission Losses Monthly')

    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    if df.empty:
        return AuxSnapshotTable(ts_ns=np.array([], dtype=np.int64), records=[], columns=[], label='Transmission Losses Monthly')

    df['MonthTs'] = df.get('Month', pd.Series(dtype='object')).apply(_parse_month_label_to_utc)
    df = df.dropna(subset=['MonthTs']).sort_values('MonthTs').reset_index(drop=True)
    if df.empty:
        return AuxSnapshotTable(ts_ns=np.array([], dtype=np.int64), records=[], columns=[], label='Transmission Losses Monthly')

    columns = ['Financial Year', 'Month', 'NGET', 'SPT', 'SHETL', 'GB totals']
    columns = [col for col in columns if col in df.columns]
    records: List[dict] = []
    for row in df[columns].itertuples(index=False, name=None):
        records.append({col: _to_iso_string(val) for col, val in zip(columns, row)})

    ts_ns = _epoch_ns(df['MonthTs'])
    return AuxSnapshotTable(ts_ns=ts_ns, records=records, columns=columns, label='Transmission Losses Monthly')


def _load_transmission_losses_financial_year_table() -> AuxSnapshotTable:
    files = sorted(TRANSMISSION_LOSSES_PARQUET.rglob('*financial*.parquet'))
    if not files:
        return AuxSnapshotTable(ts_ns=np.array([], dtype=np.int64), records=[], columns=[], label='Transmission Losses Financial Year')

    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    if df.empty:
        return AuxSnapshotTable(ts_ns=np.array([], dtype=np.int64), records=[], columns=[], label='Transmission Losses Financial Year')

    fy_col = 'Financial Year'
    if fy_col not in df.columns:
        return AuxSnapshotTable(ts_ns=np.array([], dtype=np.int64), records=[], columns=[], label='Transmission Losses Financial Year')

    def fy_to_ts(value: object) -> pd.Timestamp:
        text = str(value or '').strip()
        if not text:
            return pd.NaT
        m = re.search(r'(?:19|20)\d{2}', text)
        if not m:
            return pd.NaT
        return pd.Timestamp(f"{m.group(0)}-04-01T00:00:00Z")

    df['FinancialYearTs'] = df[fy_col].apply(fy_to_ts)
    df = df.dropna(subset=['FinancialYearTs']).sort_values('FinancialYearTs').reset_index(drop=True)
    if df.empty:
        return AuxSnapshotTable(ts_ns=np.array([], dtype=np.int64), records=[], columns=[], label='Transmission Losses Financial Year')

    columns = ['Financial Year', 'Sum of NGET', 'Sum of SPT', 'Sum of SHETL', 'Sum of GB totals']
    columns = [col for col in columns if col in df.columns]
    records: List[dict] = []
    for row in df[columns].itertuples(index=False, name=None):
        records.append({col: _to_iso_string(val) for col, val in zip(columns, row)})

    ts_ns = _epoch_ns(df['FinancialYearTs'])
    return AuxSnapshotTable(ts_ns=ts_ns, records=records, columns=columns, label='Transmission Losses Financial Year')


def _market_table_sources() -> List[tuple[str, Optional[Path], List[str], List[str]]]:
    return [
        (
            'OBP NonBM Physical Notifications',
            ROOT / 'DataSources' / 'NESO' / 'OBP_NonBMPhysicalNotifications' / 'Parquet',
            ['Time From', 'Time To', 'SettlementDate', 'Date', 'DatetimeUTC', 'StartDateTime'],
            ['Data', 'Unit ID', 'Time From', 'Level From', 'Time To', 'Level To'],
        ),
        (
            'OBP Reserve Availability',
            ROOT / 'DataSources' / 'NESO' / 'OBP_ReserveAvailability' / 'Parquet',
            ['Time From', 'Time To', 'SettlementDate', 'Date', 'DatetimeUTC', 'StartDateTime'],
            ['Data', 'Unit ID', 'Service ID', 'Pair ID', 'Time From', 'Time To', 'Availability Power', 'Utilisation Price'],
        ),
        (
            'ORPS Reactive Power Service',
            ORPS_REACTIVE_POWER_PARQUET,
            ['Month-Year', 'Time From', 'Time To', 'Date', 'DatetimeUTC', 'Month', 'Year'],
            ['Company', 'Unit', 'Default/Market', 'Location', 'Month-Year', 'LEAD', 'LAG', 'TOTAL', 'Month', 'Year'],
        ),
    ]


def _to_iso_string(value: object) -> object:
    if isinstance(value, pd.Timestamp):
        if value.tzinfo is None:
            return value.tz_localize('UTC').isoformat()
        return value.isoformat()
    if isinstance(value, np.datetime64):
        ts = pd.Timestamp(value)
        if ts.tzinfo is None:
            ts = ts.tz_localize('UTC')
        return ts.isoformat()
    if isinstance(value, np.generic):
        return value.item()
    return value


def _choose_timestamp_column(columns: List[str], preferred: List[str]) -> Optional[str]:
    for col in preferred:
        if col in columns:
            return col
    lowered = {col.lower(): col for col in columns}
    for key, col in lowered.items():
        if any(token in key for token in ('datetime', 'date', 'time', 'settlement')):
            return col
    return None


def _load_snapshot_table(
    parquet_root: Path,
    start_ts: pd.Timestamp,
    label: str,
    preferred_time_cols: List[str],
    preferred_display_cols: List[str],
    max_cols: int = 8,
) -> AuxSnapshotTable:
    files = sorted(parquet_root.rglob('*.parquet'))
    if not files:
        return AuxSnapshotTable(ts_ns=np.array([], dtype=np.int64), records=[], columns=[], label=label)

    files = _filter_files_by_year_window(files, start_ts=start_ts, window_years=1)
    frames: List[pd.DataFrame] = []
    selected_cols: List[str] = []
    ts_col: Optional[str] = None

    for file in files:
        schema_cols = list(pq.ParquetFile(file).schema.names)
        if not schema_cols:
            continue

        local_ts_col = _choose_timestamp_column(schema_cols, preferred_time_cols)
        local_selected = []
        if local_ts_col:
            local_selected.append(local_ts_col)
        for col in preferred_display_cols:
            if col in schema_cols and col not in local_selected:
                local_selected.append(col)
        for col in schema_cols:
            if len(local_selected) >= max_cols:
                break
            if col not in local_selected:
                local_selected.append(col)

        if not local_ts_col:
            continue

        file_df = pd.read_parquet(file, columns=local_selected)
        frames.append(file_df)
        if not selected_cols:
            selected_cols = local_selected
            ts_col = local_ts_col

    if not frames or not ts_col:
        return AuxSnapshotTable(ts_ns=np.array([], dtype=np.int64), records=[], columns=[], label=label)

    df = pd.concat(frames, ignore_index=True)
    if ts_col not in df.columns:
        return AuxSnapshotTable(ts_ns=np.array([], dtype=np.int64), records=[], columns=[], label=label)

    df[ts_col] = _to_utc_series(df[ts_col])
    df = df.dropna(subset=[ts_col]).sort_values(ts_col).reset_index(drop=True)
    if df.empty:
        return AuxSnapshotTable(ts_ns=np.array([], dtype=np.int64), records=[], columns=[], label=label)

    display_cols = [col for col in selected_cols if col in df.columns]
    records = []
    for row in df[display_cols].itertuples(index=False, name=None):
        rec = {col: _to_iso_string(val) for col, val in zip(display_cols, row)}
        records.append(rec)

    ts_ns = _epoch_ns(df[ts_col])
    return AuxSnapshotTable(ts_ns=ts_ns, records=records, columns=display_cols, label=label)


def _snapshot_rows(table: AuxSnapshotTable, target_ns: int, row_count: int = 8) -> dict:
    if table.ts_ns.size == 0 or not table.records:
        return {
            'label': table.label,
            'columns': [],
            'rows': [],
            'has_data': False,
            'message': 'No data available',
        }

    idx = int(np.searchsorted(table.ts_ns, target_ns, side='left'))
    if idx >= table.ts_ns.size:
        idx = table.ts_ns.size - 1
    if idx > 0 and abs(int(table.ts_ns[idx - 1]) - target_ns) < abs(int(table.ts_ns[idx]) - target_ns):
        idx -= 1

    half = row_count // 2
    start = max(0, idx - half)
    end = min(len(table.records), start + row_count)
    start = max(0, end - row_count)

    rows = table.records[start:end]
    return {
        'label': table.label,
        'columns': table.columns,
        'rows': rows,
        'has_data': True,
        'message': '',
    }


def _load_site_snapshot_tables(
    parquet_path: Path,
    start_ts: pd.Timestamp,
    label: str,
    preferred_time_cols: List[str],
    preferred_site_cols: List[str],
    preferred_display_cols: List[str],
    max_cols: int = 10,
) -> Dict[str, AuxSnapshotTable]:
    files = [parquet_path] if parquet_path.exists() else []
    if not files:
        return {}

    frames: List[pd.DataFrame] = []
    selected_cols: List[str] = []
    ts_col: Optional[str] = None
    site_col: Optional[str] = None

    for file in files:
        schema_cols = list(pq.ParquetFile(file).schema.names)
        if not schema_cols:
            continue

        local_ts_col = _choose_timestamp_column(schema_cols, preferred_time_cols)
        if not local_ts_col:
            continue

        local_site_col = None
        for col in preferred_site_cols:
            if col in schema_cols:
                local_site_col = col
                break
        if local_site_col is None:
            local_site_col = 'location_hint' if 'location_hint' in schema_cols else None

        local_selected: List[str] = [local_ts_col]
        if local_site_col and local_site_col not in local_selected:
            local_selected.append(local_site_col)
        for col in preferred_display_cols:
            if col in schema_cols and col not in local_selected:
                local_selected.append(col)
        for col in schema_cols:
            if len(local_selected) >= max_cols:
                break
            if col not in local_selected:
                local_selected.append(col)

        file_df = pd.read_parquet(file, columns=local_selected)
        file_df['__source_ts__'] = _to_utc_series(file_df[local_ts_col])
        if local_site_col and local_site_col in file_df.columns:
            file_df['__site__'] = file_df[local_site_col]
        else:
            file_df['__site__'] = 'All Sites'

        frames.append(file_df)
        if not selected_cols:
            selected_cols = local_selected
            ts_col = local_ts_col
            site_col = local_site_col

    if not frames or not ts_col:
        return {}

    df = pd.concat(frames, ignore_index=True)
    df = df.dropna(subset=['__source_ts__']).sort_values('__source_ts__').reset_index(drop=True)
    if df.empty:
        return {}

    display_cols = [col for col in selected_cols if col in df.columns and col != site_col]
    grouped: Dict[str, AuxSnapshotTable] = {}

    for site_value, site_df in df.groupby('__site__', dropna=False):
        site_label = str(site_value).strip() if site_value is not None else ''
        if not site_label or site_label.lower() == 'nan':
            site_label = 'Unknown Site'

        records: List[dict] = []
        for row in site_df[display_cols].itertuples(index=False, name=None):
            records.append({col: _to_iso_string(val) for col, val in zip(display_cols, row)})

        ts_ns = _epoch_ns(site_df['__source_ts__'])
        if ts_ns.size == 0 or not records:
            continue

        grouped[site_label] = AuxSnapshotTable(
            ts_ns=ts_ns,
            records=records,
            columns=display_cols,
            label=f'{label} · {site_label}',
        )

    return grouped


def _nearest_snapshot_row(table: AuxSnapshotTable, target_ns: int) -> tuple[Optional[dict], Optional[int]]:
    if table.ts_ns.size == 0 or not table.records:
        return None, None

    idx = int(np.searchsorted(table.ts_ns, target_ns, side='left'))
    if idx >= table.ts_ns.size:
        idx = table.ts_ns.size - 1
    if idx > 0 and abs(int(table.ts_ns[idx - 1]) - target_ns) < abs(int(table.ts_ns[idx]) - target_ns):
        idx -= 1

    if idx < 0 or idx >= len(table.records):
        return None, None
    return table.records[idx], int(table.ts_ns[idx])


def _nearest_snapshot_row_with_cursor(
    table: AuxSnapshotTable,
    target_ns: int,
    cursor_idx: Optional[int],
) -> tuple[Optional[dict], Optional[int], Optional[int]]:
    if table.ts_ns.size == 0 or not table.records:
        return None, None, None

    n = int(table.ts_ns.size)
    idx: int

    if cursor_idx is not None and 0 <= cursor_idx < n and int(table.ts_ns[cursor_idx]) <= target_ns:
        idx = int(cursor_idx)
        while idx + 1 < n and int(table.ts_ns[idx + 1]) <= target_ns:
            idx += 1
        best = idx
        if idx + 1 < n and abs(int(table.ts_ns[idx + 1]) - target_ns) < abs(int(table.ts_ns[idx]) - target_ns):
            best = idx + 1
        if best < 0 or best >= len(table.records):
            return None, None, None
        return table.records[best], int(table.ts_ns[best]), best

    idx = int(np.searchsorted(table.ts_ns, target_ns, side='left'))
    if idx >= n:
        idx = n - 1
    if idx > 0 and abs(int(table.ts_ns[idx - 1]) - target_ns) < abs(int(table.ts_ns[idx]) - target_ns):
        idx -= 1
    if idx < 0 or idx >= len(table.records):
        return None, None, None
    return table.records[idx], int(table.ts_ns[idx]), idx


def _extract_row_timestamp(row: dict) -> str:
    for col in TIMESTAMP_COLUMN_CANDIDATES:
        value = row.get(col)
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        parsed = pd.to_datetime(text, utc=True, errors='coerce')
        if pd.notna(parsed):
            return parsed.isoformat()
        return text
    return ''


def _infer_nominal_kv(site_name: str) -> int:
    text = str(site_name or '').strip().lower()
    if not text:
        return 33
    match = re.search(r'(\d{2,3})\s*k\s*v', text)
    if match:
        try:
            return int(match.group(1))
        except Exception:
            return 33
    return 33


def _site_seed(site_name: str) -> int:
    text = str(site_name or '').strip().lower()
    if not text:
        return 17
    return max(1, sum(ord(ch) for ch in text) % 997)


def _build_test_fallback_row(site_name: str, target_ns: int, source_set: str = 'synthetic_test') -> dict:
    nominal_kv = _infer_nominal_kv(site_name)
    nominal_volts = float(nominal_kv) * 1000.0
    seed = _site_seed(site_name)

    mw = float(20 + (seed % 240))
    power_factor = 0.92 + ((seed % 7) * 0.01)
    mva = mw / power_factor if power_factor > 0 else mw
    mvar = float(np.sqrt(max((mva * mva) - (mw * mw), 0.0)))
    amps = (mva * 1_000_000.0) / (np.sqrt(3.0) * nominal_volts) if nominal_volts > 0 else np.nan

    ts = pd.Timestamp(target_ns, unit='ns', tz='UTC').isoformat()
    return {
        'site': site_name,
        'timestamp_utc': ts,
        'source_set': source_set,
        'Voltage Nominal (kV)': float(nominal_kv),
        'Volts': nominal_volts,
        'MVA': float(mva),
        'MVAr': float(mvar),
        'MW': float(mw),
        'Amps': float(amps),
        'Current Inst': float(amps),
        'MVA Inst': float(mva),
        'MVAr Inst': float(mvar),
        'MW Inst': float(mw),
        'Volts Inst': nominal_volts,
        'Fallback': 'Yes',
    }


def _snapshot_master_set(
    table_by_site: Dict[str, AuxSnapshotTable],
    target_ns: int,
    label: str,
    source_set: str,
    cursor_by_site: Dict[str, int],
    cache_bucket_ns: Optional[int] = None,
) -> dict:
    cache_key_ns = int(cache_bucket_ns if cache_bucket_ns is not None else target_ns)
    cache_key = f'{source_set}:{cache_key_ns}'
    cached = _site_table_cache_get(cache_key)
    if cached is not None:
        return cached

    all_sites = sorted(table_by_site.keys())
    if not all_sites and ENABLE_NG_MASTER_TEST_FALLBACK:
        all_sites = ['Test Site 11kV', 'Test Site 33kV', 'Test Site 62kV']
    elif not all_sites:
        return {
            'label': label,
            'has_data': False,
            'columns': [],
            'rows': [],
            'message': 'No data available',
        }

    rows: List[dict] = []
    dynamic_columns: List[str] = []

    for site_name in all_sites:
        table = table_by_site.get(site_name)
        row: Optional[dict]
        row_ns: Optional[int]
        cursor_idx = cursor_by_site.get(site_name)
        if table is not None:
            row, row_ns, resolved_idx = _nearest_snapshot_row_with_cursor(table, target_ns, cursor_idx)
            if resolved_idx is not None:
                cursor_by_site[site_name] = resolved_idx
        else:
            row, row_ns = None, None

        if (row is None or row_ns is None) and ENABLE_NG_MASTER_TEST_FALLBACK:
            output_row = _build_test_fallback_row(site_name, target_ns, source_set=source_set)
            for key in output_row.keys():
                if key not in ('site', 'timestamp_utc', 'source_set') and key not in dynamic_columns:
                    dynamic_columns.append(key)
            rows.append(output_row)
            continue

        if row is None or row_ns is None:
            continue

        output_row = {
            'site': site_name,
            'timestamp_utc': _extract_row_timestamp(row) or '--',
            'source_set': source_set,
        }

        for key, value in row.items():
            if key in TIMESTAMP_COLUMN_SET:
                continue
            output_row[key] = value
            if key not in dynamic_columns:
                dynamic_columns.append(key)

        rows.append(output_row)

    if not rows:
        if ENABLE_NG_MASTER_TEST_FALLBACK:
            fallback_sites = ['Test Site 11kV', 'Test Site 33kV', 'Test Site 62kV']
            for site_name in fallback_sites:
                output_row = _build_test_fallback_row(site_name, target_ns, source_set=source_set)
                for key in output_row.keys():
                    if key not in ('site', 'timestamp_utc', 'source_set') and key not in dynamic_columns:
                        dynamic_columns.append(key)
                rows.append(output_row)

        if not rows:
            return {
                'label': label,
                'has_data': False,
                'columns': [],
                'rows': [],
                'message': 'No aligned site rows available',
            }

    columns = ['site', 'timestamp_utc', 'source_set', *dynamic_columns]
    payload = {
        'label': label,
        'has_data': True,
        'columns': columns,
        'rows': rows,
        'message': '',
    }
    _site_table_cache_put(cache_key, payload)
    return payload


def _nearest_record(table: AuxFrameTable, target_ns: int, tolerance_s: int = 7200) -> Optional[dict]:
    if table.ts_ns.size == 0:
        return None

    idx = int(np.searchsorted(table.ts_ns, target_ns, side='left'))
    candidates = []
    if idx < table.ts_ns.size:
        candidates.append(idx)
    if idx > 0:
        candidates.append(idx - 1)
    if not candidates:
        return None

    best = min(candidates, key=lambda i: abs(int(table.ts_ns[i]) - target_ns))
    if abs(int(table.ts_ns[best]) - target_ns) > tolerance_s * 1_000_000_000:
        return None
    return table.records[best]


def _frequency_files_sorted() -> List[Path]:
    files = list(FREQUENCY_PARQUET.rglob('*.parquet'))

    def key_for(path: Path) -> tuple[int, int, str]:
        m = FREQ_FILE_PATTERN.search(path.name)
        if m:
            return (int(m.group(1)), int(m.group(2)), path.name)
        return (0, 0, path.name)

    return sorted(files, key=key_for)


def _frequency_bounds() -> tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
    files = _frequency_files_sorted()
    if not files:
        return None, None

    first_df = pd.read_parquet(files[0], columns=['Date'])
    last_df = pd.read_parquet(files[-1], columns=['Date'])

    first_series = _to_utc_series(first_df['Date']).dropna()
    last_series = _to_utc_series(last_df['Date']).dropna()

    min_ts = first_series.min() if not first_series.empty else None
    max_ts = last_series.max() if not last_series.empty else None
    return min_ts, max_ts


def _iter_frequency_rows(start_ts: pd.Timestamp) -> Iterator[tuple[pd.Timestamp, float]]:
    files = _frequency_files_sorted()
    if not files:
        return

    for file in files:
        m = FREQ_FILE_PATTERN.search(file.name)
        if m:
            year, month = int(m.group(1)), int(m.group(2))
            if (year, month) < (start_ts.year, start_ts.month):
                continue

        parquet_file = pq.ParquetFile(file)
        for batch in parquet_file.iter_batches(batch_size=200_000, columns=['Date', 'Value']):
            chunk = batch.to_pandas()
            chunk['Date'] = _to_utc_series(chunk['Date'])
            chunk['Value'] = pd.to_numeric(chunk['Value'], errors='coerce')
            chunk = chunk.dropna(subset=['Date', 'Value'])
            chunk = chunk[chunk['Date'] >= start_ts]

            if chunk.empty:
                continue

            for row in chunk.itertuples(index=False):
                yield getattr(row, 'Date'), float(getattr(row, 'Value'))


def _safe_float(value: object, default: float = np.nan) -> float:
    try:
        f = float(value)
        if np.isfinite(f):
            return f
    except Exception:
        pass
    return default


def _json_safe(value: object) -> object:
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [_json_safe(v) for v in value]
    if isinstance(value, float):
        return value if np.isfinite(value) else None
    if isinstance(value, np.floating):
        v = float(value)
        return v if np.isfinite(v) else None
    return value


def _build_frame(
    ts: pd.Timestamp,
    f_start: float,
    f_end: float,
    rocof: float,
    context: StreamContext,
    include_site_table_keys: Optional[set[str]] = None,
) -> dict:
    ts_ns = int(ts.value)
    g = _nearest_record(context.generation, ts_ns) or {}
    d = _nearest_record(context.demand, ts_ns) or {}
    i = _nearest_record(context.inertia, ts_ns, tolerance_s=86400 * 3650) or {}
    w = _nearest_record(context.weather, ts_ns) or {}
    tl = _nearest_record(context.transmission_losses, ts_ns, tolerance_s=86400 * 3650) or {}

    snapshot_tables = {
        'nonbm_instructions': _snapshot_rows(context.table_nonbm_instructions, ts_ns, row_count=8),
        'nonbm_window_prices': _snapshot_rows(context.table_nonbm_window_prices, ts_ns, row_count=8),
        'bsad_aggregated': _snapshot_rows(context.table_bsad_aggregated, ts_ns, row_count=8),
        'bsad_dissaggregated': _snapshot_rows(context.table_bsad_dissaggregated, ts_ns, row_count=8),
        'bsad_forward': _snapshot_rows(context.table_bsad_forward, ts_ns, row_count=8),
        'obp_source_1': _snapshot_rows(context.table_obp_source_1, ts_ns, row_count=8),
        'obp_source_2': _snapshot_rows(context.table_obp_source_2, ts_ns, row_count=8),
        'orps_reactive_power': _snapshot_rows(context.table_orps_reactive_power, ts_ns, row_count=8),
        'transmission_losses_monthly': _snapshot_rows(context.table_transmission_losses_monthly, ts_ns, row_count=8),
        'transmission_losses_financial_year': _snapshot_rows(context.table_transmission_losses_financial_year, ts_ns, row_count=8),
    }

    site_snapshot_tables = {}
    include_site_table_keys = include_site_table_keys or set()
    if 'nationalgrid_live_primary_master' in include_site_table_keys:
        period_ns = max(1, int(context.ng_set_period_ns.get('nationalgrid_live_primary_master', 1800 * 1_000_000_000)))
        bucket_ns = (ts_ns // period_ns) * period_ns
        site_snapshot_tables['nationalgrid_live_primary_master'] = _snapshot_master_set(
            context.table_ng_live_primary_sites,
            ts_ns,
            label='National Grid Live Primary (Site-Aligned)',
            source_set='live_primary',
            cursor_by_site=context.cursor_ng_live_primary_sites,
            cache_bucket_ns=bucket_ns,
        )
    if 'nationalgrid_live_gsp_master' in include_site_table_keys:
        period_ns = max(1, int(context.ng_set_period_ns.get('nationalgrid_live_gsp_master', 1800 * 1_000_000_000)))
        bucket_ns = (ts_ns // period_ns) * period_ns
        site_snapshot_tables['nationalgrid_live_gsp_master'] = _snapshot_master_set(
            context.table_ng_live_gsp_sites,
            ts_ns,
            label='National Grid Live GSP (Site-Aligned)',
            source_set='live_gsp',
            cursor_by_site=context.cursor_ng_live_gsp_sites,
            cache_bucket_ns=bucket_ns,
        )
    if 'nationalgrid_bsp_master' in include_site_table_keys:
        period_ns = max(1, int(context.ng_set_period_ns.get('nationalgrid_bsp_master', 1800 * 1_000_000_000)))
        bucket_ns = (ts_ns // period_ns) * period_ns
        site_snapshot_tables['nationalgrid_bsp_master'] = _snapshot_master_set(
            context.table_ng_bsp_sites,
            ts_ns,
            label='National Grid BSP (Site-Aligned)',
            source_set='bsp',
            cursor_by_site=context.cursor_ng_bsp_sites,
            cache_bucket_ns=bucket_ns,
        )

    total_gen = _safe_float(g.get('total_generation_mw'))
    est_demand = _safe_float(d.get('estimated_demand_mw'), default=total_gen)

    frame = {
        'rocof_timestamp': ts.isoformat(),
        'f_start_hz': _safe_float(f_start, default=f_end),
        'f_end_hz': _safe_float(f_end),
        'rocof_hz_per_s': _safe_float(rocof, default=0.0),
        'total_generation_mw': total_gen,
        'estimated_demand_mw': est_demand,
        'outturn_inertia': _safe_float(i.get('outturn_inertia')),
        'temperature_c': _safe_float(w.get('temperature_c')),
        'wind_speed_100m_kph': _safe_float(w.get('wind_speed_100m_kph')),
        'solar_radiation_w_m2': _safe_float(w.get('solar_radiation_w_m2')),
        'transmission_financial_year': str(tl.get('transmission_financial_year') or ''),
        'transmission_month': str(tl.get('transmission_month') or ''),
        'transmission_nget': _safe_float(tl.get('transmission_nget')),
        'transmission_spt': _safe_float(tl.get('transmission_spt')),
        'transmission_shetl': _safe_float(tl.get('transmission_shetl')),
        'transmission_gb_totals': _safe_float(tl.get('transmission_gb_totals')),
        'transmission_sum_nget': _safe_float(tl.get('transmission_sum_nget')),
        'transmission_sum_spt': _safe_float(tl.get('transmission_sum_spt')),
        'transmission_sum_shetl': _safe_float(tl.get('transmission_sum_shetl')),
        'transmission_sum_gb_totals': _safe_float(tl.get('transmission_sum_gb_totals')),
        'transmission_losses_full_series': context.transmission_losses_series,
        'tables': snapshot_tables,
        'site_tables': site_snapshot_tables,
        'available_fuels': FUEL_COLS,
        'available_interconnectors': [*INTERCONNECTOR_COLS, 'NET_INTERCONNECTOR_FLOW'],
    }

    for fuel in FUEL_COLS:
        frame[fuel] = _safe_float(g.get(fuel), default=0.0)

    for col in INTERCONNECTOR_COLS:
        frame[col] = _safe_float(d.get(col), default=0.0)
    frame['NET_INTERCONNECTOR_FLOW'] = _safe_float(d.get('NET_INTERCONNECTOR_FLOW'), default=0.0)

    return frame


async def _send_json(websocket: WebSocket, payload: dict) -> None:
    try:
        await websocket.send_text(json.dumps(_json_safe(payload), allow_nan=False))
    except (WebSocketDisconnect, RuntimeError):
        return


async def stream_from_start(
    websocket: WebSocket,
    start_iso: str,
    stop_event: asyncio.Event,
    runtime: StreamRuntime,
) -> None:
    try:
        start_ts = pd.to_datetime(start_iso, utc=True, errors='coerce')
        if pd.isna(start_ts):
            await _send_json(websocket, {'type': 'info', 'message': 'Invalid start date'})
            return

        global _context, _context_year
        if _context is None or _context_year != int(start_ts.year):
            _site_table_cache_clear()
            market_sources = _market_table_sources()
            market_tables: List[AuxSnapshotTable] = []
            for source_name, source_parquet, time_cols, display_cols in market_sources:
                if source_parquet is None:
                    market_tables.append(
                        AuxSnapshotTable(
                            ts_ns=np.array([], dtype=np.int64),
                            records=[],
                            columns=[],
                            label=f'{source_name} (not found)',
                        )
                    )
                    continue

                market_tables.append(
                    _load_snapshot_table(
                        parquet_root=source_parquet,
                        start_ts=start_ts,
                        label=source_name,
                        preferred_time_cols=time_cols,
                        preferred_display_cols=display_cols,
                        max_cols=10,
                    )
                )

            transmission_losses_frame, transmission_losses_series = _load_transmission_losses(start_ts=start_ts)

            _context = StreamContext(
                generation=_load_generation(),
                demand=_load_demand(start_ts=start_ts),
                inertia=_load_inertia(start_ts=start_ts),
                weather=_load_weather(start_ts=start_ts),
                transmission_losses=transmission_losses_frame,
                transmission_losses_series=transmission_losses_series,
                table_nonbm_instructions=_load_snapshot_table(
                    parquet_root=NONBM_INSTR_PARQUET,
                    start_ts=start_ts,
                    label='NonBM ASDP Instructions',
                    preferred_time_cols=['InstructionStartDateTime', 'DispatchDateTimeStamp', 'InstructionCeaseDateTime'],
                    preferred_display_cols=['EntryID', 'ServiceType', 'InstructionStartDateTime', 'MW', 'IndicativePrice', 'InstructionCeaseDateTime'],
                    max_cols=8,
                ),
                table_nonbm_window_prices=_load_snapshot_table(
                    parquet_root=NONBM_WINDOW_PRICE_PARQUET,
                    start_ts=start_ts,
                    label='NonBM ASDP Window Prices',
                    preferred_time_cols=['SPStartDateTime', 'CreatedTime', 'SPEndDateTime'],
                    preferred_display_cols=['EntryID', 'ServiceType', 'SPStartDateTime', 'SPEndDateTime', 'WindowPrice', 'PartyID', 'AssetID'],
                    max_cols=8,
                ),
                table_bsad_aggregated=_load_snapshot_table(
                    parquet_root=BSAD_AGG_PARQUET,
                    start_ts=start_ts,
                    label='BSAD Aggregated',
                    preferred_time_cols=['Date'],
                    preferred_display_cols=['Date', 'Settlement Period', 'EBCA (£)', 'EBVA (MWh)', 'ESCA (£)', 'ESVA (MWh)', 'BPA (£/MWh)', 'SPA (£/MWh)'],
                    max_cols=8,
                ),
                table_bsad_dissaggregated=_load_snapshot_table(
                    parquet_root=BSAD_DISS_PARQUET,
                    start_ts=start_ts,
                    label='BSAD DissAggregated',
                    preferred_time_cols=['Date', 'DatetimeUTC', 'SettlementDate'],
                    preferred_display_cols=['Date', 'Settlement Period'],
                    max_cols=8,
                ),
                table_bsad_forward=_load_snapshot_table(
                    parquet_root=BSAD_FWD_PARQUET,
                    start_ts=start_ts,
                    label='BSAD Forward Contracts',
                    preferred_time_cols=['Date'],
                    preferred_display_cols=['Date', 'Settlement Period', 'BCA (£)', 'BSA (MWh)', 'BVA (MWh)', 'SCA (£)', 'SSA (MWh)', 'SVA (MWh)'],
                    max_cols=8,
                ),
                table_obp_source_1=market_tables[0],
                table_obp_source_2=market_tables[1],
                table_orps_reactive_power=market_tables[2],
                table_transmission_losses_monthly=_load_transmission_losses_monthly_table(),
                table_transmission_losses_financial_year=_load_transmission_losses_financial_year_table(),
                table_ng_live_primary_sites=_load_site_snapshot_tables(
                    parquet_path=NATIONAL_GRID_PARQUET / 'live_primary_all.parquet',
                    start_ts=start_ts,
                    label='National Grid Live Primary',
                    preferred_time_cols=['timestamp_utc', 'time', 'ValueDate'],
                    preferred_site_cols=['site', 'location_hint', 'Location', 'Unit', 'unit'],
                    preferred_display_cols=['timestamp_utc', 'ValueDate', 'time', 'unit', 'value', 'MW', 'MVA', 'MVAr', 'Volts', 'Current Inst'],
                    max_cols=10,
                ),
                table_ng_live_gsp_sites=_load_site_snapshot_tables(
                    parquet_path=NATIONAL_GRID_PARQUET / 'live_gsp_all.parquet',
                    start_ts=start_ts,
                    label='National Grid Live GSP',
                    preferred_time_cols=['timestamp_utc', 'Timestamp'],
                    preferred_site_cols=['location_hint', 'site', 'Location', 'Unit', 'unit'],
                    preferred_display_cols=['timestamp_utc', 'Timestamp', 'Net Demand', 'Generation', 'Import', 'Solar', 'Wind', 'STOR', 'Other'],
                    max_cols=10,
                ),
                table_ng_bsp_sites=_load_site_snapshot_tables(
                    parquet_path=NATIONAL_GRID_PARQUET / 'bsp_all.parquet',
                    start_ts=start_ts,
                    label='National Grid BSP Power Flow',
                    preferred_time_cols=['timestamp_utc', 'ValueDate'],
                    preferred_site_cols=['location_hint', 'site', 'Location', 'Unit', 'unit'],
                    preferred_display_cols=['timestamp_utc', 'ValueDate', 'MW', 'MVA', 'MVAr', 'Volts', 'Amps', 'Current Inst', 'MW Inst', 'MVAr Inst'],
                    max_cols=10,
                ),
                cursor_ng_live_primary_sites={},
                cursor_ng_live_gsp_sites={},
                cursor_ng_bsp_sites={},
                ng_set_period_ns={},
                ng_set_next_update_ns={},
            )
            _context.ng_set_period_ns = {
                'nationalgrid_live_primary_master': _estimate_set_period_ns(_context.table_ng_live_primary_sites, default_seconds=1800),
                'nationalgrid_live_gsp_master': _estimate_set_period_ns(_context.table_ng_live_gsp_sites, default_seconds=1800),
                'nationalgrid_bsp_master': _estimate_set_period_ns(_context.table_ng_bsp_sites, default_seconds=1800),
            }
            _context.ng_set_next_update_ns = {
                'nationalgrid_live_primary_master': 0,
                'nationalgrid_live_gsp_master': 0,
                'nationalgrid_bsp_master': 0,
            }
            _context_year = int(start_ts.year)

        fps_value = max(1, min(144, int(runtime.fps)))
        await _send_json(websocket, {'type': 'info', 'message': f'Streaming from {start_ts.isoformat()} at {fps_value} FPS'})

        prev_ts: Optional[pd.Timestamp] = None
        prev_freq: Optional[float] = None
        frames_sent = 0

        for ts, freq in _iter_frequency_rows(start_ts):
            if stop_event.is_set():
                break

            if prev_ts is None or prev_freq is None:
                prev_ts = ts
                prev_freq = freq
                continue

            dt_s = max((ts - prev_ts).total_seconds(), 1e-6)
            rocof = (freq - prev_freq) / dt_s

            fps_now = max(1, min(144, int(runtime.fps)))
            ts_ns = int(ts.value)
            include_site_table_keys: set[str] = set()
            for set_key, next_update_ns in _context.ng_set_next_update_ns.items():
                if frames_sent == 0 or ts_ns >= int(next_update_ns):
                    include_site_table_keys.add(set_key)
                    period_ns = max(1, int(_context.ng_set_period_ns.get(set_key, 1800 * 1_000_000_000)))
                    _context.ng_set_next_update_ns[set_key] = ts_ns + period_ns

            frame = _build_frame(
                ts=ts,
                f_start=prev_freq,
                f_end=freq,
                rocof=rocof,
                context=_context,
                include_site_table_keys=include_site_table_keys,
            )
            await _send_json(websocket, {'type': 'frame', 'frame': frame})
            frames_sent += 1

            prev_ts = ts
            prev_freq = freq

            await asyncio.sleep(1.0 / fps_now)

        if frames_sent == 0:
            min_ts, max_ts = _frequency_bounds()
            await _send_json(
                websocket,
                {
                    'type': 'info',
                    'message': (
                        'No frequency samples found at/after requested start. '
                        f"Available range: {min_ts.isoformat() if min_ts is not None else 'unknown'} "
                        f"to {max_ts.isoformat() if max_ts is not None else 'unknown'}"
                    ),
                },
            )

        await _send_json(websocket, {'type': 'done'})
    except Exception as exc:
        await _send_json(websocket, {'type': 'info', 'message': f'Stream error: {exc}'})


@app.get('/')
def index() -> FileResponse:
    return FileResponse(APP_DIR / 'index.html')


@app.get('/about')
def about() -> FileResponse:
    return FileResponse(APP_DIR / 'about.html')


@app.get('/meta')
def metadata() -> dict:
    min_ts, max_ts = _frequency_bounds()
    return {
        'frequency_min_timestamp': min_ts.isoformat() if min_ts is not None else None,
        'frequency_max_timestamp': max_ts.isoformat() if max_ts is not None else None,
        'has_frequency_data': min_ts is not None and max_ts is not None,
    }


@app.get('/styles.css')
def app_styles() -> FileResponse:
    return FileResponse(APP_DIR / 'styles.css', media_type='text/css')


@app.get('/app.js')
def app_script() -> FileResponse:
    return FileResponse(APP_DIR / 'app.js', media_type='application/javascript')


@app.get('/RoCoF-Reply/styles.css')
def shared_reply_styles() -> FileResponse:
    return FileResponse(REPLY_DIR / 'styles.css', media_type='text/css')


@app.get('/favicon.ico')
def favicon() -> Response:
    if (APP_DIR / 'favicon.ico').exists():
        return FileResponse(APP_DIR / 'favicon.ico')
    return Response(status_code=204)


@app.websocket('/ws/replay')
async def replay_ws(websocket: WebSocket) -> None:
    await websocket.accept()
    await _send_json(websocket, {'type': 'info', 'message': 'WebSocket connected'})

    stream_task: Optional[asyncio.Task] = None
    stop_event = asyncio.Event()
    runtime = StreamRuntime(fps=1)

    try:
        while True:
            raw = await websocket.receive_text()
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                await _send_json(websocket, {'type': 'info', 'message': 'Invalid JSON command'})
                continue

            action = (msg.get('action') or '').lower()

            if action == 'start':
                start = msg.get('start')
                if not isinstance(start, str):
                    await _send_json(websocket, {'type': 'info', 'message': 'Missing start timestamp'})
                    continue

                raw_fps = msg.get('fps', 1)
                try:
                    fps = int(raw_fps)
                except Exception:
                    fps = 1
                runtime.fps = max(1, min(144, fps))

                if stream_task and not stream_task.done():
                    stop_event.set()
                    await stream_task

                stop_event = asyncio.Event()
                stream_task = asyncio.create_task(stream_from_start(websocket, start, stop_event, runtime))

            elif action == 'stop':
                if stream_task and not stream_task.done():
                    stop_event.set()
                    await _send_json(websocket, {'type': 'info', 'message': 'Stop requested'})
                else:
                    await _send_json(websocket, {'type': 'info', 'message': 'No active stream'})

            elif action == 'set_speed':
                raw_fps = msg.get('fps', runtime.fps)
                try:
                    runtime.fps = max(1, min(144, int(raw_fps)))
                    await _send_json(websocket, {'type': 'info', 'message': f'Playback speed updated to {runtime.fps} FPS'})
                except Exception:
                    await _send_json(websocket, {'type': 'info', 'message': 'Invalid fps value'})

            elif action == 'seek':
                start = msg.get('start')
                if not isinstance(start, str):
                    await _send_json(websocket, {'type': 'info', 'message': 'Missing seek timestamp'})
                    continue

                if stream_task and not stream_task.done():
                    stop_event.set()
                    await stream_task

                stop_event = asyncio.Event()
                stream_task = asyncio.create_task(stream_from_start(websocket, start, stop_event, runtime))
                await _send_json(websocket, {'type': 'info', 'message': f'Seeking to {start}'})

            else:
                await _send_json(websocket, {'type': 'info', 'message': f'Unknown action: {action}'})

    except WebSocketDisconnect:
        if stream_task and not stream_task.done():
            stop_event.set()
    finally:
        if stream_task and not stream_task.done():
            stop_event.set()
            await stream_task


if __name__ == '__main__':
    import uvicorn

    uvicorn.run('server:app', host='127.0.0.1', port=8765, reload=False)
