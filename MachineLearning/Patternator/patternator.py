from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor


def project_root() -> Path:
    return next(path for path in Path(__file__).resolve().parents if path.name == "GDA")


def _sort_frequency_file(path: Path) -> tuple[int, int, str]:
    match = re.search(r"f-(\d{4})-(\d{1,2})\.csv$", path.name)
    if match:
        return int(match.group(1)), int(match.group(2)), path.name
    return (9999, 99, path.name)


def _detect_time_value_columns(csv_file: Path) -> tuple[str, str]:
    preview = pd.read_csv(csv_file, nrows=0)
    cols = list(preview.columns)
    lowered = [str(c).strip().lower() for c in cols]
    if "dtm" in lowered and "f" in lowered:
        return cols[lowered.index("dtm")], cols[lowered.index("f")]
    if "date" in lowered and "value" in lowered:
        return cols[lowered.index("date")], cols[lowered.index("value")]
    if len(cols) >= 2:
        return cols[0], cols[1]
    raise ValueError(f"Unable to detect time/value columns in {csv_file}")


def _settlement_to_datetime_utc(date_series: pd.Series, period_series: pd.Series) -> pd.Series:
    base = pd.to_datetime(date_series, errors="coerce", utc=True, format="mixed", dayfirst=True)
    period = pd.to_numeric(period_series, errors="coerce").fillna(1).astype(int)
    period = period.clip(lower=1)
    return base + pd.to_timedelta((period - 1) * 30, unit="m")


def _as_utc_datetime(series: pd.Series, dayfirst: bool = False) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce", utc=True, dayfirst=dayfirst)
    return parsed


def _normalize_resample_rule(rule: str) -> str:
    unit_map = {
        "H": "h",
        "T": "min",
        "S": "s",
        "L": "ms",
    }

    def repl(match: re.Match[str]) -> str:
        count = match.group(1) or "1"
        unit = match.group(2)
        if unit in unit_map:
            return f"{count}{unit_map[unit]}"
        return f"{count}{unit}"

    return re.sub(r"(\d*)([A-Za-z]+)", repl, rule)


def _robust_zscore(series: pd.Series) -> pd.Series:
    clean = pd.to_numeric(series, errors="coerce")
    median = clean.median()
    mad = (clean - median).abs().median()
    return (clean - median) / (mad + 1e-9)


def _read_csv_chunks(path: Path, usecols: list[str] | None = None, chunksize: int = 750_000) -> Iterable[pd.DataFrame]:
    return pd.read_csv(path, usecols=usecols, chunksize=chunksize)


def load_frequency(root: Path, max_files: int | None, row_stride: int) -> pd.DataFrame:
    freq_dir = root / "DataSources" / "NESO" / "Frequency"
    files = sorted([p for p in freq_dir.glob("f-*.csv") if p.is_file()], key=_sort_frequency_file)
    if max_files is not None:
        files = files[: max(max_files, 0)]
    if not files:
        raise FileNotFoundError(f"No frequency files found in {freq_dir}")

    parts: list[pd.DataFrame] = []
    for file in files:
        time_col, value_col = _detect_time_value_columns(file)
        for chunk in _read_csv_chunks(file, usecols=[time_col, value_col]):
            if row_stride > 1:
                chunk = chunk.iloc[::row_stride].copy()
            chunk.columns = ["Date", "f"]
            chunk["Date"] = _as_utc_datetime(chunk["Date"], dayfirst=True)
            chunk["f"] = pd.to_numeric(chunk["f"], errors="coerce")
            chunk = chunk.dropna(subset=["Date", "f"])
            parts.append(chunk)

    if not parts:
        raise ValueError("Frequency files were read but no valid rows were parsed")

    freq = pd.concat(parts, ignore_index=True)
    freq = freq.sort_values("Date").drop_duplicates(subset=["Date"], keep="last")
    freq = freq.set_index("Date")
    return freq


def build_frequency_targets(freq: pd.DataFrame, resample_rule: str) -> pd.DataFrame:
    resample_rule = _normalize_resample_rule(resample_rule)
    series = freq["f"].sort_index()
    grouped = series.resample(resample_rule)
    summary = pd.DataFrame(
        {
            "freq_mean": grouped.mean(),
            "freq_std": grouped.std(),
            "freq_min": grouped.min(),
            "freq_max": grouped.max(),
            "freq_count": grouped.count(),
            "freq_abs_dev_mean": grouped.apply(lambda s: (s - 50.0).abs().mean()),
            "freq_outside_49p95_50p05_share": grouped.apply(lambda s: ((s < 49.95) | (s > 50.05)).mean()),
            "rocof_abs_p95": grouped.apply(lambda s: s.diff().abs().quantile(0.95) if len(s) > 2 else np.nan),
            "rocof_abs_max": grouped.apply(lambda s: s.diff().abs().max() if len(s) > 1 else np.nan),
        }
    )

    z_std = _robust_zscore(summary["freq_std"])
    z_dev = _robust_zscore(summary["freq_abs_dev_mean"])
    z_out = _robust_zscore(summary["freq_outside_49p95_50p05_share"])
    summary["instability_score"] = 0.5 * z_std + 0.3 * z_dev + 0.2 * z_out
    return summary


def load_weather(root: Path, resample_rule: str) -> pd.DataFrame:
    weather_file = root / "DataSources" / "Weather" / "uk_weather_data_2010-01-01_2025-12-31.csv"
    if not weather_file.exists():
        return pd.DataFrame()
    weather = pd.read_csv(weather_file)
    if "Date" not in weather.columns:
        return pd.DataFrame()
    weather["Date"] = _as_utc_datetime(weather["Date"])
    weather = weather.dropna(subset=["Date"]).set_index("Date").sort_index()
    num_cols = weather.select_dtypes(include=[np.number]).columns.tolist()
    if not num_cols:
        return pd.DataFrame(index=weather.index)
    weather = weather[num_cols].resample(resample_rule).mean()
    weather.columns = [f"weather_{col}" for col in weather.columns]
    return weather


def load_generation(root: Path, resample_rule: str) -> pd.DataFrame:
    generation_file = root / "DataSources" / "NESO" / "HistoricalGenerationData" / "df_fuel_ckan.csv"
    if not generation_file.exists():
        return pd.DataFrame()
    generation = pd.read_csv(generation_file)
    if "DATETIME" not in generation.columns:
        return pd.DataFrame()
    generation["DATETIME"] = _as_utc_datetime(generation["DATETIME"])
    generation = generation.dropna(subset=["DATETIME"]).set_index("DATETIME").sort_index()
    keep_cols = [
        "GAS",
        "COAL",
        "NUCLEAR",
        "WIND",
        "WIND_EMB",
        "HYDRO",
        "IMPORTS",
        "BIOMASS",
        "SOLAR",
        "GENERATION",
        "CARBON_INTENSITY",
        "LOW_CARBON",
        "ZERO_CARBON",
        "RENEWABLE",
        "FOSSIL",
        "GAS_perc",
        "COAL_perc",
        "NUCLEAR_perc",
        "WIND_perc",
        "WIND_EMB_perc",
        "SOLAR_perc",
        "LOW_CARBON_perc",
        "ZERO_CARBON_perc",
        "RENEWABLE_perc",
        "FOSSIL_perc",
    ]
    cols = [c for c in keep_cols if c in generation.columns]
    if not cols:
        return pd.DataFrame(index=generation.index)
    generation = generation[cols].resample(resample_rule).mean()
    generation.columns = [f"gen_{col}" for col in generation.columns]
    return generation


def load_inertia(root: Path, resample_rule: str) -> pd.DataFrame:
    inertia_files = sorted([p for p in (root / "DataSources" / "NESO" / "Inertia").glob("inertia*.csv") if p.is_file()])
    if not inertia_files:
        return pd.DataFrame()
    parts: list[pd.DataFrame] = []
    for file in inertia_files:
        frame = pd.read_csv(file)
        required = {"Settlement Date", "Settlement Period"}
        if not required.issubset(frame.columns):
            continue
        frame["Date"] = _settlement_to_datetime_utc(frame["Settlement Date"], frame["Settlement Period"])
        cols = [c for c in ["Outturn Inertia", "Market Provided Inertia"] if c in frame.columns]
        if not cols:
            continue
        frame = frame[["Date", *cols]].dropna(subset=["Date"]).set_index("Date").sort_index()
        parts.append(frame)
    if not parts:
        return pd.DataFrame()
    inertia = pd.concat(parts).sort_index()
    if {"Outturn Inertia", "Market Provided Inertia"}.issubset(inertia.columns):
        inertia["Inertia Gap"] = inertia["Outturn Inertia"] - inertia["Market Provided Inertia"]
    inertia = inertia.resample(resample_rule).mean()
    inertia.columns = [f"inertia_{c.replace(' ', '_')}" for c in inertia.columns]
    return inertia


def load_demand(root: Path, resample_rule: str) -> pd.DataFrame:
    demand_files = sorted([p for p in (root / "DataSources" / "NESO" / "DemandData").glob("demanddata_*.csv") if p.is_file()])
    if not demand_files:
        return pd.DataFrame()
    parts: list[pd.DataFrame] = []
    for file in demand_files:
        frame = pd.read_csv(file)
        required = {"SETTLEMENT_DATE", "SETTLEMENT_PERIOD"}
        if not required.issubset(frame.columns):
            continue
        frame["Date"] = _settlement_to_datetime_utc(frame["SETTLEMENT_DATE"], frame["SETTLEMENT_PERIOD"])
        target_cols = [
            "ND",
            "TSD",
            "ENGLAND_WALES_DEMAND",
            "EMBEDDED_WIND_GENERATION",
            "EMBEDDED_SOLAR_GENERATION",
        ]
        flow_cols = [c for c in frame.columns if c.endswith("_FLOW")]
        cols = [c for c in target_cols if c in frame.columns] + flow_cols
        if not cols:
            continue
        frame = frame[["Date", *cols]].dropna(subset=["Date"]).set_index("Date").sort_index()
        if flow_cols:
            existing_flows = [c for c in flow_cols if c in frame.columns]
            frame["NET_INTERCONNECTOR_FLOW"] = frame[existing_flows].sum(axis=1, skipna=True)
        parts.append(frame)
    if not parts:
        return pd.DataFrame()
    demand = pd.concat(parts).sort_index()
    demand = demand.resample(resample_rule).mean()
    demand.columns = [f"demand_{c}" for c in demand.columns]
    return demand


def build_feature_table(root: Path, resample_rule: str, max_frequency_files: int | None, row_stride: int) -> pd.DataFrame:
    resample_rule = _normalize_resample_rule(resample_rule)
    freq_raw = load_frequency(root=root, max_files=max_frequency_files, row_stride=row_stride)
    freq_targets = build_frequency_targets(freq=freq_raw, resample_rule=resample_rule)
    weather = load_weather(root=root, resample_rule=resample_rule)
    generation = load_generation(root=root, resample_rule=resample_rule)
    inertia = load_inertia(root=root, resample_rule=resample_rule)
    demand = load_demand(root=root, resample_rule=resample_rule)

    merged = freq_targets.join(weather, how="left")
    merged = merged.join(generation, how="left")
    merged = merged.join(inertia, how="left")
    merged = merged.join(demand, how="left")
    merged = merged.sort_index()

    threshold = int(0.98 * len(merged))
    drop_cols = [c for c in merged.columns if merged[c].isna().sum() > threshold]
    if drop_cols:
        merged = merged.drop(columns=drop_cols)
    return merged


def correlation_report(df: pd.DataFrame, target_col: str, top_n: int) -> pd.DataFrame:
    num = df.select_dtypes(include=[np.number]).copy()
    if target_col not in num.columns:
        return pd.DataFrame(columns=["feature", "corr", "abs_corr"])
    corr = num.corr(numeric_only=True)[target_col].dropna()
    corr = corr.drop(labels=[target_col], errors="ignore")
    out = pd.DataFrame({"feature": corr.index, "corr": corr.values})
    out["abs_corr"] = out["corr"].abs()
    out = out.sort_values("abs_corr", ascending=False).head(top_n).reset_index(drop=True)
    return out


def feature_importance_report(df: pd.DataFrame, target_col: str, top_n: int, max_rows: int) -> pd.DataFrame:
    numeric = df.select_dtypes(include=[np.number]).copy()
    if target_col not in numeric.columns:
        return pd.DataFrame(columns=["feature", "importance"])

    y = numeric[target_col]
    x = numeric.drop(columns=[target_col])
    valid = y.notna()
    x = x.loc[valid]
    y = y.loc[valid]
    if len(x) < 100 or x.shape[1] == 0:
        return pd.DataFrame(columns=["feature", "importance"])

    if len(x) > max_rows:
        idx = np.linspace(0, len(x) - 1, num=max_rows, dtype=int)
        x = x.iloc[idx]
        y = y.iloc[idx]

    x = x.replace([np.inf, -np.inf], np.nan)
    x = x.fillna(x.median(numeric_only=True))

    model = RandomForestRegressor(
        n_estimators=350,
        random_state=42,
        n_jobs=-1,
        min_samples_leaf=5,
    )
    model.fit(x, y)
    importances = pd.DataFrame({"feature": x.columns, "importance": model.feature_importances_})
    importances = importances.sort_values("importance", ascending=False).head(top_n).reset_index(drop=True)
    return importances


def generation_mix_patterns(df: pd.DataFrame, target_col: str) -> dict[str, pd.DataFrame]:
    outputs: dict[str, pd.DataFrame] = {}
    candidates = [
        "gen_GAS_perc",
        "gen_WIND_perc",
        "gen_FOSSIL_perc",
        "gen_LOW_CARBON_perc",
        "gen_RENEWABLE_perc",
    ]

    for col in candidates:
        if col not in df.columns:
            continue
        tmp = df[[col, target_col]].dropna()
        if len(tmp) < 250 or tmp[col].nunique() < 5:
            continue
        try:
            tmp["bucket"] = pd.qcut(tmp[col], q=5, duplicates="drop")
        except ValueError:
            continue
        grouped = tmp.groupby("bucket", observed=True)[target_col].agg(["count", "mean", "median", "max"]).reset_index()
        grouped = grouped.rename(
            columns={
                "count": "window_count",
                "mean": "instability_mean",
                "median": "instability_median",
                "max": "instability_max",
            }
        )
        outputs[col] = grouped
    return outputs


def run_patternator(
    root: Path,
    output_dir: Path,
    resample_rule: str,
    max_frequency_files: int | None,
    row_stride: int,
    top_n: int,
    max_model_rows: int,
) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)

    merged = build_feature_table(
        root=root,
        resample_rule=resample_rule,
        max_frequency_files=max_frequency_files,
        row_stride=row_stride,
    )
    merged.to_csv(output_dir / "patternator_timeseries.csv")

    target = "instability_score"
    corr = correlation_report(merged, target_col=target, top_n=top_n)
    corr.to_csv(output_dir / "top_correlations.csv", index=False)

    importance = feature_importance_report(merged, target_col=target, top_n=top_n, max_rows=max_model_rows)
    importance.to_csv(output_dir / "feature_importance.csv", index=False)

    mix_reports = generation_mix_patterns(merged, target_col=target)
    for name, report in mix_reports.items():
        report.to_csv(output_dir / f"pattern_{name}.csv", index=False)

    summary = {
        "root": str(root),
        "resample_rule": resample_rule,
        "max_frequency_files": max_frequency_files,
        "row_stride": row_stride,
        "rows": int(len(merged)),
        "columns": int(len(merged.columns)),
        "time_start": str(merged.index.min()) if len(merged) else None,
        "time_end": str(merged.index.max()) if len(merged) else None,
        "top_correlations": corr.to_dict(orient="records"),
        "top_feature_importance": importance.to_dict(orient="records"),
        "generation_mix_reports": list(mix_reports.keys()),
        "outputs": {
            "timeseries": str(output_dir / "patternator_timeseries.csv"),
            "correlations": str(output_dir / "top_correlations.csv"),
            "feature_importance": str(output_dir / "feature_importance.csv"),
        },
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Patternator: coarse frequency-centric grid pattern surfacing")
    parser.add_argument("--root", default=str(project_root()), help="Project root path (GDA folder)")
    parser.add_argument(
        "--output-dir",
        default=str(project_root() / "MachineLearning" / "Patternator" / "output"),
        help="Directory where Patternator outputs will be written",
    )
    parser.add_argument("--resample", default="1H", help="Pandas resample rule for alignment, e.g. 30T, 1H, 3H")
    parser.add_argument("--max-frequency-files", type=int, default=12, help="Limit frequency monthly files for coarse runs")
    parser.add_argument("--row-stride", type=int, default=5, help="Read every Nth frequency row to control runtime")
    parser.add_argument("--top-n", type=int, default=25, help="Top-N features to keep in reports")
    parser.add_argument("--max-model-rows", type=int, default=180000, help="Max rows for random-forest importance model")
    parser.add_argument("--print-json", action="store_true", help="Print summary JSON to stdout")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    summary = run_patternator(
        root=Path(args.root).resolve(),
        output_dir=Path(args.output_dir).resolve(),
        resample_rule=args.resample,
        max_frequency_files=args.max_frequency_files,
        row_stride=max(args.row_stride, 1),
        top_n=max(args.top_n, 1),
        max_model_rows=max(args.max_model_rows, 1000),
    )

    if args.print_json:
        print(json.dumps(summary, indent=2))
    else:
        print("=== Patternator complete ===")
        print(f"Rows: {summary['rows']} | Columns: {summary['columns']}")
        print(f"Window: {summary['time_start']} -> {summary['time_end']}")
        print(f"Outputs: {summary['outputs']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
