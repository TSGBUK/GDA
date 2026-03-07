#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

CONDA_BASE="${CONDA_BASE:-$HOME/miniconda3}"
CONDA_ENV="${CONDA_ENV:-tsgb_rapids}"
DEVICE="${DEVICE:-cuda}"
N_ESTIMATORS="${N_ESTIMATORS:-30}"
OOM_RETRY_ESTIMATORS="${OOM_RETRY_ESTIMATORS:-12}"
FREQUENCY_MAX_ROWS="${FREQUENCY_MAX_ROWS:-0}"
FREQUENCY_BATCH_SIZE="${FREQUENCY_BATCH_SIZE:-250000}"
FREQUENCY_TRAINING_MODE="${FREQUENCY_TRAINING_MODE:-auto}"
FREQUENCY_CUDA_SHARD_ROWS="${FREQUENCY_CUDA_SHARD_ROWS:-0}"
FREQUENCY_CUDA_TARGET_VRAM_GB="${FREQUENCY_CUDA_TARGET_VRAM_GB:-0}"
FREQUENCY_CUDA_TARGET_VRAM_FRACTION="${FREQUENCY_CUDA_TARGET_VRAM_FRACTION:-0.7}"
FREQUENCY_CUDA_TARGET_VRAM_CAP_GB="${FREQUENCY_CUDA_TARGET_VRAM_CAP_GB:-8}"
FREQUENCY_CUDA_GPUS="${FREQUENCY_CUDA_GPUS:-auto}"
FREQUENCY_CUDA_BATCH_MAX_ROWS="${FREQUENCY_CUDA_BATCH_MAX_ROWS:-5000000}"
FREQUENCY_PROGRESS_EVERY_CHUNKS="${FREQUENCY_PROGRESS_EVERY_CHUNKS:-25}"
GENERATION_MAX_ROWS="${GENERATION_MAX_ROWS:-0}"
GRIDWATCH_MAX_ROWS="${GRIDWATCH_MAX_ROWS:-0}"
GRIDWATCH_BATCH_SIZE="${GRIDWATCH_BATCH_SIZE:-250000}"
GRIDWATCH_TRAINING_MODE="${GRIDWATCH_TRAINING_MODE:-auto}"
GRIDWATCH_CUDA_SHARD_ROWS="${GRIDWATCH_CUDA_SHARD_ROWS:-400000}"
GRIDWATCH_CUDA_BATCH_MAX_ROWS="${GRIDWATCH_CUDA_BATCH_MAX_ROWS:-800000}"
GRIDWATCH_PROGRESS_EVERY_CHUNKS="${GRIDWATCH_PROGRESS_EVERY_CHUNKS:-25}"
WEATHER_GENERATION_MAX_ROWS="${WEATHER_GENERATION_MAX_ROWS:-0}"
ADDITIONAL_MAX_ROWS_PER_DATASET="${ADDITIONAL_MAX_ROWS_PER_DATASET:-0}"
ADDITIONAL_BATCH_SIZE="${ADDITIONAL_BATCH_SIZE:-100000}"
ADDITIONAL_TRAINING_MODE="${ADDITIONAL_TRAINING_MODE:-auto}"
ADDITIONAL_INCREMENTAL_THRESHOLD_ROWS="${ADDITIONAL_INCREMENTAL_THRESHOLD_ROWS:-1000000}"
ADDITIONAL_PROGRESS_EVERY_CHUNKS="${ADDITIONAL_PROGRESS_EVERY_CHUNKS:-25}"
MONSTER_ROUNDS="${MONSTER_ROUNDS:-12}"
MONSTER_ESTIMATORS_BASE="${MONSTER_ESTIMATORS_BASE:-800}"
MONSTER_MAX_FREQUENCY_FILES="${MONSTER_MAX_FREQUENCY_FILES:-12}"
MONSTER_ROW_STRIDE="${MONSTER_ROW_STRIDE:-12}"
MONSTER_MAX_ROWS="${MONSTER_MAX_ROWS:-300000}"
MONSTER_MAX_FREQUENCY_RAW_ROWS="${MONSTER_MAX_FREQUENCY_RAW_ROWS:-2000000}"
MONSTER_SOURCE_MAX_ROWS="${MONSTER_SOURCE_MAX_ROWS:-300000}"
MONSTER_SOURCE_BATCH_SIZE="${MONSTER_SOURCE_BATCH_SIZE:-100000}"
MONSTER_DISABLE_EXPERT_FEATURES="${MONSTER_DISABLE_EXPERT_FEATURES:-1}"
MONSTER_REQUIRED="${MONSTER_REQUIRED:-0}"
OOM_RETRY_MONSTER_ROUNDS="${OOM_RETRY_MONSTER_ROUNDS:-3}"
OOM_RETRY_MONSTER_BASE_ESTIMATORS="${OOM_RETRY_MONSTER_BASE_ESTIMATORS:-120}"
OOM_RETRY_MONSTER_MAX_FREQUENCY_FILES="${OOM_RETRY_MONSTER_MAX_FREQUENCY_FILES:-4}"
OOM_RETRY_MONSTER_ROW_STRIDE="${OOM_RETRY_MONSTER_ROW_STRIDE:-24}"
OOM_RETRY_MONSTER_MAX_ROWS="${OOM_RETRY_MONSTER_MAX_ROWS:-120000}"
OOM_RETRY_MONSTER_MAX_FREQUENCY_RAW_ROWS="${OOM_RETRY_MONSTER_MAX_FREQUENCY_RAW_ROWS:-800000}"
OOM_RETRY_MONSTER_SOURCE_MAX_ROWS="${OOM_RETRY_MONSTER_SOURCE_MAX_ROWS:-120000}"
OOM_RETRY_MONSTER_SOURCE_BATCH_SIZE="${OOM_RETRY_MONSTER_SOURCE_BATCH_SIZE:-50000}"
OOM_RETRY_GENERATION_MAX_ROWS="${OOM_RETRY_GENERATION_MAX_ROWS:-120000}"
OOM_RETRY_GRIDWATCH_MAX_ROWS="${OOM_RETRY_GRIDWATCH_MAX_ROWS:-400000}"
OOM_RETRY_GRIDWATCH_BATCH_SIZE="${OOM_RETRY_GRIDWATCH_BATCH_SIZE:-100000}"
OOM_RETRY_GRIDWATCH_CUDA_SHARD_ROWS="${OOM_RETRY_GRIDWATCH_CUDA_SHARD_ROWS:-250000}"
OOM_RETRY_ADDITIONAL_MAX_ROWS_PER_DATASET="${OOM_RETRY_ADDITIONAL_MAX_ROWS_PER_DATASET:-120000}"

C_RESET=""
C_BOLD=""
C_DIM=""
C_CYAN=""
C_BLUE=""
C_MAGENTA=""
C_GREEN=""
C_YELLOW=""
C_RED=""

if [[ -t 1 && -z "${NO_COLOR:-}" ]] && command -v tput >/dev/null 2>&1; then
  if [[ "$(tput colors 2>/dev/null || echo 0)" -ge 8 ]]; then
    C_RESET="$(tput sgr0)"
    C_BOLD="$(tput bold)"
    C_DIM="$(tput dim)"
    C_CYAN="$(tput setaf 6)"
    C_BLUE="$(tput setaf 4)"
    C_MAGENTA="$(tput setaf 5)"
    C_GREEN="$(tput setaf 2)"
    C_YELLOW="$(tput setaf 3)"
    C_RED="$(tput setaf 1)"
  fi
fi

print_divider() {
  printf "%b============================================================%b\n" "$C_DIM" "$C_RESET"
}

log_step() {
  printf "%b[step]%b %s\n" "$C_BOLD$C_BLUE" "$C_RESET" "$*"
}

log_cmd() {
  printf "%b[cmd ]%b %s\n" "$C_BOLD$C_MAGENTA" "$C_RESET" "$*"
}

log_info() {
  printf "%b[info]%b %s\n" "$C_CYAN" "$C_RESET" "$*"
}

log_warn() {
  printf "%b[warn]%b %s\n" "$C_YELLOW" "$C_RESET" "$*"
}

log_error() {
  printf "%b[error]%b %s\n" "$C_RED" "$C_RESET" "$*"
}

log_done() {
  printf "%b[done]%b %s\n" "$C_GREEN" "$C_RESET" "$*"
}

declare -a STEP_ORDER=()
declare -A STEP_SECONDS=()
declare -A STEP_RESULT=()
declare -A STEP_NOTE=()
RUN_STARTED_EPOCH="$(date +%s)"

format_duration() {
  local total="$1"
  if [[ -z "$total" ]]; then
    total=0
  fi
  if [[ "$total" -lt 0 ]]; then
    total=0
  fi
  local h=$(( total / 3600 ))
  local m=$(( (total % 3600) / 60 ))
  local s=$(( total % 60 ))
  if [[ $h -gt 0 ]]; then
    printf "%dh %02dm %02ds" "$h" "$m" "$s"
  elif [[ $m -gt 0 ]]; then
    printf "%dm %02ds" "$m" "$s"
  else
    printf "%ds" "$s"
  fi
}

record_step_result() {
  local name="$1"
  local seconds="$2"
  local result="$3"
  local note="${4:-}"
  STEP_ORDER+=("$name")
  STEP_SECONDS["$name"]="$seconds"
  STEP_RESULT["$name"]="$result"
  STEP_NOTE["$name"]="$note"
}

json_get() {
  local file_path="$1"
  local dotted_path="$2"
  if [[ ! -f "$file_path" ]]; then
    return 0
  fi
  python - "$file_path" "$dotted_path" <<'PY'
import json
import sys

file_path = sys.argv[1]
path = [p for p in sys.argv[2].split('.') if p]

try:
    with open(file_path, 'r', encoding='utf-8') as fh:
        data = json.load(fh)
except Exception:
    sys.exit(0)

cur = data
for part in path:
    if isinstance(cur, dict) and part in cur:
        cur = cur[part]
    else:
        sys.exit(0)

if isinstance(cur, float):
    print(f"{cur:.6f}")
elif isinstance(cur, (int, str)):
    print(cur)
PY
}

print_final_summary() {
  local run_finished_epoch="$(date +%s)"
  local run_elapsed=$(( run_finished_epoch - RUN_STARTED_EPOCH ))

  echo ""
  print_divider
  printf "%bRun Summary%b\n" "$C_BOLD$C_GREEN" "$C_RESET"
  print_divider

  printf "%bStep Timings%b\n" "$C_BOLD$C_BLUE" "$C_RESET"
  printf "%-42s %-12s %-10s %s\n" "Step" "Duration" "Result" "Note"
  printf "%-42s %-12s %-10s %s\n" "------------------------------------------" "------------" "----------" "-----------------------------"

  local step_name
  local step_duration
  local step_result
  local step_note
  local total_steps=0
  local ok_steps=0
  local warn_steps=0

  for step_name in "${STEP_ORDER[@]}"; do
    step_duration="$(format_duration "${STEP_SECONDS[$step_name]:-0}")"
    step_result="${STEP_RESULT[$step_name]:-unknown}"
    step_note="${STEP_NOTE[$step_name]:-}"
    printf "%-42s %-12s %-10s %s\n" "$step_name" "$step_duration" "$step_result" "$step_note"
    total_steps=$(( total_steps + 1 ))
    if [[ "$step_result" == "ok" ]]; then
      ok_steps=$(( ok_steps + 1 ))
    else
      warn_steps=$(( warn_steps + 1 ))
    fi
  done

  log_info "Overall runtime: $(format_duration "$run_elapsed")"
  log_info "Steps: total=$total_steps ok=$ok_steps non-ok=$warn_steps"

  echo ""
  printf "%bMetrics Snapshot%b\n" "$C_BOLD$C_BLUE" "$C_RESET"

  local artifacts_dir="$SCRIPT_DIR/pre-trained-experts"

  print_metric_line() {
    local label="$1"
    local json_file="$2"
    local rows
    local elapsed
    local r2

    rows="$(json_get "$json_file" "performance.rows_total")"
    if [[ -z "$rows" ]]; then
      rows="$(json_get "$json_file" "data.rows_total")"
    fi

    elapsed="$(json_get "$json_file" "performance.elapsed_seconds")"
    r2="$(json_get "$json_file" "overall.r2_mean")"

    if [[ -z "$rows" && -z "$elapsed" && -z "$r2" ]]; then
      return 0
    fi

    [[ -z "$rows" ]] && rows="-"
    [[ -z "$elapsed" ]] && elapsed="-"
    [[ -z "$r2" ]] && r2="-"

    printf "%-34s rows=%-12s elapsed_s=%-12s r2=%s\n" "$label" "$rows" "$elapsed" "$r2"
  }

  print_metric_line "Generation" "$artifacts_dir/generation_expert_metrics.json"
  print_metric_line "Weather" "$artifacts_dir/weather_expert_metrics.json"
  print_metric_line "Demand" "$artifacts_dir/demand_expert_metrics.json"
  print_metric_line "Inertia" "$artifacts_dir/inertia_expert_metrics.json"
  print_metric_line "Gridwatch" "$artifacts_dir/gridwatch_expert_metrics.json"
  print_metric_line "Frequency" "$artifacts_dir/frequency_expert_metrics.json"
  print_metric_line "Balancing" "$artifacts_dir/balancing_expert_metrics.json"
  print_metric_line "Weather+Generation" "$artifacts_dir/weather_generation_expert_metrics.json"
  print_metric_line "Weather+Inertia" "$artifacts_dir/weather_inertia_expert_metrics.json"

  local monster_summary="$artifacts_dir/monster/monster_run_summary.json"
  if [[ -f "$monster_summary" ]]; then
    local monster_rows_clean
    local monster_feature_count
    local monster_r2
    monster_rows_clean="$(json_get "$monster_summary" "rows_clean_for_training")"
    monster_feature_count="$(json_get "$monster_summary" "feature_count")"
    monster_r2="$(json_get "$monster_summary" "best_round_by_r2.r2_mean")"
    [[ -z "$monster_rows_clean" ]] && monster_rows_clean="-"
    [[ -z "$monster_feature_count" ]] && monster_feature_count="-"
    [[ -z "$monster_r2" ]] && monster_r2="-"
    printf "%-34s rows_clean=%-10s features=%-8s best_r2=%s\n" "Monster" "$monster_rows_clean" "$monster_feature_count" "$monster_r2"
  fi
}

usage() {
  cat <<EOF
Run all expert trainers in dependency-safe order.

Usage:
  $(basename "$0") [--device cpu|cuda|auto] [--n-estimators N] [--frequency-max-rows N]
                    [--oom-retry-estimators N]
                    [--generation-max-rows N]
                    [--gridwatch-max-rows N] [--gridwatch-batch-size N]
                    [--gridwatch-training-mode MODE] [--gridwatch-cuda-shard-rows N]
                    [--gridwatch-cuda-batch-max-rows N] [--gridwatch-progress-every-chunks N]
                    [--frequency-batch-size N] [--frequency-training-mode MODE]
                    [--frequency-cuda-shard-rows N] [--frequency-cuda-gpus IDS]
                    [--frequency-cuda-target-vram-gb N]
                    [--frequency-cuda-target-vram-fraction N] [--frequency-cuda-target-vram-cap-gb N]
                    [--frequency-cuda-batch-max-rows N]
                    [--frequency-progress-every-chunks N]
                    [--monster-rounds N] [--monster-estimators-base N]
                    [--weather-generation-max-rows N]
                    [--additional-max-rows-per-dataset N] [--additional-batch-size N]
                    [--additional-training-mode MODE] [--additional-incremental-threshold-rows N]
                    [--additional-progress-every-chunks N]
                    [--monster-max-frequency-files N] [--monster-row-stride N] [--monster-max-rows N]
                    [--monster-max-frequency-raw-rows N] [--monster-source-max-rows N] [--monster-source-batch-size N]
                    [--monster-disable-expert-features 0|1]
                    [--monster-required 0|1]
                    [--oom-retry-gridwatch-max-rows N] [--oom-retry-gridwatch-batch-size N]
                    [--oom-retry-gridwatch-cuda-shard-rows N]
                    [--oom-retry-additional-max-rows-per-dataset N]
                    [--conda-env NAME] [--conda-base PATH]

Defaults:
  --device $DEVICE
  --n-estimators $N_ESTIMATORS
  --oom-retry-estimators $OOM_RETRY_ESTIMATORS
  --generation-max-rows $GENERATION_MAX_ROWS
  --gridwatch-max-rows $GRIDWATCH_MAX_ROWS
  --gridwatch-batch-size $GRIDWATCH_BATCH_SIZE
  --gridwatch-training-mode $GRIDWATCH_TRAINING_MODE
  --gridwatch-cuda-shard-rows $GRIDWATCH_CUDA_SHARD_ROWS
  --gridwatch-cuda-batch-max-rows $GRIDWATCH_CUDA_BATCH_MAX_ROWS
  --gridwatch-progress-every-chunks $GRIDWATCH_PROGRESS_EVERY_CHUNKS
  --frequency-max-rows $FREQUENCY_MAX_ROWS
  --frequency-batch-size $FREQUENCY_BATCH_SIZE
  --frequency-training-mode $FREQUENCY_TRAINING_MODE
  --frequency-cuda-shard-rows $FREQUENCY_CUDA_SHARD_ROWS
  --frequency-cuda-target-vram-gb $FREQUENCY_CUDA_TARGET_VRAM_GB
  --frequency-cuda-target-vram-fraction $FREQUENCY_CUDA_TARGET_VRAM_FRACTION
  --frequency-cuda-target-vram-cap-gb $FREQUENCY_CUDA_TARGET_VRAM_CAP_GB
  --frequency-cuda-gpus $FREQUENCY_CUDA_GPUS
  --frequency-cuda-batch-max-rows $FREQUENCY_CUDA_BATCH_MAX_ROWS
  --frequency-progress-every-chunks $FREQUENCY_PROGRESS_EVERY_CHUNKS
  --weather-generation-max-rows $WEATHER_GENERATION_MAX_ROWS
  --additional-max-rows-per-dataset $ADDITIONAL_MAX_ROWS_PER_DATASET
  --additional-batch-size $ADDITIONAL_BATCH_SIZE
  --additional-training-mode $ADDITIONAL_TRAINING_MODE
  --additional-incremental-threshold-rows $ADDITIONAL_INCREMENTAL_THRESHOLD_ROWS
  --additional-progress-every-chunks $ADDITIONAL_PROGRESS_EVERY_CHUNKS
  --monster-rounds $MONSTER_ROUNDS
  --monster-estimators-base $MONSTER_ESTIMATORS_BASE
  --monster-max-frequency-files $MONSTER_MAX_FREQUENCY_FILES
  --monster-row-stride $MONSTER_ROW_STRIDE
  --monster-max-rows $MONSTER_MAX_ROWS
  --monster-max-frequency-raw-rows $MONSTER_MAX_FREQUENCY_RAW_ROWS
  --monster-source-max-rows $MONSTER_SOURCE_MAX_ROWS
  --monster-source-batch-size $MONSTER_SOURCE_BATCH_SIZE
  --monster-disable-expert-features $MONSTER_DISABLE_EXPERT_FEATURES
  --monster-required $MONSTER_REQUIRED
  --oom-retry-monster-rounds $OOM_RETRY_MONSTER_ROUNDS
  --oom-retry-monster-base-estimators $OOM_RETRY_MONSTER_BASE_ESTIMATORS
  --oom-retry-monster-max-frequency-files $OOM_RETRY_MONSTER_MAX_FREQUENCY_FILES
  --oom-retry-monster-row-stride $OOM_RETRY_MONSTER_ROW_STRIDE
  --oom-retry-monster-max-rows $OOM_RETRY_MONSTER_MAX_ROWS
  --oom-retry-monster-max-frequency-raw-rows $OOM_RETRY_MONSTER_MAX_FREQUENCY_RAW_ROWS
  --oom-retry-monster-source-max-rows $OOM_RETRY_MONSTER_SOURCE_MAX_ROWS
  --oom-retry-monster-source-batch-size $OOM_RETRY_MONSTER_SOURCE_BATCH_SIZE
  --oom-retry-generation-max-rows $OOM_RETRY_GENERATION_MAX_ROWS
  --oom-retry-gridwatch-max-rows $OOM_RETRY_GRIDWATCH_MAX_ROWS
  --oom-retry-gridwatch-batch-size $OOM_RETRY_GRIDWATCH_BATCH_SIZE
  --oom-retry-gridwatch-cuda-shard-rows $OOM_RETRY_GRIDWATCH_CUDA_SHARD_ROWS
  --oom-retry-additional-max-rows-per-dataset $OOM_RETRY_ADDITIONAL_MAX_ROWS_PER_DATASET
  --conda-env $CONDA_ENV
  --conda-base $CONDA_BASE
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --device) DEVICE="$2"; shift 2 ;;
    --n-estimators) N_ESTIMATORS="$2"; shift 2 ;;
    --oom-retry-estimators) OOM_RETRY_ESTIMATORS="$2"; shift 2 ;;
    --generation-max-rows) GENERATION_MAX_ROWS="$2"; shift 2 ;;
    --gridwatch-max-rows) GRIDWATCH_MAX_ROWS="$2"; shift 2 ;;
    --gridwatch-batch-size) GRIDWATCH_BATCH_SIZE="$2"; shift 2 ;;
    --gridwatch-training-mode) GRIDWATCH_TRAINING_MODE="$2"; shift 2 ;;
    --gridwatch-cuda-shard-rows) GRIDWATCH_CUDA_SHARD_ROWS="$2"; shift 2 ;;
    --gridwatch-cuda-batch-max-rows) GRIDWATCH_CUDA_BATCH_MAX_ROWS="$2"; shift 2 ;;
    --gridwatch-progress-every-chunks) GRIDWATCH_PROGRESS_EVERY_CHUNKS="$2"; shift 2 ;;
    --frequency-max-rows) FREQUENCY_MAX_ROWS="$2"; shift 2 ;;
    --frequency-batch-size) FREQUENCY_BATCH_SIZE="$2"; shift 2 ;;
    --frequency-training-mode) FREQUENCY_TRAINING_MODE="$2"; shift 2 ;;
    --frequency-cuda-shard-rows) FREQUENCY_CUDA_SHARD_ROWS="$2"; shift 2 ;;
    --frequency-cuda-target-vram-gb) FREQUENCY_CUDA_TARGET_VRAM_GB="$2"; shift 2 ;;
    --frequency-cuda-target-vram-fraction) FREQUENCY_CUDA_TARGET_VRAM_FRACTION="$2"; shift 2 ;;
    --frequency-cuda-target-vram-cap-gb) FREQUENCY_CUDA_TARGET_VRAM_CAP_GB="$2"; shift 2 ;;
    --frequency-cuda-gpus) FREQUENCY_CUDA_GPUS="$2"; shift 2 ;;
    --frequency-cuda-batch-max-rows) FREQUENCY_CUDA_BATCH_MAX_ROWS="$2"; shift 2 ;;
    --frequency-progress-every-chunks) FREQUENCY_PROGRESS_EVERY_CHUNKS="$2"; shift 2 ;;
    --weather-generation-max-rows) WEATHER_GENERATION_MAX_ROWS="$2"; shift 2 ;;
    --additional-max-rows-per-dataset) ADDITIONAL_MAX_ROWS_PER_DATASET="$2"; shift 2 ;;
    --additional-batch-size) ADDITIONAL_BATCH_SIZE="$2"; shift 2 ;;
    --additional-training-mode) ADDITIONAL_TRAINING_MODE="$2"; shift 2 ;;
    --additional-incremental-threshold-rows) ADDITIONAL_INCREMENTAL_THRESHOLD_ROWS="$2"; shift 2 ;;
    --additional-progress-every-chunks) ADDITIONAL_PROGRESS_EVERY_CHUNKS="$2"; shift 2 ;;
    --monster-rounds) MONSTER_ROUNDS="$2"; shift 2 ;;
    --monster-estimators-base) MONSTER_ESTIMATORS_BASE="$2"; shift 2 ;;
    --monster-max-frequency-files) MONSTER_MAX_FREQUENCY_FILES="$2"; shift 2 ;;
    --monster-row-stride) MONSTER_ROW_STRIDE="$2"; shift 2 ;;
    --monster-max-rows) MONSTER_MAX_ROWS="$2"; shift 2 ;;
    --monster-max-frequency-raw-rows) MONSTER_MAX_FREQUENCY_RAW_ROWS="$2"; shift 2 ;;
    --monster-source-max-rows) MONSTER_SOURCE_MAX_ROWS="$2"; shift 2 ;;
    --monster-source-batch-size) MONSTER_SOURCE_BATCH_SIZE="$2"; shift 2 ;;
    --monster-disable-expert-features) MONSTER_DISABLE_EXPERT_FEATURES="$2"; shift 2 ;;
    --monster-required) MONSTER_REQUIRED="$2"; shift 2 ;;
    --oom-retry-monster-rounds) OOM_RETRY_MONSTER_ROUNDS="$2"; shift 2 ;;
    --oom-retry-monster-base-estimators) OOM_RETRY_MONSTER_BASE_ESTIMATORS="$2"; shift 2 ;;
    --oom-retry-monster-max-frequency-files) OOM_RETRY_MONSTER_MAX_FREQUENCY_FILES="$2"; shift 2 ;;
    --oom-retry-monster-row-stride) OOM_RETRY_MONSTER_ROW_STRIDE="$2"; shift 2 ;;
    --oom-retry-monster-max-rows) OOM_RETRY_MONSTER_MAX_ROWS="$2"; shift 2 ;;
    --oom-retry-monster-max-frequency-raw-rows) OOM_RETRY_MONSTER_MAX_FREQUENCY_RAW_ROWS="$2"; shift 2 ;;
    --oom-retry-monster-source-max-rows) OOM_RETRY_MONSTER_SOURCE_MAX_ROWS="$2"; shift 2 ;;
    --oom-retry-monster-source-batch-size) OOM_RETRY_MONSTER_SOURCE_BATCH_SIZE="$2"; shift 2 ;;
    --oom-retry-generation-max-rows) OOM_RETRY_GENERATION_MAX_ROWS="$2"; shift 2 ;;
    --oom-retry-gridwatch-max-rows) OOM_RETRY_GRIDWATCH_MAX_ROWS="$2"; shift 2 ;;
    --oom-retry-gridwatch-batch-size) OOM_RETRY_GRIDWATCH_BATCH_SIZE="$2"; shift 2 ;;
    --oom-retry-gridwatch-cuda-shard-rows) OOM_RETRY_GRIDWATCH_CUDA_SHARD_ROWS="$2"; shift 2 ;;
    --oom-retry-additional-max-rows-per-dataset) OOM_RETRY_ADDITIONAL_MAX_ROWS_PER_DATASET="$2"; shift 2 ;;
    --conda-env) CONDA_ENV="$2"; shift 2 ;;
    --conda-base) CONDA_BASE="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if [[ ! -x "$CONDA_BASE/bin/conda" ]]; then
  log_error "conda not found at: $CONDA_BASE/bin/conda" >&2
  log_warn "pass --conda-base /path/to/miniconda3" >&2
  exit 1
fi

export PATH="$CONDA_BASE/bin:$PATH"
eval "$($CONDA_BASE/bin/conda shell.bash hook)"
conda activate "$CONDA_ENV"

if [[ "$DEVICE" == "cuda" || "$DEVICE" == "auto" ]]; then
  log_info "CUDA preflight: python=$(which python)"
  set +e
  python - <<'PY'
import importlib, sys
missing = []
for mod in ("cudf", "cuml"):
    try:
        importlib.import_module(mod)
    except Exception:
        missing.append(mod)
if missing:
    print("[error] Missing CUDA packages in active interpreter:", ", ".join(missing))
    print("[hint] Ensure this script runs in the RAPIDS conda env (e.g. tsgb_rapids).")
    sys.exit(3)
print("[info] CUDA preflight: cudf/cuml import OK")
PY
  preflight_status=$?
  set -e
  if [[ $preflight_status -ne 0 ]]; then
    exit $preflight_status
  fi
fi

cd "$SCRIPT_DIR"

run_step() {
  local title="$1"
  shift
  local step_started_epoch="$(date +%s)"
  echo ""
  print_divider
  log_step "$title"
  log_cmd "$*"
  print_divider

  set +e
  "$@"
  local status=$?
  set -e

  if [[ $status -eq 0 ]]; then
    local step_finished_epoch="$(date +%s)"
    record_step_result "$title" $(( step_finished_epoch - step_started_epoch )) "ok" "primary"
    return 0
  fi

  if [[ $status -eq 137 ]]; then
    log_warn "Step '$title' was OOM-killed (exit 137). Retrying with safer settings..."

    if [[ "$*" == *"train_system_state_monster.py"* ]]; then
      local monster_cmd=()
      local prev_rounds=0
      local prev_base=0
      local prev_files=0
      local prev_stride=0
      local prev_rows=0

      for arg in "$@"; do
        if [[ $prev_rounds -eq 1 ]]; then
          monster_cmd+=("$OOM_RETRY_MONSTER_ROUNDS")
          prev_rounds=0
          continue
        fi
        if [[ $prev_base -eq 1 ]]; then
          monster_cmd+=("$OOM_RETRY_MONSTER_BASE_ESTIMATORS")
          prev_base=0
          continue
        fi
        if [[ $prev_files -eq 1 ]]; then
          monster_cmd+=("$OOM_RETRY_MONSTER_MAX_FREQUENCY_FILES")
          prev_files=0
          continue
        fi
        if [[ $prev_stride -eq 1 ]]; then
          monster_cmd+=("$OOM_RETRY_MONSTER_ROW_STRIDE")
          prev_stride=0
          continue
        fi
        if [[ $prev_rows -eq 1 ]]; then
          monster_cmd+=("$OOM_RETRY_MONSTER_MAX_ROWS")
          prev_rows=0
          continue
        fi

        monster_cmd+=("$arg")
        case "$arg" in
          --rounds) prev_rounds=1 ;;
          --base-estimators) prev_base=1 ;;
          --max-frequency-files) prev_files=1 ;;
          --row-stride) prev_stride=1 ;;
          --max-rows) prev_rows=1 ;;
        esac
      done

      log_cmd "${monster_cmd[*]}"
      "${monster_cmd[@]}"
      local step_finished_epoch="$(date +%s)"
      record_step_result "$title" $(( step_finished_epoch - step_started_epoch )) "ok" "oom->monster-retry"
      return 0
    fi

    if [[ "$*" == *"train_gridwatch_expert.py"* ]]; then
      local gridwatch_cuda_cmd=()
      local prev_is_device=0
      local prev_is_estimators=0
      local prev_is_max_rows=0
      local prev_is_batch_size=0
      local prev_is_training_mode=0
      local prev_is_cuda_shard_rows=0

      for arg in "$@"; do
        if [[ $prev_is_device -eq 1 ]]; then
          gridwatch_cuda_cmd+=("cuda")
          prev_is_device=0
          continue
        fi

        if [[ $prev_is_estimators -eq 1 ]]; then
          gridwatch_cuda_cmd+=("$OOM_RETRY_ESTIMATORS")
          prev_is_estimators=0
          continue
        fi

        if [[ $prev_is_max_rows -eq 1 ]]; then
          gridwatch_cuda_cmd+=("0")
          prev_is_max_rows=0
          continue
        fi

        if [[ $prev_is_batch_size -eq 1 ]]; then
          gridwatch_cuda_cmd+=("$OOM_RETRY_GRIDWATCH_BATCH_SIZE")
          prev_is_batch_size=0
          continue
        fi

        if [[ $prev_is_training_mode -eq 1 ]]; then
          gridwatch_cuda_cmd+=("cuda-sharded")
          prev_is_training_mode=0
          continue
        fi

        if [[ $prev_is_cuda_shard_rows -eq 1 ]]; then
          gridwatch_cuda_cmd+=("$OOM_RETRY_GRIDWATCH_CUDA_SHARD_ROWS")
          prev_is_cuda_shard_rows=0
          continue
        fi

        gridwatch_cuda_cmd+=("$arg")
        if [[ "$arg" == "--device" ]]; then
          prev_is_device=1
        elif [[ "$arg" == "--n-estimators" ]]; then
          prev_is_estimators=1
        elif [[ "$arg" == "--max-rows" ]]; then
          prev_is_max_rows=1
        elif [[ "$arg" == "--batch-size" ]]; then
          prev_is_batch_size=1
        elif [[ "$arg" == "--training-mode" ]]; then
          prev_is_training_mode=1
        elif [[ "$arg" == "--cuda-shard-rows" ]]; then
          prev_is_cuda_shard_rows=1
        fi
      done

      if [[ ! " ${gridwatch_cuda_cmd[*]} " =~ " --training-mode " ]]; then
        gridwatch_cuda_cmd+=(--training-mode cuda-sharded)
      fi
      if [[ ! " ${gridwatch_cuda_cmd[*]} " =~ " --cuda-shard-rows " ]]; then
        gridwatch_cuda_cmd+=(--cuda-shard-rows "$OOM_RETRY_GRIDWATCH_CUDA_SHARD_ROWS")
      fi
      if [[ ! " ${gridwatch_cuda_cmd[*]} " =~ " --batch-size " ]]; then
        gridwatch_cuda_cmd+=(--batch-size "$OOM_RETRY_GRIDWATCH_BATCH_SIZE")
      fi
      if [[ ! " ${gridwatch_cuda_cmd[*]} " =~ " --max-rows " ]]; then
        gridwatch_cuda_cmd+=(--max-rows 0)
      fi

      log_info "Gridwatch CUDA-first retry before CPU fallback."
      log_cmd "${gridwatch_cuda_cmd[*]}"
      set +e
      "${gridwatch_cuda_cmd[@]}"
      local gridwatch_cuda_status=$?
      set -e

      if [[ $gridwatch_cuda_status -eq 0 ]]; then
        local step_finished_epoch="$(date +%s)"
        record_step_result "$title" $(( step_finished_epoch - step_started_epoch )) "ok" "oom->cuda-sharded-retry"
        return 0
      fi

      log_warn "Gridwatch CUDA retry failed (exit $gridwatch_cuda_status), falling back to CPU-safe retry."
    fi

    local fallback_cmd=()
    local prev_is_device=0
    local prev_is_estimators=0
    local prev_is_max_rows=0
    local prev_is_max_rows_per_dataset=0
    for arg in "$@"; do
      if [[ $prev_is_device -eq 1 ]]; then
        fallback_cmd+=("cpu")
        prev_is_device=0
        continue
      fi

      if [[ $prev_is_estimators -eq 1 ]]; then
        fallback_cmd+=("$OOM_RETRY_ESTIMATORS")
        prev_is_estimators=0
        continue
      fi

      if [[ $prev_is_max_rows -eq 1 ]]; then
        fallback_cmd+=("$OOM_RETRY_GENERATION_MAX_ROWS")
        prev_is_max_rows=0
        continue
      fi

      if [[ $prev_is_max_rows_per_dataset -eq 1 ]]; then
        fallback_cmd+=("$OOM_RETRY_ADDITIONAL_MAX_ROWS_PER_DATASET")
        prev_is_max_rows_per_dataset=0
        continue
      fi

      fallback_cmd+=("$arg")
      if [[ "$arg" == "--device" ]]; then
        prev_is_device=1
      elif [[ "$arg" == "--n-estimators" ]]; then
        prev_is_estimators=1
      elif [[ "$arg" == "--max-rows" ]]; then
        prev_is_max_rows=1
      elif [[ "$arg" == "--max-rows-per-dataset" ]]; then
        prev_is_max_rows_per_dataset=1
      fi
    done

    log_cmd "${fallback_cmd[*]}"
    "${fallback_cmd[@]}"
    local step_finished_epoch="$(date +%s)"
    record_step_result "$title" $(( step_finished_epoch - step_started_epoch )) "ok" "oom->cpu-fallback"
    return 0
  fi

  local step_finished_epoch="$(date +%s)"
  record_step_result "$title" $(( step_finished_epoch - step_started_epoch )) "failed" "exit=$status"
  return $status
}

log_info "Project root : $PROJECT_ROOT"
log_info "Experts dir  : $SCRIPT_DIR"
log_info "Conda env    : $CONDA_ENV"
log_info "Device       : $DEVICE"
log_info "Estimators   : $N_ESTIMATORS"
log_info "OOM retry est: $OOM_RETRY_ESTIMATORS"
log_info "Gen rows     : $GENERATION_MAX_ROWS (oom retry -> $OOM_RETRY_GENERATION_MAX_ROWS)"
log_info "Gridwatch OOM retry rows (CUDA-first): $OOM_RETRY_GRIDWATCH_MAX_ROWS"
if [[ "$GRIDWATCH_MAX_ROWS" == "0" ]]; then
  log_info "Grid rows    : all batch=$GRIDWATCH_BATCH_SIZE mode=$GRIDWATCH_TRAINING_MODE shard_rows=$GRIDWATCH_CUDA_SHARD_ROWS cuda_batch_cap=$GRIDWATCH_CUDA_BATCH_MAX_ROWS progress_chunks=$GRIDWATCH_PROGRESS_EVERY_CHUNKS"
else
  log_info "Grid rows    : $GRIDWATCH_MAX_ROWS batch=$GRIDWATCH_BATCH_SIZE mode=$GRIDWATCH_TRAINING_MODE shard_rows=$GRIDWATCH_CUDA_SHARD_ROWS cuda_batch_cap=$GRIDWATCH_CUDA_BATCH_MAX_ROWS progress_chunks=$GRIDWATCH_PROGRESS_EVERY_CHUNKS"
fi
log_info "Freq mode    : $FREQUENCY_TRAINING_MODE shard_rows=$FREQUENCY_CUDA_SHARD_ROWS target_vram_gb=$FREQUENCY_CUDA_TARGET_VRAM_GB fraction=$FREQUENCY_CUDA_TARGET_VRAM_FRACTION cap_gb=$FREQUENCY_CUDA_TARGET_VRAM_CAP_GB gpus=$FREQUENCY_CUDA_GPUS cuda_batch_cap=$FREQUENCY_CUDA_BATCH_MAX_ROWS progress_chunks=$FREQUENCY_PROGRESS_EVERY_CHUNKS"
log_info "Wx+Gen rows  : $WEATHER_GENERATION_MAX_ROWS"
log_info "Additional   : rows=$ADDITIONAL_MAX_ROWS_PER_DATASET batch=$ADDITIONAL_BATCH_SIZE mode=$ADDITIONAL_TRAINING_MODE threshold=$ADDITIONAL_INCREMENTAL_THRESHOLD_ROWS progress_chunks=$ADDITIONAL_PROGRESS_EVERY_CHUNKS"
log_info "Monster caps : files=$MONSTER_MAX_FREQUENCY_FILES stride=$MONSTER_ROW_STRIDE rows=$MONSTER_MAX_ROWS raw_freq_rows=$MONSTER_MAX_FREQUENCY_RAW_ROWS source_rows=$MONSTER_SOURCE_MAX_ROWS source_batch=$MONSTER_SOURCE_BATCH_SIZE"
log_info "Monster retry: rounds=$OOM_RETRY_MONSTER_ROUNDS base=$OOM_RETRY_MONSTER_BASE_ESTIMATORS files=$OOM_RETRY_MONSTER_MAX_FREQUENCY_FILES stride=$OOM_RETRY_MONSTER_ROW_STRIDE rows=$OOM_RETRY_MONSTER_MAX_ROWS raw_freq_rows=$OOM_RETRY_MONSTER_MAX_FREQUENCY_RAW_ROWS source_rows=$OOM_RETRY_MONSTER_SOURCE_MAX_ROWS source_batch=$OOM_RETRY_MONSTER_SOURCE_BATCH_SIZE"
log_info "Monster expert features disabled: $MONSTER_DISABLE_EXPERT_FEATURES"
log_info "Monster required: $MONSTER_REQUIRED"

echo ""
log_info "Training sequence:"
printf "  %b1)%b Base experts (independent datasets)\n" "$C_BOLD$C_BLUE" "$C_RESET"
printf "  %b2)%b Composite experts (weather+generation/inertia)\n" "$C_BOLD$C_BLUE" "$C_RESET"
printf "  %b3)%b Additional parquet experts\n" "$C_BOLD$C_BLUE" "$C_RESET"
printf "  %b4)%b Monster model (uses expert artifacts from previous steps)\n" "$C_BOLD$C_BLUE" "$C_RESET"

# 1) Base experts (independent)
run_step "Generation expert" \
  python train_generation_expert.py --device "$DEVICE" --n-estimators "$N_ESTIMATORS" \
    --max-rows "$GENERATION_MAX_ROWS"

run_step "Weather expert" \
  python train_weather_expert.py --device "$DEVICE" --n-estimators "$N_ESTIMATORS"

run_step "Demand expert" \
  python train_demand_expert.py --device "$DEVICE" --n-estimators "$N_ESTIMATORS"

run_step "Inertia expert" \
  python train_inertia_expert.py --device "$DEVICE" --n-estimators "$N_ESTIMATORS"

run_step "Gridwatch expert" \
  python train_gridwatch_expert.py --device "$DEVICE" --n-estimators "$N_ESTIMATORS" \
    --max-rows "$GRIDWATCH_MAX_ROWS" --batch-size "$GRIDWATCH_BATCH_SIZE" \
    --training-mode "$GRIDWATCH_TRAINING_MODE" \
    --cuda-shard-rows "$GRIDWATCH_CUDA_SHARD_ROWS" \
    --cuda-batch-max-rows "$GRIDWATCH_CUDA_BATCH_MAX_ROWS" \
    --progress-every-chunks "$GRIDWATCH_PROGRESS_EVERY_CHUNKS"

run_step "Frequency expert" \
  python train_frequency_expert.py --device "$DEVICE" --n-estimators "$N_ESTIMATORS" \
    --max-rows "$FREQUENCY_MAX_ROWS" --batch-size "$FREQUENCY_BATCH_SIZE" \
    --training-mode "$FREQUENCY_TRAINING_MODE" \
    --cuda-shard-rows "$FREQUENCY_CUDA_SHARD_ROWS" \
    --cuda-target-vram-gb "$FREQUENCY_CUDA_TARGET_VRAM_GB" \
    --cuda-target-vram-fraction "$FREQUENCY_CUDA_TARGET_VRAM_FRACTION" \
    --cuda-target-vram-cap-gb "$FREQUENCY_CUDA_TARGET_VRAM_CAP_GB" \
    --cuda-gpus "$FREQUENCY_CUDA_GPUS" \
    --cuda-batch-max-rows "$FREQUENCY_CUDA_BATCH_MAX_ROWS" \
    --progress-every-chunks "$FREQUENCY_PROGRESS_EVERY_CHUNKS"

run_step "Balancing expert" \
  python train_balancing_expert.py --device "$DEVICE" --n-estimators "$N_ESTIMATORS"

# 2) Composite experts
run_step "Weather + Generation expert" \
  python train_weather_generation_expert.py --device "$DEVICE" --n-estimators "$N_ESTIMATORS" \
    --max-rows "$WEATHER_GENERATION_MAX_ROWS"

run_step "Weather + Inertia expert" \
  python train_weather_inertia_expert.py --device "$DEVICE" --n-estimators "$N_ESTIMATORS"

# 3) Additional parquet experts
run_step "Additional parquet experts" \
  python train_additional_parquet_experts.py --device "$DEVICE" --n-estimators "$N_ESTIMATORS" \
    --max-rows-per-dataset "$ADDITIONAL_MAX_ROWS_PER_DATASET" --batch-size "$ADDITIONAL_BATCH_SIZE" \
    --training-mode "$ADDITIONAL_TRAINING_MODE" \
    --incremental-threshold-rows "$ADDITIONAL_INCREMENTAL_THRESHOLD_ROWS" \
    --progress-every-chunks "$ADDITIONAL_PROGRESS_EVERY_CHUNKS"

# 4) Monster model (depends on artifacts from prior experts)
monster_started_epoch="$(date +%s)"
echo ""
print_divider
if [[ "$MONSTER_DISABLE_EXPERT_FEATURES" == "1" ]]; then
  log_step "System-state monster model (no expert features)"
else
  log_step "System-state monster model"
fi
print_divider

monster_cmd=(
  python train_system_state_monster.py
  --rounds "$MONSTER_ROUNDS"
  --base-estimators "$MONSTER_ESTIMATORS_BASE"
  --max-frequency-files "$MONSTER_MAX_FREQUENCY_FILES"
  --row-stride "$MONSTER_ROW_STRIDE"
  --max-rows "$MONSTER_MAX_ROWS"
  --max-frequency-raw-rows "$MONSTER_MAX_FREQUENCY_RAW_ROWS"
  --source-max-rows "$MONSTER_SOURCE_MAX_ROWS"
  --source-batch-size "$MONSTER_SOURCE_BATCH_SIZE"
)

if [[ "$MONSTER_DISABLE_EXPERT_FEATURES" == "1" ]]; then
  monster_cmd+=(--disable-expert-features)
fi

log_cmd "${monster_cmd[*]}"
set +e
"${monster_cmd[@]}"
monster_status=$?
set -e

if [[ $monster_status -ne 0 ]]; then
  log_warn "Monster primary run failed (exit $monster_status). Trying low-memory fallback..."

  retry_rounds=$(( MONSTER_ROUNDS / 2 ))
  if [[ $retry_rounds -lt 1 ]]; then
    retry_rounds=1
  fi
  if [[ $retry_rounds -gt $OOM_RETRY_MONSTER_ROUNDS ]]; then
    retry_rounds=$OOM_RETRY_MONSTER_ROUNDS
  fi

  retry_base=$(( MONSTER_ESTIMATORS_BASE / 2 ))
  if [[ $retry_base -lt 10 ]]; then
    retry_base=10
  fi
  if [[ $retry_base -gt $OOM_RETRY_MONSTER_BASE_ESTIMATORS ]]; then
    retry_base=$OOM_RETRY_MONSTER_BASE_ESTIMATORS
  fi

  retry_files=$(( MONSTER_MAX_FREQUENCY_FILES / 2 ))
  if [[ $retry_files -lt 1 ]]; then
    retry_files=1
  fi
  if [[ $retry_files -gt $OOM_RETRY_MONSTER_MAX_FREQUENCY_FILES ]]; then
    retry_files=$OOM_RETRY_MONSTER_MAX_FREQUENCY_FILES
  fi

  retry_stride=$(( MONSTER_ROW_STRIDE * 2 ))
  if [[ $retry_stride -lt $OOM_RETRY_MONSTER_ROW_STRIDE ]]; then
    retry_stride=$OOM_RETRY_MONSTER_ROW_STRIDE
  fi

  retry_rows=$(( MONSTER_MAX_ROWS / 2 ))
  if [[ $retry_rows -lt 10000 ]]; then
    retry_rows=10000
  fi
  if [[ $retry_rows -gt $OOM_RETRY_MONSTER_MAX_ROWS ]]; then
    retry_rows=$OOM_RETRY_MONSTER_MAX_ROWS
  fi

  retry_raw_freq_rows=$(( MONSTER_MAX_FREQUENCY_RAW_ROWS / 2 ))
  if [[ $retry_raw_freq_rows -lt 100000 ]]; then
    retry_raw_freq_rows=100000
  fi
  if [[ $retry_raw_freq_rows -gt $OOM_RETRY_MONSTER_MAX_FREQUENCY_RAW_ROWS ]]; then
    retry_raw_freq_rows=$OOM_RETRY_MONSTER_MAX_FREQUENCY_RAW_ROWS
  fi

  retry_source_rows=$(( MONSTER_SOURCE_MAX_ROWS / 2 ))
  if [[ $retry_source_rows -lt 50000 ]]; then
    retry_source_rows=50000
  fi
  if [[ $retry_source_rows -gt $OOM_RETRY_MONSTER_SOURCE_MAX_ROWS ]]; then
    retry_source_rows=$OOM_RETRY_MONSTER_SOURCE_MAX_ROWS
  fi

  retry_source_batch=$MONSTER_SOURCE_BATCH_SIZE
  if [[ $retry_source_batch -gt $OOM_RETRY_MONSTER_SOURCE_BATCH_SIZE ]]; then
    retry_source_batch=$OOM_RETRY_MONSTER_SOURCE_BATCH_SIZE
  fi

  monster_retry_cmd=(
    python train_system_state_monster.py
    --rounds "$retry_rounds"
    --base-estimators "$retry_base"
    --max-frequency-files "$retry_files"
    --row-stride "$retry_stride"
    --max-rows "$retry_rows"
    --max-frequency-raw-rows "$retry_raw_freq_rows"
    --source-max-rows "$retry_source_rows"
    --source-batch-size "$retry_source_batch"
  )
  if [[ "$MONSTER_DISABLE_EXPERT_FEATURES" == "1" ]]; then
    monster_retry_cmd+=(--disable-expert-features)
  fi

  log_cmd "${monster_retry_cmd[*]}"
  set +e
  "${monster_retry_cmd[@]}"
  monster_retry_status=$?
  set -e

  if [[ $monster_retry_status -ne 0 ]]; then
    log_warn "Monster low-memory retry failed (exit $monster_retry_status). Trying ultra-safe fallback..."

    ultra_rounds=1
    ultra_base=20
    ultra_files=1
    ultra_stride=$(( retry_stride * 2 ))
    if [[ $ultra_stride -lt 48 ]]; then
      ultra_stride=48
    fi
    ultra_rows=$(( retry_rows / 2 ))
    if [[ $ultra_rows -lt 20000 ]]; then
      ultra_rows=20000
    fi
    ultra_raw_freq_rows=$(( retry_raw_freq_rows / 2 ))
    if [[ $ultra_raw_freq_rows -lt 100000 ]]; then
      ultra_raw_freq_rows=100000
    fi
    ultra_source_rows=$(( retry_source_rows / 2 ))
    if [[ $ultra_source_rows -lt 30000 ]]; then
      ultra_source_rows=30000
    fi
    ultra_source_batch=$retry_source_batch
    if [[ $ultra_source_batch -gt 25000 ]]; then
      ultra_source_batch=25000
    fi

    monster_ultra_cmd=(
      python train_system_state_monster.py
      --rounds "$ultra_rounds"
      --base-estimators "$ultra_base"
      --max-frequency-files "$ultra_files"
      --row-stride "$ultra_stride"
      --max-rows "$ultra_rows"
      --max-frequency-raw-rows "$ultra_raw_freq_rows"
      --source-max-rows "$ultra_source_rows"
      --source-batch-size "$ultra_source_batch"
    )
    if [[ "$MONSTER_DISABLE_EXPERT_FEATURES" == "1" ]]; then
      monster_ultra_cmd+=(--disable-expert-features)
    fi

    log_cmd "${monster_ultra_cmd[*]}"
    set +e
    "${monster_ultra_cmd[@]}"
    monster_ultra_status=$?
    set -e

    if [[ $monster_ultra_status -eq 0 ]]; then
      log_info "Monster ultra-safe fallback completed successfully."
      monster_retry_status=0
    fi
  fi

  if [[ $monster_retry_status -ne 0 ]]; then
    if [[ "$MONSTER_REQUIRED" == "1" ]]; then
      log_error "Monster training failed and MONSTER_REQUIRED=1, aborting."
      monster_finished_epoch="$(date +%s)"
      record_step_result "System-state monster model" $(( monster_finished_epoch - monster_started_epoch )) "failed" "required=1 exit=$monster_retry_status"
      exit $monster_retry_status
    fi
    log_warn "Monster training skipped after repeated failures (primary=$monster_status, retry=$monster_retry_status)."
    monster_finished_epoch="$(date +%s)"
    record_step_result "System-state monster model" $(( monster_finished_epoch - monster_started_epoch )) "warn" "skipped primary=$monster_status retry=$monster_retry_status"
  else
    monster_finished_epoch="$(date +%s)"
    record_step_result "System-state monster model" $(( monster_finished_epoch - monster_started_epoch )) "ok" "fallback"
  fi
else
  monster_finished_epoch="$(date +%s)"
  record_step_result "System-state monster model" $(( monster_finished_epoch - monster_started_epoch )) "ok" "primary"
fi

print_final_summary

echo ""
log_done "All expert training steps completed successfully."
log_done "Artifacts directory: $SCRIPT_DIR/pre-trained-experts"
