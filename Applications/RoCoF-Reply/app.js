const state = {
  frames: [],
  frameIndex: 0,
  fps: 30,
  timer: null,
  fuelMax: {},
  availableFuels: [],
  eventLines: ['[events] ready'],
  lastLoggedFrameIndex: -1,
  prevFlowLabel: null,
  prevRocofLabel: null,
  prevFrequencyLabel: null,
  frequencyHistory: [],
  rocofHistory: [],
  rpmHistory: [],
  torqueDemandHistory: [],
  torqueActualHistory: [],
  instantBalanceHistory: [],
  availableInterconnectors: []
};

const fpsInput = document.getElementById('fpsInput');
const gvaInput = document.getElementById('gvaInput');
const hGasInput = document.getElementById('hGasInput');
const hCoalInput = document.getElementById('hCoalInput');
const hHydroInput = document.getElementById('hHydroInput');
const hMechInput = document.getElementById('hMechInput');
const playBtn = document.getElementById('playBtn');
const pauseBtn = document.getElementById('pauseBtn');
const fileInput = document.getElementById('jsonFileInput');
const slider = document.getElementById('frameSlider');
const leftSidebar = document.getElementById('leftSidebar');
const rightSidebar = document.getElementById('rightSidebar');
const toggleLeftBtn = document.getElementById('toggleLeftBtn');
const toggleRightBtn = document.getElementById('toggleRightBtn');
const toggleLeftCenterBtn = document.getElementById('toggleLeftCenterBtn');
const toggleRightCenterBtn = document.getElementById('toggleRightCenterBtn');

const rocofValue = document.getElementById('rocofValue');
const rocofStatus = document.getElementById('rocofStatus');
const timestampLabel = document.getElementById('timestampLabel');
const totalGeneration = document.getElementById('totalGeneration');
const estimatedDemand = document.getElementById('estimatedDemand');
const frequencyBand = document.getElementById('frequencyBand');
const frequencyValue = document.getElementById('frequencyValue');
const frequencyStatus = document.getElementById('frequencyStatus');
const rpmValue = document.getElementById('rpmValue');
const torqueDemandValue = document.getElementById('torqueDemandValue');
const torqueActualValue = document.getElementById('torqueActualValue');
const systemFlow = document.getElementById('systemFlow');
const systemFlowDetail = document.getElementById('systemFlowDetail');
const inertiaAvailable = document.getElementById('inertiaAvailable');
const inertiaTarget = document.getElementById('inertiaTarget');
const inertiaDiff = document.getElementById('inertiaDiff');
const frameCounter = document.getElementById('frameCounter');
const instantFactor = document.getElementById('instantFactor');
const instantGeneration = document.getElementById('instantGeneration');
const instantBalance = document.getElementById('instantBalance');
const instantBalanceStatus = document.getElementById('instantBalanceStatus');
const instantInertia = document.getElementById('instantInertia');
const instantCoverage = document.getElementById('instantCoverage');
const instantInertiaDelta = document.getElementById('instantInertiaDelta');
const instantInertiaDeltaStatus = document.getElementById('instantInertiaDeltaStatus');
const instantBalanceSparkline = document.getElementById('instantBalanceSparkline');
const instantBalanceSparklineCtx = instantBalanceSparkline?.getContext('2d');
const fuelDials = document.getElementById('fuelDials');
const interconnectorDials = document.getElementById('interconnectorDials');
const eventConsole = document.getElementById('eventConsole');
const frequencyChartCanvas = document.getElementById('frequencyChart');
const frequencyChartCtx = frequencyChartCanvas?.getContext('2d');

const gaugeCanvas = document.getElementById('rocofGauge');
const gaugeCtx = gaugeCanvas.getContext('2d');
const rpmCanvas = document.getElementById('rpmChart');
const rpmCtx = rpmCanvas?.getContext('2d');
const torqueCanvas = document.getElementById('torqueChart');
const torqueCtx = torqueCanvas?.getContext('2d');

const DEFAULT_JSON_PATH = './derived_rocof_replay.json';
const ROCOF_RANGE = 0.5;
const ROCOF_DEADBAND = 0.005;
const FREQUENCY_HISTORY_LIMIT = 240;
const ROCOF_HISTORY_LIMIT = 240;
const RPM_HISTORY_LIMIT = 240;
const TORQUE_HISTORY_LIMIT = 240;
const INSTANT_HISTORY_LIMIT = 240;
const FREQUENCY_MIN_HZ = 49.0;
const FREQUENCY_MAX_HZ = 51.0;
const FREQUENCY_TICKS_HZ = [49.0, 49.1, 49.5, 49.95, 50.0, 50.05, 50.5, 50.9, 51.0];
const TORQUE_NM_PER_MW = 3183;
const ALL_TIME_FUEL_MAX_MW = {
  GAS: 27868,
  COAL: 26044,
  NUCLEAR: 9342,
  WIND: 18382,
  WIND_EMB: 5947,
  SOLAR: 14035,
  HYDRO: 1403,
  BIOMASS: 3393,
  STORAGE: 2660,
  IMPORTS: 9148,
  OTHER: 3187,
};

function fmtMw(v) {
  if (v == null || Number.isNaN(v)) return '-- MW';
  return `${Math.round(v).toLocaleString()} MW`;
}

function fmtHz(v) {
  if (v == null || Number.isNaN(v)) return '-- Hz';
  return `${v.toFixed(3)} Hz`;
}

function clamp(v, lo, hi) {
  return Math.max(lo, Math.min(hi, v));
}

function normalizeRocof(raw) {
  if (!Number.isFinite(raw)) return NaN;
  if (Math.abs(raw) < ROCOF_DEADBAND) return 0;
  return raw;
}

function directionColour(rocof) {
  if (!Number.isFinite(rocof) || rocof === 0) return '#4ea1ff';
  return rocof > 0 ? '#e74c3c' : '#2ecc71';
}

function rocofBandColor(value) {
  const absVal = Math.abs(value);
  if (absVal <= 0.5) return '#62b6ff';
  if (absVal > 0.5 && absVal < 0.7) return '#f6c76b';
  return '#ff6f6f';
}

function setClass(el, className) {
  if (!el) return;
  el.className = className;
}

function updateSidebarToggleLabels() {
  const leftCollapsed = document.body.classList.contains('left-collapsed');
  const rightCollapsed = document.body.classList.contains('right-collapsed');

  if (toggleLeftBtn) {
    toggleLeftBtn.textContent = leftCollapsed ? '▶' : '◀';
    toggleLeftBtn.setAttribute('aria-label', leftCollapsed ? 'Expand left sidebar' : 'Collapse left sidebar');
  }
  if (toggleRightBtn) {
    toggleRightBtn.textContent = rightCollapsed ? '◀' : '▶';
    toggleRightBtn.setAttribute('aria-label', rightCollapsed ? 'Expand right sidebar' : 'Collapse right sidebar');
  }
  if (toggleLeftCenterBtn) {
    toggleLeftCenterBtn.textContent = leftCollapsed ? 'Show Left' : 'Hide Left';
  }
  if (toggleRightCenterBtn) {
    toggleRightCenterBtn.textContent = rightCollapsed ? 'Show Right' : 'Hide Right';
  }
}

function setCanvasSquareSize(canvas, minSize = 300, maxSize = 680) {
  if (!canvas) return false;
  const panel = canvas.closest('.chart-panel');
  if (!panel) return false;

  const panelWidth = Math.max(0, panel.clientWidth - 22);
  const viewportDriven = Math.floor(window.innerHeight * 0.38);
  const size = clamp(Math.floor(Math.min(panelWidth, viewportDriven)), minSize, maxSize);
  if (!Number.isFinite(size) || size <= 0) return false;

  const changed = canvas.width !== size || canvas.height !== size;
  if (changed) {
    canvas.width = size;
    canvas.height = size;
  }
  return changed;
}

function resizeCharts(redraw = true) {
  const changed = [gaugeCanvas, frequencyChartCanvas, rpmCanvas, torqueCanvas]
    .map((canvas) => setCanvasSquareSize(canvas))
    .some(Boolean);

  if (!redraw || (!changed && state.frames.length)) return;

  drawGauge(state.rocofHistory.length ? state.rocofHistory[state.rocofHistory.length - 1] : 0);
  drawFrequencyChart();
  drawRpmChart();
  drawTorqueChart();
  drawInstantBalanceSparkline();
}

function toggleSidebar(side) {
  if (side === 'left' && leftSidebar) {
    document.body.classList.toggle('left-collapsed');
  }
  if (side === 'right' && rightSidebar) {
    document.body.classList.toggle('right-collapsed');
  }
  updateSidebarToggleLabels();
  resizeCharts(true);
}

function getInertiaHForFuel(fuel) {
  if (fuel === 'IMPORTS' || fuel === 'WIND' || fuel === 'WIND_EMB' || fuel === 'SOLAR') return 0;
  if (fuel === 'COAL') return Number(hCoalInput?.value) || 6;
  if (fuel === 'NUCLEAR') return 6;
  if (fuel === 'GAS') return Number(hGasInput?.value) || 5;
  if (fuel === 'HYDRO') return Number(hHydroInput?.value) || 3;
  return Number(hMechInput?.value) || 2.5;
}

function estimateFuelInertiaGva(mw, hConstant) {
  if (!Number.isFinite(mw) || !Number.isFinite(hConstant)) return 0;
  return (mw * hConstant) / 1000;
}

function getInertiaStatus(diffGva) {
  if (!Number.isFinite(diffGva)) {
    return { label: 'Δ -- GVA', cls: 'status-inline status-balanced' };
  }
  if (Math.abs(diffGva) <= 10) {
    return { label: `IDEAL Δ ${diffGva.toFixed(1)} GVA`, cls: 'status-inline status-stable' };
  }
  if (diffGva < 0) {
    return { label: `LIGHT Δ ${diffGva.toFixed(1)} GVA`, cls: 'status-inline status-import' };
  }
  return { label: `HEAVY Δ +${diffGva.toFixed(1)} GVA`, cls: 'status-inline status-export' };
}

function getRocofStatus(absRocof) {
  if (!Number.isFinite(absRocof) || absRocof < 0.005) {
    return { label: 'STABLE', cls: 'status-chip status-stable', flash: false };
  }
  if (absRocof > 0.005 && absRocof < 0.05) {
    return { label: 'SHIVERING', cls: 'status-chip status-shiver', flash: false };
  }
  if (absRocof >= 0.05 && absRocof < 0.75) {
    return { label: 'STRESSED', cls: 'status-chip status-stressed-red', flash: false };
  }
  return { label: 'LFDD ARMED', cls: 'status-chip status-lfdd', flash: true };
}

function getFrequencyStatus(freqHz) {
  if (!Number.isFinite(freqHz)) {
    return { label: 'UNKNOWN', cls: 'status-inline status-balanced' };
  }

  if (freqHz < 49.1 || freqHz > 50.9) {
    return { label: 'LFDD ARMED', cls: 'status-inline status-lfdd' };
  }

  if ((freqHz >= 49.1 && freqHz < 49.5) || (freqHz > 50.5 && freqHz <= 50.9)) {
    return { label: 'OUTSIDE STAT LIMIT', cls: 'status-inline status-warning-yellow' };
  }

  if (freqHz >= 49.95 && freqHz <= 50.05) {
    return { label: 'STABLE', cls: 'status-inline status-stable' };
  }

  if ((freqHz >= 49.5 && freqHz < 49.95) || (freqHz > 50.05 && freqHz <= 50.5)) {
    return { label: 'STRESSED', cls: 'status-inline status-stressed-blue' };
  }

  return { label: 'STABLE', cls: 'status-inline status-stable' };
}

function getSystemFlowStatus(genMw, demandMw) {
  const g = Number.isFinite(genMw) ? genMw : 0;
  const d = Number.isFinite(demandMw) ? demandMw : 0;
  const delta = g - d;

  if (delta > 500) {
    return { label: 'EXPORTING', detail: `Δ +${Math.round(delta).toLocaleString()} MW`, cls: 'status-inline status-export' };
  }
  if (delta < -500) {
    return { label: 'IMPORTING', detail: `Δ ${Math.round(delta).toLocaleString()} MW`, cls: 'status-inline status-import' };
  }
  return { label: 'BALANCED', detail: `Δ ${Math.round(delta).toLocaleString()} MW`, cls: 'status-inline status-balanced' };
}

function getInstantBalanceStatus(deltaMw) {
  if (!Number.isFinite(deltaMw)) {
    return { label: 'UNKNOWN', detail: 'Δ -- MW', cls: 'status-inline status-balanced' };
  }

  if (deltaMw > 500) {
    return { label: 'INSTANT EXPORT', detail: `Δ +${Math.round(deltaMw).toLocaleString()} MW`, cls: 'status-inline status-export' };
  }

  if (deltaMw < -500) {
    return { label: 'INSTANT IMPORT', detail: `Δ ${Math.round(deltaMw).toLocaleString()} MW`, cls: 'status-inline status-import' };
  }

  return { label: 'BALANCED', detail: `Δ ${Math.round(deltaMw).toLocaleString()} MW`, cls: 'status-inline status-balanced' };
}

function drawInstantBalanceSparkline() {
  if (!instantBalanceSparkline || !instantBalanceSparklineCtx) return;

  const ctx = instantBalanceSparklineCtx;
  const cssWidth = Math.max(220, Math.floor(instantBalanceSparkline.clientWidth || 300));
  const cssHeight = 72;
  if (instantBalanceSparkline.width !== cssWidth || instantBalanceSparkline.height !== cssHeight) {
    instantBalanceSparkline.width = cssWidth;
    instantBalanceSparkline.height = cssHeight;
  }

  const w = instantBalanceSparkline.width;
  const h = instantBalanceSparkline.height;
  const pad = { left: 8, right: 8, top: 6, bottom: 14 };
  const innerW = w - pad.left - pad.right;
  const innerH = h - pad.top - pad.bottom;

  ctx.clearRect(0, 0, w, h);
  ctx.fillStyle = '#0f1620';
  ctx.fillRect(0, 0, w, h);

  const series = state.instantBalanceHistory;
  if (!series.length) return;

  let minVal = Math.min(...series, -1);
  let maxVal = Math.max(...series, 1);
  if (minVal === maxVal) {
    minVal -= 1;
    maxVal += 1;
  }

  const yFromValue = (value) => pad.top + ((maxVal - value) / (maxVal - minVal)) * innerH;
  const yZero = yFromValue(0);

  ctx.strokeStyle = '#2a394d';
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(pad.left, yZero);
  ctx.lineTo(w - pad.right, yZero);
  ctx.stroke();

  ctx.beginPath();
  series.forEach((value, i) => {
    const x = pad.left + (i / Math.max(series.length - 1, 1)) * innerW;
    const y = yFromValue(value);
    if (i === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });
  const latest = series[series.length - 1];
  const balanceColor = latest > 500 ? '#45d38b' : latest < -500 ? '#ff8f6b' : '#62b6ff';
  ctx.strokeStyle = balanceColor;
  ctx.lineWidth = 2;
  ctx.stroke();

  const latestY = yFromValue(latest);
  ctx.beginPath();
  ctx.arc(w - pad.right, latestY, 3, 0, Math.PI * 2);
  ctx.fillStyle = balanceColor;
  ctx.fill();
}

function pushInstantBalanceSample(value) {
  if (!Number.isFinite(value)) return;
  state.instantBalanceHistory.push(value);
  if (state.instantBalanceHistory.length > INSTANT_HISTORY_LIMIT) {
    state.instantBalanceHistory = state.instantBalanceHistory.slice(
      state.instantBalanceHistory.length - INSTANT_HISTORY_LIMIT
    );
  }
}

function drawGauge(rocof) {
  const w = gaugeCanvas.width;
  const h = gaugeCanvas.height;
  const pad = { left: 52, right: 12, top: 20, bottom: 28 };
  const innerW = w - pad.left - pad.right;
  const innerH = h - pad.top - pad.bottom;
  const minRocof = -1.0;
  const maxRocof = 1.0;

  gaugeCtx.clearRect(0, 0, w, h);

  gaugeCtx.fillStyle = '#0f1620';
  gaugeCtx.fillRect(0, 0, w, h);

  const yFromRocof = (value) => pad.top + ((maxRocof - value) / (maxRocof - minRocof)) * innerH;

  const yBlueTop = yFromRocof(0.5);
  const yBlueBottom = yFromRocof(-0.5);
  gaugeCtx.fillStyle = 'rgba(98, 182, 255, 0.16)';
  gaugeCtx.fillRect(pad.left, yBlueTop, innerW, yBlueBottom - yBlueTop);

  const yYellowLowTop = yFromRocof(-0.5);
  const yYellowLowBottom = yFromRocof(-0.7);
  const yYellowHighTop = yFromRocof(0.7);
  const yYellowHighBottom = yFromRocof(0.5);
  gaugeCtx.fillStyle = 'rgba(243, 156, 18, 0.18)';
  gaugeCtx.fillRect(pad.left, yYellowLowTop, innerW, yYellowLowBottom - yYellowLowTop);
  gaugeCtx.fillRect(pad.left, yYellowHighTop, innerW, yYellowHighBottom - yYellowHighTop);

  const yRedLowTop = yFromRocof(-0.7);
  const yRedLowBottom = yFromRocof(-1.0);
  const yRedHighTop = yFromRocof(1.0);
  const yRedHighBottom = yFromRocof(0.7);
  gaugeCtx.fillStyle = 'rgba(231, 76, 60, 0.18)';
  gaugeCtx.fillRect(pad.left, yRedLowTop, innerW, yRedLowBottom - yRedLowTop);
  gaugeCtx.fillRect(pad.left, yRedHighTop, innerW, yRedHighBottom - yRedHighTop);

  gaugeCtx.strokeStyle = '#2a394d';
  gaugeCtx.lineWidth = 1;
  [-1.0, -0.7, -0.5, -0.005, 0, 0.005, 0.5, 0.7, 1.0].forEach((tick) => {
    const y = yFromRocof(tick);
    gaugeCtx.beginPath();
    gaugeCtx.moveTo(pad.left, y);
    gaugeCtx.lineTo(w - pad.right, y);
    gaugeCtx.stroke();

    gaugeCtx.fillStyle = '#8fa0b3';
    gaugeCtx.font = '11px Segoe UI';
    gaugeCtx.textAlign = 'right';
    gaugeCtx.textBaseline = 'middle';
    gaugeCtx.fillText(tick.toFixed(3), pad.left - 6, y);
  });

  gaugeCtx.strokeStyle = '#39495f';
  gaugeCtx.strokeRect(pad.left, pad.top, innerW, innerH);

  const history = state.rocofHistory;
  if (history.length > 1) {
    gaugeCtx.beginPath();
    history.forEach((value, i) => {
      const x = pad.left + (i / (Math.max(history.length - 1, 1))) * innerW;
      const y = yFromRocof(clamp(value, minRocof, maxRocof));
      if (i === 0) gaugeCtx.moveTo(x, y);
      else gaugeCtx.lineTo(x, y);
    });
    gaugeCtx.strokeStyle = '#62b6ff';
    gaugeCtx.lineWidth = 2;
    gaugeCtx.stroke();
  }

  if (history.length) {
    const latest = history[history.length - 1];
    const x = pad.left + innerW;
    const y = yFromRocof(clamp(latest, minRocof, maxRocof));
    gaugeCtx.beginPath();
    gaugeCtx.arc(x, y, 3.5, 0, Math.PI * 2);
    gaugeCtx.fillStyle = rocofBandColor(latest);
    gaugeCtx.fill();
  }
}

function pushRocofSample(value) {
  if (!Number.isFinite(value)) return;
  state.rocofHistory.push(value);
  if (state.rocofHistory.length > ROCOF_HISTORY_LIMIT) {
    state.rocofHistory = state.rocofHistory.slice(state.rocofHistory.length - ROCOF_HISTORY_LIMIT);
  }
}

function drawFrequencyChart() {
  if (!frequencyChartCtx || !frequencyChartCanvas) return;

  const ctx = frequencyChartCtx;
  const canvas = frequencyChartCanvas;
  const w = canvas.width;
  const h = canvas.height;
  const pad = { left: 52, right: 12, top: 20, bottom: 28 };
  const innerW = w - pad.left - pad.right;
  const innerH = h - pad.top - pad.bottom;
  const minHz = FREQUENCY_MIN_HZ;
  const maxHz = FREQUENCY_MAX_HZ;

  ctx.clearRect(0, 0, w, h);

  ctx.fillStyle = '#0f1620';
  ctx.fillRect(0, 0, w, h);

  const yFromHz = (hz) => pad.top + ((maxHz - hz) / (maxHz - minHz)) * innerH;

  const yLfddLowTop = yFromHz(49.1);
  const yLfddLowBottom = yFromHz(49.0);
  const yLfddHighTop = yFromHz(51.0);
  const yLfddHighBottom = yFromHz(50.9);
  ctx.fillStyle = 'rgba(231, 76, 60, 0.18)';
  ctx.fillRect(pad.left, yLfddLowTop, innerW, yLfddLowBottom - yLfddLowTop);
  ctx.fillRect(pad.left, yLfddHighTop, innerW, yLfddHighBottom - yLfddHighTop);

  const yStatLowTop = yFromHz(49.5);
  const yStatLowBottom = yFromHz(49.1);
  const yStatHighTop = yFromHz(50.9);
  const yStatHighBottom = yFromHz(50.5);
  ctx.fillStyle = 'rgba(243, 156, 18, 0.18)';
  ctx.fillRect(pad.left, yStatLowTop, innerW, yStatLowBottom - yStatLowTop);
  ctx.fillRect(pad.left, yStatHighTop, innerW, yStatHighBottom - yStatHighTop);

  const yNominalTop = yFromHz(50.5);
  const yNominalBottom = yFromHz(49.5);
  ctx.fillStyle = 'rgba(46, 204, 113, 0.12)';
  ctx.fillRect(pad.left, yNominalTop, innerW, yNominalBottom - yNominalTop);

  const yTightTop = yFromHz(50.05);
  const yTightBottom = yFromHz(49.95);
  ctx.fillStyle = 'rgba(98, 182, 255, 0.16)';
  ctx.fillRect(pad.left, yTightTop, innerW, yTightBottom - yTightTop);

  ctx.strokeStyle = '#2a394d';
  ctx.lineWidth = 1;
  FREQUENCY_TICKS_HZ.forEach((tick) => {
    const y = yFromHz(tick);
    ctx.beginPath();
    ctx.moveTo(pad.left, y);
    ctx.lineTo(w - pad.right, y);
    ctx.stroke();

    ctx.fillStyle = '#8fa0b3';
    ctx.font = '11px Segoe UI';
    ctx.textAlign = 'right';
    ctx.textBaseline = 'middle';
    ctx.fillText(tick.toFixed(2), pad.left - 6, y);
  });

  ctx.strokeStyle = '#39495f';
  ctx.strokeRect(pad.left, pad.top, innerW, innerH);

  const history = state.frequencyHistory;
  if (history.length > 1) {
    ctx.beginPath();
    history.forEach((hz, i) => {
      const x = pad.left + (i / (Math.max(history.length - 1, 1))) * innerW;
      const y = yFromHz(clamp(hz, minHz, maxHz));
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    });
    ctx.strokeStyle = '#62b6ff';
    ctx.lineWidth = 2;
    ctx.stroke();
  }

  if (history.length) {
    const latest = history[history.length - 1];
    const x = pad.left + innerW;
    const y = yFromHz(clamp(latest, minHz, maxHz));
    ctx.beginPath();
    ctx.arc(x, y, 3.5, 0, Math.PI * 2);
    ctx.fillStyle = '#e5f1ff';
    ctx.fill();
  }
}

function drawRpmChart() {
  if (!rpmCtx || !rpmCanvas) return;

  const ctx = rpmCtx;
  const canvas = rpmCanvas;
  const w = canvas.width;
  const h = canvas.height;
  const pad = { left: 52, right: 12, top: 20, bottom: 28 };
  const innerW = w - pad.left - pad.right;
  const innerH = h - pad.top - pad.bottom;
  const minRpm = (FREQUENCY_MIN_HZ / 50) * 3000;
  const maxRpm = (FREQUENCY_MAX_HZ / 50) * 3000;

  ctx.clearRect(0, 0, w, h);
  ctx.fillStyle = '#0f1620';
  ctx.fillRect(0, 0, w, h);

  const yFromRpm = (rpm) => pad.top + ((maxRpm - rpm) / (maxRpm - minRpm)) * innerH;

  const hzToRpm = (hz) => (hz / 50) * 3000;

  const yLfddLowTop = yFromRpm(hzToRpm(49.1));
  const yLfddLowBottom = yFromRpm(hzToRpm(49.0));
  const yLfddHighTop = yFromRpm(hzToRpm(51.0));
  const yLfddHighBottom = yFromRpm(hzToRpm(50.9));
  ctx.fillStyle = 'rgba(231, 76, 60, 0.18)';
  ctx.fillRect(pad.left, yLfddLowTop, innerW, yLfddLowBottom - yLfddLowTop);
  ctx.fillRect(pad.left, yLfddHighTop, innerW, yLfddHighBottom - yLfddHighTop);

  const yStatLowTop = yFromRpm(hzToRpm(49.5));
  const yStatLowBottom = yFromRpm(hzToRpm(49.1));
  const yStatHighTop = yFromRpm(hzToRpm(50.9));
  const yStatHighBottom = yFromRpm(hzToRpm(50.5));
  ctx.fillStyle = 'rgba(243, 156, 18, 0.18)';
  ctx.fillRect(pad.left, yStatLowTop, innerW, yStatLowBottom - yStatLowTop);
  ctx.fillRect(pad.left, yStatHighTop, innerW, yStatHighBottom - yStatHighTop);

  const yNominalTop = yFromRpm(hzToRpm(50.5));
  const yNominalBottom = yFromRpm(hzToRpm(49.5));
  ctx.fillStyle = 'rgba(46, 204, 113, 0.12)';
  ctx.fillRect(pad.left, yNominalTop, innerW, yNominalBottom - yNominalTop);

  const yTightTop = yFromRpm(hzToRpm(50.05));
  const yTightBottom = yFromRpm(hzToRpm(49.95));
  ctx.fillStyle = 'rgba(98, 182, 255, 0.16)';
  ctx.fillRect(pad.left, yTightTop, innerW, yTightBottom - yTightTop);

  ctx.strokeStyle = '#2a394d';
  ctx.lineWidth = 1;
  FREQUENCY_TICKS_HZ.map(hzToRpm).forEach((tick) => {
    const y = yFromRpm(tick);
    ctx.beginPath();
    ctx.moveTo(pad.left, y);
    ctx.lineTo(w - pad.right, y);
    ctx.stroke();

    ctx.fillStyle = '#8fa0b3';
    ctx.font = '11px Segoe UI';
    ctx.textAlign = 'right';
    ctx.textBaseline = 'middle';
    ctx.fillText(String(Math.round(tick)), pad.left - 6, y);
  });

  ctx.strokeStyle = '#39495f';
  ctx.strokeRect(pad.left, pad.top, innerW, innerH);

  const history = state.rpmHistory;
  if (history.length > 1) {
    ctx.beginPath();
    history.forEach((value, i) => {
      const x = pad.left + (i / (Math.max(history.length - 1, 1))) * innerW;
      const y = yFromRpm(clamp(value, minRpm, maxRpm));
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    });
    ctx.strokeStyle = '#62b6ff';
    ctx.lineWidth = 2;
    ctx.stroke();
  }

  if (history.length) {
    const latest = history[history.length - 1];
    const x = pad.left + innerW;
    const y = yFromRpm(clamp(latest, minRpm, maxRpm));
    ctx.beginPath();
    ctx.arc(x, y, 3.5, 0, Math.PI * 2);
    ctx.fillStyle = '#e5f1ff';
    ctx.fill();
  }
}

function frequencyToRpm(freqHz) {
  if (!Number.isFinite(freqHz)) return NaN;
  return (freqHz / 50) * 3000;
}

function pushFrequencySample(freqHz) {
  if (!Number.isFinite(freqHz)) return;
  state.frequencyHistory.push(freqHz);
  if (state.frequencyHistory.length > FREQUENCY_HISTORY_LIMIT) {
    state.frequencyHistory = state.frequencyHistory.slice(state.frequencyHistory.length - FREQUENCY_HISTORY_LIMIT);
  }
}

function pushRpmSample(rpm) {
  if (!Number.isFinite(rpm)) return;
  state.rpmHistory.push(rpm);
  if (state.rpmHistory.length > RPM_HISTORY_LIMIT) {
    state.rpmHistory = state.rpmHistory.slice(state.rpmHistory.length - RPM_HISTORY_LIMIT);
  }
}

function pushTorqueSamples(demandNm, actualNm) {
  if (Number.isFinite(demandNm)) {
    state.torqueDemandHistory.push(demandNm);
    if (state.torqueDemandHistory.length > TORQUE_HISTORY_LIMIT) {
      state.torqueDemandHistory = state.torqueDemandHistory.slice(state.torqueDemandHistory.length - TORQUE_HISTORY_LIMIT);
    }
  }
  if (Number.isFinite(actualNm)) {
    state.torqueActualHistory.push(actualNm);
    if (state.torqueActualHistory.length > TORQUE_HISTORY_LIMIT) {
      state.torqueActualHistory = state.torqueActualHistory.slice(state.torqueActualHistory.length - TORQUE_HISTORY_LIMIT);
    }
  }
}

function drawTorqueChart() {
  if (!torqueCtx || !torqueCanvas) return;

  const ctx = torqueCtx;
  const canvas = torqueCanvas;
  const w = canvas.width;
  const h = canvas.height;
  const pad = { left: 52, right: 12, top: 20, bottom: 28 };
  const innerW = w - pad.left - pad.right;
  const innerH = h - pad.top - pad.bottom;

  ctx.clearRect(0, 0, w, h);
  ctx.fillStyle = '#0f1620';
  ctx.fillRect(0, 0, w, h);

  const currentDemand = state.torqueDemandHistory.length
    ? state.torqueDemandHistory[state.torqueDemandHistory.length - 1]
    : 0;

  const bandBlue = 1_000_000;
  const bandGreen = 2_000_000;
  const fullSpan = 4_000_000;

  const minTorque = currentDemand - fullSpan;
  const maxTorque = currentDemand + fullSpan;
  const yFromTorque = (value) => pad.top + ((maxTorque - value) / (maxTorque - minTorque || 1)) * innerH;

  const yGreenTop = yFromTorque(currentDemand + bandGreen);
  const yGreenBottom = yFromTorque(currentDemand - bandGreen);
  const yBlueTop = yFromTorque(currentDemand + bandBlue);
  const yBlueBottom = yFromTorque(currentDemand - bandBlue);

  ctx.fillStyle = 'rgba(243, 156, 18, 0.16)';
  ctx.fillRect(pad.left, pad.top, innerW, innerH);
  ctx.fillStyle = 'rgba(46, 204, 113, 0.14)';
  ctx.fillRect(pad.left, yGreenTop, innerW, yGreenBottom - yGreenTop);
  ctx.fillStyle = 'rgba(98, 182, 255, 0.16)';
  ctx.fillRect(pad.left, yBlueTop, innerW, yBlueBottom - yBlueTop);

  ctx.strokeStyle = '#2a394d';
  ctx.lineWidth = 1;
  const tickValues = [
    currentDemand - fullSpan,
    currentDemand - bandGreen,
    currentDemand - bandBlue,
    currentDemand,
    currentDemand + bandBlue,
    currentDemand + bandGreen,
    currentDemand + fullSpan,
  ];

  tickValues.forEach((value) => {
    const y = yFromTorque(value);
    ctx.beginPath();
    ctx.moveTo(pad.left, y);
    ctx.lineTo(w - pad.right, y);
    ctx.stroke();
    ctx.fillStyle = '#8fa0b3';
    ctx.font = '11px Segoe UI';
    ctx.textAlign = 'right';
    ctx.textBaseline = 'middle';
    ctx.fillText((value / 1_000_000).toFixed(1), pad.left - 6, y);
  });

  ctx.strokeStyle = '#39495f';
  ctx.strokeRect(pad.left, pad.top, innerW, innerH);

  const yDemand = yFromTorque(currentDemand);
  ctx.beginPath();
  ctx.setLineDash([6, 4]);
  ctx.strokeStyle = '#e5f1ff';
  ctx.lineWidth = 2;
  ctx.moveTo(pad.left, yDemand);
  ctx.lineTo(w - pad.right, yDemand);
  ctx.stroke();
  ctx.setLineDash([]);

  const drawLine = (series, color) => {
    if (series.length <= 1) return;
    ctx.beginPath();
    series.forEach((value, i) => {
      const x = pad.left + (i / Math.max(series.length - 1, 1)) * innerW;
      const y = yFromTorque(clamp(value, minTorque, maxTorque));
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    });
    ctx.strokeStyle = color;
    ctx.lineWidth = 2;
    ctx.stroke();
  };

  drawLine(state.torqueActualHistory, '#ff8f6b');
}

function renderEventConsole() {
  if (!eventConsole) return;
  eventConsole.textContent = state.eventLines.join('\n');
  eventConsole.scrollTop = eventConsole.scrollHeight;
}

function pushEvent(message) {
  const line = `${new Date().toISOString()} ${message}`;
  state.eventLines.push(line);
  if (state.eventLines.length > 140) {
    state.eventLines = state.eventLines.slice(state.eventLines.length - 140);
  }
  renderEventConsole();
}

function initFuelDials(fuels, frames) {
  state.availableFuels = fuels;
  state.fuelMax = {};

  fuels.forEach((fuel) => {
    const historicalMax = Number(ALL_TIME_FUEL_MAX_MW[fuel]);
    if (Number.isFinite(historicalMax) && historicalMax > 0) {
      state.fuelMax[fuel] = historicalMax;
    } else {
      const vals = frames.map((f) => Number(f[fuel]) || 0);
      state.fuelMax[fuel] = Math.max(...vals, 1);
    }
  });

  fuelDials.innerHTML = '';
  fuels.forEach((fuel) => {
    const id = fuel.toLowerCase();
    const card = document.createElement('div');
    card.className = 'dial-card';
    card.id = `dialCard_${id}`;
    card.innerHTML = `
      <div class="dial-label">${fuel.replace(/_/g, ' ')}</div>
      <div id="dialValue_${id}" class="dial-value">-- MW</div>
      <div id="dialMax_${id}" class="dial-max">MAX: -- MW</div>
      <div id="dialInertia_${id}" class="dial-inertia">Est Mech Inertia: -- GVA</div>
      <div class="dial-bar">
        <div id="dialFill_${id}" class="dial-fill"></div>
      </div>
    `;
    fuelDials.appendChild(card);

    const maxEl = document.getElementById(`dialMax_${id}`);
    if (maxEl) {
      maxEl.textContent = `MAX: ${Math.round(state.fuelMax[fuel] || 0).toLocaleString()} MW`;
    }
  });
}

function initInterconnectorDials(interconnectorCols) {
  if (!interconnectorDials) return;
  interconnectorDials.innerHTML = '';

  interconnectorCols.forEach((col) => {
    const id = col.toLowerCase();
    const card = document.createElement('div');
    card.className = 'dial-card';
    card.id = `icCard_${id}`;
    card.innerHTML = `
      <div class="dial-label">${col.replace(/_/g, ' ')}</div>
      <div id="icValue_${id}" class="dial-value">-- MW</div>
      <div id="icFlow_${id}" class="flow-badge flow-neutral">BALANCED</div>
      <div class="dial-bar">
        <div id="icFill_${id}" class="dial-fill"></div>
      </div>
    `;
    interconnectorDials.appendChild(card);
  });
}

function updateInterconnectorDials(frame, interconnectorCols) {
  if (!interconnectorCols || !interconnectorCols.length) return;

  let maxAbs = 1;
  interconnectorCols.forEach((col) => {
    const value = Math.abs(Number(frame[col]));
    if (Number.isFinite(value)) maxAbs = Math.max(maxAbs, value);
  });

  interconnectorCols.forEach((col) => {
    const id = col.toLowerCase();
    const raw = Number(frame[col]);
    const val = Number.isFinite(raw) ? raw : 0;
    const magnitude = Math.abs(val);
    const pct = clamp((magnitude / maxAbs) * 100, 0, 100);

    const valueEl = document.getElementById(`icValue_${id}`);
    const flowEl = document.getElementById(`icFlow_${id}`);
    const fillEl = document.getElementById(`icFill_${id}`);

    if (valueEl) valueEl.textContent = `${Math.round(val).toLocaleString()} MW`;
    if (fillEl) fillEl.style.width = `${pct.toFixed(1)}%`;

    if (flowEl) {
      if (val < 0) {
        flowEl.textContent = 'IMPORTING';
        flowEl.className = 'flow-badge flow-import';
      } else if (val > 0) {
        flowEl.textContent = 'EXPORTING';
        flowEl.className = 'flow-badge flow-export';
      } else {
        flowEl.textContent = 'BALANCED';
        flowEl.className = 'flow-badge flow-neutral';
      }
    }
  });
}

function updateFuelDials(frame) {
  let totalEstimatedInertiaGva = 0;

  state.availableFuels.forEach((fuel) => {
    const id = fuel.toLowerCase();
    const val = Number(frame[fuel]);
    const max = state.fuelMax[fuel] || 1;
    const pct = clamp(((val || 0) / max) * 100, 0, 100);

    const cardEl = document.getElementById(`dialCard_${id}`);
    const valEl = document.getElementById(`dialValue_${id}`);
    const inertiaEl = document.getElementById(`dialInertia_${id}`);
    const fillEl = document.getElementById(`dialFill_${id}`);

    const hConstant = getInertiaHForFuel(fuel);
    const estInertia = estimateFuelInertiaGva(val, hConstant);
    totalEstimatedInertiaGva += estInertia;

    if (valEl) {
      valEl.textContent = fmtMw(val);
      if (Number.isFinite(val) && val > max) {
        valEl.classList.add('record-flash');
      } else {
        valEl.classList.remove('record-flash');
      }
    }
    if (inertiaEl) inertiaEl.textContent = `Est Mech Inertia: ${estInertia.toFixed(2)} GVA`;
    if (fillEl) fillEl.style.width = `${pct.toFixed(1)}%`;
    if (cardEl) {
      if (Number.isFinite(val) && val > max) {
        cardEl.classList.add('record-card');
      } else {
        cardEl.classList.remove('record-card');
      }
    }
  });

  const targetGva = Number(gvaInput?.value);
  const diffGva = Number.isFinite(targetGva) ? totalEstimatedInertiaGva - targetGva : NaN;
  const inertiaState = getInertiaStatus(diffGva);

  if (inertiaAvailable) inertiaAvailable.textContent = `Available (Mechanical): ${totalEstimatedInertiaGva.toFixed(2)} GVA`;
  if (inertiaTarget) inertiaTarget.textContent = `Target: ${Number.isFinite(targetGva) ? targetGva.toFixed(2) : '--'} GVA`;
  if (inertiaDiff) {
    inertiaDiff.textContent = inertiaState.label;
    setClass(inertiaDiff, inertiaState.cls);
  }

  return {
    nominalMechanicalInertiaGva: totalEstimatedInertiaGva,
    targetGva,
    diffGva,
  };
}

function updateFrame() {
  if (!state.frames.length) return;
  const frame = state.frames[state.frameIndex];
  if (!frame) return;

  const rocof = normalizeRocof(Number(frame.rocof_hz_per_s));
  const absRocof = Math.abs(Number.isFinite(rocof) ? rocof : 0);
  pushRocofSample(rocof);
  drawGauge(rocof);
  rocofValue.textContent = Number.isFinite(rocof) ? rocof.toFixed(3) : '--';
  rocofValue.style.color = directionColour(rocof);

  const totalGenMw = Number(frame.total_generation_mw);
  const estDemandMw = Number(frame.estimated_demand_mw);
  const freqEndHz = Number(frame.f_end_hz);
  const gridRpm = frequencyToRpm(freqEndHz);
  const torqueDemandNm = Number.isFinite(totalGenMw) ? totalGenMw * TORQUE_NM_PER_MW : NaN;
  const rocofFactor = Number.isFinite(rocof) ? clamp(1 - (rocof / 0.7), 0.2, 1.8) : 1;
  const torqueActualNm = Number.isFinite(torqueDemandNm) ? torqueDemandNm * rocofFactor : NaN;
  pushFrequencySample(freqEndHz);
  drawFrequencyChart();
  pushRpmSample(gridRpm);
  drawRpmChart();
  pushTorqueSamples(torqueDemandNm, torqueActualNm);
  drawTorqueChart();

  totalGeneration.textContent = fmtMw(totalGenMw);
  estimatedDemand.textContent = fmtMw(estDemandMw);
  frequencyBand.textContent = `${fmtHz(Number(frame.f_start_hz))} → ${fmtHz(freqEndHz)}`;
  if (frequencyValue) frequencyValue.textContent = Number.isFinite(freqEndHz) ? freqEndHz.toFixed(3) : '--';
  if (rpmValue) rpmValue.textContent = Number.isFinite(gridRpm) ? gridRpm.toFixed(0) : '--';
  if (torqueDemandValue) torqueDemandValue.textContent = Number.isFinite(torqueDemandNm) ? (torqueDemandNm / 1_000_000).toFixed(2) : '--';
  if (torqueActualValue) torqueActualValue.textContent = Number.isFinite(torqueActualNm) ? (torqueActualNm / 1_000_000).toFixed(2) : '--';

  const rocofState = getRocofStatus(absRocof);
  if (rocofStatus) {
    rocofStatus.textContent = rocofState.label;
    setClass(rocofStatus, rocofState.cls + (rocofState.flash ? ' flash-red' : ''));
  }

  const freqState = getFrequencyStatus(freqEndHz);
  if (frequencyStatus) {
    frequencyStatus.textContent = freqState.label;
    setClass(frequencyStatus, freqState.cls);
  }

  const flowState = getSystemFlowStatus(totalGenMw, estDemandMw);
  if (systemFlow) systemFlow.textContent = flowState.label;
  if (systemFlowDetail) {
    systemFlowDetail.textContent = flowState.detail;
    setClass(systemFlowDetail, flowState.cls);
  }

  timestampLabel.textContent = frame.rocof_timestamp || '--';
  frameCounter.textContent = `${state.frameIndex + 1} / ${state.frames.length}`;
  slider.value = String(state.frameIndex);

  if (state.lastLoggedFrameIndex !== state.frameIndex) {
    const ts = frame.rocof_timestamp || '--';

    if (rocofState.label !== 'STABLE') {
      pushEvent(`[${ts}] RoCoF ${rocofState.label} (${Number.isFinite(rocof) ? rocof.toFixed(4) : '--'} Hz/s)`);
    } else if (state.prevRocofLabel && state.prevRocofLabel !== 'STABLE') {
      pushEvent(`[${ts}] RoCoF returned to STABLE`);
    }

    if (freqState.label !== 'STABLE') {
      pushEvent(`[${ts}] Frequency ${freqState.label} (${Number.isFinite(freqEndHz) ? freqEndHz.toFixed(3) : '--'} Hz)`);
    } else if (state.prevFrequencyLabel && state.prevFrequencyLabel !== 'STABLE') {
      pushEvent(`[${ts}] Frequency returned to STABLE`);
    }

    if (state.prevFlowLabel !== flowState.label) {
      pushEvent(`[${ts}] System flow changed to ${flowState.label} (${flowState.detail})`);
    }

    state.prevRocofLabel = rocofState.label;
    state.prevFrequencyLabel = freqState.label;
    state.prevFlowLabel = flowState.label;
    state.lastLoggedFrameIndex = state.frameIndex;
  }

  const inertiaSummary = updateFuelDials(frame);

  const freqFactor = Number.isFinite(freqEndHz) ? freqEndHz / 50 : NaN;
  const rpmFactor = Number.isFinite(gridRpm) ? gridRpm / 3000 : NaN;
  const instantGenMw = Number.isFinite(totalGenMw) && Number.isFinite(freqFactor)
    ? totalGenMw * freqFactor
    : NaN;
  const instantBalanceMw = Number.isFinite(instantGenMw) && Number.isFinite(estDemandMw)
    ? instantGenMw - estDemandMw
    : NaN;
  const nominalInertiaGva = Number(inertiaSummary?.nominalMechanicalInertiaGva);
  const instantInertiaGva = Number.isFinite(nominalInertiaGva) && Number.isFinite(rpmFactor)
    ? nominalInertiaGva * (rpmFactor ** 2)
    : NaN;
  const instantCoveragePct = Number.isFinite(instantGenMw) && Number.isFinite(estDemandMw) && estDemandMw !== 0
    ? (instantGenMw / estDemandMw) * 100
    : NaN;
  const instantInertiaDeltaGva = Number.isFinite(instantInertiaGva) && Number.isFinite(inertiaSummary?.targetGva)
    ? instantInertiaGva - inertiaSummary.targetGva
    : NaN;

  pushInstantBalanceSample(instantBalanceMw);
  drawInstantBalanceSparkline();

  if (instantFactor) {
    instantFactor.textContent = Number.isFinite(freqFactor)
      ? `${freqFactor.toFixed(4)}x`
      : '--';
  }

  if (instantGeneration) {
    instantGeneration.textContent = Number.isFinite(instantGenMw)
      ? `${Math.round(instantGenMw).toLocaleString()} MW`
      : '-- MW';
  }

  if (instantBalance) {
    instantBalance.textContent = Number.isFinite(instantBalanceMw)
      ? `${Math.round(instantBalanceMw).toLocaleString()} MW`
      : '-- MW';
  }

  if (instantInertia) {
    instantInertia.textContent = Number.isFinite(instantInertiaGva)
      ? `${instantInertiaGva.toFixed(2)} GVA`
      : '-- GVA';
  }

  if (instantCoverage) {
    instantCoverage.textContent = Number.isFinite(instantCoveragePct)
      ? `${instantCoveragePct.toFixed(2)} %`
      : '-- %';
  }

  if (instantInertiaDelta) {
    instantInertiaDelta.textContent = Number.isFinite(instantInertiaDeltaGva)
      ? `${instantInertiaDeltaGva >= 0 ? '+' : ''}${instantInertiaDeltaGva.toFixed(2)} GVA`
      : '-- GVA';
  }

  const instantState = getInstantBalanceStatus(instantBalanceMw);
  if (instantBalanceStatus) {
    instantBalanceStatus.textContent = `${instantState.label} (${instantState.detail})`;
    setClass(instantBalanceStatus, instantState.cls);
  }

  if (instantInertiaDeltaStatus) {
    const instantInertiaState = getInertiaStatus(instantInertiaDeltaGva);
    instantInertiaDeltaStatus.textContent = instantInertiaState.label;
    setClass(instantInertiaDeltaStatus, instantInertiaState.cls);
  }

  const interconnectorCols = Array.isArray(state.availableInterconnectors)
    ? state.availableInterconnectors
    : [];
  updateInterconnectorDials(frame, interconnectorCols);
}

function tick() {
  state.frameIndex += 1;
  if (state.frameIndex >= state.frames.length) state.frameIndex = 0;
  updateFrame();
}

function play() {
  pause();
  state.fps = clamp(Number(fpsInput.value) || 30, 1, 240);
  fpsInput.value = String(state.fps);
  state.timer = setInterval(tick, 1000 / state.fps);
}

function pause() {
  if (state.timer) {
    clearInterval(state.timer);
    state.timer = null;
  }
}

function setFrames(payload) {
  const frames = Array.isArray(payload.frames) ? payload.frames : [];
  if (!frames.length) {
    alert('No frames found in JSON payload.');
    return;
  }

  state.frames = frames;
  state.availableInterconnectors = Array.isArray(payload.available_interconnectors)
    ? payload.available_interconnectors
    : Object.keys(frames[0] || {}).filter((k) => k.endsWith('_FLOW') || k === 'NET_INTERCONNECTOR_FLOW');
  state.frameIndex = 0;
  state.lastLoggedFrameIndex = -1;
  state.prevFlowLabel = null;
  state.prevRocofLabel = null;
  state.prevFrequencyLabel = null;
  state.frequencyHistory = [];
  state.rocofHistory = [];
  state.rpmHistory = [];
  state.torqueDemandHistory = [];
  state.torqueActualHistory = [];
  state.instantBalanceHistory = [];
  state.eventLines = ['[events] ready'];
  renderEventConsole();

  const fuels = Array.isArray(payload.available_fuels)
    ? payload.available_fuels
    : ['GAS', 'COAL', 'NUCLEAR', 'WIND', 'SOLAR', 'HYDRO'];

  fpsInput.value = String(payload.fps || 30);
  slider.max = String(Math.max(0, frames.length - 1));
  slider.value = '0';

  initFuelDials(fuels, frames);
  initInterconnectorDials(state.availableInterconnectors);
  updateFrame();
}

async function loadDefaultJson() {
  try {
    const res = await fetch(DEFAULT_JSON_PATH, { cache: 'no-store' });
    if (!res.ok) return;
    const payload = await res.json();
    setFrames(payload);
  } catch (_) {
    // noop: user can load file manually
  }
}

playBtn.addEventListener('click', play);
pauseBtn.addEventListener('click', pause);

fpsInput.addEventListener('change', () => {
  if (state.timer) play();
});

gvaInput?.addEventListener('change', () => {
  pushEvent(`System GVA set to ${Number(gvaInput.value) || 0}`);
  updateFrame();
});

[hGasInput, hCoalInput, hHydroInput, hMechInput].forEach((input) => {
  input?.addEventListener('change', () => {
    pushEvent('Inertia H constants updated');
    updateFrame();
  });
});

slider.addEventListener('input', () => {
  state.frameIndex = Number(slider.value) || 0;
  updateFrame();
});

fileInput.addEventListener('change', async (event) => {
  const file = event.target.files?.[0];
  if (!file) return;
  const text = await file.text();
  const payload = JSON.parse(text);
  setFrames(payload);
});

toggleLeftBtn?.addEventListener('click', () => toggleSidebar('left'));
toggleRightBtn?.addEventListener('click', () => toggleSidebar('right'));
toggleLeftCenterBtn?.addEventListener('click', () => toggleSidebar('left'));
toggleRightCenterBtn?.addEventListener('click', () => toggleSidebar('right'));

window.addEventListener('resize', () => {
  resizeCharts(true);
});

loadDefaultJson();
updateSidebarToggleLabels();
resizeCharts(false);
drawGauge(0);
drawFrequencyChart();
drawRpmChart();
drawTorqueChart();
renderEventConsole();
