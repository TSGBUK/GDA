const state = {
  ws: null,
  frequencyMinIso: null,
  frequencyMaxIso: null,
  speedMultiplier: 1,
  streamActive: false,
  lastFrameIso: null,
  currentStartIso: null,
  availableFuels: [],
  availableInterconnectors: [],
  fuelMax: {},
  eventLines: ['[events] ready'],
  prevFlowLabel: null,
  prevRocofLabel: null,
  prevFrequencyLabel: null,
  frequencyHistory: [],
  rocofHistory: [],
  rpmHistory: [],
  torqueDemandHistory: [],
  torqueActualHistory: [],
  instantBalanceHistory: [],
  instantCoverageHistory: [],
  fuelDialRefs: {},
  interconnectorDialRefs: {},
  pendingFrame: null,
  renderScheduled: false,
  lastPersistMs: 0,
  wsFramesSinceSample: 0,
  renderedFramesSinceSample: 0,
  droppedFramesTotal: 0,
  perfLastSampleTs: performance.now(),
  lastTableRenderMs: 0,
  transmissionLossesSeries: null,
  regimeFossilHistory: [],
  regimeRenewHistory: [],
  regimeOutturnInertiaHistory: [],
  regimeCalculatedInertiaHistory: [],
};

const FREQUENCY_HISTORY_LIMIT = 240;
const ROCOF_HISTORY_LIMIT = 240;
const RPM_HISTORY_LIMIT = 240;
const TORQUE_HISTORY_LIMIT = 240;
const INSTANT_HISTORY_LIMIT = 240;
const ROCOF_DEADBAND = 0.005;
const FREQUENCY_MIN_HZ = 49.0;
const FREQUENCY_MAX_HZ = 51.0;
const FREQUENCY_TICKS_HZ = [49.0, 49.1, 49.5, 49.95, 50.0, 50.05, 50.5, 50.9, 51.0];
const TORQUE_NM_PER_MW = 3183;
const CO2_TONS_PER_MWH = 0.38;
const INSTANT_BALANCE_MIN_MW = -10000;
const INSTANT_BALANCE_MAX_MW = 10000;
const INSTANT_BALANCE_BLUE_MIN_MW = -2500;
const INSTANT_BALANCE_BLUE_MAX_MW = 2500;
const INSTANT_COVERAGE_MIN_PCT = 75;
const INSTANT_COVERAGE_MAX_PCT = 150;
const INSTANT_COVERAGE_BLUE_MIN_PCT = 90;
const INSTANT_COVERAGE_BLUE_MAX_PCT = 110;
const CHART_HISTORY_LIMIT = 600;
const LAST_FRAME_PERSIST_INTERVAL_MS = 2000;
const STABILITY_DEADBAND_MW = 500;
const STABILITY_HISTORY_LIMIT = 240;
const GAS_STABILITY_MAX_PCT = 15;

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

const FOSSIL_FUELS = ['GAS', 'COAL', 'OTHER'];
const RENEWABLE_FUELS = ['WIND', 'WIND_EMB', 'SOLAR', 'HYDRO', 'BIOMASS', 'STORAGE'];

const wsUrlInput = document.getElementById('wsUrlInput');
const startDateInput = document.getElementById('startDateInput');
const fpsInput = document.getElementById('fpsInput');
const playbackInfo = document.getElementById('playbackInfo');
const connectBtn = document.getElementById('connectBtn');
const disconnectBtn = document.getElementById('disconnectBtn');
const startBtn = document.getElementById('startBtn');
const stopBtn = document.getElementById('stopBtn');
const connectionStatus = document.getElementById('connectionStatus');
const slow4Btn = document.getElementById('slow4Btn');
const slow2Btn = document.getElementById('slow2Btn');
const normalSpeedBtn = document.getElementById('normalSpeedBtn');
const fast2Btn = document.getElementById('fast2Btn');
const fast4Btn = document.getElementById('fast4Btn');
const back60sBtn = document.getElementById('back60sBtn');
const back15mBtn = document.getElementById('back15mBtn');
const back30mBtn = document.getElementById('back30mBtn');

const gvaInput = document.getElementById('gvaInput');
const hGasInput = document.getElementById('hGasInput');
const hCoalInput = document.getElementById('hCoalInput');
const hHydroInput = document.getElementById('hHydroInput');
const hNuclearInput = document.getElementById('hNuclearInput');
const hMechInput = document.getElementById('hMechInput');

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
const instantFactor = document.getElementById('instantFactor');
const instantGeneration = document.getElementById('instantGeneration');
const instantBalance = document.getElementById('instantBalance');
const instantBalanceStatus = document.getElementById('instantBalanceStatus');
const instantInertia = document.getElementById('instantInertia');
const instantCoverage = document.getElementById('instantCoverage');
const instantInertiaDelta = document.getElementById('instantInertiaDelta');
const instantInertiaDeltaStatus = document.getElementById('instantInertiaDeltaStatus');
const weatherTemp = document.getElementById('weatherTemp');
const weatherWind = document.getElementById('weatherWind');
const weatherSolar = document.getElementById('weatherSolar');
const co2Estimate = document.getElementById('co2Estimate');
const perfRenderFps = document.getElementById('perfRenderFps');
const perfWsFps = document.getElementById('perfWsFps');
const perfDropped = document.getElementById('perfDropped');
const tblNonbmInstructions = document.getElementById('tblNonbmInstructions');
const tblNonbmInstructionsStatus = document.getElementById('tblNonbmInstructionsStatus');
const tblNonbmPrices = document.getElementById('tblNonbmPrices');
const tblNonbmPricesStatus = document.getElementById('tblNonbmPricesStatus');
const tblBsadAgg = document.getElementById('tblBsadAgg');
const tblBsadAggStatus = document.getElementById('tblBsadAggStatus');
const tblBsadDiss = document.getElementById('tblBsadDiss');
const tblBsadDissStatus = document.getElementById('tblBsadDissStatus');
const tblBsadFwd = document.getElementById('tblBsadFwd');
const tblBsadFwdStatus = document.getElementById('tblBsadFwdStatus');
const tblObp1 = document.getElementById('tblObp1');
const tblObp1Status = document.getElementById('tblObp1Status');
const tblObp2 = document.getElementById('tblObp2');
const tblObp2Status = document.getElementById('tblObp2Status');
const tblOrps = document.getElementById('tblOrps');
const tblOrpsStatus = document.getElementById('tblOrpsStatus');
const tblTransMonthly = document.getElementById('tblTransMonthly');
const tblTransMonthlyStatus = document.getElementById('tblTransMonthlyStatus');
const tblTransFy = document.getElementById('tblTransFy');
const tblTransFyStatus = document.getElementById('tblTransFyStatus');
const tblNgPrimary = document.getElementById('tblNgPrimary');
const tblNgPrimaryStatus = document.getElementById('tblNgPrimaryStatus');
const tblNgGsp = document.getElementById('tblNgGsp');
const tblNgGspStatus = document.getElementById('tblNgGspStatus');
const tblNgBsp = document.getElementById('tblNgBsp');
const tblNgBspStatus = document.getElementById('tblNgBspStatus');

const transMonthlyNgetEl = document.getElementById('transMonthlyNget');
const transMonthlySptEl = document.getElementById('transMonthlySpt');
const transMonthlyShetlEl = document.getElementById('transMonthlyShetl');
const transMonthlyTotalEl = document.getElementById('transMonthlyTotal');
const transMonthlyMetaEl = document.getElementById('transMonthlyMeta');
const transFyTotalEl = document.getElementById('transFyTotal');
const transFyMetaEl = document.getElementById('transFyMeta');
const transMonthlyGbEl = document.getElementById('transMonthlyGb');
const transFyGbEl = document.getElementById('transFyGb');
const stabilityMode = document.getElementById('stabilityMode');
const stabilityModeChip = document.getElementById('stabilityModeChip');
const stabilityDeltaDetail = document.getElementById('stabilityDeltaDetail');
const fossilTotalMw = document.getElementById('fossilTotalMw');
const renewableTotalMw = document.getElementById('renewableTotalMw');
const fossilTotalFill = document.getElementById('fossilTotalFill');
const renewableTotalFill = document.getElementById('renewableTotalFill');
const renewablesRatio = document.getElementById('renewablesRatio');
const stabilityOutturnInertia = document.getElementById('stabilityOutturnInertia');
const stabilityCalculatedInertia = document.getElementById('stabilityCalculatedInertia');
const gasStabilityMargin = document.getElementById('gasStabilityMargin');
const gasStabilityMarginChip = document.getElementById('gasStabilityMarginChip');
const gasStabilityMarginDetail = document.getElementById('gasStabilityMarginDetail');

const fuelDials = document.getElementById('fuelDials');
const interconnectorDials = document.getElementById('interconnectorDials');
const eventConsole = document.getElementById('eventConsole');

const gaugeCanvas = document.getElementById('rocofGauge');
const gaugeCtx = gaugeCanvas.getContext('2d');
const frequencyChartCanvas = document.getElementById('frequencyChart');
const frequencyChartCtx = frequencyChartCanvas.getContext('2d');
const rpmCanvas = document.getElementById('rpmChart');
const rpmCtx = rpmCanvas.getContext('2d');
const torqueCanvas = document.getElementById('torqueChart');
const torqueCtx = torqueCanvas.getContext('2d');
const instantBalanceChartCanvas = document.getElementById('instantBalanceChart');
const instantBalanceChartCtx = instantBalanceChartCanvas.getContext('2d');
const instantCoverageChartCanvas = document.getElementById('instantCoverageChart');
const instantCoverageChartCtx = instantCoverageChartCanvas.getContext('2d');
const transmissionLossesChartCanvas = document.getElementById('transmissionLossesChart');
const transmissionLossesChartCtx = transmissionLossesChartCanvas.getContext('2d');
const stabilityMixChartCanvas = document.getElementById('stabilityMixChart');
const stabilityMixChartCtx = stabilityMixChartCanvas.getContext('2d');
const stabilityInertiaChartCanvas = document.getElementById('stabilityInertiaChart');
const stabilityInertiaChartCtx = stabilityInertiaChartCanvas.getContext('2d');

function clamp(v, lo, hi) { return Math.max(lo, Math.min(hi, v)); }

function fmtMw(v) {
  if (!Number.isFinite(v)) return '-- MW';
  return `${Math.round(v).toLocaleString()} MW`;
}

function fmtHz(v) {
  if (!Number.isFinite(v)) return '-- Hz';
  return `${v.toFixed(3)} Hz`;
}

function fmtNumber(value, decimals = 1) {
  if (!Number.isFinite(value)) return '--';
  return value.toFixed(decimals);
}

function fmtTwh(v, decimals = 3) {
  if (!Number.isFinite(v)) return '-- TWh';
  return `${v.toFixed(decimals)} TWh`;
}

function setClass(el, className) {
  if (!el) return;
  el.className = className;
}

function pushEvent(message) {
  const line = `${new Date().toISOString()} ${message}`;
  state.eventLines.push(line);
  if (state.eventLines.length > 160) {
    state.eventLines = state.eventLines.slice(state.eventLines.length - 160);
  }
  eventConsole.textContent = state.eventLines.join('\n');
  eventConsole.scrollTop = eventConsole.scrollHeight;
}

function normalizeRocof(raw) {
  if (!Number.isFinite(raw)) return NaN;
  return Math.abs(raw) < ROCOF_DEADBAND ? 0 : raw;
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

function getRocofStatus(absRocof) {
  if (!Number.isFinite(absRocof) || absRocof < 0.005) return { label: 'STABLE', cls: 'status-chip status-stable', flash: false };
  if (absRocof < 0.05) return { label: 'SHIVERING', cls: 'status-chip status-shiver', flash: false };
  if (absRocof < 0.75) return { label: 'STRESSED', cls: 'status-chip status-stressed-red', flash: false };
  return { label: 'LFDD ARMED', cls: 'status-chip status-lfdd', flash: true };
}

function getFrequencyStatus(freqHz) {
  if (!Number.isFinite(freqHz)) return { label: 'UNKNOWN', cls: 'status-inline status-balanced' };
  if (freqHz < 49.1 || freqHz > 50.9) return { label: 'LFDD ARMED', cls: 'status-inline status-lfdd' };
  if ((freqHz >= 49.1 && freqHz < 49.5) || (freqHz > 50.5 && freqHz <= 50.9)) return { label: 'OUTSIDE STAT LIMIT', cls: 'status-inline status-warning-yellow' };
  if (freqHz >= 49.95 && freqHz <= 50.05) return { label: 'STABLE', cls: 'status-inline status-stable' };
  return { label: 'STRESSED', cls: 'status-inline status-stressed-blue' };
}

function getSystemFlowStatus(genMw, demandMw) {
  const delta = (Number.isFinite(genMw) ? genMw : 0) - (Number.isFinite(demandMw) ? demandMw : 0);
  if (delta > 500) return { label: 'EXPORTING', detail: `Δ +${Math.round(delta).toLocaleString()} MW`, cls: 'status-inline status-export' };
  if (delta < -500) return { label: 'IMPORTING', detail: `Δ ${Math.round(delta).toLocaleString()} MW`, cls: 'status-inline status-import' };
  return { label: 'BALANCED', detail: `Δ ${Math.round(delta).toLocaleString()} MW`, cls: 'status-inline status-balanced' };
}

function getInstantBalanceStatus(deltaMw) {
  if (!Number.isFinite(deltaMw)) return { label: 'UNKNOWN', detail: 'Δ -- MW', cls: 'status-inline status-balanced' };
  if (deltaMw > 500) return { label: 'INSTANT EXPORT', detail: `Δ +${Math.round(deltaMw).toLocaleString()} MW`, cls: 'status-inline status-export' };
  if (deltaMw < -500) return { label: 'INSTANT IMPORT', detail: `Δ ${Math.round(deltaMw).toLocaleString()} MW`, cls: 'status-inline status-import' };
  return { label: 'BALANCED', detail: `Δ ${Math.round(deltaMw).toLocaleString()} MW`, cls: 'status-inline status-balanced' };
}

function getInertiaHForFuel(fuel) {
  if (fuel === 'IMPORTS' || fuel === 'WIND' || fuel === 'WIND_EMB' || fuel === 'SOLAR') return 0;
  if (fuel === 'COAL') return Number(hCoalInput?.value) || 6;
  if (fuel === 'NUCLEAR') return Number(hNuclearInput?.value) || 6;
  if (fuel === 'GAS') return Number(hGasInput?.value) || 5;
  if (fuel === 'HYDRO') return Number(hHydroInput?.value) || 3;
  return Number(hMechInput?.value) || 2.5;
}

function getSpeedLabel(multiplier) {
  if (multiplier >= 1) return `${multiplier.toFixed(multiplier % 1 ? 2 : 0)}x`;
  const inv = Math.round(1 / multiplier);
  return `1/${inv}x`;
}

function computeEffectiveFps() {
  const base = clamp(Math.round(Number(fpsInput?.value) || 30), 1, 144);
  if (fpsInput) fpsInput.value = String(base);
  const effective = clamp(Math.round(base * state.speedMultiplier), 1, 144);
  return { base, effective };
}

function historyLimitPoints() {
  return CHART_HISTORY_LIMIT;
}

function updatePlaybackInfo() {
  const { base, effective } = computeEffectiveFps();
  if (playbackInfo) {
    playbackInfo.textContent = `Base ${base} FPS • Speed ${getSpeedLabel(state.speedMultiplier)} • Effective ${effective} FPS`;
  }
}

function setSpeedMultiplier(multiplier) {
  state.speedMultiplier = multiplier;
  [slow4Btn, slow2Btn, normalSpeedBtn, fast2Btn, fast4Btn].forEach((btn) => btn?.classList.remove('active'));
  if (multiplier === 0.25) slow4Btn?.classList.add('active');
  if (multiplier === 0.5) slow2Btn?.classList.add('active');
  if (multiplier === 1) normalSpeedBtn?.classList.add('active');
  if (multiplier === 2) fast2Btn?.classList.add('active');
  if (multiplier === 4) fast4Btn?.classList.add('active');
  updatePlaybackInfo();
  sendLiveSpeedUpdate();
}

function estimateFuelInertiaGva(mw, hConstant) {
  if (!Number.isFinite(mw) || !Number.isFinite(hConstant)) return 0;
  return (mw * hConstant) / 1000;
}

function getInertiaStatus(diffGva) {
  if (!Number.isFinite(diffGva)) return { label: 'Δ -- GVA', cls: 'status-inline status-balanced' };
  if (Math.abs(diffGva) <= 10) return { label: `IDEAL Δ ${diffGva.toFixed(1)} GVA`, cls: 'status-inline status-stable' };
  if (diffGva < 0) return { label: `LIGHT Δ ${diffGva.toFixed(1)} GVA`, cls: 'status-inline status-import' };
  return { label: `HEAVY Δ +${diffGva.toFixed(1)} GVA`, cls: 'status-inline status-export' };
}

function frequencyToRpm(freqHz) {
  if (!Number.isFinite(freqHz)) return NaN;
  return (freqHz / 50) * 3000;
}

function pushLimited(series, value, limit) {
  if (!Number.isFinite(value)) return;
  series.push(value);
  if (series.length > limit) {
    series.splice(0, series.length - limit);
  }
}

function getFrameNumber(frame, key) {
  if (!frame || !key) return NaN;
  const direct = Number(frame[key]);
  if (Number.isFinite(direct)) return direct;
  const lowerKey = key.toLowerCase();
  for (const frameKey of Object.keys(frame)) {
    if (frameKey.toLowerCase() === lowerKey) {
      const candidate = Number(frame[frameKey]);
      if (Number.isFinite(candidate)) return candidate;
    }
  }
  return NaN;
}

function drawGauge() {
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

  const zones = [
    { hi: 0.5, lo: -0.5, color: 'rgba(98, 182, 255, 0.16)' },
    { hi: -0.5, lo: -0.7, color: 'rgba(243, 156, 18, 0.18)' },
    { hi: 0.7, lo: 0.5, color: 'rgba(243, 156, 18, 0.18)' },
    { hi: -0.7, lo: -1.0, color: 'rgba(231, 76, 60, 0.18)' },
    { hi: 1.0, lo: 0.7, color: 'rgba(231, 76, 60, 0.18)' },
  ];
  zones.forEach((z) => {
    gaugeCtx.fillStyle = z.color;
    gaugeCtx.fillRect(pad.left, yFromRocof(z.hi), innerW, yFromRocof(z.lo) - yFromRocof(z.hi));
  });

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

  if (state.rocofHistory.length > 1) {
    gaugeCtx.beginPath();
    state.rocofHistory.forEach((value, i) => {
      const x = pad.left + (i / Math.max(state.rocofHistory.length - 1, 1)) * innerW;
      const y = yFromRocof(clamp(value, minRocof, maxRocof));
      if (i === 0) gaugeCtx.moveTo(x, y); else gaugeCtx.lineTo(x, y);
    });
    gaugeCtx.strokeStyle = '#62b6ff';
    gaugeCtx.lineWidth = 2;
    gaugeCtx.stroke();
    const latest = state.rocofHistory[state.rocofHistory.length - 1];
    gaugeCtx.beginPath();
    gaugeCtx.arc(pad.left + innerW, yFromRocof(clamp(latest, minRocof, maxRocof)), 3.5, 0, Math.PI * 2);
    gaugeCtx.fillStyle = rocofBandColor(latest);
    gaugeCtx.fill();
  }
}

function drawFrequencyChart() {
  const ctx = frequencyChartCtx;
  const w = frequencyChartCanvas.width;
  const h = frequencyChartCanvas.height;
  const pad = { left: 52, right: 12, top: 20, bottom: 28 };
  const innerW = w - pad.left - pad.right;
  const innerH = h - pad.top - pad.bottom;
  const yFromHz = (hz) => pad.top + ((FREQUENCY_MAX_HZ - hz) / (FREQUENCY_MAX_HZ - FREQUENCY_MIN_HZ)) * innerH;

  ctx.clearRect(0, 0, w, h);
  ctx.fillStyle = '#0f1620';
  ctx.fillRect(0, 0, w, h);

  const bands = [
    [49.1, 49.0, 'rgba(231, 76, 60, 0.18)'], [51.0, 50.9, 'rgba(231, 76, 60, 0.18)'],
    [49.5, 49.1, 'rgba(243, 156, 18, 0.18)'], [50.9, 50.5, 'rgba(243, 156, 18, 0.18)'],
    [50.5, 49.5, 'rgba(46, 204, 113, 0.12)'], [50.05, 49.95, 'rgba(98, 182, 255, 0.16)'],
  ];
  bands.forEach(([hi, lo, color]) => {
    ctx.fillStyle = color;
    ctx.fillRect(pad.left, yFromHz(hi), innerW, yFromHz(lo) - yFromHz(hi));
  });

  ctx.strokeStyle = '#2a394d';
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

  if (state.frequencyHistory.length > 1) {
    ctx.beginPath();
    state.frequencyHistory.forEach((hz, i) => {
      const x = pad.left + (i / Math.max(state.frequencyHistory.length - 1, 1)) * innerW;
      const y = yFromHz(clamp(hz, FREQUENCY_MIN_HZ, FREQUENCY_MAX_HZ));
      if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    });
    ctx.strokeStyle = '#62b6ff';
    ctx.lineWidth = 2;
    ctx.stroke();
  }
}

function drawRpmChart() {
  const ctx = rpmCtx;
  const w = rpmCanvas.width;
  const h = rpmCanvas.height;
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

  const bands = [
    [49.1, 49.0, 'rgba(231, 76, 60, 0.18)'], [51.0, 50.9, 'rgba(231, 76, 60, 0.18)'],
    [49.5, 49.1, 'rgba(243, 156, 18, 0.18)'], [50.9, 50.5, 'rgba(243, 156, 18, 0.18)'],
    [50.5, 49.5, 'rgba(46, 204, 113, 0.12)'], [50.05, 49.95, 'rgba(98, 182, 255, 0.16)'],
  ];
  bands.forEach(([hiHz, loHz, color]) => {
    ctx.fillStyle = color;
    ctx.fillRect(pad.left, yFromRpm(hzToRpm(hiHz)), innerW, yFromRpm(hzToRpm(loHz)) - yFromRpm(hzToRpm(hiHz)));
  });

  ctx.strokeStyle = '#2a394d';
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

  if (state.rpmHistory.length > 1) {
    ctx.beginPath();
    state.rpmHistory.forEach((value, i) => {
      const x = pad.left + (i / Math.max(state.rpmHistory.length - 1, 1)) * innerW;
      const y = yFromRpm(clamp(value, minRpm, maxRpm));
      if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    });
    ctx.strokeStyle = '#62b6ff';
    ctx.lineWidth = 2;
    ctx.stroke();
  }
}

function drawTorqueChart() {
  const ctx = torqueCtx;
  const w = torqueCanvas.width;
  const h = torqueCanvas.height;
  const pad = { left: 52, right: 12, top: 20, bottom: 28 };
  const innerW = w - pad.left - pad.right;
  const innerH = h - pad.top - pad.bottom;

  ctx.clearRect(0, 0, w, h);
  ctx.fillStyle = '#0f1620';
  ctx.fillRect(0, 0, w, h);

  const currentDemand = state.torqueDemandHistory.length ? state.torqueDemandHistory[state.torqueDemandHistory.length - 1] : 0;
  const bandBlue = 1_000_000;
  const bandGreen = 2_000_000;
  const fullSpan = 4_000_000;
  const minTorque = currentDemand - fullSpan;
  const maxTorque = currentDemand + fullSpan;
  const yFromTorque = (value) => pad.top + ((maxTorque - value) / (maxTorque - minTorque || 1)) * innerH;

  ctx.fillStyle = 'rgba(243, 156, 18, 0.16)';
  ctx.fillRect(pad.left, pad.top, innerW, innerH);
  ctx.fillStyle = 'rgba(46, 204, 113, 0.14)';
  ctx.fillRect(pad.left, yFromTorque(currentDemand + bandGreen), innerW, yFromTorque(currentDemand - bandGreen) - yFromTorque(currentDemand + bandGreen));
  ctx.fillStyle = 'rgba(98, 182, 255, 0.16)';
  ctx.fillRect(pad.left, yFromTorque(currentDemand + bandBlue), innerW, yFromTorque(currentDemand - bandBlue) - yFromTorque(currentDemand + bandBlue));

  const tickValues = [
    currentDemand - fullSpan,
    currentDemand - bandGreen,
    currentDemand - bandBlue,
    currentDemand,
    currentDemand + bandBlue,
    currentDemand + bandGreen,
    currentDemand + fullSpan,
  ];

  ctx.strokeStyle = '#2a394d';
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

  const drawLine = (series, color) => {
    if (series.length <= 1) return;
    ctx.beginPath();
    series.forEach((value, i) => {
      const x = pad.left + (i / Math.max(series.length - 1, 1)) * innerW;
      const y = yFromTorque(clamp(value, minTorque, maxTorque));
      if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    });
    ctx.strokeStyle = color;
    ctx.lineWidth = 2;
    ctx.stroke();
  };

  drawLine(state.torqueActualHistory, '#ff8f6b');

  ctx.beginPath();
  ctx.setLineDash([6, 4]);
  ctx.strokeStyle = '#e5f1ff';
  ctx.lineWidth = 2;
  const yDemand = yFromTorque(currentDemand);
  ctx.moveTo(pad.left, yDemand);
  ctx.lineTo(w - pad.right, yDemand);
  ctx.stroke();
  ctx.setLineDash([]);
}

function drawInstantBalanceChart() {
  const ctx = instantBalanceChartCtx;
  const w = instantBalanceChartCanvas.width;
  const h = instantBalanceChartCanvas.height;
  const pad = { left: 52, right: 12, top: 20, bottom: 28 };
  const innerW = w - pad.left - pad.right;
  const innerH = h - pad.top - pad.bottom;

  ctx.clearRect(0, 0, w, h);
  ctx.fillStyle = '#0f1620';
  ctx.fillRect(0, 0, w, h);

  const series = state.instantBalanceHistory;
  const minVal = INSTANT_BALANCE_MIN_MW;
  const maxVal = INSTANT_BALANCE_MAX_MW;
  const yFromValue = (value) => pad.top + ((maxVal - value) / (maxVal - minVal)) * innerH;

  ctx.fillStyle = 'rgba(231, 76, 60, 0.16)';
  ctx.fillRect(pad.left, yFromValue(maxVal), innerW, yFromValue(INSTANT_BALANCE_BLUE_MAX_MW) - yFromValue(maxVal));
  ctx.fillRect(pad.left, yFromValue(INSTANT_BALANCE_BLUE_MIN_MW), innerW, yFromValue(minVal) - yFromValue(INSTANT_BALANCE_BLUE_MIN_MW));

  ctx.fillStyle = 'rgba(46, 204, 113, 0.14)';
  ctx.fillRect(pad.left, yFromValue(maxVal), innerW, yFromValue(INSTANT_BALANCE_BLUE_MAX_MW) - yFromValue(maxVal));

  ctx.fillStyle = 'rgba(98, 182, 255, 0.16)';
  ctx.fillRect(
    pad.left,
    yFromValue(INSTANT_BALANCE_BLUE_MAX_MW),
    innerW,
    yFromValue(INSTANT_BALANCE_BLUE_MIN_MW) - yFromValue(INSTANT_BALANCE_BLUE_MAX_MW),
  );

  if (!series.length) return;
  const yZero = yFromValue(0);

  ctx.strokeStyle = '#2a394d';
  ctx.beginPath();
  ctx.moveTo(pad.left, yZero);
  ctx.lineTo(w - pad.right, yZero);
  ctx.stroke();

  const ticks = [
    INSTANT_BALANCE_MIN_MW,
    INSTANT_BALANCE_BLUE_MIN_MW,
    0,
    INSTANT_BALANCE_BLUE_MAX_MW,
    INSTANT_BALANCE_MAX_MW,
  ];
  ticks.forEach((tick) => {
    const y = yFromValue(tick);
    ctx.beginPath();
    ctx.moveTo(pad.left, y);
    ctx.lineTo(w - pad.right, y);
    ctx.strokeStyle = '#2a394d';
    ctx.stroke();
    ctx.fillStyle = '#8fa0b3';
    ctx.font = '11px Segoe UI';
    ctx.textAlign = 'right';
    ctx.textBaseline = 'middle';
    ctx.fillText(Math.round(tick).toLocaleString(), pad.left - 6, y);
  });

  ctx.strokeStyle = '#39495f';
  ctx.strokeRect(pad.left, pad.top, innerW, innerH);

  ctx.beginPath();
  series.forEach((value, i) => {
    const x = pad.left + (i / Math.max(series.length - 1, 1)) * innerW;
    const y = yFromValue(clamp(value, minVal, maxVal));
    if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
  });
  const latest = series[series.length - 1];
  const balanceColor = latest > INSTANT_BALANCE_BLUE_MAX_MW
    ? '#45d38b'
    : latest < INSTANT_BALANCE_BLUE_MIN_MW
      ? '#ff8f6b'
      : '#62b6ff';
  ctx.strokeStyle = balanceColor;
  ctx.lineWidth = 2;
  ctx.stroke();
}

function drawInstantCoverageChart() {
  const ctx = instantCoverageChartCtx;
  const w = instantCoverageChartCanvas.width;
  const h = instantCoverageChartCanvas.height;
  const pad = { left: 52, right: 12, top: 20, bottom: 28 };
  const innerW = w - pad.left - pad.right;
  const innerH = h - pad.top - pad.bottom;

  ctx.clearRect(0, 0, w, h);
  ctx.fillStyle = '#0f1620';
  ctx.fillRect(0, 0, w, h);

  const series = state.instantCoverageHistory;
  const minVal = INSTANT_COVERAGE_MIN_PCT;
  const maxVal = INSTANT_COVERAGE_MAX_PCT;
  const yFromValue = (value) => pad.top + ((maxVal - value) / (maxVal - minVal)) * innerH;

  ctx.fillStyle = 'rgba(231, 76, 60, 0.16)';
  ctx.fillRect(
    pad.left,
    yFromValue(INSTANT_COVERAGE_BLUE_MIN_PCT),
    innerW,
    yFromValue(minVal) - yFromValue(INSTANT_COVERAGE_BLUE_MIN_PCT),
  );

  ctx.fillStyle = 'rgba(46, 204, 113, 0.14)';
  ctx.fillRect(
    pad.left,
    yFromValue(maxVal),
    innerW,
    yFromValue(INSTANT_COVERAGE_BLUE_MAX_PCT) - yFromValue(maxVal),
  );

  ctx.fillStyle = 'rgba(98, 182, 255, 0.16)';
  ctx.fillRect(
    pad.left,
    yFromValue(INSTANT_COVERAGE_BLUE_MAX_PCT),
    innerW,
    yFromValue(INSTANT_COVERAGE_BLUE_MIN_PCT) - yFromValue(INSTANT_COVERAGE_BLUE_MAX_PCT),
  );

  if (!series.length) return;

  const ticks = [
    INSTANT_COVERAGE_MIN_PCT,
    INSTANT_COVERAGE_BLUE_MIN_PCT,
    100,
    INSTANT_COVERAGE_BLUE_MAX_PCT,
    INSTANT_COVERAGE_MAX_PCT,
  ];
  ticks.forEach((tick) => {
    const y = yFromValue(tick);
    ctx.beginPath();
    ctx.moveTo(pad.left, y);
    ctx.lineTo(w - pad.right, y);
    ctx.strokeStyle = '#2a394d';
    ctx.stroke();
    ctx.fillStyle = '#8fa0b3';
    ctx.font = '11px Segoe UI';
    ctx.textAlign = 'right';
    ctx.textBaseline = 'middle';
    ctx.fillText(`${tick.toFixed(1)}%`, pad.left - 6, y);
  });

  ctx.strokeStyle = '#39495f';
  ctx.strokeRect(pad.left, pad.top, innerW, innerH);

  ctx.beginPath();
  series.forEach((value, i) => {
    const x = pad.left + (i / Math.max(series.length - 1, 1)) * innerW;
    const y = yFromValue(clamp(value, minVal, maxVal));
    if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
  });
  ctx.strokeStyle = '#62b6ff';
  ctx.lineWidth = 2;
  ctx.stroke();
}

function drawTransmissionLossesChart() {
  const ctx = transmissionLossesChartCtx;
  const w = transmissionLossesChartCanvas.width;
  const h = transmissionLossesChartCanvas.height;
  const pad = { left: 52, right: 12, top: 20, bottom: 28 };
  const innerW = w - pad.left - pad.right;
  const innerH = h - pad.top - pad.bottom;

  ctx.clearRect(0, 0, w, h);
  ctx.fillStyle = '#0f1620';
  ctx.fillRect(0, 0, w, h);

  const series = state.transmissionLossesSeries;
  const gb = Array.isArray(series?.gb_totals) ? series.gb_totals.map((v) => Number(v)).filter(Number.isFinite) : [];
  const nget = Array.isArray(series?.nget) ? series.nget.map((v) => Number(v)).filter(Number.isFinite) : [];
  const spt = Array.isArray(series?.spt) ? series.spt.map((v) => Number(v)).filter(Number.isFinite) : [];
  const shetl = Array.isArray(series?.shetl) ? series.shetl.map((v) => Number(v)).filter(Number.isFinite) : [];

  const maxLen = Math.max(gb.length, nget.length, spt.length, shetl.length);
  if (maxLen <= 1) {
    ctx.strokeStyle = '#39495f';
    ctx.strokeRect(pad.left, pad.top, innerW, innerH);
    return;
  }

  const allVals = gb.concat(nget, spt, shetl);
  const minVal = Math.min(...allVals);
  const maxVal = Math.max(...allVals);
  const span = Math.max(1e-6, maxVal - minVal);
  const yFromValue = (v) => pad.top + ((maxVal - v) / span) * innerH;

  const tickCount = 5;
  for (let i = 0; i < tickCount; i += 1) {
    const frac = i / (tickCount - 1);
    const value = maxVal - (span * frac);
    const y = yFromValue(value);
    ctx.beginPath();
    ctx.moveTo(pad.left, y);
    ctx.lineTo(w - pad.right, y);
    ctx.strokeStyle = '#2a394d';
    ctx.stroke();
    ctx.fillStyle = '#8fa0b3';
    ctx.font = '11px Segoe UI';
    ctx.textAlign = 'right';
    ctx.textBaseline = 'middle';
    ctx.fillText(value.toFixed(3), pad.left - 6, y);
  }

  ctx.strokeStyle = '#39495f';
  ctx.strokeRect(pad.left, pad.top, innerW, innerH);

  const drawSeries = (values, color) => {
    if (!Array.isArray(values) || values.length <= 1) return;
    ctx.beginPath();
    values.forEach((raw, i) => {
      const value = Number(raw);
      if (!Number.isFinite(value)) return;
      const x = pad.left + (i / Math.max(values.length - 1, 1)) * innerW;
      const y = yFromValue(value);
      if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    });
    ctx.strokeStyle = color;
    ctx.lineWidth = 2;
    ctx.stroke();
  };

  drawSeries(gb, '#62b6ff');
  drawSeries(nget, '#45d38b');
  drawSeries(spt, '#f6c76b');
  drawSeries(shetl, '#ff8f6b');
}

function sumFuelGroup(frame, fuels) {
  return fuels.reduce((sum, fuel) => {
    const val = getFrameNumber(frame, fuel);
    return sum + (Number.isFinite(val) ? val : 0);
  }, 0);
}

function getStabilityMode(fossilMw, renewableMw) {
  const deltaMw = fossilMw - renewableMw;
  if (Math.abs(deltaMw) <= STABILITY_DEADBAND_MW) {
    return {
      mode: 'Hybrid Anchored',
      cls: 'status-inline status-balanced',
      detail: `Δ ${Math.round(deltaMw).toLocaleString()} MW (deadband ±${STABILITY_DEADBAND_MW.toLocaleString()} MW)`,
    };
  }
  if (fossilMw > renewableMw) {
    return {
      mode: 'Intrinsic',
      cls: 'status-inline status-export',
      detail: `Δ +${Math.round(deltaMw).toLocaleString()} MW (fossil-led)`,
    };
  }
  return {
    mode: 'Digital Mediation',
    cls: 'status-inline status-import',
    detail: `Δ ${Math.round(deltaMw).toLocaleString()} MW (renewables-led)`,
  };
}

function drawRegimeSeriesChart(ctx, canvas, firstSeries, secondSeries, firstColor, secondColor) {
  if (!ctx || !canvas) return;
  const w = canvas.width;
  const h = canvas.height;
  const pad = { left: 6, right: 6, top: 6, bottom: 6 };
  const innerW = w - pad.left - pad.right;
  const innerH = h - pad.top - pad.bottom;

  ctx.clearRect(0, 0, w, h);
  ctx.fillStyle = '#0f1620';
  ctx.fillRect(0, 0, w, h);

  const finiteValues = [...firstSeries, ...secondSeries].filter(Number.isFinite);
  if (finiteValues.length <= 1) {
    return;
  }

  const minVal = Math.min(...finiteValues);
  const maxVal = Math.max(...finiteValues);
  const span = Math.max(1e-6, maxVal - minVal);
  const paddedMin = minVal - (span * 0.1);
  const paddedMax = maxVal + (span * 0.1);
  const paddedSpan = Math.max(1e-6, paddedMax - paddedMin);
  const yFromValue = (value) => pad.top + ((paddedMax - value) / paddedSpan) * innerH;

  const drawLine = (series, color) => {
    if (!series || series.length <= 1) return;
    ctx.beginPath();
    series.forEach((value, i) => {
      if (!Number.isFinite(value)) return;
      const x = pad.left + (i / Math.max(series.length - 1, 1)) * innerW;
      const y = yFromValue(value);
      if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    });
    ctx.strokeStyle = color;
    ctx.lineWidth = 2;
    ctx.stroke();
  };

  ctx.strokeStyle = '#1b2a3b';
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(pad.left, pad.top + (innerH / 2));
  ctx.lineTo(w - pad.right, pad.top + (innerH / 2));
  ctx.stroke();

  drawLine(firstSeries, firstColor);
  drawLine(secondSeries, secondColor);
}

function drawStabilityMixChart() {
  setSparklineCanvasSize(stabilityMixChartCanvas);
  drawRegimeSeriesChart(
    stabilityMixChartCtx,
    stabilityMixChartCanvas,
    state.regimeFossilHistory,
    state.regimeRenewHistory,
    '#ff8f6b',
    '#45d38b',
  );
}

function drawStabilityInertiaChart() {
  setSparklineCanvasSize(stabilityInertiaChartCanvas);
  drawRegimeSeriesChart(
    stabilityInertiaChartCtx,
    stabilityInertiaChartCanvas,
    state.regimeOutturnInertiaHistory,
    state.regimeCalculatedInertiaHistory,
    '#62b6ff',
    '#f6c76b',
  );
}

function setSparklineCanvasSize(canvas, targetHeight = 72) {
  if (!canvas) return false;
  const parent = canvas.parentElement;
  if (!parent) return false;
  const width = Math.max(120, Math.floor(parent.clientWidth - 2));
  const height = targetHeight;
  const changed = canvas.width !== width || canvas.height !== height;
  if (changed) {
    canvas.width = width;
    canvas.height = height;
  }
  return changed;
}

function initFuelDials(fuels) {
  state.availableFuels = fuels;
  state.fuelDialRefs = {};
  fuelDials.innerHTML = '';
  fuels.forEach((fuel) => {
    state.fuelMax[fuel] = Number(ALL_TIME_FUEL_MAX_MW[fuel]) || 1;
    const id = fuel.toLowerCase();
    const card = document.createElement('div');
    card.className = 'dial-card';
    card.id = `dialCard_${id}`;
    card.innerHTML = `
      <div class="dial-label">${fuel.replace(/_/g, ' ')}</div>
      <div id="dialValue_${id}" class="dial-value">-- MW</div>
      <div id="dialMax_${id}" class="dial-max">MAX: ${Math.round(state.fuelMax[fuel]).toLocaleString()} MW</div>
      <div id="dialInertia_${id}" class="dial-inertia">Est Mech Inertia: -- GVA</div>
      <div class="dial-bar"><div id="dialFill_${id}" class="dial-fill"></div></div>
    `;
    fuelDials.appendChild(card);
    state.fuelDialRefs[fuel] = {
      valEl: document.getElementById(`dialValue_${id}`),
      inertiaEl: document.getElementById(`dialInertia_${id}`),
      fillEl: document.getElementById(`dialFill_${id}`),
    };
  });
}

function initInterconnectorDials(interconnectorCols) {
  state.availableInterconnectors = interconnectorCols;
  state.interconnectorDialRefs = {};
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
      <div class="dial-bar"><div id="icFill_${id}" class="dial-fill"></div></div>
    `;
    interconnectorDials.appendChild(card);
    state.interconnectorDialRefs[col] = {
      valueEl: document.getElementById(`icValue_${id}`),
      flowEl: document.getElementById(`icFlow_${id}`),
      fillEl: document.getElementById(`icFill_${id}`),
    };
  });
}

function updateInterconnectorDials(frame) {
  const interconnectorCols = state.availableInterconnectors;
  if (!interconnectorCols?.length) return;

  let maxAbs = 1;
  interconnectorCols.forEach((col) => {
    const value = Math.abs(getFrameNumber(frame, col));
    if (Number.isFinite(value)) maxAbs = Math.max(maxAbs, value);
  });

  interconnectorCols.forEach((col) => {
    const raw = getFrameNumber(frame, col);
    const val = Number.isFinite(raw) ? raw : 0;
    const pct = clamp((Math.abs(val) / maxAbs) * 100, 0, 100);

    const refs = state.interconnectorDialRefs[col] || {};
    const valueEl = refs.valueEl;
    const flowEl = refs.flowEl;
    const fillEl = refs.fillEl;

    if (valueEl) valueEl.textContent = `${val.toFixed(1)} MW`;
    if (fillEl) fillEl.style.width = `${pct.toFixed(1)}%`;

    if (flowEl) {
      if (val < 0) { flowEl.textContent = 'IMPORTING'; flowEl.className = 'flow-badge flow-import'; }
      else if (val > 0) { flowEl.textContent = 'EXPORTING'; flowEl.className = 'flow-badge flow-export'; }
      else { flowEl.textContent = 'BALANCED'; flowEl.className = 'flow-badge flow-neutral'; }
    }
  });
}

function updateFuelDials(frame) {
  let totalEstimatedInertiaGva = 0;
  state.availableFuels.forEach((fuel) => {
    const val = Number(frame[fuel]);
    const max = state.fuelMax[fuel] || 1;
    const pct = clamp(((val || 0) / max) * 100, 0, 100);

    const refs = state.fuelDialRefs[fuel] || {};
    const valEl = refs.valEl;
    const inertiaEl = refs.inertiaEl;
    const fillEl = refs.fillEl;

    const hConstant = getInertiaHForFuel(fuel);
    const estInertia = estimateFuelInertiaGva(val, hConstant);
    totalEstimatedInertiaGva += estInertia;

    if (valEl) valEl.textContent = fmtMw(val);
    if (inertiaEl) inertiaEl.textContent = `Est Mech Inertia: ${estInertia.toFixed(2)} GVA`;
    if (fillEl) fillEl.style.width = `${pct.toFixed(1)}%`;
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

  return { nominalMechanicalInertiaGva: totalEstimatedInertiaGva, targetGva };
}

function updateFromFrame(frame) {
  if (!frame) return;
  state.lastFrameIso = frame.rocof_timestamp || state.lastFrameIso;
  if (state.lastFrameIso) {
    const now = Date.now();
    if (now - state.lastPersistMs >= LAST_FRAME_PERSIST_INTERVAL_MS) {
      localStorage.setItem('rocofapp.lastFrameIso', state.lastFrameIso);
      state.lastPersistMs = now;
    }
  }

  if (!state.availableFuels.length) {
    const fuels = Array.isArray(frame.available_fuels)
      ? frame.available_fuels
      : ['GAS', 'COAL', 'NUCLEAR', 'WIND', 'WIND_EMB', 'SOLAR', 'HYDRO', 'BIOMASS', 'STORAGE', 'IMPORTS', 'OTHER'];
    initFuelDials(fuels);
  }
  if (!state.availableInterconnectors.length) {
    const interconnectors = Array.isArray(frame.available_interconnectors)
      ? frame.available_interconnectors
      : Object.keys(frame).filter((k) => k.endsWith('_FLOW') || k === 'NET_INTERCONNECTOR_FLOW');
    initInterconnectorDials(interconnectors);
  }

  const rocof = normalizeRocof(Number(frame.rocof_hz_per_s));
  const absRocof = Math.abs(Number.isFinite(rocof) ? rocof : 0);
  const freqEndHz = Number(frame.f_end_hz);
  const totalGenMw = Number(frame.total_generation_mw);
  const estDemandMw = Number(frame.estimated_demand_mw);
  const gridRpm = frequencyToRpm(freqEndHz);
  const torqueDemandNm = Number.isFinite(totalGenMw) ? totalGenMw * TORQUE_NM_PER_MW : NaN;
  const rocofFactor = Number.isFinite(rocof) ? clamp(1 - (rocof / 0.7), 0.2, 1.8) : 1;
  const torqueActualNm = Number.isFinite(torqueDemandNm) ? torqueDemandNm * rocofFactor : NaN;

  const historyLimit = historyLimitPoints();
  pushLimited(state.rocofHistory, rocof, historyLimit);
  pushLimited(state.frequencyHistory, freqEndHz, historyLimit);
  pushLimited(state.rpmHistory, gridRpm, historyLimit);
  pushLimited(state.torqueDemandHistory, torqueDemandNm, historyLimit);
  pushLimited(state.torqueActualHistory, torqueActualNm, historyLimit);

  rocofValue.textContent = Number.isFinite(rocof) ? rocof.toFixed(3) : '--';
  rocofValue.style.color = directionColour(rocof);
  totalGeneration.textContent = fmtMw(totalGenMw);
  estimatedDemand.textContent = fmtMw(estDemandMw);
  frequencyBand.textContent = `${fmtHz(Number(frame.f_start_hz))} → ${fmtHz(freqEndHz)}`;
  frequencyValue.textContent = Number.isFinite(freqEndHz) ? freqEndHz.toFixed(3) : '--';
  rpmValue.textContent = Number.isFinite(gridRpm) ? gridRpm.toFixed(0) : '--';
  torqueDemandValue.textContent = Number.isFinite(torqueDemandNm) ? (torqueDemandNm / 1_000_000).toFixed(2) : '--';
  torqueActualValue.textContent = Number.isFinite(torqueActualNm) ? (torqueActualNm / 1_000_000).toFixed(2) : '--';
  timestampLabel.textContent = frame.rocof_timestamp || '--';

  const rocofState = getRocofStatus(absRocof);
  rocofStatus.textContent = rocofState.label;
  setClass(rocofStatus, rocofState.cls + (rocofState.flash ? ' flash-red' : ''));

  const freqState = getFrequencyStatus(freqEndHz);
  frequencyStatus.textContent = freqState.label;
  setClass(frequencyStatus, freqState.cls);

  const flowState = getSystemFlowStatus(totalGenMw, estDemandMw);
  systemFlow.textContent = flowState.label;
  systemFlowDetail.textContent = flowState.detail;
  setClass(systemFlowDetail, flowState.cls);

  const inertiaSummary = updateFuelDials(frame);
  updateInterconnectorDials(frame);

  const freqFactor = Number.isFinite(freqEndHz) ? freqEndHz / 50 : NaN;
  const rpmFactor = Number.isFinite(gridRpm) ? gridRpm / 3000 : NaN;
  const instantGenMw = Number.isFinite(totalGenMw) && Number.isFinite(freqFactor) ? totalGenMw * freqFactor : NaN;
  const instantBalanceMw = Number.isFinite(instantGenMw) && Number.isFinite(estDemandMw) ? instantGenMw - estDemandMw : NaN;
  const instantCoveragePct = Number.isFinite(instantGenMw) && Number.isFinite(estDemandMw) && estDemandMw !== 0
    ? (instantGenMw / estDemandMw) * 100
    : NaN;
  const instantInertiaGva = Number.isFinite(inertiaSummary.nominalMechanicalInertiaGva) && Number.isFinite(rpmFactor)
    ? inertiaSummary.nominalMechanicalInertiaGva * (rpmFactor ** 2)
    : NaN;
  const instantInertiaDeltaGva = Number.isFinite(instantInertiaGva) && Number.isFinite(inertiaSummary.targetGva)
    ? instantInertiaGva - inertiaSummary.targetGva
    : NaN;
  const temperatureC = Number(frame.temperature_c);
  const windKph = Number(frame.wind_speed_100m_kph);
  const solarWm2 = Number(frame.solar_radiation_w_m2);
  const co2TonsPerHour = Number.isFinite(totalGenMw) ? totalGenMw * CO2_TONS_PER_MWH : NaN;
  const transmissionMonthlyNget = Number(frame.transmission_nget);
  const transmissionMonthlySpt = Number(frame.transmission_spt);
  const transmissionMonthlyShetl = Number(frame.transmission_shetl);
  const transmissionMonthlyTotal = Number(frame.transmission_gb_totals);
  const transmissionFyTotal = Number(frame.transmission_sum_gb_totals);
  const transmissionFyLabel = frame.transmission_financial_year || '--';
  const transmissionMonthLabel = frame.transmission_month || '--';

  if (frame.transmission_losses_full_series && Array.isArray(frame.transmission_losses_full_series.gb_totals)) {
    state.transmissionLossesSeries = frame.transmission_losses_full_series;
  }

  const fossilMw = sumFuelGroup(frame, FOSSIL_FUELS);
  const renewableMw = sumFuelGroup(frame, RENEWABLE_FUELS);
  const stability = getStabilityMode(fossilMw, renewableMw);
  const ratio = fossilMw > 0 ? (renewableMw / fossilMw) : NaN;
  const gasMw = getFrameNumber(frame, 'GAS');
  const gasPctOfMax = Number.isFinite(gasMw) ? (gasMw / (ALL_TIME_FUEL_MAX_MW.GAS || 1)) * 100 : NaN;
  const isExporting = flowState.label === 'EXPORTING';
  const gasMarginTrue = renewableMw > fossilMw && isExporting && Number.isFinite(gasPctOfMax) && gasPctOfMax < GAS_STABILITY_MAX_PCT;
  const outturnInertia = Number(frame.outturn_inertia);
  const calcInertia = Number.isFinite(inertiaSummary.nominalMechanicalInertiaGva)
    ? inertiaSummary.nominalMechanicalInertiaGva
    : NaN;

  pushLimited(state.regimeFossilHistory, fossilMw, STABILITY_HISTORY_LIMIT);
  pushLimited(state.regimeRenewHistory, renewableMw, STABILITY_HISTORY_LIMIT);
  pushLimited(state.regimeOutturnInertiaHistory, outturnInertia, STABILITY_HISTORY_LIMIT);
  pushLimited(state.regimeCalculatedInertiaHistory, calcInertia, STABILITY_HISTORY_LIMIT);

  pushLimited(state.instantBalanceHistory, instantBalanceMw, historyLimit);
  pushLimited(state.instantCoverageHistory, instantCoveragePct, historyLimit);
  drawGauge();
  drawFrequencyChart();
  drawRpmChart();
  drawTorqueChart();
  drawInstantBalanceChart();
  drawInstantCoverageChart();
  drawTransmissionLossesChart();
  drawStabilityMixChart();
  drawStabilityInertiaChart();

  instantFactor.textContent = Number.isFinite(freqFactor) ? `${freqFactor.toFixed(4)}x` : '--';
  instantGeneration.textContent = Number.isFinite(instantGenMw) ? `${Math.round(instantGenMw).toLocaleString()} MW` : '-- MW';
  instantBalance.textContent = Number.isFinite(instantBalanceMw) ? `${Math.round(instantBalanceMw).toLocaleString()}` : '--';
  instantInertia.textContent = Number.isFinite(instantInertiaGva) ? `${instantInertiaGva.toFixed(2)} GVA` : '-- GVA';
  instantCoverage.textContent = Number.isFinite(instantCoveragePct) ? `${instantCoveragePct.toFixed(2)}` : '--';
  instantInertiaDelta.textContent = Number.isFinite(instantInertiaDeltaGva)
    ? `${instantInertiaDeltaGva >= 0 ? '+' : ''}${instantInertiaDeltaGva.toFixed(2)} GVA`
    : '-- GVA';

  const instantState = getInstantBalanceStatus(instantBalanceMw);
  instantBalanceStatus.textContent = `${instantState.label} (${instantState.detail})`;
  setClass(instantBalanceStatus, instantState.cls);

  const instantInertiaState = getInertiaStatus(instantInertiaDeltaGva);
  instantInertiaDeltaStatus.textContent = instantInertiaState.label;
  setClass(instantInertiaDeltaStatus, instantInertiaState.cls);

  if (weatherTemp) weatherTemp.textContent = Number.isFinite(temperatureC) ? `${fmtNumber(temperatureC, 1)} °C` : '-- °C';
  if (weatherWind) weatherWind.textContent = Number.isFinite(windKph) ? `${fmtNumber(windKph, 1)} kph` : '-- kph';
  if (weatherSolar) weatherSolar.textContent = Number.isFinite(solarWm2) ? `${fmtNumber(solarWm2, 0)} W/m²` : '-- W/m²';
  if (co2Estimate) co2Estimate.textContent = Number.isFinite(co2TonsPerHour) ? `${Math.round(co2TonsPerHour).toLocaleString()} tCO2/h` : '-- tCO2/h';
  if (transMonthlyNgetEl) transMonthlyNgetEl.textContent = fmtTwh(transmissionMonthlyNget, 3);
  if (transMonthlySptEl) transMonthlySptEl.textContent = fmtTwh(transmissionMonthlySpt, 3);
  if (transMonthlyShetlEl) transMonthlyShetlEl.textContent = fmtTwh(transmissionMonthlyShetl, 3);
  if (transMonthlyTotalEl) transMonthlyTotalEl.textContent = fmtTwh(transmissionMonthlyTotal, 3);
  if (transFyTotalEl) transFyTotalEl.textContent = fmtTwh(transmissionFyTotal, 3);
  if (transMonthlyMetaEl) transMonthlyMetaEl.textContent = `${transmissionFyLabel} • ${transmissionMonthLabel}`;
  if (transFyMetaEl) transFyMetaEl.textContent = `Financial Year ${transmissionFyLabel}`;
  if (transMonthlyGbEl) transMonthlyGbEl.textContent = Number.isFinite(transmissionMonthlyTotal) ? transmissionMonthlyTotal.toFixed(3) : '--';
  if (transFyGbEl) transFyGbEl.textContent = Number.isFinite(transmissionFyTotal) ? transmissionFyTotal.toFixed(3) : '--';
  if (stabilityMode) stabilityMode.textContent = stability.mode;
  if (stabilityModeChip) {
    stabilityModeChip.textContent = stability.mode;
    setClass(stabilityModeChip, stability.cls);
  }
  if (stabilityDeltaDetail) stabilityDeltaDetail.textContent = stability.detail;
  if (fossilTotalMw) fossilTotalMw.textContent = fmtMw(fossilMw);
  if (renewableTotalMw) renewableTotalMw.textContent = fmtMw(renewableMw);
  if (renewablesRatio) renewablesRatio.textContent = Number.isFinite(ratio) ? `${ratio.toFixed(3)}x` : '--';
  if (stabilityOutturnInertia) stabilityOutturnInertia.textContent = Number.isFinite(outturnInertia) ? `${outturnInertia.toFixed(2)} GVA` : '-- GVA';
  if (stabilityCalculatedInertia) stabilityCalculatedInertia.textContent = Number.isFinite(calcInertia) ? `${calcInertia.toFixed(2)} GVA` : '-- GVA';
  if (gasStabilityMargin) gasStabilityMargin.textContent = gasMarginTrue ? 'TRUE' : 'FALSE';
  if (gasStabilityMarginChip) {
    gasStabilityMarginChip.textContent = gasMarginTrue ? 'TRUE' : 'FALSE';
    setClass(gasStabilityMarginChip, gasMarginTrue ? 'status-inline status-stable' : 'status-inline status-import');
  }
  if (gasStabilityMarginDetail) {
    gasStabilityMarginDetail.textContent = `Renew>${Math.round(renewableMw).toLocaleString()} MW Fossil>${Math.round(fossilMw).toLocaleString()} MW? ${renewableMw > fossilMw ? 'Yes' : 'No'} • Exporting? ${isExporting ? 'Yes' : 'No'} • Gas ${Number.isFinite(gasPctOfMax) ? gasPctOfMax.toFixed(1) : '--'}% of max (<${GAS_STABILITY_MAX_PCT}%)`;
  }

  const regimeMax = Math.max(1, fossilMw, renewableMw);
  if (fossilTotalFill) fossilTotalFill.style.width = `${clamp((fossilMw / regimeMax) * 100, 0, 100).toFixed(1)}%`;
  if (renewableTotalFill) renewableTotalFill.style.width = `${clamp((renewableMw / regimeMax) * 100, 0, 100).toFixed(1)}%`;

  renderFrameTables(frame);

  if (state.prevFlowLabel !== flowState.label) {
    pushEvent(`[${frame.rocof_timestamp || '--'}] Flow ${flowState.label} (${flowState.detail})`);
    state.prevFlowLabel = flowState.label;
  }
}

function setConnectionStatus(label, cls) {
  connectionStatus.textContent = label;
  setClass(connectionStatus, cls);
}

function toLocalDatetimeInput(isoString) {
  if (!isoString) return '';
  const utc = new Date(isoString);
  const offsetMs = utc.getTimezoneOffset() * 60000;
  return new Date(utc.getTime() - offsetMs).toISOString().slice(0, 16);
}

async function initializeStartDateFromMeta() {
  try {
    const res = await fetch('/meta', { cache: 'no-store' });
    if (!res.ok) return;
    const meta = await res.json();
    state.frequencyMinIso = meta.frequency_min_timestamp || null;
    state.frequencyMaxIso = meta.frequency_max_timestamp || null;

    if (state.frequencyMinIso && !startDateInput.value) {
      startDateInput.value = toLocalDatetimeInput(state.frequencyMinIso);
      pushEvent(`[meta] Available parquet range: ${state.frequencyMinIso || '--'} to ${state.frequencyMaxIso}`);
    }
  } catch {
    pushEvent('[warn] Could not load parquet metadata bounds');
  }
}

function resetStreamingState() {
  state.prevFlowLabel = null;
  state.prevRocofLabel = null;
  state.prevFrequencyLabel = null;
  state.lastFrameIso = null;
  state.frequencyHistory = [];
  state.rocofHistory = [];
  state.rpmHistory = [];
  state.torqueDemandHistory = [];
  state.torqueActualHistory = [];
  state.instantBalanceHistory = [];
  state.instantCoverageHistory = [];
  state.regimeFossilHistory = [];
  state.regimeRenewHistory = [];
  state.regimeOutturnInertiaHistory = [];
  state.regimeCalculatedInertiaHistory = [];
  state.pendingFrame = null;
  state.renderScheduled = false;
  state.wsFramesSinceSample = 0;
  state.renderedFramesSinceSample = 0;
  state.perfLastSampleTs = performance.now();
  state.lastTableRenderMs = 0;
  state.transmissionLossesSeries = null;
}

function restoreLastFrameTimestamp() {
  const stored = localStorage.getItem('rocofapp.lastFrameIso');
  if (!stored) return;
  state.lastFrameIso = stored;
  if (startDateInput && !startDateInput.value) {
    startDateInput.value = toLocalDatetimeInput(stored);
  }
}

function scheduleFrameRender(frame) {
  if (state.pendingFrame) {
    state.droppedFramesTotal += 1;
  }
  state.pendingFrame = frame;
  if (state.renderScheduled) return;

  state.renderScheduled = true;
  requestAnimationFrame(() => {
    state.renderScheduled = false;
    const queuedFrame = state.pendingFrame;
    state.pendingFrame = null;
    if (queuedFrame) {
      state.renderedFramesSinceSample += 1;
      updateFromFrame(queuedFrame);
    }
  });
}

function updatePerformanceHud() {
  const now = performance.now();
  const elapsedMs = now - state.perfLastSampleTs;
  if (elapsedMs < 1000) return;

  const elapsedSec = elapsedMs / 1000;
  const renderFps = state.renderedFramesSinceSample / elapsedSec;
  const wsFps = state.wsFramesSinceSample / elapsedSec;

  if (perfRenderFps) perfRenderFps.textContent = renderFps.toFixed(1);
  if (perfWsFps) perfWsFps.textContent = wsFps.toFixed(1);
  if (perfDropped) perfDropped.textContent = String(state.droppedFramesTotal);

  state.renderedFramesSinceSample = 0;
  state.wsFramesSinceSample = 0;
  state.perfLastSampleTs = now;
}

function shortCell(value) {
  if (value === null || value === undefined || value === '') return '--';
  const text = String(value);
  return text.length > 42 ? `${text.slice(0, 39)}...` : text;
}

function renderDataTable(tableEl, statusEl, payload) {
  if (!tableEl || !statusEl) return;
  const columns = Array.isArray(payload?.columns) ? payload.columns : [];
  const rows = Array.isArray(payload?.rows) ? payload.rows : [];
  const hasData = Boolean(payload?.has_data && columns.length && rows.length);

  if (!hasData) {
    statusEl.textContent = payload?.message || 'No data available';
    tableEl.innerHTML = '';
    return;
  }

  statusEl.textContent = `${rows.length} row(s)`;
  const thead = `<thead><tr>${columns.map((c) => `<th>${shortCell(c)}</th>`).join('')}</tr></thead>`;
  const tbody = `<tbody>${rows.map((r) => `<tr>${columns.map((c) => `<td>${shortCell(r?.[c])}</td>`).join('')}</tr>`).join('')}</tbody>`;
  tableEl.innerHTML = `${thead}${tbody}`;
}

function renderFrameTables(frame) {
  const now = Date.now();
  if (now - state.lastTableRenderMs < 1000) return;
  state.lastTableRenderMs = now;

  const tables = frame?.tables || {};
  renderDataTable(tblNonbmInstructions, tblNonbmInstructionsStatus, tables.nonbm_instructions);
  renderDataTable(tblNonbmPrices, tblNonbmPricesStatus, tables.nonbm_window_prices);
  renderDataTable(tblBsadAgg, tblBsadAggStatus, tables.bsad_aggregated);
  renderDataTable(tblBsadDiss, tblBsadDissStatus, tables.bsad_dissaggregated);
  renderDataTable(tblBsadFwd, tblBsadFwdStatus, tables.bsad_forward);
  renderDataTable(tblObp1, tblObp1Status, tables.obp_source_1);
  renderDataTable(tblObp2, tblObp2Status, tables.obp_source_2);
  renderDataTable(tblOrps, tblOrpsStatus, tables.orps_reactive_power);
  renderDataTable(tblTransMonthly, tblTransMonthlyStatus, tables.transmission_losses_monthly);
  renderDataTable(tblTransFy, tblTransFyStatus, tables.transmission_losses_financial_year);

  const siteTables = frame?.site_tables;
  if (siteTables && typeof siteTables === 'object' && Object.keys(siteTables).length > 0) {
    if (Object.prototype.hasOwnProperty.call(siteTables, 'nationalgrid_live_primary_master')) {
      renderDataTable(tblNgPrimary, tblNgPrimaryStatus, siteTables.nationalgrid_live_primary_master);
    }
    if (Object.prototype.hasOwnProperty.call(siteTables, 'nationalgrid_live_gsp_master')) {
      renderDataTable(tblNgGsp, tblNgGspStatus, siteTables.nationalgrid_live_gsp_master);
    }
    if (Object.prototype.hasOwnProperty.call(siteTables, 'nationalgrid_bsp_master')) {
      renderDataTable(tblNgBsp, tblNgBspStatus, siteTables.nationalgrid_bsp_master);
    }
  }
}

function isWsOpen() {
  return Boolean(state.ws && state.ws.readyState === WebSocket.OPEN);
}

function sendLiveSpeedUpdate() {
  if (!state.streamActive || !isWsOpen()) return;
  const { base, effective } = computeEffectiveFps();
  state.ws.send(JSON.stringify({ action: 'set_speed', fps: effective }));
  pushEvent(`[stream] speed set to ${getSpeedLabel(state.speedMultiplier)} (base ${base}, effective ${effective} FPS)`);
}

function seekBack(seconds) {
  if (!isWsOpen()) {
    pushEvent('[warn] websocket not connected');
    return;
  }
  const pivotIso = state.lastFrameIso || state.currentStartIso || state.frequencyMinIso;
  if (!pivotIso) {
    pushEvent('[warn] no seek pivot timestamp available');
    return;
  }
  const pivot = new Date(pivotIso);
  const target = new Date(pivot.getTime() - (seconds * 1000));
  const targetIso = target.toISOString();
  state.currentStartIso = targetIso;
  resetStreamingState();
  state.ws.send(JSON.stringify({ action: 'seek', start: targetIso }));
  pushEvent(`[stream] seek back ${seconds}s -> ${targetIso}`);
}

function connectSocket() {
  if (state.ws && state.ws.readyState === WebSocket.OPEN) return Promise.resolve();
  if (state.ws && state.ws.readyState === WebSocket.CONNECTING) {
    return new Promise((resolve, reject) => {
      const started = Date.now();
      const timer = setInterval(() => {
        if (!state.ws) {
          clearInterval(timer);
          reject(new Error('WebSocket unavailable'));
          return;
        }
        if (state.ws.readyState === WebSocket.OPEN) {
          clearInterval(timer);
          resolve();
          return;
        }
        if (Date.now() - started > 7000) {
          clearInterval(timer);
          reject(new Error('Timed out waiting for WebSocket connect'));
        }
      }, 120);
    });
  }

  const url = wsUrlInput.value.trim();
  if (!url) {
    pushEvent('[error] WebSocket URL is required');
    return Promise.reject(new Error('WebSocket URL is required'));
  }

  return new Promise((resolve, reject) => {
    state.ws = new WebSocket(url);
    setConnectionStatus('CONNECTING', 'status-inline status-warning-yellow');

    state.ws.onopen = () => {
      setConnectionStatus('CONNECTED', 'status-inline status-stable');
      pushEvent('[ws] connected');
      resolve();
    };

    state.ws.onclose = () => {
      setConnectionStatus('DISCONNECTED', 'status-inline status-balanced');
      pushEvent('[ws] disconnected');
      state.streamActive = false;
      state.ws = null;
    };

    state.ws.onerror = () => {
      setConnectionStatus('ERROR', 'status-inline status-lfdd');
      pushEvent('[ws] connection error');
      reject(new Error('WebSocket connection error'));
    };

    state.ws.onmessage = (event) => {
      try {
        const payload = JSON.parse(event.data);
        if (payload.type === 'frame' && payload.frame) {
          state.streamActive = true;
          state.wsFramesSinceSample += 1;
          scheduleFrameRender(payload.frame);
          updatePerformanceHud();
        } else if (payload.type === 'info') {
          pushEvent(`[info] ${payload.message || 'stream info'}`);
        } else if (payload.type === 'done') {
          state.streamActive = false;
          pushEvent('[stream] completed');
        }
      } catch {
        pushEvent('[warn] non-json message received');
      }
    };
  });
}

function disconnectSocket() {
  if (!state.ws) return;
  state.ws.close();
}

async function startStream() {
  try {
    await connectSocket();
  } catch (err) {
    pushEvent(`[error] ${err?.message || 'Unable to connect websocket'}`);
    return;
  }

  const raw = startDateInput.value;
  let iso = null;
  if (raw) {
    iso = new Date(raw).toISOString();
  } else if (state.lastFrameIso) {
    iso = state.lastFrameIso;
    startDateInput.value = toLocalDatetimeInput(state.lastFrameIso);
    pushEvent(`[info] Start date empty, resuming from last frame ${state.lastFrameIso}`);
  }

  if (!iso) {
    pushEvent('[warn] start date is required');
    return;
  }

  if (state.frequencyMinIso && iso < state.frequencyMinIso) {
    pushEvent(`[warn] Start date before available parquet range, clamping to ${state.frequencyMinIso}`);
  }
  if (state.frequencyMaxIso && iso > state.frequencyMaxIso) {
    pushEvent(`[warn] Start date after available parquet range, clamping to ${state.frequencyMaxIso}`);
  }

  const boundedIso = state.frequencyMinIso && iso < state.frequencyMinIso
    ? state.frequencyMinIso
    : state.frequencyMaxIso && iso > state.frequencyMaxIso
      ? state.frequencyMaxIso
      : iso;

  const { base, effective } = computeEffectiveFps();

  resetStreamingState();
  state.currentStartIso = boundedIso;
  state.streamActive = true;
  state.ws.send(JSON.stringify({ action: 'start', start: boundedIso, fps: effective }));
  pushEvent(`[stream] start at ${boundedIso} (base ${base} FPS, speed ${getSpeedLabel(state.speedMultiplier)}, effective ${effective} FPS)`);
}

function stopStream() {
  if (!state.ws || state.ws.readyState !== WebSocket.OPEN) return;
  state.ws.send(JSON.stringify({ action: 'stop' }));
  state.streamActive = false;
  pushEvent('[stream] stop requested');
}

function updateSidebarToggleLabels() {
  const leftOpen = document.body.classList.contains('left-open');
  const rightOpen = document.body.classList.contains('right-open');

  if (toggleLeftBtn) toggleLeftBtn.textContent = leftOpen ? '◀' : '▶';
  if (toggleRightBtn) toggleRightBtn.textContent = rightOpen ? '▶' : '◀';
  if (toggleLeftCenterBtn) toggleLeftCenterBtn.textContent = leftOpen ? 'Hide Left' : 'Show Left';
  if (toggleRightCenterBtn) toggleRightCenterBtn.textContent = rightOpen ? 'Hide Right' : 'Show Right';
}

function setCanvasSquareSize(canvas, minSize = 300, maxSize = 680) {
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

function resizeCharts() {
  [
    gaugeCanvas,
    frequencyChartCanvas,
    rpmCanvas,
    torqueCanvas,
    instantBalanceChartCanvas,
    instantCoverageChartCanvas,
    transmissionLossesChartCanvas,
  ].forEach((canvas) => setCanvasSquareSize(canvas, 220, 520));
  setSparklineCanvasSize(stabilityMixChartCanvas);
  setSparklineCanvasSize(stabilityInertiaChartCanvas);
  drawGauge();
  drawFrequencyChart();
  drawRpmChart();
  drawTorqueChart();
  drawInstantBalanceChart();
  drawInstantCoverageChart();
  drawTransmissionLossesChart();
  drawStabilityMixChart();
  drawStabilityInertiaChart();
}

function toggleSidebar(side) {
  if (side === 'left' && leftSidebar) document.body.classList.toggle('left-open');
  if (side === 'right' && rightSidebar) document.body.classList.toggle('right-open');
  updateSidebarToggleLabels();
  resizeCharts();
}

connectBtn.addEventListener('click', connectSocket);
disconnectBtn.addEventListener('click', disconnectSocket);
startBtn.addEventListener('click', startStream);
stopBtn.addEventListener('click', stopStream);

[gvaInput, hGasInput, hCoalInput, hHydroInput, hMechInput].forEach((input) => {
  input?.addEventListener('change', () => {
    pushEvent('[config] inertia constants updated');
  });
});

hNuclearInput?.addEventListener('change', () => {
  pushEvent('[config] inertia constants updated');
});

fpsInput?.addEventListener('change', () => {
  updatePlaybackInfo();
  sendLiveSpeedUpdate();
});

slow4Btn?.addEventListener('click', () => setSpeedMultiplier(0.25));
slow2Btn?.addEventListener('click', () => setSpeedMultiplier(0.5));
normalSpeedBtn?.addEventListener('click', () => setSpeedMultiplier(1));
fast2Btn?.addEventListener('click', () => setSpeedMultiplier(2));
fast4Btn?.addEventListener('click', () => setSpeedMultiplier(4));
back60sBtn?.addEventListener('click', () => seekBack(60));
back15mBtn?.addEventListener('click', () => seekBack(15 * 60));
back30mBtn?.addEventListener('click', () => seekBack(30 * 60));

toggleLeftBtn?.addEventListener('click', () => toggleSidebar('left'));
toggleRightBtn?.addEventListener('click', () => toggleSidebar('right'));
toggleLeftCenterBtn?.addEventListener('click', () => toggleSidebar('left'));
toggleRightCenterBtn?.addEventListener('click', () => toggleSidebar('right'));
window.addEventListener('resize', resizeCharts);

startDateInput.value = '';
updateSidebarToggleLabels();
setConnectionStatus('DISCONNECTED', 'status-inline status-balanced');
setSpeedMultiplier(1);
resizeCharts();
drawInstantBalanceChart();
drawInstantCoverageChart();
drawTransmissionLossesChart();
drawStabilityMixChart();
drawStabilityInertiaChart();
initializeStartDateFromMeta();
restoreLastFrameTimestamp();
updatePerformanceHud();
