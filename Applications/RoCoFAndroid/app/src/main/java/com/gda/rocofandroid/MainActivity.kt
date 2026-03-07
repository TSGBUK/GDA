package com.gda.rocofandroid

import android.net.Uri
import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.foundation.Canvas
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.aspectRatio
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.material3.Button
import androidx.compose.material3.Card
import androidx.compose.material3.CardDefaults
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Slider
import androidx.compose.material3.SnackbarHost
import androidx.compose.material3.SnackbarHostState
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.material3.TopAppBar
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableIntStateOf
import androidx.compose.runtime.mutableStateListOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.geometry.Offset
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.StrokeCap
import androidx.compose.ui.graphics.drawscope.Stroke
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import com.gda.rocofandroid.data.ReplayRepository
import com.gda.rocofandroid.model.DerivedMetrics
import com.gda.rocofandroid.model.InertiaConstants
import com.gda.rocofandroid.model.ReplayFrame
import com.gda.rocofandroid.model.calculateMetrics
import com.gda.rocofandroid.model.fuelBreakdown
import com.gda.rocofandroid.model.interconnectorFlows
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlin.math.PI
import kotlin.math.abs
import kotlin.math.cos
import kotlin.math.sin

private val RocofBg = Color(0xFF0D1117)
private val RocofPanel = Color(0xFF151B23)
private val RocofPanel2 = Color(0xFF1B2430)
private val RocofTrack = Color(0xFF2A394D)
private val RocofAccent = Color(0xFF4EA1FF)
private val RocofOk = Color(0xFF2ECC71)
private val RocofWarn = Color(0xFFF39C12)
private val RocofDanger = Color(0xFFE74C3C)
private val RocofImport = Color(0xFFFF8F6B)
private val RocofExport = Color(0xFF45D38B)
private val RocofNeutral = Color(0xFF9CC8FF)

private val FuelMaxMw = mapOf(
    "GAS" to 27868.0,
    "COAL" to 26044.0,
    "NUCLEAR" to 9342.0,
    "WIND" to 18382.0,
    "WIND_EMB" to 5947.0,
    "SOLAR" to 14035.0,
    "HYDRO" to 1403.0,
    "BIOMASS" to 3393.0,
    "STORAGE" to 2660.0,
    "IMPORTS" to 9148.0,
    "OTHER" to 3187.0,
)

class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        enableEdgeToEdge()
        setContent {
            MaterialTheme {
                RoCoFDashboard()
            }
        }
    }
}

@Composable
@OptIn(ExperimentalMaterial3Api::class)
private fun RoCoFDashboard() {
    val repository = remember { ReplayRepository() }
    val snackbarHostState = remember { SnackbarHostState() }
    val scope = rememberCoroutineScope()
    val context = androidx.compose.ui.platform.LocalContext.current

    var frames by remember { mutableStateOf<List<ReplayFrame>>(emptyList()) }
    var isPlaying by remember { mutableStateOf(false) }
    var fps by remember { mutableIntStateOf(30) }
    var frameIndex by remember { mutableIntStateOf(0) }
    var sourceLabel by remember { mutableStateOf("No JSON loaded") }
    var constants by remember { mutableStateOf(InertiaConstants()) }

    val frequencyHistory = remember { mutableStateListOf<Double>() }
    val rocofHistory = remember { mutableStateListOf<Double>() }

    fun resetPlayback(newFrames: List<ReplayFrame>, newFps: Int, label: String) {
        frames = newFrames
        fps = newFps.coerceIn(1, 120)
        frameIndex = 0
        isPlaying = false
        sourceLabel = label
        frequencyHistory.clear()
        rocofHistory.clear()
    }

    fun loadUri(uri: Uri) {
        scope.launch {
            runCatching {
                context.contentResolver.openInputStream(uri)?.use { input ->
                    repository.loadFromInputStream(input)
                } ?: error("Unable to open selected JSON file.")
            }.onSuccess { replay ->
                resetPlayback(replay.frames, replay.fps, uri.lastPathSegment ?: "Selected JSON")
            }.onFailure { ex ->
                snackbarHostState.showSnackbar("JSON load failed: ${ex.message}")
            }
        }
    }

    val openJsonPicker = rememberLauncherForActivityResult(
        contract = ActivityResultContracts.OpenDocument()
    ) { uri ->
        if (uri != null) {
            loadUri(uri)
        }
    }

    LaunchedEffect(isPlaying, fps, frames.size) {
        if (!isPlaying || frames.isEmpty()) return@LaunchedEffect
        while (isPlaying && frames.isNotEmpty()) {
            delay((1000L / fps.coerceAtLeast(1)).coerceAtLeast(16L))
            frameIndex = if (frameIndex >= frames.lastIndex) 0 else frameIndex + 1
        }
    }

    val currentFrame = frames.getOrNull(frameIndex)
    val metrics = currentFrame?.calculateMetrics(constants)

    LaunchedEffect(frameIndex, metrics?.frequencyHz, metrics?.rocofHzPerS) {
        if (metrics == null) return@LaunchedEffect
        frequencyHistory.add(metrics.frequencyHz)
        rocofHistory.add(metrics.rocofHzPerS)
        if (frequencyHistory.size > 240) frequencyHistory.removeAt(0)
        if (rocofHistory.size > 240) rocofHistory.removeAt(0)
    }

    Scaffold(
        containerColor = RocofBg,
        snackbarHost = { SnackbarHost(hostState = snackbarHostState) },
        topBar = {
            TopAppBar(
                title = { Text("RoCoFAndroid", color = Color(0xFFD7E0EA)) },
                actions = {
                    val status = metrics?.systemStatus ?: "STABLE"
                    StatusPill(status)
                }
            )
        }
    ) { innerPadding ->
        LazyColumn(
            modifier = Modifier
                .fillMaxSize()
                .padding(innerPadding)
                .background(RocofBg)
                .padding(horizontal = 10.dp, vertical = 8.dp),
            verticalArrangement = Arrangement.spacedBy(10.dp)
        ) {
            item {
                ControlPanel(
                    sourceLabel = sourceLabel,
                    fps = fps,
                    isPlaying = isPlaying,
                    hasFrames = frames.isNotEmpty(),
                    current = frameIndex,
                    total = frames.size,
                    onPickJson = { openJsonPicker.launch(arrayOf("application/json")) },
                    onLoadSample = {
                        scope.launch {
                            runCatching { repository.loadDefaultAsset(context) }
                                .onSuccess { replay ->
                                    resetPlayback(replay.frames, replay.fps, "Bundled sample JSON")
                                }
                                .onFailure {
                                    snackbarHostState.showSnackbar(
                                        "No bundled sample found. Use Load JSON file."
                                    )
                                }
                        }
                    },
                    onPlayPause = { isPlaying = !isPlaying },
                    onStep = {
                        if (frames.isNotEmpty()) {
                            frameIndex = if (frameIndex >= frames.lastIndex) 0 else frameIndex + 1
                        }
                    },
                    onFpsChanged = { fps = it.coerceIn(1, 120) }
                )
            }

            if (frames.isNotEmpty()) {
                item {
                    Card(colors = CardDefaults.cardColors(containerColor = RocofPanel)) {
                        Column(Modifier.padding(10.dp)) {
                            Text("Timeline", color = Color(0xFFB8C8D9))
                            Slider(
                                value = frameIndex.toFloat(),
                                onValueChange = { frameIndex = it.toInt().coerceIn(0, frames.lastIndex) },
                                valueRange = 0f..frames.lastIndex.toFloat(),
                                modifier = Modifier.fillMaxWidth()
                            )
                            Text(
                                text = currentFrame?.timestamp ?: "--",
                                color = Color(0xFF8FA0B3),
                                style = MaterialTheme.typography.labelMedium
                            )
                        }
                    }
                }
            }

            item { ConstantsPanel(constants = constants, onChange = { constants = it }) }
            item { OverviewPanel(metrics = metrics) }
            item { GaugePanel(metrics = metrics) }
            item { FuelPanel(frame = currentFrame, constants = constants) }
            item { InterconnectorPanel(frame = currentFrame) }

            item {
                ChartCard(
                    title = "Frequency history (Hz)",
                    values = frequencyHistory,
                    lineColor = Color(0xFF62B6FF)
                )
            }

            item {
                ChartCard(
                    title = "RoCoF history (Hz/s)",
                    values = rocofHistory,
                    lineColor = Color(0xFFFF8F6B),
                    centerLineAtZero = true
                )
            }
        }
    }
}

@Composable
private fun ControlPanel(
    sourceLabel: String,
    fps: Int,
    isPlaying: Boolean,
    hasFrames: Boolean,
    current: Int,
    total: Int,
    onPickJson: () -> Unit,
    onLoadSample: () -> Unit,
    onPlayPause: () -> Unit,
    onStep: () -> Unit,
    onFpsChanged: (Int) -> Unit
) {
    Card(colors = CardDefaults.cardColors(containerColor = RocofPanel)) {
        Column(modifier = Modifier.padding(12.dp), verticalArrangement = Arrangement.spacedBy(10.dp)) {
            Text("Replay Controls", color = Color(0xFFD7E0EA), fontWeight = FontWeight.SemiBold)
            Text(sourceLabel, maxLines = 1, overflow = TextOverflow.Ellipsis, color = Color(0xFF8FA0B3))
            Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                Button(onClick = onPickJson) { Text("Load JSON") }
                TextButton(onClick = onLoadSample) { Text("Bundled Sample") }
            }

            Row(horizontalArrangement = Arrangement.spacedBy(8.dp), verticalAlignment = Alignment.CenterVertically) {
                Button(onClick = onPlayPause, enabled = hasFrames) {
                    Text(if (isPlaying) "Pause" else "Play")
                }
                Button(onClick = onStep, enabled = hasFrames) { Text("Step") }
                Text("Frame ${if (total == 0) 0 else current + 1} / $total", color = Color(0xFFD7E0EA))
            }

            Text("FPS: $fps", color = Color(0xFFB8C8D9))
            Slider(
                value = fps.toFloat(),
                onValueChange = { onFpsChanged(it.toInt()) },
                valueRange = 1f..120f,
                modifier = Modifier.fillMaxWidth()
            )
        }
    }
}

@Composable
private fun ConstantsPanel(
    constants: InertiaConstants,
    onChange: (InertiaConstants) -> Unit
) {
    Card(colors = CardDefaults.cardColors(containerColor = RocofPanel)) {
        Column(modifier = Modifier.padding(12.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
            Text("Inertia Constants", color = Color(0xFFD7E0EA), fontWeight = FontWeight.SemiBold)
            ConstantSlider("System GVA", constants.targetGva, 50.0, 500.0) {
                onChange(constants.copy(targetGva = it))
            }
            ConstantSlider("Gas H", constants.gasH, 0.0, 12.0) { onChange(constants.copy(gasH = it)) }
            ConstantSlider("Coal H", constants.coalH, 0.0, 12.0) { onChange(constants.copy(coalH = it)) }
            ConstantSlider("Hydro H", constants.hydroH, 0.0, 12.0) { onChange(constants.copy(hydroH = it)) }
            ConstantSlider("Mech H", constants.mechH, 0.0, 12.0) { onChange(constants.copy(mechH = it)) }
        }
    }
}

@Composable
private fun ConstantSlider(
    label: String,
    value: Double,
    min: Double,
    max: Double,
    onChange: (Double) -> Unit
) {
    Column {
        Text("$label: ${"%.1f".format(value)}", color = Color(0xFFB8C8D9))
        Slider(
            value = value.toFloat(),
            onValueChange = { onChange(it.toDouble()) },
            valueRange = min.toFloat()..max.toFloat(),
            modifier = Modifier.fillMaxWidth()
        )
    }
}

@Composable
private fun OverviewPanel(metrics: DerivedMetrics?) {
    Card(colors = CardDefaults.cardColors(containerColor = RocofPanel)) {
        Column(Modifier.padding(12.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
            Text("System Totals", color = Color(0xFFD7E0EA), fontWeight = FontWeight.SemiBold)
            val rows = if (metrics == null) {
                listOf("Status" to "Load JSON to start")
            } else {
                listOf(
                    "Flow" to "${metrics.flowLabel} (${metrics.flowDetail})",
                    "Generation" to "${fmtMw(metrics.generationMw)}",
                    "Demand" to "${fmtMw(metrics.demandMw)}",
                    "Frequency Band" to "${"%.3f".format(metrics.frequencyStartHz)} → ${"%.3f".format(metrics.frequencyHz)} Hz",
                    "Frequency State" to metrics.frequencyStatus,
                    "RoCoF" to "${"%.4f".format(metrics.rocofHzPerS)} Hz/s",
                    "RPM" to "${"%.0f".format(metrics.rpm)}",
                    "Torque (MNm)" to "${"%.2f".format(metrics.torqueDemandMNm)} / ${"%.2f".format(metrics.torqueActualMNm)}",
                    "Instant Generation" to "${fmtMw(metrics.instantGenerationMw)}",
                    "Instant Balance" to "${fmtMw(metrics.instantBalanceMw)}",
                    "Inertia (Nominal/Instant)" to "${"%.2f".format(metrics.nominalInertiaGva)} / ${"%.2f".format(metrics.instantInertiaGva)} GVA",
                    "Inertia Delta" to "${"%.2f".format(metrics.inertiaDeltaGva)} GVA",
                    "Demand Coverage" to "${"%.2f".format(metrics.demandCoveragePct)} %",
                    "Net Interconnector" to "${fmtMw(metrics.interconnectorFlowMw)}",
                )
            }

            rows.forEach { (k, v) ->
                Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.SpaceBetween) {
                    Text(k, color = Color(0xFF8FA0B3))
                    Text(v, color = Color(0xFFD7E0EA), fontWeight = FontWeight.Medium)
                }
            }
        }
    }
}

@Composable
private fun GaugePanel(metrics: DerivedMetrics?) {
    Card(colors = CardDefaults.cardColors(containerColor = RocofPanel)) {
        Column(Modifier.padding(12.dp), verticalArrangement = Arrangement.spacedBy(10.dp)) {
            Text("Live Gauges", color = Color(0xFFD7E0EA), fontWeight = FontWeight.SemiBold)

            Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                GaugeCard(
                    modifier = Modifier.weight(1f),
                    title = "RoCoF",
                    value = metrics?.rocofHzPerS ?: 0.0,
                    min = -0.75,
                    max = 0.75,
                    valueText = if (metrics == null) "--" else "${"%.4f".format(metrics.rocofHzPerS)} Hz/s",
                    color = when {
                        metrics == null -> RocofAccent
                        abs(metrics.rocofHzPerS) < 0.05 -> RocofAccent
                        abs(metrics.rocofHzPerS) < 0.75 -> RocofDanger
                        else -> RocofDanger
                    }
                )
                GaugeCard(
                    modifier = Modifier.weight(1f),
                    title = "Frequency",
                    value = metrics?.frequencyHz ?: 50.0,
                    min = 49.0,
                    max = 51.0,
                    valueText = if (metrics == null) "--" else "${"%.3f".format(metrics.frequencyHz)} Hz",
                    color = when {
                        metrics == null -> RocofAccent
                        metrics.frequencyStatus == "STABLE" -> RocofOk
                        metrics.frequencyStatus == "OUTSIDE STAT LIMIT" -> RocofWarn
                        else -> RocofDanger
                    }
                )
            }

            Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                GaugeCard(
                    modifier = Modifier.weight(1f),
                    title = "Coverage",
                    value = metrics?.demandCoveragePct ?: 0.0,
                    min = 80.0,
                    max = 120.0,
                    valueText = if (metrics == null) "--" else "${"%.1f".format(metrics.demandCoveragePct)}%",
                    color = if ((metrics?.demandCoveragePct ?: 100.0) in 98.0..102.0) RocofOk else RocofWarn
                )
                GaugeCard(
                    modifier = Modifier.weight(1f),
                    title = "Inertia Δ",
                    value = metrics?.inertiaDeltaGva ?: 0.0,
                    min = -100.0,
                    max = 100.0,
                    valueText = if (metrics == null) "--" else "${"%.1f".format(metrics.inertiaDeltaGva)}",
                    color = if (metrics != null && abs(metrics.inertiaDeltaGva) <= 10) RocofOk else RocofAccent
                )
            }
        }
    }
}

@Composable
private fun GaugeCard(
    modifier: Modifier,
    title: String,
    value: Double,
    min: Double,
    max: Double,
    valueText: String,
    color: Color
) {
    Card(modifier = modifier, colors = CardDefaults.cardColors(containerColor = RocofPanel2)) {
        Column(
            Modifier
                .fillMaxWidth()
                .padding(10.dp),
            horizontalAlignment = Alignment.CenterHorizontally,
            verticalArrangement = Arrangement.spacedBy(4.dp)
        ) {
            Text(title, color = Color(0xFFB8C8D9), style = MaterialTheme.typography.labelMedium)
            DialGauge(value = value, min = min, max = max, color = color)
            Text(valueText, color = Color(0xFFE4EEF9), fontWeight = FontWeight.SemiBold)
        }
    }
}

@Composable
private fun DialGauge(value: Double, min: Double, max: Double, color: Color) {
    Canvas(
        modifier = Modifier
            .fillMaxWidth()
            .aspectRatio(1.6f)
    ) {
        val sweep = 240f
        val start = 150f
        val progress = ((value - min) / (max - min)).toFloat().coerceIn(0f, 1f)
        drawArc(
            color = RocofTrack,
            startAngle = start,
            sweepAngle = sweep,
            useCenter = false,
            style = Stroke(width = 14f, cap = StrokeCap.Round)
        )
        drawArc(
            color = color,
            startAngle = start,
            sweepAngle = sweep * progress,
            useCenter = false,
            style = Stroke(width = 14f, cap = StrokeCap.Round)
        )

        val angleRad = Math.toRadians((start + sweep * progress).toDouble())
        val center = Offset(size.width / 2f, size.height / 1.18f)
        val radius = size.width * 0.33f
        val needle = Offset(
            x = center.x + (cos(angleRad) * radius).toFloat(),
            y = center.y + (sin(angleRad) * radius).toFloat(),
        )

        drawLine(color = Color(0xFFE4EEF9), start = center, end = needle, strokeWidth = 6f)
        drawCircle(color = Color(0xFFE4EEF9), radius = 7f, center = center)
    }
}

@Composable
private fun FuelPanel(frame: ReplayFrame?, constants: InertiaConstants) {
    Card(colors = CardDefaults.cardColors(containerColor = RocofPanel)) {
        Column(Modifier.padding(12.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
            Text("Generation Dials", color = Color(0xFFD7E0EA), fontWeight = FontWeight.SemiBold)

            if (frame == null) {
                Text("Load JSON to view fuel dials", color = Color(0xFF8FA0B3))
                return@Column
            }

            frame.fuelBreakdown().forEach { (fuel, mw) ->
                val max = FuelMaxMw[fuel] ?: 1.0
                val pct = (mw / max).coerceIn(0.0, 1.0).toFloat()
                val inertia = estimateFuelInertia(fuel, mw, constants)

                Card(colors = CardDefaults.cardColors(containerColor = RocofPanel2)) {
                    Column(Modifier.padding(10.dp), verticalArrangement = Arrangement.spacedBy(4.dp)) {
                        Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.SpaceBetween) {
                            Text(fuel.replace("_", " "), color = Color(0xFFC8D4E2))
                            Text("${fmtMw(mw)}", color = Color(0xFFE4EEF9), fontWeight = FontWeight.SemiBold)
                        }
                        Text("MAX: ${fmtMw(max)}", color = Color(0xFF8FA0B3), style = MaterialTheme.typography.labelSmall)
                        Text("Est Mech Inertia: ${"%.2f".format(inertia)} GVA", color = Color(0xFF9FC0DE), style = MaterialTheme.typography.labelSmall)
                        Box(
                            Modifier
                                .fillMaxWidth()
                                .height(8.dp)
                                .background(RocofBg, CircleShape)
                        ) {
                            Box(
                                Modifier
                                    .fillMaxWidth(pct)
                                    .height(8.dp)
                                    .background(RocofAccent, CircleShape)
                            )
                        }
                    }
                }
            }
        }
    }
}

@Composable
private fun InterconnectorPanel(frame: ReplayFrame?) {
    Card(colors = CardDefaults.cardColors(containerColor = RocofPanel)) {
        Column(Modifier.padding(12.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
            Text("Interconnectors", color = Color(0xFFD7E0EA), fontWeight = FontWeight.SemiBold)

            if (frame == null) {
                Text("Load JSON to view interconnector flows", color = Color(0xFF8FA0B3))
                return@Column
            }

            val flows = frame.interconnectorFlows()
            val maxAbs = flows.maxOfOrNull { abs(it.second) }?.coerceAtLeast(1.0) ?: 1.0

            flows.forEach { (key, value) ->
                val pct = (abs(value) / maxAbs).coerceIn(0.0, 1.0).toFloat()
                val (chipText, chipColor) = when {
                    value > 0 -> "EXPORTING" to RocofExport
                    value < 0 -> "IMPORTING" to RocofImport
                    else -> "BALANCED" to RocofNeutral
                }

                Card(colors = CardDefaults.cardColors(containerColor = RocofPanel2)) {
                    Column(Modifier.padding(10.dp), verticalArrangement = Arrangement.spacedBy(6.dp)) {
                        Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.SpaceBetween) {
                            Text(key.replace("_", " "), color = Color(0xFFC8D4E2))
                            Text(fmtMw(value), color = Color(0xFFE4EEF9), fontWeight = FontWeight.SemiBold)
                        }

                        Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(6.dp)) {
                            Box(
                                Modifier
                                    .background(chipColor.copy(alpha = 0.16f), CircleShape)
                                    .padding(horizontal = 10.dp, vertical = 3.dp)
                            ) {
                                Text(chipText, color = chipColor, style = MaterialTheme.typography.labelSmall, fontWeight = FontWeight.Bold)
                            }
                        }

                        Box(
                            Modifier
                                .fillMaxWidth()
                                .height(8.dp)
                                .background(RocofBg, CircleShape)
                        ) {
                            Box(
                                Modifier
                                    .fillMaxWidth(pct)
                                    .height(8.dp)
                                    .background(chipColor, CircleShape)
                            )
                        }
                    }
                }
            }
        }
    }
}

@Composable
private fun ChartCard(
    title: String,
    values: List<Double>,
    lineColor: Color,
    centerLineAtZero: Boolean = false
) {
    Card(colors = CardDefaults.cardColors(containerColor = RocofPanel)) {
        Column(modifier = Modifier.padding(12.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
            Text(title, color = Color(0xFFD7E0EA), fontWeight = FontWeight.SemiBold)
            Sparkline(
                values = values,
                lineColor = lineColor,
                modifier = Modifier
                    .fillMaxWidth()
                    .height(120.dp),
                centerLineAtZero = centerLineAtZero
            )
        }
    }
}

@Composable
private fun Sparkline(
    values: List<Double>,
    lineColor: Color,
    modifier: Modifier = Modifier,
    centerLineAtZero: Boolean = false
) {
    Box(modifier = modifier.background(RocofBg)) {
        Canvas(modifier = Modifier.fillMaxSize()) {
            if (values.size < 2) return@Canvas
            val minValue = values.minOrNull() ?: return@Canvas
            val maxValue = values.maxOrNull() ?: return@Canvas
            val range = (maxValue - minValue).takeIf { it > 0 } ?: 1.0

            if (centerLineAtZero && minValue <= 0.0 && maxValue >= 0.0) {
                val zeroY = ((maxValue - 0.0) / range).toFloat() * size.height
                drawLine(
                    color = RocofTrack,
                    start = Offset(0f, zeroY),
                    end = Offset(size.width, zeroY),
                    strokeWidth = 2f
                )
            }

            val stepX = size.width / (values.size - 1)
            var previous: Offset? = null
            values.forEachIndexed { index, value ->
                val x = stepX * index
                val y = ((maxValue - value) / range).toFloat() * size.height
                val current = Offset(x, y)
                previous?.let {
                    drawLine(
                        color = lineColor,
                        start = it,
                        end = current,
                        strokeWidth = 4f,
                        cap = StrokeCap.Round
                    )
                }
                previous = current
            }

            drawCircle(
                color = lineColor,
                radius = 5f,
                center = previous ?: Offset.Zero,
                style = Stroke(width = 4f)
            )
        }
    }
}

@Composable
private fun StatusPill(status: String) {
    val color = when (status) {
        "STABLE" -> RocofOk
        "SHIVERING" -> RocofAccent
        "STRESSED" -> RocofDanger
        "LFDD ARMED" -> RocofDanger
        else -> RocofNeutral
    }
    Box(
        modifier = Modifier
            .background(color.copy(alpha = 0.14f), CircleShape)
            .padding(horizontal = 10.dp, vertical = 4.dp)
    ) {
        Text(status, color = color, style = MaterialTheme.typography.labelMedium, fontWeight = FontWeight.Bold)
    }
}

private fun estimateFuelInertia(fuel: String, mw: Double, constants: InertiaConstants): Double {
    val h = when (fuel) {
        "IMPORTS", "WIND", "WIND_EMB", "SOLAR" -> 0.0
        "COAL" -> constants.coalH
        "NUCLEAR" -> 6.0
        "GAS" -> constants.gasH
        "HYDRO" -> constants.hydroH
        else -> constants.mechH
    }
    return (mw * h) / 1000.0
}

private fun fmtMw(value: Double): String = "${value.toInt()} MW"
