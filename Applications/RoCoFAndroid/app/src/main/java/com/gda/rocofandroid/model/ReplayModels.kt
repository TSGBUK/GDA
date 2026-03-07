package com.gda.rocofandroid.model

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
data class ReplayFile(
    val schema: String? = null,
    val fps: Int = 30,
    val frames: List<ReplayFrame> = emptyList()
)

@Serializable
data class ReplayFrame(
    @SerialName("rocof_timestamp") val timestamp: String? = null,
    @SerialName("rocof_hz_per_s") val rocofHzPerS: Double? = null,
    @SerialName("f_start_hz") val frequencyStartHz: Double? = null,
    @SerialName("f_end_hz") val frequencyEndHz: Double? = null,
    @SerialName("delta_t_s") val deltaSeconds: Double? = null,
    @SerialName("total_generation_mw") val totalGenerationMw: Double? = null,
    @SerialName("estimated_demand_mw") val estimatedDemandMw: Double? = null,
    @SerialName("GAS") val gasMw: Double? = null,
    @SerialName("COAL") val coalMw: Double? = null,
    @SerialName("NUCLEAR") val nuclearMw: Double? = null,
    @SerialName("WIND") val windMw: Double? = null,
    @SerialName("WIND_EMB") val windEmbeddedMw: Double? = null,
    @SerialName("SOLAR") val solarMw: Double? = null,
    @SerialName("HYDRO") val hydroMw: Double? = null,
    @SerialName("BIOMASS") val biomassMw: Double? = null,
    @SerialName("STORAGE") val storageMw: Double? = null,
    @SerialName("IMPORTS") val importsMw: Double? = null,
    @SerialName("OTHER") val otherMw: Double? = null,
    @SerialName("IFA_FLOW") val ifaFlowMw: Double? = null,
    @SerialName("IFA2_FLOW") val ifa2FlowMw: Double? = null,
    @SerialName("BRITNED_FLOW") val britnedFlowMw: Double? = null,
    @SerialName("MOYLE_FLOW") val moyleFlowMw: Double? = null,
    @SerialName("EAST_WEST_FLOW") val eastWestFlowMw: Double? = null,
    @SerialName("NEMO_FLOW") val nemoFlowMw: Double? = null,
    @SerialName("NSL_FLOW") val nslFlowMw: Double? = null,
    @SerialName("ELECLINK_FLOW") val eleclinkFlowMw: Double? = null,
    @SerialName("VIKING_FLOW") val vikingFlowMw: Double? = null,
    @SerialName("GREENLINK_FLOW") val greenlinkFlowMw: Double? = null,
    @SerialName("NET_INTERCONNECTOR_FLOW") val netInterconnectorFlowMw: Double? = null
)

data class InertiaConstants(
    val gasH: Double = 5.0,
    val coalH: Double = 6.0,
    val hydroH: Double = 3.0,
    val mechH: Double = 2.5,
    val targetGva: Double = 150.0
)

data class DerivedMetrics(
    val frequencyStartHz: Double,
    val frequencyHz: Double,
    val rocofHzPerS: Double,
    val rocofAbsHzPerS: Double,
    val rpm: Double,
    val generationMw: Double,
    val demandMw: Double,
    val deltaMw: Double,
    val flowLabel: String,
    val flowDetail: String,
    val frequencyStatus: String,
    val instantGenerationMw: Double,
    val instantBalanceMw: Double,
    val freqFactor: Double,
    val rpmFactor: Double,
    val torqueDemandMNm: Double,
    val torqueActualMNm: Double,
    val nominalInertiaGva: Double,
    val instantInertiaGva: Double,
    val inertiaDeltaGva: Double,
    val demandCoveragePct: Double,
    val interconnectorFlowMw: Double,
    val systemStatus: String
)

fun ReplayFrame.calculateMetrics(constants: InertiaConstants): DerivedMetrics {
    val frequencyStart = (frequencyStartHz ?: frequencyEndHz ?: 50.0)
    val frequency = (frequencyEndHz ?: frequencyStartHz ?: 50.0)
    val rawRocof = rocofHzPerS ?: 0.0
    val rocof = if (kotlin.math.abs(rawRocof) < 0.005) 0.0 else rawRocof
    val rocofAbs = kotlin.math.abs(rocof)
    val rpm = frequency * 60.0
    val totalGeneration = totalGenerationMw ?: 0.0
    val demand = estimatedDemandMw ?: totalGeneration
    val delta = totalGeneration - demand

    val flowLabel = when {
        delta > 500 -> "EXPORTING"
        delta < -500 -> "IMPORTING"
        else -> "BALANCED"
    }
    val flowDetail = "Δ ${if (delta > 0) "+" else ""}${delta.toInt()} MW"

    val frequencyStatus = when {
        frequency < 49.1 || frequency > 50.9 -> "LFDD ARMED"
        (frequency in 49.1..<49.5) || (frequency > 50.5 && frequency <= 50.9) -> "OUTSIDE STAT LIMIT"
        frequency in 49.95..50.05 -> "STABLE"
        (frequency in 49.5..<49.95) || (frequency > 50.05 && frequency <= 50.5) -> "STRESSED"
        else -> "STABLE"
    }

    val factor = frequency / 50.0
    val rpmFactor = rpm / 3000.0
    val instantGeneration = totalGeneration * factor
    val instantBalance = instantGeneration - demand

    val torqueDemandNm = totalGeneration * 3183.0
    val rocofFactor = (1 - (rocof / 0.7)).coerceIn(0.2, 1.8)
    val torqueActualNm = torqueDemandNm * rocofFactor

    val mechanicalInertia = (
        (gasMw ?: 0.0) * constants.gasH +
            (coalMw ?: 0.0) * constants.coalH +
            (nuclearMw ?: 0.0) * 6.0 +
            (hydroMw ?: 0.0) * constants.hydroH +
            (biomassMw ?: 0.0) * constants.mechH +
            (storageMw ?: 0.0) * constants.mechH +
            (otherMw ?: 0.0) * constants.mechH
        ) / 1000.0

    val instantInertia = mechanicalInertia * rpmFactor * rpmFactor
    val inertiaDelta = instantInertia - constants.targetGva
    val coveragePct = if (demand > 0.0) (instantGeneration / demand) * 100.0 else 0.0

    val status = when {
        rocofAbs < 0.005 -> "STABLE"
        rocofAbs < 0.05 -> "SHIVERING"
        rocofAbs < 0.75 -> "STRESSED"
        else -> "LFDD ARMED"
    }

    return DerivedMetrics(
        frequencyStartHz = frequencyStart,
        frequencyHz = frequency,
        rocofHzPerS = rocof,
        rocofAbsHzPerS = rocofAbs,
        rpm = rpm,
        generationMw = totalGeneration,
        demandMw = demand,
        deltaMw = delta,
        flowLabel = flowLabel,
        flowDetail = flowDetail,
        frequencyStatus = frequencyStatus,
        instantGenerationMw = instantGeneration,
        instantBalanceMw = instantBalance,
        freqFactor = factor,
        rpmFactor = rpmFactor,
        torqueDemandMNm = torqueDemandNm / 1_000_000.0,
        torqueActualMNm = torqueActualNm / 1_000_000.0,
        nominalInertiaGva = mechanicalInertia,
        instantInertiaGva = instantInertia,
        inertiaDeltaGva = inertiaDelta,
        demandCoveragePct = coveragePct,
        interconnectorFlowMw = netInterconnectorFlowMw ?: 0.0,
        systemStatus = status
    )
}

fun ReplayFrame.fuelBreakdown(): List<Pair<String, Double>> = listOf(
    "GAS" to (gasMw ?: 0.0),
    "COAL" to (coalMw ?: 0.0),
    "NUCLEAR" to (nuclearMw ?: 0.0),
    "WIND" to (windMw ?: 0.0),
    "WIND_EMB" to (windEmbeddedMw ?: 0.0),
    "SOLAR" to (solarMw ?: 0.0),
    "HYDRO" to (hydroMw ?: 0.0),
    "BIOMASS" to (biomassMw ?: 0.0),
    "STORAGE" to (storageMw ?: 0.0),
    "IMPORTS" to (importsMw ?: 0.0),
    "OTHER" to (otherMw ?: 0.0)
)

fun ReplayFrame.interconnectorFlows(): List<Pair<String, Double>> = listOf(
    "IFA_FLOW" to (ifaFlowMw ?: 0.0),
    "IFA2_FLOW" to (ifa2FlowMw ?: 0.0),
    "BRITNED_FLOW" to (britnedFlowMw ?: 0.0),
    "MOYLE_FLOW" to (moyleFlowMw ?: 0.0),
    "EAST_WEST_FLOW" to (eastWestFlowMw ?: 0.0),
    "NEMO_FLOW" to (nemoFlowMw ?: 0.0),
    "NSL_FLOW" to (nslFlowMw ?: 0.0),
    "ELECLINK_FLOW" to (eleclinkFlowMw ?: 0.0),
    "VIKING_FLOW" to (vikingFlowMw ?: 0.0),
    "GREENLINK_FLOW" to (greenlinkFlowMw ?: 0.0),
    "NET_INTERCONNECTOR_FLOW" to (netInterconnectorFlowMw ?: 0.0)
)
