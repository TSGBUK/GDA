package com.gda.rocofandroid.data

import android.content.Context
import com.gda.rocofandroid.model.ReplayFile
import kotlinx.serialization.json.Json
import java.io.InputStream

class ReplayRepository {
    private val json = Json {
        ignoreUnknownKeys = true
        isLenient = true
        coerceInputValues = true
    }

    fun loadFromInputStream(inputStream: InputStream): ReplayFile {
        val payload = inputStream.bufferedReader().use { it.readText() }
        return json.decodeFromString(ReplayFile.serializer(), payload)
    }

    fun loadDefaultAsset(context: Context, assetName: String = "derived_rocof_replay.json"): ReplayFile {
        return context.assets.open(assetName).use(::loadFromInputStream)
    }
}
