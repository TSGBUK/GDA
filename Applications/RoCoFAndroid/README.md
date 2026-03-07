# RoCoFAndroid

Native Android dashboard for RoCoF replay JSON.

## What this app does

- Loads replay JSON (`derive-rocof-replay-v1`) from device storage using Android file picker.
- Plays frames at configurable FPS.
- Calculates and shows key live metrics (RoCoF, frequency, RPM, flow, inertia, coverage).
- Renders native dashboard cards and sparkline charts in Jetpack Compose.

## Current JSON source options

1. **Load JSON file** (recommended): pick `derived_rocof_replay.json` from storage.
2. **Load bundled sample**: works only if you place a file named `derived_rocof_replay.json` in `app/src/main/assets/`.

## Prerequisites

- Android Studio (recommended to open/build this project)
- JDK 17
- Android SDK + emulator/device

### Tooling status on this machine

- JDK 17 installed via winget at:
  - `C:\Program Files\Microsoft\jdk-17.0.18.8-hotspot\bin\java.exe`
- Android platform-tools installed via winget at:
  - `C:\Users\shir1\AppData\Local\Microsoft\WinGet\Packages\Google.PlatformTools_Microsoft.Winget.Source_8wekyb3d8bbwe\platform-tools\adb.exe`

If `java` or `adb` are not found in terminal, restart VS Code (or open a new terminal) so PATH updates apply.

## Open and run

1. Open `DataVisualizations/RoCoFAndroid` in Android Studio.
2. Let Gradle sync finish.
3. Select an emulator or connected Android device.
4. Run app.

## Optional: CLI build (after Gradle wrapper is generated)

From `DataVisualizations/RoCoFAndroid`:

```powershell
.\gradlew.bat assembleDebug
```
