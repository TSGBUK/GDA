param(
    [switch]$CleanupParquet,
    [string[]]$LiveOutputSuppressKeywords = @("[skip]"),
    [switch]$Resume,
    [switch]$ResetResume,
    [switch]$Validate,
    [switch]$Help
)

$ErrorActionPreference = "Stop"

if ($args -contains "--help") { $Help = $true }
if ($args -contains "--validate") { $Validate = $true }
if ($args -contains "--resume") { $Resume = $true }
if ($args -contains "--reset-resume") { $ResetResume = $true }
if ($args -contains "--cleanup-parquet") { $CleanupParquet = $true }

$reservedLongFlags = @("--help", "--validate", "--resume", "--reset-resume", "--cleanup-parquet")
if ($LiveOutputSuppressKeywords) {
    foreach ($flag in $reservedLongFlags) {
        if ($LiveOutputSuppressKeywords -contains $flag) {
            switch ($flag) {
                "--help" { $Help = $true }
                "--validate" { $Validate = $true }
                "--resume" { $Resume = $true }
                "--reset-resume" { $ResetResume = $true }
                "--cleanup-parquet" { $CleanupParquet = $true }
            }
        }
    }
    $LiveOutputSuppressKeywords = @(
        $LiveOutputSuppressKeywords | Where-Object { $reservedLongFlags -notcontains $_ }
    )
    if ($LiveOutputSuppressKeywords.Count -eq 0) {
        $LiveOutputSuppressKeywords = @("[skip]")
    }
}

function Show-InstallerHelp {
    Write-Host ""
    Write-Host "GDA Installer / Pipeline Runner" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Usage:" -ForegroundColor Yellow
    Write-Host "  .\Installer.ps1 [options]"
    Write-Host ""
    Write-Host "Options:" -ForegroundColor Yellow
    Write-Host "  -Help or --help                 Show this help and exit"
    Write-Host "  -Validate or --validate         Run ONLY CSV-to-Parquet validation"
    Write-Host "  -Resume or --resume             Resume from saved completed steps"
    Write-Host "  -ResetResume or --reset-resume  Clear saved resume checkpoint first"
    Write-Host "  -CleanupParquet or --cleanup-parquet"
    Write-Host "                                    Remove existing Parquet trees in step 4"
    Write-Host "  -LiveOutputSuppressKeywords <string[]>"
    Write-Host "                                    Hide matching step-7 live output lines"
    Write-Host ""
    Write-Host "Default pipeline commands (7 steps):" -ForegroundColor Yellow
    Write-Host "  1) pip install -r requirements.txt"
    Write-Host "  2) parquet backend check/repair (pyarrow, fastparquet)"
    Write-Host "  3) verify_setup.py"
    Write-Host "  4) check_parquet.py (optional cleanup)"
    Write-Host "  5) dedupe.py"
    Write-Host "  6) split_csv.py"
    Write-Host "  7) run_parquet_conversions.py --run --raw"
    Write-Host ""
    Write-Host "Validate mode command:" -ForegroundColor Yellow
    Write-Host "  validate_parquet_vs_csv.py --root .. --report parquet_validation.txt"
    Write-Host ""
    Write-Host "Warnings:" -ForegroundColor Yellow
    Write-Host "  - Full conversion runs can take several hours on large/chunked datasets."
    Write-Host "  - Do not use -CleanupParquet unless you intend to rebuild Parquet outputs."
    Write-Host "  - Validate mode is read-only and can still take a long time on huge trees."
    Write-Host ""
    Write-Host "Examples:" -ForegroundColor Yellow
    Write-Host "  .\Installer.ps1"
    Write-Host "  .\Installer.ps1 -Resume"
    Write-Host "  .\Installer.ps1 -Resume -ResetResume"
    Write-Host "  .\Installer.ps1 -Validate"
    Write-Host "  .\Installer.ps1 --validate"
    Write-Host ""
}

if ($Help) {
    Show-InstallerHelp
    exit 0
}

if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
    $PSNativeCommandUseErrorActionPreference = $false
}

$GridPython = "..\grid\Scripts\python.exe"
$DotVenvPython = "..\.venv\Scripts\python.exe"
$ActiveVenvPython = if ($env:VIRTUAL_ENV) {
    Join-Path $env:VIRTUAL_ENV "Scripts\python.exe"
}
else {
    $null
}

$VenvName = "system"
$VenvExists = $false

if ($ActiveVenvPython -and (Test-Path $ActiveVenvPython)) {
    $PythonExe = (Resolve-Path $ActiveVenvPython).Path
    $VenvName = Split-Path $env:VIRTUAL_ENV -Leaf
    $VenvExists = $true
}
elseif (Test-Path $GridPython) {
    $PythonExe = (Resolve-Path $GridPython).Path
    $VenvName = "grid"
    $VenvExists = $true
}
elseif (Test-Path $DotVenvPython) {
    $PythonExe = (Resolve-Path $DotVenvPython).Path
    $VenvName = ".venv"
    $VenvExists = $true
}
else {
    $PythonExe = "python"
}

$ParquetCheckCmd = "`"$PythonExe`" -c `"import pyarrow.parquet as _pq; import fastparquet as _fp`""
$ParquetRepairCmd = "`"$PythonExe`" -m pip install --no-cache-dir --force-reinstall pyarrow==22.0.0 fastparquet==2025.12.0"
$CheckParquetCmd = "`"$PythonExe`" check_parquet.py ../ --report parq_clean.txt"
$ValidateCmd = "`"$PythonExe`" validate_parquet_vs_csv.py --root .. --report parquet_validation.txt"
if ($CleanupParquet) {
    $CheckParquetCmd = "$CheckParquetCmd --cleanup"
}

$Steps = @(
    "`"$PythonExe`" -m pip install -r ../requirements.txt",
    "$ParquetCheckCmd || $ParquetRepairCmd",
    "`"$PythonExe`" verify_setup.py",
    "$CheckParquetCmd",
    "`"$PythonExe`" dedupe.py",
    "`"$PythonExe`" split_csv.py",
    "`"$PythonExe`" -u run_parquet_conversions.py --report parq_run.txt --run --raw"
)

if ($Validate) {
    $Steps = @($ValidateCmd)
}

$Tick  = [char]0x2714   # ✔
$Cross = [char]0x2716   # ✖
$Arrow = [char]0x27A4   # ➤

$TotalSteps = $Steps.Count
$CurrentStep = 0
$Results = @()
$LogFile = "pipeline_$(Get-Date -Format 'yyyyMMdd_HHmmss').log"
$ResumeStateFile = ".installer_resume.json"
$CompletedSteps = [System.Collections.Generic.HashSet[int]]::new()

if ($ResetResume -and (Test-Path $ResumeStateFile)) {
    Remove-Item $ResumeStateFile -Force -ErrorAction SilentlyContinue
}

if ($Resume -and (Test-Path $ResumeStateFile)) {
    try {
        $state = Get-Content $ResumeStateFile -Raw | ConvertFrom-Json
        $savedSteps = @($state.Steps)
        $currentSteps = @($Steps)
        if (($savedSteps -join "`n") -eq ($currentSteps -join "`n")) {
            foreach ($stepNo in @($state.CompletedSteps)) {
                [void]$CompletedSteps.Add([int]$stepNo)
            }
        }
        else {
            Write-Host "[resume] Step list changed; ignoring old resume state." -ForegroundColor DarkYellow
        }
    }
    catch {
        Write-Host "[resume] Could not parse resume state; starting fresh." -ForegroundColor DarkYellow
    }
}

Write-Host ""
Write-Host "====================================================" -ForegroundColor DarkCyan
Write-Host "         PYTHON DATA PIPELINE CONTROLLER           " -ForegroundColor Cyan
Write-Host "====================================================" -ForegroundColor DarkCyan
Write-Host "Venv Name: $VenvName" -ForegroundColor DarkGray
Write-Host "Venv Exists: $VenvExists" -ForegroundColor DarkGray
Write-Host "Python: $PythonExe" -ForegroundColor DarkGray
Write-Host "Parquet Cleanup: $CleanupParquet" -ForegroundColor DarkGray
Write-Host "Validate Only: $Validate" -ForegroundColor DarkGray
Write-Host "Live Output Suppress Keywords: $($LiveOutputSuppressKeywords -join ', ')" -ForegroundColor DarkGray
Write-Host "Resume: $Resume" -ForegroundColor DarkGray
Write-Host "Resume State File: $ResumeStateFile" -ForegroundColor DarkGray
Write-Host "Log File: $LogFile" -ForegroundColor DarkGray
Write-Host ""

function Save-ResumeState([int]$stepNumber) {
    [void]$CompletedSteps.Add($stepNumber)
    $state = [ordered]@{
        Steps = $Steps
        CompletedSteps = @($CompletedSteps | Sort-Object)
        UpdatedAt = (Get-Date).ToString("o")
    }
    $state | ConvertTo-Json -Depth 6 | Set-Content -Path $ResumeStateFile -Encoding utf8
}

function Should-SuppressLiveLine([string]$line) {
    if (-not $LiveOutputSuppressKeywords -or $LiveOutputSuppressKeywords.Count -eq 0) {
        return $false
    }
    foreach ($keyword in $LiveOutputSuppressKeywords) {
        if ([string]::IsNullOrWhiteSpace($keyword)) {
            continue
        }
        if ($line.IndexOf($keyword, [System.StringComparison]::OrdinalIgnoreCase) -ge 0) {
            return $true
        }
    }
    return $false
}

function Normalize-VerifySetupText([string]$line) {
    if ($null -eq $line) { return "" }
    $fixed = $line
    $fixed = [regex]::Replace($fixed, "[^\x09\x0A\x0D\x20-\x7E]", "")
    return $fixed
}

function Write-FormattedLine([string]$line, [bool]$isVerifySetup) {
    $text = if ($isVerifySetup) { Normalize-VerifySetupText $line } else { $line }

    if (-not $isVerifySetup) {
        Write-Host $text
        return
    }

    if ([string]::IsNullOrWhiteSpace($text)) {
        Write-Host ""
        return
    }

    if ($text -match "^=+") {
        Write-Host $text -ForegroundColor DarkCyan
        return
    }
    if ($text -match "^\[[0-9]+\]") {
        Write-Host $text -ForegroundColor Cyan
        return
    }
    if ($text -match "^Summary:") {
        Write-Host $text -ForegroundColor Cyan
        return
    }
    if ($text -match "MISSING|No NVIDIA GPU|CUDA toolkit .* not found|\bfailed\b|Traceback") {
        Write-Host $text -ForegroundColor Red
        return
    }
    if ($text -match "optional, not installed|CPU-only mode|may not be fully installed") {
        Write-Host $text -ForegroundColor Yellow
        return
    }
    if ($text -match "version|successful|ready for TSGB data processing|All core dependencies installed correctly") {
        Write-Host $text -ForegroundColor Green
        return
    }

    Write-Host $text
}

function Get-RequirementsPackages([string]$requirementsPath) {
    if (-not (Test-Path $requirementsPath)) {
        return @()
    }

    $packages = New-Object System.Collections.Generic.List[string]
    foreach ($raw in Get-Content $requirementsPath) {
        $line = $raw.Trim()
        if ([string]::IsNullOrWhiteSpace($line)) { continue }
        if ($line.StartsWith("#")) { continue }
        if ($line.StartsWith("-")) { continue }

        $name = ($line -split "[<>=!~]", 2)[0].Trim()
        if ($name.Contains("[")) {
            $name = $name.Substring(0, $name.IndexOf("["))
        }
        if (-not [string]::IsNullOrWhiteSpace($name)) {
            $packages.Add($name)
        }
    }

    return @($packages | Select-Object -Unique)
}

function Check-InstalledPackages([string]$pythonExe, [string]$requirementsPath, [string]$logFilePath) {
    $importMap = @{
        "scikit-learn" = "sklearn"
        "python-dateutil" = "dateutil"
    }

    $pkgs = Get-RequirementsPackages $requirementsPath
    if (-not $pkgs -or $pkgs.Count -eq 0) {
        return
    }

    Write-Host ""
    Write-Host "[pkg-check] Verifying required packages" -ForegroundColor DarkCyan
    "[pkg-check] Verifying required packages" | Out-File -FilePath $logFilePath -Append -Encoding utf8

    $found = 0
    $missing = 0
    $unknown = 0

    foreach ($pkg in $pkgs) {
        $module = if ($importMap.ContainsKey($pkg)) { $importMap[$pkg] } else { $pkg.Replace("-", "_") }

        if ([string]::IsNullOrWhiteSpace($module)) {
            $line = "=  $pkg (not detected)"
            Write-Host $line -ForegroundColor Yellow
            $line | Out-File -FilePath $logFilePath -Append -Encoding utf8
            $unknown++
            continue
        }

        $probe = & $pythonExe -c "import importlib.util; import sys; m=sys.argv[1]; print('FOUND' if importlib.util.find_spec(m) else 'MISSING')" $module
        $probeText = "$probe".Trim()

        if ($probeText -eq "FOUND") {
            $line = "$Tick $pkg"
            Write-Host $line -ForegroundColor Green
            $line | Out-File -FilePath $logFilePath -Append -Encoding utf8
            $found++
        }
        elseif ($probeText -eq "MISSING") {
            $line = "$Cross $pkg"
            Write-Host $line -ForegroundColor Red
            $line | Out-File -FilePath $logFilePath -Append -Encoding utf8
            $missing++
        }
        else {
            $line = "=  $pkg (not detected)"
            Write-Host $line -ForegroundColor Yellow
            $line | Out-File -FilePath $logFilePath -Append -Encoding utf8
            $unknown++
        }
    }

    $summary = "[pkg-check] found=$found missing=$missing not-detected=$unknown"
    Write-Host $summary -ForegroundColor DarkCyan
    Write-Host "[pkg-check] '=' usually means detection ambiguity, not necessarily missing." -ForegroundColor DarkYellow
    $summary | Out-File -FilePath $logFilePath -Append -Encoding utf8
}

function Test-CriticalImportsAndRepair([string]$pythonExe, [string]$logFilePath) {
    $checks = @(
        @{ Module = "numpy.rec"; Repair = "numpy" },
        @{ Module = "pandas"; Repair = "pandas" },
        @{ Module = "pyarrow.parquet"; Repair = "pyarrow==22.0.0" },
        @{ Module = "fastparquet"; Repair = "fastparquet==2025.12.0" }
    )

    Write-Host "[health] Checking critical imports..." -ForegroundColor DarkCyan
    "[health] Checking critical imports..." | Out-File -FilePath $logFilePath -Append -Encoding utf8

    foreach ($check in $checks) {
        $module = $check.Module
        $repair = $check.Repair
        $probe = & $pythonExe -c "import importlib,sys; m=sys.argv[1]; importlib.import_module(m); print('OK')" $module 2>$null
        $ok = "$probe".Trim() -eq "OK"

        if ($ok) {
            $line = "$Tick import $module"
            Write-Host $line -ForegroundColor Green
            $line | Out-File -FilePath $logFilePath -Append -Encoding utf8
            continue
        }

        $warn = "$Cross import $module (attempting repair: $repair)"
        Write-Host $warn -ForegroundColor Red
        $warn | Out-File -FilePath $logFilePath -Append -Encoding utf8

        $rOut = [System.IO.Path]::GetTempFileName()
        $rErr = [System.IO.Path]::GetTempFileName()
        $repairCmd = "set PYTHONIOENCODING=utf-8 && `"$pythonExe`" -m pip install --no-cache-dir --force-reinstall $repair"
        $rProc = Start-Process -FilePath "cmd.exe" `
            -ArgumentList "/c", $repairCmd `
            -NoNewWindow -Wait -PassThru `
            -RedirectStandardOutput $rOut `
            -RedirectStandardError $rErr

        if (Test-Path $rOut) {
            Get-Content $rOut | Out-File -FilePath $logFilePath -Append -Encoding utf8
        }
        if (Test-Path $rErr) {
            Get-Content $rErr | Out-File -FilePath $logFilePath -Append -Encoding utf8
        }
        Remove-Item $rOut, $rErr -ErrorAction SilentlyContinue

        $probe2 = & $pythonExe -c "import importlib,sys; m=sys.argv[1]; importlib.import_module(m); print('OK')" $module 2>$null
        if ("$probe2".Trim() -eq "OK") {
            $line = "$Tick repaired $module"
            Write-Host $line -ForegroundColor Green
            $line | Out-File -FilePath $logFilePath -Append -Encoding utf8
        }
        else {
            $line = "$Cross repair failed for $module"
            Write-Host $line -ForegroundColor Red
            $line | Out-File -FilePath $logFilePath -Append -Encoding utf8
        }

        if ($rProc.ExitCode -ne 0) {
            throw "Critical import repair command failed for $module"
        }
    }
}

function Run-Step($command, [int]$stepNumber) {

    $script:CurrentStep = $stepNumber

    Write-Host "[$stepNumber/$TotalSteps] $Arrow $command" -ForegroundColor Cyan
    Write-Host "----------------------------------------------------" -ForegroundColor DarkGray

    $start = Get-Date
    $cmdWithEnv = "set PYTHONIOENCODING=utf-8 && $command"
    $isLongRunner = $command -match "run_parquet_conversions\.py"
    $isRequirementsInstall = $command -match "-m pip install -r \.\./requirements\.txt"
    $isVerifySetup = $command -like "*verify_setup.py*"

    if ($isLongRunner) {
        $combinedFile = [System.IO.Path]::GetTempFileName()
        $wrappedCmd = "$cmdWithEnv > `"$combinedFile`" 2>&1"
        $proc = Start-Process -FilePath "cmd.exe" `
            -ArgumentList "/c", $wrappedCmd `
            -NoNewWindow -PassThru

        $lineCount = 0
        $lastActivity = Get-Date
        $workLineActive = $false
        $workLineWidth = 0

        while (-not $proc.HasExited) {
            if (Test-Path $combinedFile) {
                $lines = @(Get-Content $combinedFile)
                if ($lines.Count -gt $lineCount) {
                    $newLines = $lines[$lineCount..($lines.Count - 1)]
                    $lineCount = $lines.Count
                    $lastActivity = Get-Date
                    foreach ($line in $newLines) {
                        $line | Out-File -FilePath $LogFile -Append -Encoding utf8
                        $isRunningLine = $line.TrimStart().StartsWith("running ", [System.StringComparison]::OrdinalIgnoreCase)
                        $isWorkLine = $line.IndexOf("[work]", [System.StringComparison]::OrdinalIgnoreCase) -ge 0

                        if ($isRunningLine) {
                            if ($workLineActive) {
                                Write-Host ""
                                $workLineActive = $false
                                $workLineWidth = 0
                            }
                            Write-Host $line
                            continue
                        }

                        if (-not $isWorkLine) {
                            continue
                        }

                        if (Should-SuppressLiveLine $line) {
                            continue
                        }

                        $padding = ""
                        if ($workLineWidth -gt $line.Length) {
                            $padding = " " * ($workLineWidth - $line.Length)
                        }
                        [Console]::Write("`r$line$padding")
                        $workLineActive = $true
                        $workLineWidth = [Math]::Max($workLineWidth, $line.Length)
                    }
                }
                elseif (((Get-Date) - $lastActivity).TotalSeconds -ge 5) {
                    $heartbeat = "[work] still running... $(Get-Date -Format 'HH:mm:ss')"
                    $padding = ""
                    if ($workLineWidth -gt $heartbeat.Length) {
                        $padding = " " * ($workLineWidth - $heartbeat.Length)
                    }
                    [Console]::Write("`r$heartbeat$padding")
                    $workLineActive = $true
                    $workLineWidth = [Math]::Max($workLineWidth, $heartbeat.Length)
                    $heartbeat | Out-File -FilePath $LogFile -Append -Encoding utf8
                    $lastActivity = Get-Date
                }
            }
            Start-Sleep -Milliseconds 500
        }

        if (Test-Path $combinedFile) {
            $finalLines = @(Get-Content $combinedFile)
            if ($finalLines.Count -gt $lineCount) {
                foreach ($line in $finalLines[$lineCount..($finalLines.Count - 1)]) {
                    $line | Out-File -FilePath $LogFile -Append -Encoding utf8
                    $isRunningLine = $line.TrimStart().StartsWith("running ", [System.StringComparison]::OrdinalIgnoreCase)
                    $isWorkLine = $line.IndexOf("[work]", [System.StringComparison]::OrdinalIgnoreCase) -ge 0

                    if ($isRunningLine) {
                        if ($workLineActive) {
                            Write-Host ""
                            $workLineActive = $false
                            $workLineWidth = 0
                        }
                        Write-Host $line
                        continue
                    }

                    if (-not $isWorkLine) {
                        continue
                    }

                    if (Should-SuppressLiveLine $line) {
                        continue
                    }

                    $padding = ""
                    if ($workLineWidth -gt $line.Length) {
                        $padding = " " * ($workLineWidth - $line.Length)
                    }
                    [Console]::Write("`r$line$padding")
                    $workLineActive = $true
                    $workLineWidth = [Math]::Max($workLineWidth, $line.Length)
                }
            }
            if ($workLineActive) {
                Write-Host ""
                $workLineActive = $false
                $workLineWidth = 0
            }
            Remove-Item $combinedFile -ErrorAction SilentlyContinue
        }

        $exitCode = $proc.ExitCode
    }
    else {
        $stdoutFile = [System.IO.Path]::GetTempFileName()
        $stderrFile = [System.IO.Path]::GetTempFileName()
        $pipUpgradeNoticeSeen = $false

        $proc = Start-Process -FilePath "cmd.exe" `
            -ArgumentList "/c", $cmdWithEnv `
            -NoNewWindow -Wait -PassThru `
            -RedirectStandardOutput $stdoutFile `
            -RedirectStandardError $stderrFile

        if (Test-Path $stdoutFile) {
            foreach ($line in Get-Content $stdoutFile) {
                $line | Out-File -FilePath $LogFile -Append -Encoding utf8
                $displayLine = if ($isVerifySetup) { Normalize-VerifySetupText $line } else { $line }
                if ($line.IndexOf("[notice] A new release of pip is available", [System.StringComparison]::OrdinalIgnoreCase) -ge 0) {
                    $pipUpgradeNoticeSeen = $true
                }
                if ($isRequirementsInstall -and $line.IndexOf("Requirement already satisfied:", [System.StringComparison]::OrdinalIgnoreCase) -ge 0) {
                    continue
                }
                Write-FormattedLine -line $displayLine -isVerifySetup $isVerifySetup
            }
        }
        if (Test-Path $stderrFile) {
            foreach ($line in Get-Content $stderrFile) {
                $line | Out-File -FilePath $LogFile -Append -Encoding utf8
                $displayLine = if ($isVerifySetup) { Normalize-VerifySetupText $line } else { $line }
                if ($line.IndexOf("[notice] A new release of pip is available", [System.StringComparison]::OrdinalIgnoreCase) -ge 0) {
                    $pipUpgradeNoticeSeen = $true
                }
                if ($isRequirementsInstall -and $line.IndexOf("Requirement already satisfied:", [System.StringComparison]::OrdinalIgnoreCase) -ge 0) {
                    continue
                }
                Write-FormattedLine -line $displayLine -isVerifySetup $isVerifySetup
            }
        }

        Remove-Item $stdoutFile, $stderrFile -ErrorAction SilentlyContinue
        $exitCode = $proc.ExitCode

        if ($isRequirementsInstall -and $pipUpgradeNoticeSeen -and $exitCode -eq 0) {
            Write-Host "[pip] New pip release detected, upgrading pip..." -ForegroundColor DarkGray

            $upgradeStdout = [System.IO.Path]::GetTempFileName()
            $upgradeStderr = [System.IO.Path]::GetTempFileName()
            $upgradeCmdWithEnv = "set PYTHONIOENCODING=utf-8 && `"$PythonExe`" -m pip install --upgrade pip"

            $upgradeProc = Start-Process -FilePath "cmd.exe" `
                -ArgumentList "/c", $upgradeCmdWithEnv `
                -NoNewWindow -Wait -PassThru `
                -RedirectStandardOutput $upgradeStdout `
                -RedirectStandardError $upgradeStderr

            if (Test-Path $upgradeStdout) {
                foreach ($line in Get-Content $upgradeStdout) {
                    $line | Out-File -FilePath $LogFile -Append -Encoding utf8
                    if ($line.IndexOf("Requirement already satisfied:", [System.StringComparison]::OrdinalIgnoreCase) -ge 0) {
                        continue
                    }
                    Write-Host $line
                }
            }
            if (Test-Path $upgradeStderr) {
                foreach ($line in Get-Content $upgradeStderr) {
                    $line | Out-File -FilePath $LogFile -Append -Encoding utf8
                    if ($line.IndexOf("Requirement already satisfied:", [System.StringComparison]::OrdinalIgnoreCase) -ge 0) {
                        continue
                    }
                    Write-Host $line
                }
            }

            Remove-Item $upgradeStdout, $upgradeStderr -ErrorAction SilentlyContinue

            if ($upgradeProc.ExitCode -ne 0) {
                $exitCode = $upgradeProc.ExitCode
            }
        }

        if ($isRequirementsInstall -and $exitCode -eq 0) {
            Check-InstalledPackages -pythonExe $PythonExe -requirementsPath "../requirements.txt" -logFilePath $LogFile
            Test-CriticalImportsAndRepair -pythonExe $PythonExe -logFilePath $LogFile
        }
    }
    $end = Get-Date
    $duration = [math]::Round(($end - $start).TotalSeconds, 2)

    if ($exitCode -eq 0) {
        Save-ResumeState $stepNumber
        Write-Host ""
        Write-Host "$Tick Step completed in $duration s" -ForegroundColor Green
        Write-Host ""
        $Results += [PSCustomObject]@{
            Step     = $stepNumber
            Status   = "Success"
            Duration = $duration
        }
    }
    else {
        Write-Host ""
        Write-Host "$Cross Step failed (Exit Code: $exitCode)" -ForegroundColor Red
        Write-Host "See log file: $LogFile" -ForegroundColor DarkRed
        $Results += [PSCustomObject]@{
            Step     = $stepNumber
            Status   = "Failed"
            Duration = $duration
        }
        Show-Summary
        exit $exitCode
    }
}

function Show-Summary {

    Write-Host ""
    Write-Host "================== PIPELINE SUMMARY ==================" -ForegroundColor Yellow

    foreach ($r in $Results) {
        if ($r.Status -eq "Success") {
            Write-Host " $Tick Step $($r.Step) — $($r.Duration)s" -ForegroundColor Green
        }
        else {
            Write-Host " $Cross Step $($r.Step) — $($r.Duration)s" -ForegroundColor Red
        }
    }

    $totalTime = ($Results | Measure-Object -Property Duration -Sum).Sum
    Write-Host "------------------------------------------------------" -ForegroundColor Yellow
    Write-Host " Total Runtime: $([math]::Round($totalTime,2)) s" -ForegroundColor Yellow
    Write-Host "======================================================" -ForegroundColor Yellow
    Write-Host ""
}

for ($i = 0; $i -lt $Steps.Count; $i++) {
    $step = $Steps[$i]
    $stepNumber = $i + 1
    if ($Resume -and $CompletedSteps.Contains($stepNumber)) {
        $script:CurrentStep = $stepNumber
        Write-Host "[$stepNumber/$TotalSteps] $Arrow skipped (resume): already completed" -ForegroundColor DarkGray
        $Results += [PSCustomObject]@{
            Step     = $stepNumber
            Status   = "Resumed"
            Duration = 0
        }
        continue
    }
    Run-Step $step $stepNumber
}

Show-Summary
if (Test-Path $ResumeStateFile) {
    Remove-Item $ResumeStateFile -Force -ErrorAction SilentlyContinue
}
Write-Host "$Tick PIPELINE FINISHED SUCCESSFULLY" -ForegroundColor Green
Write-Host ""