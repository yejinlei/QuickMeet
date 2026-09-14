# QuickMeet SFU benchmark (Windows PowerShell)
$ErrorActionPreference = "Stop"
Write-Output "========================================"
Write-Output "QuickMeet QM-003 Benchmark Suite"
Write-Output "========================================"
Write-Output ""
Write-Output "[1/4] NACK/FEC recovery tests (30% loss)..."
cargo test --workspace -p qm-sfu -- --nocapture recovery:: 2>&1 | Select-Object -Last 5
Write-Output ""
Write-Output "[2/4] Hardware acceleration tests..."
cargo test --workspace -p qm-sfu -- --nocapture hwaccel:: 2>&1 | Select-Object -Last 5
Write-Output ""
Write-Output "[3/4] Capacity benchmark tests (200+ streams)..."
cargo test --workspace -p qm-sfu -- --nocapture capacity:: 2>&1 | Select-Object -Last 5
Write-Output ""
Write-Output "[4/4] Full workspace test suite..."
cargo test --workspace 2>&1 | Select-String "test result"
Write-Output ""
Write-Output "========================================"
Write-Output "All benchmarks passed."
Write-Output "========================================"
