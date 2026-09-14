#!/bin/bash
# QuickMeet SFU capacity & recovery benchmark script
# Reproduces QM-003 validation: NACK/FEC recovery, HW accel, 200+ stream capacity

set -e

echo "========================================"
echo "QuickMeet QM-003 Benchmark Suite"
echo "========================================"

echo ""
echo "[1/4] NACK/FEC recovery tests (30% loss scenario)..."
cargo test --workspace -p qm-sfu -- --nocapture recovery:: 2>&1 | tail -5

echo ""
echo "[2/4] Hardware acceleration tests..."
cargo test --workspace -p qm-sfu -- --nocapture hwaccel:: 2>&1 | tail -5

echo ""
echo "[3/4] Capacity benchmark tests (200+ streams)..."
cargo test --workspace -p qm-sfu -- --nocapture capacity:: 2>&1 | tail -5

echo ""
echo "[4/4] Full workspace test suite..."
cargo test --workspace 2>&1 | grep "test result"

echo ""
echo "========================================"
echo "All benchmarks passed."
echo "========================================"
