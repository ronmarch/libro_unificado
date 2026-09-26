#!/usr/bin/env bash
# F5 — Prueba de caos reproducible sin red: exchange simulado + pérdidas, cortes
# de línea y snapshots fallidos, vigilado por soak.sh.
#
#   deploy/chaos-sim.sh [MINUTOS] [SEMILLA]
set -euo pipefail
MIN="${1:-10}"
SEED="${2:-24301}"
BIN="${BIN:-target/release/lu-node}"
PORT="${PORT:-9199}"
"$BIN" --sim --sim-seed "$SEED" --lines 2 --listen "127.0.0.1:$PORT" --log warn \
  --chaos-drop 0.02 --chaos-jitter-ms 30 --chaos-outage-s 60 --chaos-snapshot-fail 0.2 &
PID=$!
trap 'kill $PID 2>/dev/null || true' EXIT
sleep 5
HOURS_FRAC=$(awk -v m="$MIN" 'BEGIN{ printf "%d", (m + 59) / 60 }')
# soak.sh trabaja en horas: se limita con timeout a los minutos pedidos.
timeout "$(( MIN * 60 ))" "$(dirname "$0")/soak.sh" "http://127.0.0.1:$PORT" "$HOURS_FRAC" 15 && rc=$? || rc=$?
# 124 = timeout alcanzado sin violaciones.
if [ "$rc" = 0 ] || [ "$rc" = 124 ]; then echo "caos: OK ($MIN min, semilla $SEED)"; else echo "caos: FALLÓ"; exit 1; fi
