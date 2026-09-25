#!/usr/bin/env bash
# F5 — Soak test: vigila las invariantes de un lu-node en marcha (real o --sim).
#
#   deploy/soak.sh [URL] [HORAS] [INTERVALO_S]
#   deploy/soak.sh http://127.0.0.1:9100 72 60
#
# Cada intervalo guarda una fila CSV en soak-<fecha>.csv y verifica:
#   * lu_sync_unaccounted == 0 y lu_flow_unaccounted == 0   (conservación)
#   * lu_sim_checks_total{result="mismatch"} == 0           (libro == verdad, solo --sim)
#   * lu_flow_mirror_mismatch_total == 0                     (espejo F2 == libro)
#   * lu_ingest_dropped_total no crece                       (motor saturado)
# Termina con código 1 ante la primera violación (con la fila y el motivo).
set -euo pipefail
URL="${1:-http://127.0.0.1:9100}"
HOURS="${2:-72}"
EVERY="${3:-60}"
OUT="soak-$(date -u +%Y%m%dT%H%M%SZ).csv"
END=$(( $(date +%s) + HOURS * 3600 ))

sum() { # suma de una métrica (todas las etiquetas) que cumple el filtro
  awk -v re="$1" '$0 !~ /^#/ && $0 ~ re { s += $NF } END { printf "%.0f", s + 0 }' <<<"$M"
}

echo "ts,live,resyncs,unaccounted_diffs,unaccounted_trades,sim_ok,sim_mismatch,mirror_mismatch,ingest_dropped,late,aligned,walls,chaos_dropped,chaos_outages" > "$OUT"
prev_drop=""
echo "soak: $URL durante ${HOURS} h cada ${EVERY} s → $OUT"
while [ "$(date +%s)" -lt "$END" ]; do
  if ! M="$(curl -fsS --max-time 10 "$URL/metrics")"; then
    echo "$(date -u +%FT%TZ) VIOLACIÓN: /metrics no responde"; exit 1
  fi
  row=(
    "$(date -u +%FT%TZ)"
    "$(sum '^lu_book_live')"
    "$(sum '^lu_sync_resync_total')"
    "$(sum '^lu_sync_unaccounted')"
    "$(sum '^lu_flow_unaccounted')"
    "$(sum '^lu_sim_checks_total.*result="ok"')"
    "$(sum '^lu_sim_checks_total.*result="mismatch"')"
    "$(sum '^lu_flow_mirror_mismatch_total')"
    "$(sum '^lu_ingest_dropped_total')"
    "$(sum '^lu_flow_trades_total.*state="late"')"
    "$(sum '^lu_flow_trades_total.*state="aligned"')"
    "$(sum '^lu_walls_retired_total')"
    "$(sum '^lu_chaos_dropped_total')"
    "$(sum '^lu_chaos_outages_total')"
  )
  (IFS=,; echo "${row[*]}") >> "$OUT"
  fail=""
  [ "${row[3]}" != 0 ] && fail="diffs sin contabilizar = ${row[3]}"
  [ "${row[4]}" != 0 ] && fail="trades sin contabilizar = ${row[4]}"
  [ "${row[6]}" != 0 ] && fail="libro publicado distinto de la verdad (${row[6]})"
  [ "${row[7]}" != 0 ] && fail="espejo F2 distinto del libro (${row[7]})"
  [ -n "$prev_drop" ] && [ "${row[8]}" != "$prev_drop" ] && fail="cola del motor descartando eventos (${prev_drop} → ${row[8]})"
  prev_drop="${row[8]}"
  if [ -n "$fail" ]; then
    echo "${row[0]} VIOLACIÓN: $fail"; exit 1
  fi
  echo "${row[0]} ok · live=${row[1]} resyncs=${row[2]} verdad=${row[5]}✓ late=${row[9]} muros=${row[11]}"
  sleep "$EVERY"
done
echo "soak completo sin violaciones → $OUT"
