# libro-unificado · lu-node

Libros L2 exactos de Binance **spot y USDⓈ-M** (SOLUSDT) en RAM y en tiempo real,
con líneas WebSocket redundantes, sincronización verificada por propiedades,
alineador trades ↔ depth (F2), velas footprint y detector de muros retirados (F3),
API WebSocket + interfaz web (F4), simulador con caos y soak (F5). Fases F0–F5 del
libro unificado multi-exchange (venue: Binance).
Diseño completo: [`ARCHITECTURE.md`](ARCHITECTURE.md).

## Requisitos

* Rust estable vía rustup (probado con 1.98.1). Linux x86_64 o aarch64 (Oracle Ampere).
* Compilador C (`gcc`/`cc`): lo usa `ring` para TLS. No requiere cmake ni OpenSSL.
* Servidor en una región donde Binance **no** responda 451 (fuera de EE. UU.).
* `chrony` activo (la latencia se mide contra el reloj del exchange).

## Compilar y verificar

```bash
cargo test --workspace                                              # 54 tests
PROPTEST_CASES=20000 cargo test -p lu-book --test prop_sync         # estrés (libro)
PROPTEST_CASES=20000 cargo test -p lu-flow --test prop_flow         # estrés (F2)
PROPTEST_CASES=20000 cargo test -p lu-metrics --test prop_metrics   # estrés (F3)
cargo clippy --workspace --all-targets -- -D warnings          # 0 advertencias
cargo build --release -p lu-node                              # → target/release/lu-node
```

## Ejecutar

```bash
./target/release/lu-node --symbol SOLUSDT --markets spot,perp --lines 2 --listen 127.0.0.1:9100
```

| Opción | Default | Uso |
|---|---|---|
| `--symbol` | `SOLUSDT` | símbolo |
| `--markets` | `spot,perp` | mercados |
| `--lines` | `2` | conexiones redundantes por stream (1–4) |
| `--listen` | `127.0.0.1:9100` | API de observabilidad |
| `--io-threads` | `1` | hilos del runtime de E/S |
| `--pin` | apagado | fija cada motor a un núcleo propio |
| `--ca-file` | `SSL_CERT_FILE` | CA adicional (redes con proxy TLS) |
| `--log` | `info` | `error`/`warn`/`info`/`debug`/`trace` |
| `--sim` | apagado | exchange sintético con verdad conocida (sin red) |
| `--sim-seed` | `24301` | semilla del simulador |
| `--chaos-drop` | `0` | probabilidad de perder cada evento en cada línea |
| `--chaos-jitter-ms` | `20` | latencia adicional máxima por línea |
| `--chaos-outage-s` | `0` | segundos medios entre cortes de línea |
| `--chaos-snapshot-fail` | `0` | probabilidad de que un snapshot no llegue |

### Sin red: simulador y caos

```bash
./target/release/lu-node --sim                                   # demo: abrir http://127.0.0.1:9100
deploy/chaos-sim.sh 10                                           # 10 min de caos vigilado (sale 1 si falla)
deploy/soak.sh http://127.0.0.1:9100 72 60                       # soak 72 h contra un nodo en marcha
```

## API

```bash
curl -s 127.0.0.1:9100/health        # estado por mercado
curl -s 127.0.0.1:9100/ready         # 200 solo si todos los libros están Live; si no, 503
curl -s 127.0.0.1:9100/book/spot     # vista completa (top 10, cobertura, líneas, latencias, trades, flow F2)
curl -s 127.0.0.1:9100/metrics       # Prometheus
curl -s 127.0.0.1:9100/footprint/perp  # F3: velas 15m/1h/4h por bucket, R táctica/estructural, muros
curl -s 127.0.0.1:9100/cvd             # F3: CVD del libro spot + perp (5 pares)
# Interfaz: http://127.0.0.1:9100/  ·  WebSocket: ws://127.0.0.1:9100/ws?markets=spot,perp&interval_ms=250
```

Alarmas recomendadas:

| Métrica | Condición de alarma |
|---|---|
| `lu_book_live` | 0 durante más de 30 s |
| `lu_sync_unaccounted` | distinto de 0 (defecto: nunca debería ocurrir) |
| `lu_sync_resync_total` | crece más de 3 veces por hora |
| `lu_ingest_dropped_total` | cualquier incremento (motor saturado) |
| `lu_line_up{stream="depth"}` | todas las líneas de un mercado en 0 |
| `lu_line_latency_ms{quantile="0.99"}` | sostenido sobre 250 ms |
| `lu_flow_unaccounted` | distinto de 0 (defecto) |
| `lu_flow_trades_total{state="late"}` | crece de forma sostenida (subir la marca de agua) |
| `lu_flow_mirror_mismatch_total` | cualquier incremento (defecto) |
| `lu_sim_checks_total{result="mismatch"}` | distinto de 0 (solo `--sim`; defecto) |

Reglas listas para Prometheus: `deploy/alerts.yml`.

## Despliegue en Oracle Always Free (ARM Ampere)

```bash
# 1. Verificar que Binance no bloquea la región (debe imprimir 200 dos veces)
curl -s -o /dev/null -w "%{http_code}\n" https://api.binance.com/api/v3/ping
curl -s -o /dev/null -w "%{http_code}\n" https://fapi.binance.com/fapi/v1/ping

# 2. Reloj
sudo systemctl enable --now chronyd && chronyc tracking

# 3. Compilar en la propia VM (recomendado)
curl https://sh.rustup.rs -sSf | sh -s -- -y && . "$HOME/.cargo/env"
cargo build --release -p lu-node

# 4. Instalar como servicio
sudo useradd --system --no-create-home lu
sudo install -D -m 0755 target/release/lu-node /opt/lu/lu-node
sudo cp deploy/lu-node.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now lu-node
journalctl -u lu-node -f

# 5. Ver la API desde tu equipo (la API solo escucha en localhost)
ssh -L 9100:127.0.0.1:9100 usuario@IP_DE_LA_VM   # luego: http://127.0.0.1:9100/ (interfaz)
```

Cross-compilación hacia aarch64 no verificada en este entorno: compilar en la VM.

## Repositorio

Proyecto independiente de FORJA: usar un repositorio propio.

Repositorio privado: [`ronmarch/libro_unificado`](https://github.com/ronmarch/libro_unificado).

```bash
git clone git@github.com:ronmarch/libro_unificado.git     # SSH (Ubuntu)
git clone https://github.com/ronmarch/libro_unificado.git # HTTPS (Windows)
```
