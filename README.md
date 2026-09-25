# libro-unificado · lu-node

Libros L2 exactos de Binance **spot y USDⓈ-M** (SOLUSDT) en RAM y en tiempo real,
con líneas WebSocket redundantes, sincronización verificada por propiedades y
observabilidad Prometheus. Fase F0+F1 del libro unificado multi-exchange.
Diseño completo: [`ARCHITECTURE.md`](ARCHITECTURE.md).

## Requisitos

* Rust estable vía rustup (probado con 1.98.1). Linux x86_64 o aarch64 (Oracle Ampere).
* Compilador C (`gcc`/`cc`): lo usa `ring` para TLS. No requiere cmake ni OpenSSL.
* Servidor en una región donde Binance **no** responda 451 (fuera de EE. UU.).
* `chrony` activo (la latencia se mide contra el reloj del exchange).

## Compilar y verificar

```bash
cargo test --workspace                                        # 31 tests
PROPTEST_CASES=20000 cargo test -p lu-book --test prop_sync   # estrés de propiedades
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

## API

```bash
curl -s 127.0.0.1:9100/health        # estado por mercado
curl -s 127.0.0.1:9100/ready         # 200 solo si todos los libros están Live; si no, 503
curl -s 127.0.0.1:9100/book/spot     # vista completa (top 10, cobertura, líneas, latencias, trades)
curl -s 127.0.0.1:9100/metrics       # Prometheus
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
ssh -L 9100:127.0.0.1:9100 usuario@IP_DE_LA_VM   # luego: http://127.0.0.1:9100/book/perp
```

Cross-compilación hacia aarch64 no verificada en este entorno: compilar en la VM.

## Repositorio

Proyecto independiente de FORJA: usar un repositorio propio.

```bash
git init && git add . && git commit -m "F0+F1: libro Binance spot+USDM sincronizado"
git remote add origin git@github.com:<usuario>/libro-unificado.git && git push -u origin main
```
