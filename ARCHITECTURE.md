# Libro Unificado — Arquitectura (F0 – F5)

Sistema **independiente de FORJA**. Mantiene en RAM, en tiempo real, libros L2
exactos por venue y mercado, como base del libro unificado multi-exchange
(Binance, OKX, Bybit, Coinbase, Kraken; spot y perpetuos). Sin almacenamiento:
es un sistema de monitoreo, no de validación histórica.

Estado: **F0 – F5 completas para Binance** (libro sincronizado, alineador F2, métricas F3,
API WebSocket + UI F4, simulador con caos y soak F5). **OKX** integrado y verificado en vivo (§6 bis).
Pendiente: Bybit, Coinbase, Kraken.

---

## 1. Principios de diseño

| Principio | Implementación |
|---|---|
| Nunca silenciosamente incorrecto | Ante cualquier duda (hueco, secuencia imposible, libro cruzado, cola llena) el libro sale de `Live` y se reconstruye. Verificado con propiedades. |
| Determinismo | El motor (`lu-book`) no hace E/S ni lee relojes: el tiempo se inyecta. Mismo input ⇒ mismo output ⇒ replay exacto en tests. |
| Aritmética exacta | Punto fijo i64 con escala universal 1e-8 para todos los venues. Parseo del texto del exchange sin `f64`; más de 8 decimales significativos ⇒ error, nunca redondeo. |
| Contabilidad total | Ley de conservación: todo diff recibido queda contabilizado exactamente una vez (`lu_sync_unaccounted` debe ser 0). |
| Sin locks en el camino crítico | Cada motor es dueño exclusivo de su libro; entra por cola acotada, publica vistas inmutables vía `ArcSwap`. |
| Redundancia de líneas | Dos o más conexiones por stream; el primer mensaje que llega gana; duplicados descartados por id. |
| Superficie mínima | TLS con `ring` (sin cmake/aws-lc); HTTP de observabilidad sin framework; `#![forbid(unsafe_code)]` en todo el código propio. |

---

## 2. Crates

```
libro-unificado/
├── crates/lu-core       tipos: Px/Qty punto fijo, MarketId, eventos normalizados, relojes
├── crates/lu-book       L2Book + SyncBook (máquina de estados) + SeqRule + BookObserver
├── crates/lu-flow       F2: alineador trades ↔ depth, cotas inferiores por nivel y lote
├── crates/lu-metrics    F3: velas footprint (TWA perezoso) y detector de muros retirados
├── crates/lu-telemetry  histogramas HDR de latencia + escritor Prometheus
├── crates/lu-net        TLS y líneas WebSocket redundantes genéricas por `Protocol`
├── crates/lu-binance    conector Binance: endpoints, parseo zero-copy, REST
├── crates/lu-okx        conector OKX v5: books top-400 (snapshot por WS), trades, ctVal
└── bins/lu-node         nodo: motores, snapshots, API HTTP + WebSocket + UI, simulador y caos
```

Regla de dependencias: `lu-core` ← `lu-book` ← `lu-flow` ← `lu-metrics` ← `lu-node`; `lu-core` ← `lu-binance` ← `lu-node`.
`lu-book` no conoce a ningún exchange concreto salvo por sus `SeqRule`.

---

## 3. Hilos y flujo de datos

```
 ┌──────────── runtime Tokio (lu-io) ────────────┐
 │ línea A (WS) ─┐                               │
 │ línea B (WS) ─┼─ parseo zero-copy ─ try_send ─┼──► cola acotada 65 536
 │ REST snapshot ┘   (sello rx: línea+reloj)     │         │
 │ API HTTP ◄────────────── ArcSwap<BookView> ◄──┼─────┐   ▼
 └───────────────────────────────────────────────┘   ┌─┴──────────────┐
                                                     │ motor lu-eng-* │  hilo de SO por mercado
                                                     │ SyncBook<Rule> │  (opcional: fijado a núcleo)
                                                     └────────────────┘
```

* **Cola llena ⇒ descarte contado**, nunca bloqueo del hilo de E/S. El diff perdido
  se convierte en hueco detectado ⇒ resync. Métrica: `lu_ingest_dropped_total`.
* El motor pulsa cada 50 ms (timeouts) y publica su vista cada 250 ms.
* Spot: depth y aggTrade viajan en el mismo socket. Futuros: sockets separados
  (`/public` y `/market`), por lo que F2 **no puede asumir orden** entre ambos.

---

## 4. Máquina de sincronización (`SyncBook`)

Fases: `Syncing` → `Live` → (resync) → `Syncing`.

| Regla | Spot | USDⓈ-M |
|---|---|---|
| Descartar previos al snapshot | `u ≤ lastUpdateId` | `u < lastUpdateId` |
| Primer diff válido | `U ≤ id+1 ≤ u` | `U ≤ id ≤ u` |
| Encadenamiento | `U ≤ u_prev + 1` (solapamiento idempotente) | `pu == u_prev` (ids no contiguos) |
| Snapshot | 5000 niveles (peso 250) | 1000 niveles (peso 20) |

Mecanismos:

* **Arbitraje A/B**: diff con `u ≤ local` ⇒ duplicado; diff adelantado ⇒ retenido
  hasta 750 ms esperando que la otra línea entregue el faltante; si no llega ⇒
  `GapTimeout` ⇒ resync. Los retenidos se conservan para puentear el nuevo snapshot.
* **Snapshot no puenteable** (más viejo que el buffer, o con hueco): se pide otro.
* **Libro cruzado** (bid ≥ ask) tras aplicar ⇒ resync (política configurable).
* **Épocas**: cada reconstrucción incrementa `epoch`; las métricas derivadas (F3)
  deben invalidarse al cambiar de época (`BookObserver::on_invalidate`).
* **Cobertura**: si el snapshot trae el máximo de niveles, lo que está más allá es
  desconocido (el diff solo informa cambios). `bid_floor`/`ask_ceiling` delimitan
  la región confiable; F3 debe restringir el CVD del libro a la banda cubierta
  por todos los venues.

Parámetros (`SyncConfig`): `gap_timeout_ms=750`, `max_pending=256`,
`max_buffer=20000`, `snapshot_retry_ms=10000`, `cross_policy=Resync`.

---

## 5. Propiedades garantizadas y cómo se verifican

| Propiedad | Verificación |
|---|---|
| **P1** Pérdidas en una sola línea ⇒ cero resyncs y libro final exacto | proptest, exchange simulado con pérdidas y latencias aleatorias (hasta 300 ms), spot y futuros |
| **P2** Nunca `Live` con estado distinto al real, incluso con pérdida en ambas líneas | proptest: se compara el libro con la verdad en **cada paso** |
| **P3** Conservación de mensajes | proptest: `unaccounted() == 0` en cada paso de P1 y P2 |
| Vivacidad tras hueco real | test canario: exactamente 1 resync y recuperación exacta |
| Las pruebas detectan defectos | prueba de mutación: sabotear un contador hace fallar P1, P2 y el canario |

Resultado: 31 tests (incluye 2 propiedades y 1 canario de vivacidad); estrés de 20 000 casos por propiedad en ~4,5 s.
`clippy -D warnings` limpio.

---

## 5 bis. F2 — Alineador trades ↔ depth (`lu-flow`)

El alineador es el `BookObserver` del `SyncBook` en cada motor; los trades
(ya deduplicados A/B) entran por `Aligner::on_trade`. Determinista: tiempo del exchange.

**Lote** = intervalo `(inicio, fin]` entre dos diffs aplicados. Tiempo del diff:
`T` (futuros) o `E` (spot, no trae `T`). Diffs con el mismo tiempo se fusionan
(se conserva el `q0` del primero). Por nivel y lote, con `e` = ejecutado sin RPI:

| Salida | Fórmula | Es cota inferior de | Prueba |
|---|---|---|---|
| `no_visible_min` | `max(0, e_lo − q0)` | ejecutado que no estaba visible al inicio del lote (icebergs, ocultas, recargas) | de `q0` se consumió `c0 ≤ q0` |
| `cancelado_min` | `max(0, q0 − e_hi − q1)` | cancelado en el lote | `q0 = c0 + k0 + s0`, `c0 ≤ e`, `s0 ≤ q1` |
| `agregado_min` | `max(0, q1 − q0)` | agregado en el lote | balance del nivel |

* **Incertidumbre temporal δ** (`boundary_slack_ms`, default 0 = especificación literal):
  `e_lo` suma solo trades a más de δ de las fronteras; `e_hi` incluye los ambiguos de
  ambos lotes vecinos. Las cotas siguen siendo válidas si `|T − tiempo real| ≤ δ`.
* **Marca de agua por línea**: un lote cierra cuando existe uno posterior y **todas las
  líneas vivas** entregaron un trade (único o duplicado) con `E > fin + 2δ` (orden por
  socket), o el depth avanzó `fin + δ + 250 ms` (trades quietos). Una línea cuyo `E` queda
  más de 1 s atrás de la más adelantada deja de frenar. Motivo: un trade perdido en la
  línea rápida llega después por la lenta; con una marca global llegaba `late`
  (observado en el simulador con caos: 3 tardíos → 0 tras el cambio).
  Un trade cuyo lote ya cerró se cuenta `late` (no se re-emite).
* **Épocas**: el primer lote tras un snapshot y todo lote abierto al perder la
  sincronización son **contaminados**: `q0`/`q1` exactos, cotas en 0. Nunca se mezclan épocas.
* **RPI**: `e` usa `nq`; `q − nq` se reporta aparte (`exec_rpi`).
* **Conservación de trades**: `aligned + contaminated + late + syncing + overflow +
  invalidated + duplicate + retenidos = recibidos` (`lu_flow_unaccounted` = 0).

Verificación (`crates/lu-flow/tests/prop_flow.rs`) con un motor de matching sintético
(FIFO con marca de lote por orden, icebergs que recargan, liquidez oscura, cancelaciones,
RPI, socket de trades con latencia):

| Propiedad | Contenido |
|---|---|
| F1 | cotas ≤ verdad; `q0`, `q1`, ejecutado y RPI exactos (δ = 0) |
| F2 | cotas ≤ verdad con `T` desplazado hasta ±δ |
| F3 | completitud: todo nivel con cambio o ejecución en lotes limpios se emite |
| F4 | conservación en cada paso con resyncs, trades durante sync y tardíos |

Prueba de mutación: invertir `e_lo`/`e_hi`, romper la resolución de `q0` o la fusión de
diffs hace fallar las propiedades. Dos mutantes sobreviven por ser equivalentes bajo el
modelo (margen `2δ` de la marca de agua; frontera de lote ya podada).

## 5 ter. F3 — Métricas (`lu-metrics`)

`Metrics` implementa `FlowSink`; el alineador lo alimenta junto con el resumen del nodo
(`Aligner<(FlowAgg, Metrics)>`). Aritmética entera (`i128` para áreas `qty·ms`).

* **Velas** 15 m / 1 h / 4 h alineadas a UTC, contiguas (un libro quieto también produce
  velas). Por bucket de 1 USDT y lado: TWA, foto (cantidad al cierre), persistencia =
  foto ÷ TWA, ejecutado (sin RPI), RPI aparte, fills, trades, Σ `no_visible_min`,
  Σ `cancelado_min`, Σ `agregado_min`. Por vela: ejecutado comprador/vendedor y delta de trades.
* **TWA perezoso**: `acc += qty_prev · Δt` solo al cambiar; el denominador es el tiempo con
  libro sincronizado (las pausas por resync no se promedian). Un cambio se fecha al fin
  de su lote. Vela iniciada a mitad o con pausa/lote contaminado: `clean = false`.
* **Cobertura**: buckets que tocan `bid_floor`/`ask_ceiling` se marcan `partial` y no
  entran al percentil. (Con un solo venue la banda es la del propio snapshot.)
* **Muros retirados**: implementación exacta de §9 (P, X, N, Y como configuración con los
  valores confirmados). Detalles de implementación: percentil de rango más cercano sobre
  áreas de la vela 4 h en curso (mismo lado, dentro de cobertura); la ventana empieza
  cuando el bucket estaba ≥ TWA justo antes de un cambio e incluye las ejecuciones de ese
  lote; cada ventana se evalúa una vez, en el cruce; ventana con lote contaminado ⇒ rechazo.
  Contadores de rechazo por causa.

* **Táctica vs estructural** (definición del usuario, 2026-09-25): por bucket y lado,
  `R = TWA 15 m ÷ TWA 4 h` con la vela de 4 h **anterior ya cerrada** como referencia
  (opción B: la vela contenedora acota R ≤ 16 y vale 1 en el primer bloque). Lectura:
  R > 1 táctica (candidata a spoof si luego se retira), R < 1 desarmándose (alimenta el
  detector de muros), R ≈ 1 con TWA 4 h alto estructural, R ≈ 1 sin él estable; sin
  liquidez en la referencia y con liquidez ahora ⇒ táctica (R = ∞). Sin vela de referencia
  ⇒ sin lectura. **Supuestos a confirmar**: "≈ 1" = 0,8 ≤ R ≤ 1,25; "TWA 4 h alto" = ≥ P90
  del lado en la vela de referencia (`TacticalConfig`). No confundir con la persistencia
  (foto ÷ TWA dentro de la misma vela).
* **CVD del libro** (definición del usuario, 2026-09-25): con `m` = bucket del precio,
  `Δk = (bid spot + bid perp)(m−k) − (ask spot + ask perp)(m+k)` para k = 1..5, y su
  acumulado. Ejemplo: precio 101 ⇒ bids 100 vs asks 102, 99 vs 103… El bucket `m` no
  participa. Foto actual del libro; un par que toca la zona fuera de cobertura de
  cualquiera de los dos snapshots se marca incompleto. **Supuesto**: precio de referencia =
  precio medio spot; ambos libros deben estar `Live`; se informa el desfase entre vistas.

Verificación (`crates/lu-metrics/tests/prop_metrics.rs`): **M1** TWA, tiempo observado y
Σ ejecutado de toda vela cerrada coinciden **exactamente** con una integración por fuerza
bruta, con cortes y reconstrucciones aleatorias; **M2** siete escenarios del detector
(dispara; rechazos por ejecución en el borde exacto de 20 %, distancia, percentil,
contaminación; sin cruce de X no evalúa; caída gradual acumula ejecuciones de la ventana);
**M3** los tres ejemplos del usuario (2000→2100 estructural R 1,05; 300→1500 táctica R 5;
2000→600 desarmándose R 0,3), estable y referencia vacía; **M4** CVD con el esquema
100/102, 99/103… y marca de incompleto fuera de cobertura.
Mutaciones de área, pausa, reanudación, percentil y ventana hacen fallar las pruebas.

## 5 quater. F4 — API en vivo e interfaz

* `GET /ws?markets=spot,perp&interval_ms=250`: `hello`, luego `book` (cada intervalo,
  100..5000 ms) y `footprint` (cuando cambia, ≤ 1 Hz). Máximo 16 sesiones; mensajes de
  entrada ≤ 64 KiB; un cliente que tarda > 2 s en recibir se desconecta (nunca frena al nodo).
* `GET /cvd`: CVD del libro spot + perp (también por WebSocket, mensaje `cvd`).
* `GET /footprint/<m>`: `MetricsView` (velas en curso, 12 cerradas por temporalidad, muros).
* `GET /` o `/ui`: interfaz autocontenida (sin CDN): estado, escalera top 10, flujo F2,
  footprint por temporalidad alrededor del precio medio, muros retirados y líneas A/B.
  Modo claro/oscuro, apta para móvil, reconexión con backoff.

## 5 quinquies. F5 — Simulador, caos y soak

* `--sim`: exchange sintético con reglas de secuencia de Binance (spot contiguo; perp con
  saltos y `pu`), lotes de 100 ms, límites, cancelaciones, barridos, icebergs, RPI en perp;
  responde snapshots. Entrega al **motor real** por líneas redundantes.
* Caos: `--chaos-drop` (pérdida por evento y línea), `--chaos-jitter-ms`,
  `--chaos-outage-s` (cortes de 5–30 s), `--chaos-snapshot-fail`.
* **P2 en vivo**: un verificador compara el top 10 publicado con la verdad del simulador en
  el mismo `last_update_id` (`lu_sim_checks_total{result="mismatch"}` debe ser 0).
* `deploy/soak.sh URL HORAS INTERVALO`: CSV + corte ante la primera violación de
  invariantes. `deploy/chaos-sim.sh MIN SEMILLA`: caos reproducible sin red.
  `deploy/alerts.yml`: reglas Prometheus.

Resultado (2026-09-25, release, 2 min, 2 % de pérdida por línea, cortes, 10–20 % de
snapshots fallidos): 758 verificaciones contra la verdad, **0 discrepancias**, 0 diffs y
0 trades sin contabilizar, 0 tardíos, ~81 000 trades alineados, recuperación tras cada
resync provocado. El soak de 72 h queda para la VM definitiva.

## 6. Hallazgos de la API de Binance (2025–2026) incorporados

* **Futuros USDⓈ-M, WS dividido** desde 2026-03: `wss://fstream.binance.com/public/...`
  (depth, bookTicker) y `/market/...` (aggTrade, markPrice, forceOrder). Las URLs
  sin ruta dejaron de servir streams `/market` el 2026-04-23.
* Conexión válida 24 h; ping del servidor cada 3 min, pong exigido en 10 min.
  Rotación programada escalonada (A: 23 h, B: 20 h) para que nunca caigan juntas.
* **Órdenes RPI**: no aparecen en el depth normal; `aggTrade` de futuros trae `nq`
  (cantidad solo contra órdenes normales) desde 2025-12-31. Se reporta `q − nq`
  aparte (`rpi_qty`). Existe `@rpiDepth@500ms` para F3.
* **Bloqueo regional (HTTP 451)** de `api.binance.com` y `fapi.binance.com` desde
  ciertas regiones (EE. UU. entre ellas). Spot tiene respaldo en
  `data-api.binance.vision` / `data-stream.binance.vision`; **futuros no**.

---

## 6 bis. OKX v5 (`lu-okx`) — verificado en vivo 2026-09-26

Protocolo (capturado, no de memoria): canal `books` = snapshot por WebSocket (400 niveles,
`prevSeqId = -1`) + updates cada 100 ms con `prevSeqId` = `seqId` anterior; `checksum` en 0
(OKX ya no lo calcula ⇒ integridad solo por cadena de secuencia). Canal `trades`: agregado por
orden taker (`side` = taker, `count` = fills). Perpetuo: `sz` en contratos × `ctVal` (1 SOL,
leído por REST y aplicado en punto fijo exacto; sin `ctVal` el perpetuo no arranca).

* **`OkxRule`**: puente `prevSeqId == seqId del snapshot`; cadena `prevSeqId == local`;
  latido (`prevSeqId == seqId`) ⇒ `Stale`; `seqId` que retrocede ⇒ `Invalid` ⇒ resync. P1, P2,
  P3 y el canario de vivacidad corren ahora también para OKX (20 000 casos).
* **Libro top-400 rodante**: los niveles que salen del top llegan con tamaño 0. La cobertura
  es **dinámica** (`DepthSnapshot::rolling`): más allá del peor nivel presente en cada
  instante, el estado es desconocido. Muros, percentiles y CVD la respetan.
* **Snapshot nuevo** (resync): re-suscripción al canal `books` en una línea (round-robin);
  el REST de OKX no trae `seqId`. Latido de aplicación `"ping"` cada 20 s.
* Líneas por el puerto 443 (`ws.okx.com`), alternativa `wsaws.okx.com:8443`.

Prueba en vivo (5 min, spot + perp, 2 líneas): 0 resyncs, 0 diffs/trades sin contabilizar,
0 tardíos, 0 errores; arbitraje A/B activo (perp: A primero 211, B primero 852).
**Verificación independiente**: una conexión aparte pidió 98 snapshots frescos; en los 57
cuyo `seqId` coincidió con una vista publicada por el nodo, el top 10 (precio y cantidad)
fue **idéntico en 57/57**.

Multi-venue: `--venues binance,okx` (rutas `binance.spot`, `okx.perp`, …). El CVD del libro
suma spot y perp de todos los venues en vivo (libro unificado).

## 7. Prueba en vivo (2026-09-25, binario release, servidor con 451 en `api` y `fapi`)

| Métrica | Spot SOLUSDT | Perp SOLUSDT |
|---|---|---|
| Estado | `Live`, época 1, 0 resyncs | `Syncing`: snapshot REST bloqueado (451), 12 reintentos con backoff |
| Contabilidad de diffs | 882 = 880 aplicados + 2 previos al snapshot; sin contabilizar **0** | 2078 = 1039 en buffer + 1039 duplicados A/B; sin contabilizar **0** |
| Cruzado / fuera de tick / parseo / descartes | 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 |
| Profundidad y spread | 4996 bid / 5005 ask; 0,83 bps | — |
| Arbitraje de líneas (depth) | A bloqueada (7 desconexiones), B cubrió el 100 % | ambas activas: A llegó primero 987/1039 (95 %) |
| Arbitraje de líneas (trades) | mismo socket que depth | A primero 263/276; 276 duplicados descartados |
| Latencia exchange → recepción | p50 62,6 ms, p99 65,9 ms | p50 62,1 ms, p99 64,5 ms |
| Campo `nq` (RPI) | no existe en spot | presente en 145/145 trades; 0 ejecución RPI en la ventana |

Recursos del proceso: RSS 27,5 MB, CPU 0,4 % promedio (1 vCPU), 5 hilos; binario 14,5 MB.
`/ready` respondió 503 mientras perp no estuvo `Live` (comportamiento correcto).
La latencia depende de la ubicación del servidor; en la VM definitiva debe medirse de nuevo.

---

## 7 bis. Prueba en vivo con F2/F3 (2026-09-26, spot SOLUSDT, entorno con 451 en `api`/`stream`)

| Métrica | Valor (5 min) |
|---|---|
| Estado | `Live`, época 1, 0 resyncs; 5009 / 4991 niveles; spread 0,82 bps |
| Conservación diffs / trades | 0 / 0 sin contabilizar |
| F2 | 376 trades alineados, **0 tardíos**, 0 ambiguos (δ = 0), 9845 flujos limpios, ≤ 4 lotes abiertos |
| Cierre de lotes | 659 por marca de agua de trades, 1866 por tiempo de depth (trades quietos) |
| F3 | 5 ventanas de muro evaluadas, 0 disparos |
| Líneas | `stream.binance.com` responde 451 ⇒ la línea A rota sola a `data-stream.binance.vision`; arbitraje A/B activo (B primero 505, A primero 24; duplicados descartados) |

Cambios surgidos de la prueba: la línea A usa el puerto 443 (los proxies suelen bloquear
9443) y cada línea rota a un host alternativo ante HTTP 451/403 o 3 fallos seguidos.

## 8. Límites conocidos

* La latencia medida incluye el desfase de reloj local: usar **chrony**.
* Deduplicación de trades A/B con ventana de 100 000 ids.
* Control de peso REST reactivo (429/418 con `Retry-After`) más intervalo mínimo de 1 s.
* Sin persistencia por diseño: un reinicio reconstruye desde snapshot.
* Métricas de volumen no visible con lotes de 100 ms: solo cotas inferiores (ver F2).

---

## 9. Hoja de ruta

| Fase | Contenido |
|---|---|
| ~~F2~~ | ✅ Alineador trades ↔ depth (§5 bis). Pendiente: medir en vivo `late`, `ambiguous` y calibrar δ. |
| ~~F3~~ | ✅ Velas footprint, TWA, persistencia, muros retirados, táctica vs estructural y CVD del libro (§5 ter). |
| ~~F4~~ | ✅ API WebSocket y UI (§5 quater). |
| ~~F5~~ | ✅ Simulador, caos, soak y alertas (§5 quinquies). Pendiente: correr 72 h en la VM con Binance real. |
| Venues 2–5 | OKX, Bybit, Coinbase, Kraken con la misma plantilla. SOLUSDC en modo sombra; SOLFDUSD excluido. |

### Detector de muros retirados (F3) — parámetros confirmados el 2026-09-25

Un nivel (bucket de 1 USDT, un lado) es **muro retirado** si cumple las cuatro condiciones:

| Parámetro | Valor | Condición |
|---|---|---|
| P | percentil 90 | su TWA en la vela 4 h contenedora ≥ percentil 90 de la banda |
| X | 50 % | su cantidad cae más de 50 % |
| N | 2 | el precio está a ≤ 2 niveles (buckets de 1 USDT) |
| Y | 20 % | las ejecuciones explican menos del 20 % de la caída |

Supuestos de implementación (declarados; ajustables sin tocar los parámetros):

1. Percentil sobre los TWA 4 h de los niveles del **mismo lado** dentro de la banda
   cubierta por todos los venues.
2. Caída = cantidad actual < (1 − X) × TWA 4 h del mismo nivel.
3. Ventana de la caída: desde la última vez que el nivel estuvo ≥ su TWA 4 h hasta
   que cruza el umbral. Distancia N medida contra el precio medio en ese instante.
4. Ejecuciones = Σ ejecutado en ese nivel y lado dentro de la ventana (trades
   alineados por F2; en futuros con `nq`, sin RPI) ÷ caída visible. La liquidez oculta
   solo puede **aumentar** ese cociente: el criterio es conservador (puede omitir
   retiros reales, no inventarlos).

---

## 10. Cómo extender

* **Nuevo venue** = crate `lu-<venue>` con `endpoints`, `wire` (a `MarketEvent`),
  `rest`, `line`, más su `SeqRule` en `lu-book`. El motor no cambia.
* **Nuevas métricas** = implementar `BookObserver` (despacho estático, costo cero
  si no se usa): `on_level(prev → new)`, `on_diff_applied`, `on_rebuild`, `on_invalidate`.
