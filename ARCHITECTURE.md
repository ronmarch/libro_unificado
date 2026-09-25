# Libro Unificado — Arquitectura (F0 + F1)

Sistema **independiente de FORJA**. Mantiene en RAM, en tiempo real, libros L2
exactos por venue y mercado, como base del libro unificado multi-exchange
(Binance, OKX, Bybit, Coinbase, Kraken; spot y perpetuos). Sin almacenamiento:
es un sistema de monitoreo, no de validación histórica.

Estado: **F0 (fundaciones) y F1 (libro Binance spot + USDⓈ-M sincronizado) completas.**

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
├── crates/lu-telemetry  histogramas HDR de latencia + escritor Prometheus
├── crates/lu-binance    conector: endpoints, parseo zero-copy, REST, líneas WS, TLS
└── bins/lu-node         nodo: motores por mercado, snapshots, API HTTP
```

Regla de dependencias: `lu-core` ← `lu-book` ← `lu-node`; `lu-core` ← `lu-binance` ← `lu-node`.
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
| **F2** | Alineador trades ↔ depth con marca de agua (~250 ms). Por nivel consumido: `no_visible_min = max(0, e − q0)`, `cancelado_min = max(0, q0 − e − q1)`. |
| **F3** | Métricas vía `BookObserver`: velas footprint 15 m / 1 h / 4 h por nivel de 1 USDT (CVD del libro, TWA perezoso `acc += qty_prev·Δt`, bid/ask ejecutado, n_fills, no visible), persistencia foto ÷ TWA, táctica vs estructural, muros retirados, RPI aparte. |
| **F4** | API WebSocket y UI. |
| **F5** | Soak test de 72 h + caos (cortes de línea, latencia inyectada, 429). |
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
