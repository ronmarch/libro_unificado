# CLAUDE.md — libro-unificado

Sistema independiente de FORJA: libros L2 en RAM y en tiempo real (hoy Binance
spot + USDⓈ-M, SOLUSDT), base del libro unificado de 5 venues. Leer
`ARCHITECTURE.md` antes de cambiar cualquier cosa en `crates/lu-book`.

## Comandos (todo cambio debe dejarlos en verde)

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
PROPTEST_CASES=20000 cargo test -p lu-book --test prop_sync
```

## Invariantes que no se rompen

1. **Nunca `Live` con un libro incorrecto** (propiedad P2). Ante duda: resync.
2. **Conservación**: `SyncBook::unaccounted() == 0` siempre (P3). Todo contador nuevo
   de descarte debe sumarse a la ecuación de `unaccounted()`.
3. **Sin `f64` en cálculos**: precios y cantidades son `Px`/`Qty` (i64, escala 1e-8).
   `f64` solo para presentación.
4. **`lu-book` es determinista**: sin E/S, sin relojes, sin hilos. El tiempo se inyecta.
5. **Nunca bloquear el runtime**: cola llena ⇒ descartar y contar (`try_send`).
6. `#![forbid(unsafe_code)]` en todo el código propio.
7. Un cambio de lógica en `lu-book` requiere una prueba que falle antes del cambio.

## Próximo paso: F2 — alineador trades ↔ depth

* Entradas: `AggTrade` (px, qty, `nq`, agresor, `T`, `agg_id`) y cambios de nivel vía
  `BookObserver::on_level(side, px, prev, new, exch_ts)`.
* Marca de agua ~250 ms sobre tiempo del exchange (en futuros, trades y depth llegan por sockets
  distintos: no asumir orden).
* Por nivel y lote de 100 ms, con `e` = ejecutado, `q0` = cantidad antes, `q1` = después:
  `no_visible_min = max(0, e − q0)`, `cancelado_min = max(0, q0 − e − q1)`.
  Son **cotas inferiores**, nunca estimaciones puntuales.
* Futuros: comparar depth contra `nq` (sin RPI); reportar `q − nq` aparte.
* Verificación: simulador de matching sintético (icebergs, cancelaciones, recargas)
  con propiedad "las cotas nunca exceden la verdad".
* Al cambiar de época (`on_invalidate`/`on_rebuild`) las métricas en curso se marcan
  contaminadas; no se mezclan épocas.

## Reglas de trabajo

* Fidelidad exacta a lo pedido. Declarar supuestos; no rellenar datos faltantes.
* Documentación y comentarios en español; respuestas concisas.
* Detector de muros retirados (F3): parámetros **confirmados** por el usuario
  (P = percentil 90, X = 50 %, N = 2, Y = 20 %). Definición y supuestos declarados en
  `ARCHITECTURE.md` §9. Implementarlos como configuración con esos valores por defecto;
  no cambiar los valores sin confirmación explícita.
