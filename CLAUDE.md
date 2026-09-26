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
PROPTEST_CASES=20000 cargo test -p lu-flow --test prop_flow
PROPTEST_CASES=20000 cargo test -p lu-metrics --test prop_metrics
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
8. **Conservación de trades** en `lu-flow`: `Aligner::unaccounted() == 0` siempre.
9. Las cotas de F2 son **inferiores**: toda fórmula nueva requiere prueba contra el simulador.
7. Un cambio de lógica en `lu-book` requiere una prueba que falle antes del cambio.

## Estado y próximos pasos

F0–F5 hechas para Binance (ver `ARCHITECTURE.md` §5 bis – §5 quinquies). Pendiente:

1. **Venues restantes** (Bybit, Kraken): seguir la plantilla de `crates/lu-okx`
   (`Protocol` de `lu-net` + `SeqRule` + `MarketEvent::Snapshot`). Capturar mensajes reales
   antes de escribir el parser; verificar contra snapshots independientes (ver §6 bis).
   Kraken valida con CRC32 (activo): falta soporte de checksum en `SyncBook`.
2. Supuestos de F3 confirmados por el usuario (2026-09-26): R ≈ 1 = 0,8–1,25; alto = ≥ P90;
   referencia del CVD = medio spot; cantidades = foto actual.
3. Soak de 72 h con Binance real en la VM (`deploy/soak.sh`) y calibrar δ de F2.

Antes de tocar `lu-flow` o `lu-metrics`: `deploy/chaos-sim.sh 5` debe terminar OK.

## Reglas de trabajo

* Fidelidad exacta a lo pedido. Declarar supuestos; no rellenar datos faltantes.
* Documentación y comentarios en español; respuestas concisas.
* Detector de muros retirados (F3): parámetros **confirmados** por el usuario
  (P = percentil 90, X = 50 %, N = 2, Y = 20 %). Definición y supuestos declarados en
  `ARCHITECTURE.md` §9. Implementarlos como configuración con esos valores por defecto;
  no cambiar los valores sin confirmación explícita.
