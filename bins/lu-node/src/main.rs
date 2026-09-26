//! lu-node — nodo del libro unificado (Binance y OKX; spot y perpetuos).
//!
//! Topología de hilos:
//! * `lu-io-*`   runtime Tokio: líneas WebSocket, REST, API HTTP.
//! * `lu-eng-*`  un hilo de SO por mercado, dueño exclusivo de su libro
//!   (opcionalmente fijado a un núcleo con `--pin`).
//!
//! Flujo: línea A/B → cola acotada → motor (SyncBook) → vista inmutable → HTTP.
#![forbid(unsafe_code)]

mod cvd;
mod engine;
mod flow;
mod http;
mod sim;
mod snapshot;
mod view;
mod ws;

use engine::{ChannelSink, Engine, EngineConfig, EngineMsg, IngestCounters, StreamKind};
use flow::FlowAgg;
use lu_binance::{BinanceProtocol, Endpoints, RestClient};
use lu_book::{BinanceFuturesRule, BinanceSpotRule, OkxRule, SeqRule, SyncBook, SyncConfig};
use lu_core::{init_clock, MarketId, MarketKind, Qty, Venue};
use lu_flow::{Aligner, AlignerConfig};
use lu_metrics::{Metrics, MetricsConfig};
use lu_net::{run_line, FrameSink, LineCmd, LineSpec};
use lu_coinbase::CoinbaseProtocol;
use lu_okx::{OkxEndpoints, OkxProtocol};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use view::Views;

const USAGE: &str = "\
uso: lu-node [opciones]
  --symbol SOLUSDT        símbolo (default SOLUSDT)
  --venues binance        exchanges: binance,okx,coinbase (default binance)
  --coinbase-product SOL-USD  producto Coinbase (solo spot; USD se suma como USDT)
  --markets spot,perp     mercados a sincronizar (default spot,perp)
  --lines 2               líneas redundantes por stream, 1..4 (default 2)
  --listen 127.0.0.1:9100 API de observabilidad
  --io-threads 1          hilos del runtime de E/S (default 1)
  --pin                   fija cada motor a un núcleo propio (núcleo 0 queda para SO/E/S)
  --ca-file ruta.pem      CA adicional (proxy corporativo); por defecto usa SSL_CERT_FILE si existe
  --log info              nivel: error|warn|info|debug|trace

simulación y caos (F5; sin red):
  --sim                   exchange sintético con verdad conocida en lugar de Binance
  --sim-seed 24301        semilla del generador
  --chaos-drop 0.02       probabilidad de perder cada evento en cada línea
  --chaos-jitter-ms 20    latencia adicional máxima por línea
  --chaos-outage-s 120    segundos medios entre cortes de cada línea (0 = sin cortes)
  --chaos-snapshot-fail 0.2  probabilidad de que un snapshot no llegue";

struct Args {
    symbol: String,
    venues: Vec<Venue>,
    coinbase_product: String,
    markets: Vec<MarketKind>,
    lines: usize,
    listen: SocketAddr,
    io_threads: usize,
    pin: bool,
    log: tracing::Level,
    ca_file: Option<std::path::PathBuf>,
    sim: Option<sim::SimConfig>,
}

fn parse_args() -> Result<Args, String> {
    let mut a = Args {
        symbol: "SOLUSDT".into(),
        venues: vec![Venue::Binance],
        coinbase_product: "SOL-USD".into(),
        markets: vec![MarketKind::Spot, MarketKind::Perp],
        lines: 2,
        listen: "127.0.0.1:9100".parse().map_err(|e| format!("{e}"))?,
        io_threads: 1,
        pin: false,
        log: tracing::Level::INFO,
        ca_file: std::env::var_os("SSL_CERT_FILE").map(Into::into),
        sim: None,
    };
    let mut sim_cfg = sim::SimConfig::default();
    let mut sim_on = false;
    let mut it = std::env::args().skip(1);
    while let Some(k) = it.next() {
        let mut val = || it.next().ok_or_else(|| format!("falta valor para {k}"));
        match k.as_str() {
            "--symbol" => a.symbol = val()?.to_uppercase(),
            "--coinbase-product" => a.coinbase_product = val()?.to_uppercase(),
            "--venues" => {
                a.venues = val()?
                    .split(',')
                    .map(|v| match v.trim() {
                        "binance" => Ok(Venue::Binance),
                        "okx" => Ok(Venue::Okx),
                        "coinbase" => Ok(Venue::Coinbase),
                        o => Err(format!("venue no soportado aún: {o} (binance, okx, coinbase)")),
                    })
                    .collect::<Result<_, _>>()?
            }
            "--markets" => {
                a.markets = val()?
                    .split(',')
                    .map(|m| match m.trim() {
                        "spot" => Ok(MarketKind::Spot),
                        "perp" => Ok(MarketKind::Perp),
                        o => Err(format!("mercado desconocido: {o}")),
                    })
                    .collect::<Result<_, _>>()?
            }
            "--lines" => {
                a.lines = val()?.parse().map_err(|_| "--lines inválido".to_string())?;
                if !(1..=lu_book::MAX_LINES).contains(&a.lines) {
                    return Err(format!("--lines debe estar en 1..={}", lu_book::MAX_LINES));
                }
            }
            "--listen" => {
                a.listen = val()?
                    .parse()
                    .map_err(|_| "--listen inválido".to_string())?
            }
            "--io-threads" => {
                a.io_threads = val()?
                    .parse()
                    .map_err(|_| "--io-threads inválido".to_string())?
            }
            "--pin" => a.pin = true,
            "--ca-file" => a.ca_file = Some(val()?.into()),
            "--log" => a.log = val()?.parse().map_err(|_| "--log inválido".to_string())?,
            "--sim" => sim_on = true,
            "--sim-seed" => sim_cfg.seed = val()?.parse().map_err(|_| "--sim-seed inválido")?,
            "--chaos-drop" => sim_cfg.drop = prob(&val()?)?,
            "--chaos-jitter-ms" => {
                sim_cfg.jitter_ms = val()?.parse().map_err(|_| "--chaos-jitter-ms inválido")?
            }
            "--chaos-outage-s" => {
                sim_cfg.outage_every_s = val()?.parse().map_err(|_| "--chaos-outage-s inválido")?
            }
            "--chaos-snapshot-fail" => sim_cfg.snapshot_fail = prob(&val()?)?,
            "-h" | "--help" => {
                println!("{USAGE}");
                std::process::exit(0);
            }
            o => return Err(format!("argumento desconocido: {o}")),
        }
    }
    if a.markets.is_empty() {
        return Err("sin mercados".into());
    }
    if a.venues.is_empty() {
        return Err("sin venues".into());
    }
    if sim_on {
        // El simulador reproduce las reglas de Binance.
        a.venues = vec![Venue::Binance];
    }
    a.sim = sim_on.then_some(sim_cfg);
    Ok(a)
}

fn prob(s: &str) -> Result<f64, String> {
    match s.parse::<f64>() {
        Ok(p) if (0.0..=1.0).contains(&p) => Ok(p),
        _ => Err(format!("probabilidad inválida: {s} (0..1)")),
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_engine<R: SeqRule>(
    market: MarketId,
    sync: SyncBook<R, engine::Obs>,
    rx: crossbeam_channel::Receiver<EngineMsg>,
    req: mpsc::UnboundedSender<()>,
    views: Views,
    counters: Arc<IngestCounters>,
    rest: Arc<RestClient>,
    core: Option<core_affinity::CoreId>,
) -> std::thread::JoinHandle<()> {
    let name = format!("lu-eng-{}", market.kind.as_str());
    std::thread::Builder::new()
        .name(name)
        .spawn(move || {
            if let Some(c) = core {
                if core_affinity::set_for_current(c) {
                    tracing::info!(market = %market, core = c.id, "motor fijado a núcleo");
                } else {
                    tracing::warn!(market = %market, "no se pudo fijar núcleo");
                }
            }
            Engine::new(
                market,
                sync,
                rx,
                req,
                views.book,
                views.metrics,
                counters,
                rest,
                EngineConfig::default(),
            )
            .run();
        })
        .expect("no se pudo crear el hilo del motor")
}

enum Rule {
    BinanceSpot,
    BinanceFutures,
    Okx,
}

/// Binance: líneas WebSocket (la URL suscribe), snapshots REST y `exchangeInfo`.
#[allow(clippy::too_many_arguments)]
fn start_binance(
    rt: &tokio::runtime::Runtime,
    args: &Args,
    kind: MarketKind,
    tls: &Arc<rustls::ClientConfig>,
    rest: &Arc<RestClient>,
    tx: crossbeam_channel::Sender<EngineMsg>,
    counters: Arc<IngestCounters>,
    req_rx: mpsc::UnboundedReceiver<()>,
    sd_rx: &watch::Receiver<bool>,
) {
    let ep = match kind {
        MarketKind::Spot => Endpoints::spot(&args.symbol, args.lines),
        MarketKind::Perp => Endpoints::usdm_perp(&args.symbol, args.lines),
    };
    let label = ep.market.to_string();
    for (lines, kind_s) in [
        (&ep.depth_lines, StreamKind::Depth),
        (&ep.trade_lines, StreamKind::Trade),
    ] {
        for (li, url) in lines.iter().enumerate() {
            let sink: Arc<dyn FrameSink> = Arc::new(ChannelSink::new(
                tx.clone(),
                counters.clone(),
                kind_s,
                label.clone(),
            ));
            let tag = match kind_s {
                StreamKind::Depth => format!("{label}/depth"),
                StreamKind::Trade => format!("{label}/trade"),
            };
            let alt = match kind_s {
                StreamKind::Depth => ep.depth_fallbacks.get(li).cloned().unwrap_or_default(),
                StreamKind::Trade => Vec::new(),
            };
            rt.spawn(run_line(
                LineSpec::new(tag, li as u8, url.clone(), tls.clone()).with_fallbacks(alt),
                Arc::new(BinanceProtocol),
                sink,
                None,
                sd_rx.clone(),
            ));
        }
    }
    rt.spawn(snapshot::run(
        ep.clone(),
        rest.clone(),
        req_rx,
        tx.clone(),
        sd_rx.clone(),
    ));
    // Reglas del instrumento: útiles (control de tick) pero no críticas.
    let (rest2, ep2) = (rest.clone(), ep.clone());
    rt.spawn(async move {
        let (hosts, path, market) = (ep2.rest_hosts.clone(), ep2.exchange_info_path.clone(), ep2.market.clone());
        match tokio::task::spawn_blocking(move || rest2.instrument(&hosts, &path, &market)).await {
            Ok(Ok(spec)) => snapshot::deliver(&tx, EngineMsg::Spec(spec)).await,
            Ok(Err(e)) => tracing::warn!(market = %ep2.market, error = %e, "exchangeInfo no disponible (se continúa sin control de tick)"),
            Err(e) => tracing::warn!(error = %e, "tarea exchangeInfo abortada"),
        }
    });
}

/// OKX: reglas del instrumento (ctVal) por REST, líneas con suscripción y latido,
/// y snapshots por re-suscripción (round-robin entre líneas).
#[allow(clippy::too_many_arguments)]
fn start_okx(
    rt: &tokio::runtime::Runtime,
    args: &Args,
    kind: MarketKind,
    tls: &Arc<rustls::ClientConfig>,
    tx: crossbeam_channel::Sender<EngineMsg>,
    counters: Arc<IngestCounters>,
    mut req_rx: mpsc::UnboundedReceiver<()>,
    sd_rx: &watch::Receiver<bool>,
) {
    let Some(ep) = OkxEndpoints::new(&args.symbol, kind, args.lines) else {
        tracing::error!(symbol = %args.symbol, "símbolo no convertible a instId de OKX");
        return;
    };
    let label = ep.market.to_string();
    // ctVal es imprescindible en perpetuos: sin él las cantidades serían contratos, no SOL.
    let mut spec = None;
    for intento in 1..=3 {
        match lu_okx::instrument(
            tls.clone(),
            &ep.rest_host,
            ep.inst_type,
            &ep.inst_id,
            &ep.market,
        ) {
            Ok(s) => {
                spec = Some(s);
                break;
            }
            Err(e) => {
                tracing::warn!(market = %label, intento, error = %e, "instrumento OKX no disponible");
                std::thread::sleep(Duration::from_secs(2));
            }
        }
    }
    let contract = match (spec.as_ref(), kind) {
        (Some((_, c)), _) => *c,
        (None, MarketKind::Spot) => Qty::from_units(1),
        (None, MarketKind::Perp) => {
            tracing::error!(market = %label, "sin ctVal no se puede convertir contratos a SOL: mercado desactivado");
            return;
        }
    };
    if let Some((s, _)) = spec {
        let tx2 = tx.clone();
        rt.spawn(async move { snapshot::deliver(&tx2, EngineMsg::Spec(s)).await });
    }
    let proto = Arc::new(OkxProtocol::new(ep.inst_id.clone(), contract));
    let mut cmd_txs = Vec::new();
    for (li, url) in ep.lines.iter().enumerate() {
        let (ctx, crx) = mpsc::unbounded_channel();
        cmd_txs.push(ctx);
        let sink: Arc<dyn FrameSink> = Arc::new(ChannelSink::new(
            tx.clone(),
            counters.clone(),
            StreamKind::Depth,
            label.clone(),
        ));
        rt.spawn(run_line(
            LineSpec::new(
                format!("{label}/books+trades"),
                li as u8,
                url.clone(),
                tls.clone(),
            )
            .with_fallbacks(ep.fallbacks.get(li).cloned().unwrap_or_default()),
            proto.clone(),
            sink,
            Some(crx),
            sd_rx.clone(),
        ));
    }
    // Snapshots: cada suscripción ya trae uno; los pedidos del motor re-suscriben una línea.
    let mut sd = sd_rx.clone();
    rt.spawn(async move {
        let start = tokio::time::Instant::now();
        let mut last: Option<tokio::time::Instant> = None;
        let mut next = 0usize;
        loop {
            tokio::select! {
                _ = sd.changed() => return,
                r = req_rx.recv() => if r.is_none() { return },
            }
            while req_rx.try_recv().is_ok() {}
            // Las suscripciones iniciales ya entregan snapshot; no duplicar al arrancar.
            if start.elapsed() < Duration::from_secs(3) {
                continue;
            }
            if let Some(t) = last {
                let el = t.elapsed();
                if el < Duration::from_secs(1) {
                    tokio::time::sleep(Duration::from_secs(1) - el).await;
                }
            }
            last = Some(tokio::time::Instant::now());
            if let Some(c) = cmd_txs.get(next % cmd_txs.len().max(1)) {
                let _ = c.send(LineCmd::Resnapshot);
            }
            next += 1;
        }
    });
}

/// Coinbase Advanced Trade: la línea 0 alimenta el libro (secuencia por conexión);
/// las demás solo trades. Snapshots por re-suscripción de la línea 0.
#[allow(clippy::too_many_arguments)]
fn start_coinbase(
    rt: &tokio::runtime::Runtime,
    args: &Args,
    market: MarketId,
    tls: &Arc<rustls::ClientConfig>,
    tx: crossbeam_channel::Sender<EngineMsg>,
    counters: Arc<IngestCounters>,
    mut req_rx: mpsc::UnboundedReceiver<()>,
    sd_rx: &watch::Receiver<bool>,
) {
    let label = market.to_string();
    let mut depth_cmd = None;
    for li in 0..args.lines.max(1) {
        let (ctx, crx) = mpsc::unbounded_channel();
        if li == 0 {
            depth_cmd = Some(ctx);
        }
        let sink: Arc<dyn FrameSink> = Arc::new(ChannelSink::new(
            tx.clone(),
            counters.clone(),
            StreamKind::Depth,
            label.clone(),
        ));
        let tag = if li == 0 { "level2+trades" } else { "trades" };
        rt.spawn(run_line(
            LineSpec::new(format!("{label}/{tag}"), li as u8, lu_coinbase::proto::WS_URL, tls.clone()),
            Arc::new(CoinbaseProtocol::new(args.coinbase_product.clone(), li == 0)),
            sink,
            Some(crx),
            sd_rx.clone(),
        ));
    }
    let mut sd = sd_rx.clone();
    rt.spawn(async move {
        let start = tokio::time::Instant::now();
        loop {
            tokio::select! {
                _ = sd.changed() => return,
                r = req_rx.recv() => if r.is_none() { return },
            }
            while req_rx.try_recv().is_ok() {}
            if start.elapsed() < Duration::from_secs(3) {
                continue; // la suscripción inicial ya trae snapshot
            }
            if let Some(c) = &depth_cmd {
                let _ = c.send(LineCmd::Resnapshot);
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
}

fn aligner() -> engine::Obs {
    Aligner::new(
        AlignerConfig::default(),
        (FlowAgg::default(), Metrics::new(MetricsConfig::default())),
    )
}

#[cfg(unix)]
async fn wait_for_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    match signal(SignalKind::terminate()) {
        Ok(mut term) => {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = term.recv() => {}
            }
        }
        Err(_) => {
            let _ = tokio::signal::ctrl_c().await;
        }
    }
}

#[cfg(not(unix))]
async fn wait_for_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

fn main() {
    init_clock();
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("error: {e}\n\n{USAGE}");
            std::process::exit(2);
        }
    };
    tracing_subscriber::fmt()
        .with_max_level(args.log)
        .with_target(false)
        .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stdout()))
        .init();
    let _ = rustls::crypto::ring::default_provider().install_default();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.io_threads.max(1))
        .thread_name("lu-io")
        .enable_all()
        .build()
        .expect("no se pudo crear el runtime");

    let (tls, extra_ca) = match lu_binance::client_config(args.ca_file.as_deref()) {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(error = %e, "configuración TLS inválida");
            std::process::exit(2);
        }
    };
    if extra_ca > 0 {
        tracing::info!(extra_ca, "CA adicionales cargadas");
    }
    let (sd_tx, sd_rx) = watch::channel(false);
    let rest = Arc::new(RestClient::new(tls.clone()));
    let cores = if args.pin {
        core_affinity::get_core_ids().unwrap_or_default()
    } else {
        Vec::new()
    };
    let mut registry: Vec<(String, Views)> = Vec::new();
    let mut engines = Vec::new();

    let plan: Vec<(Venue, MarketKind)> = args
        .venues
        .iter()
        .flat_map(|v| args.markets.iter().map(move |k| (*v, *k)))
        .filter(|(v, k)| {
            let ok = !(*v == Venue::Coinbase && *k == MarketKind::Perp);
            if !ok {
                tracing::warn!("Coinbase no tiene perpetuos en su exchange spot: se omite coinbase.perp");
            }
            ok
        })
        .collect();
    let multi_venue = args.venues.len() > 1;
    for (i, (venue, kind)) in plan.into_iter().enumerate() {
        let symbol = if venue == Venue::Coinbase {
            args.coinbase_product.replace('-', "")
        } else {
            args.symbol.clone()
        };
        let market = MarketId::new(venue, kind, symbol);
        let label = market.to_string();
        let (tx, rx) = crossbeam_channel::bounded::<EngineMsg>(65_536);
        let (req_tx, req_rx) = mpsc::unbounded_channel();
        let mut views = Views::new(label.clone());
        let sim_stats = args
            .sim
            .as_ref()
            .map(|_| Arc::new(sim::SimStats::default()));
        views.sim = sim_stats.clone();
        let counters = Arc::new(IngestCounters::default());
        // Clave de ruta: `spot`/`perp` con un venue (compatibilidad); `okx.perp` con varios.
        let key = if multi_venue {
            format!("{}.{}", venue.as_str(), kind.as_str())
        } else {
            kind.as_str().to_string()
        };
        registry.push((key, views.clone()));

        let core = cores.get(1 + i).copied();
        let spawn = |sync_rule: Rule| match sync_rule {
            Rule::BinanceSpot => spawn_engine(
                market.clone(),
                SyncBook::new(BinanceSpotRule, SyncConfig::default(), aligner()),
                rx,
                req_tx,
                views.clone(),
                counters.clone(),
                rest.clone(),
                core,
            ),
            Rule::BinanceFutures => spawn_engine(
                market.clone(),
                SyncBook::new(BinanceFuturesRule, SyncConfig::default(), aligner()),
                rx,
                req_tx,
                views.clone(),
                counters.clone(),
                rest.clone(),
                core,
            ),
            Rule::Okx => spawn_engine(
                market.clone(),
                SyncBook::new(OkxRule, SyncConfig::default(), aligner()),
                rx,
                req_tx,
                views.clone(),
                counters.clone(),
                rest.clone(),
                core,
            ),
        };
        let handle = spawn(match (venue, kind) {
            // Coinbase usa un contador contiguo con `prev`: misma regla que OKX.
            (Venue::Okx | Venue::Coinbase, _) => Rule::Okx,
            (_, MarketKind::Spot) => Rule::BinanceSpot,
            (_, MarketKind::Perp) => Rule::BinanceFutures,
        });
        engines.push((tx.clone(), handle));

        if let (Some(cfg), Some(stats)) = (args.sim.clone(), sim_stats) {
            rt.spawn(sim::run(
                market.clone(),
                args.lines,
                cfg,
                tx.clone(),
                counters.clone(),
                req_rx,
                views.clone(),
                stats,
                sd_rx.clone(),
            ));
            continue;
        }

        match venue {
            Venue::Coinbase => start_coinbase(
                &rt,
                &args,
                market.clone(),
                &tls,
                tx.clone(),
                counters.clone(),
                req_rx,
                &sd_rx,
            ),
            Venue::Okx => start_okx(
                &rt,
                &args,
                kind,
                &tls,
                tx.clone(),
                counters.clone(),
                req_rx,
                &sd_rx,
            ),
            _ => start_binance(
                &rt,
                &args,
                kind,
                &tls,
                &rest,
                tx.clone(),
                counters.clone(),
                req_rx,
                &sd_rx,
            ),
        }
    }

    let registry: http::Registry = Arc::new(registry);
    rt.block_on(async {
        let listener = match tokio::net::TcpListener::bind(args.listen).await {
            Ok(l) => l,
            Err(e) => {
                tracing::error!(listen = %args.listen, error = %e, "no se pudo abrir la API");
                std::process::exit(1);
            }
        };
        let keys: Vec<&str> = registry.iter().map(|(k, _)| k.as_str()).collect();
        tracing::info!(listen = %args.listen, mercados = ?keys, "API: / /ws /health /ready /metrics /cvd /book/<m> /footprint/<m>");
        let server = tokio::spawn(http::serve(listener, registry.clone(), sd_rx.clone()));
        wait_for_signal().await;
        tracing::info!("señal recibida: apagado ordenado");
        let _ = sd_tx.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(3), server).await;
    });
    for (tx, h) in engines {
        let _ = tx.send(EngineMsg::Shutdown);
        let _ = h.join();
    }
    rt.shutdown_timeout(Duration::from_secs(2));
}
