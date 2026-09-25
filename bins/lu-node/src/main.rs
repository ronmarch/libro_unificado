//! lu-node — nodo del libro unificado (F0+F1: Binance spot + USDⓈ-M).
//!
//! Topología de hilos:
//! * `lu-io-*`   runtime Tokio: líneas WebSocket, REST, API HTTP.
//! * `lu-eng-*`  un hilo de SO por mercado, dueño exclusivo de su libro
//!   (opcionalmente fijado a un núcleo con `--pin`).
//!
//! Flujo: línea A/B → cola acotada → motor (SyncBook) → vista inmutable → HTTP.
#![forbid(unsafe_code)]

mod engine;
mod http;
mod snapshot;
mod view;

use arc_swap::ArcSwap;
use engine::{ChannelSink, Engine, EngineConfig, EngineMsg, IngestCounters, StreamKind};
use lu_binance::{run_line, Endpoints, FrameSink, LineSpec, RestClient};
use lu_book::{BinanceFuturesRule, BinanceSpotRule, SeqRule, SyncBook, SyncConfig};
use lu_core::{init_clock, MarketId, MarketKind};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use view::BookView;

const USAGE: &str = "\
uso: lu-node [opciones]
  --symbol SOLUSDT        símbolo (default SOLUSDT)
  --markets spot,perp     mercados a sincronizar (default spot,perp)
  --lines 2               líneas redundantes por stream, 1..4 (default 2)
  --listen 127.0.0.1:9100 API de observabilidad
  --io-threads 1          hilos del runtime de E/S (default 1)
  --pin                   fija cada motor a un núcleo propio (núcleo 0 queda para SO/E/S)
  --ca-file ruta.pem      CA adicional (proxy corporativo); por defecto usa SSL_CERT_FILE si existe
  --log info              nivel: error|warn|info|debug|trace";

struct Args {
    symbol: String,
    markets: Vec<MarketKind>,
    lines: usize,
    listen: SocketAddr,
    io_threads: usize,
    pin: bool,
    log: tracing::Level,
    ca_file: Option<std::path::PathBuf>,
}

fn parse_args() -> Result<Args, String> {
    let mut a = Args {
        symbol: "SOLUSDT".into(),
        markets: vec![MarketKind::Spot, MarketKind::Perp],
        lines: 2,
        listen: "127.0.0.1:9100".parse().map_err(|e| format!("{e}"))?,
        io_threads: 1,
        pin: false,
        log: tracing::Level::INFO,
        ca_file: std::env::var_os("SSL_CERT_FILE").map(Into::into),
    };
    let mut it = std::env::args().skip(1);
    while let Some(k) = it.next() {
        let mut val = || it.next().ok_or_else(|| format!("falta valor para {k}"));
        match k.as_str() {
            "--symbol" => a.symbol = val()?.to_uppercase(),
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
    Ok(a)
}

#[allow(clippy::too_many_arguments)]
fn spawn_engine<R: SeqRule>(
    market: MarketId,
    sync: SyncBook<R>,
    rx: crossbeam_channel::Receiver<EngineMsg>,
    req: mpsc::UnboundedSender<()>,
    view: Arc<ArcSwap<BookView>>,
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
                view,
                counters,
                rest,
                EngineConfig::default(),
            )
            .run();
        })
        .expect("no se pudo crear el hilo del motor")
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
    let mut registry: Vec<(String, Arc<ArcSwap<BookView>>)> = Vec::new();
    let mut engines = Vec::new();

    for (i, kind) in args.markets.iter().copied().enumerate() {
        let ep = match kind {
            MarketKind::Spot => Endpoints::spot(&args.symbol, args.lines),
            MarketKind::Perp => Endpoints::usdm_perp(&args.symbol, args.lines),
        };
        let label = ep.market.to_string();
        let (tx, rx) = crossbeam_channel::bounded::<EngineMsg>(65_536);
        let (req_tx, req_rx) = mpsc::unbounded_channel();
        let view = Arc::new(ArcSwap::from_pointee(BookView::empty(label.clone())));
        let counters = Arc::new(IngestCounters::default());
        registry.push((kind.as_str().to_string(), view.clone()));

        let core = cores.get(1 + i).copied();
        let handle = match kind {
            MarketKind::Spot => spawn_engine(
                ep.market.clone(),
                SyncBook::new(BinanceSpotRule, SyncConfig::default(), ()),
                rx,
                req_tx,
                view,
                counters.clone(),
                rest.clone(),
                core,
            ),
            MarketKind::Perp => spawn_engine(
                ep.market.clone(),
                SyncBook::new(BinanceFuturesRule, SyncConfig::default(), ()),
                rx,
                req_tx,
                view,
                counters.clone(),
                rest.clone(),
                core,
            ),
        };
        engines.push((tx.clone(), handle));

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
                rt.spawn(run_line(
                    LineSpec::new(tag, li as u8, url.clone(), tls.clone()),
                    sink,
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
        let (rest2, ep2, tx2) = (rest.clone(), ep.clone(), tx.clone());
        rt.spawn(async move {
            let (hosts, path, market) = (ep2.rest_hosts.clone(), ep2.exchange_info_path.clone(), ep2.market.clone());
            match tokio::task::spawn_blocking(move || rest2.instrument(&hosts, &path, &market)).await {
                Ok(Ok(spec)) => snapshot::deliver(&tx2, EngineMsg::Spec(spec)).await,
                Ok(Err(e)) => tracing::warn!(market = %ep2.market, error = %e, "exchangeInfo no disponible (se continúa sin control de tick)"),
                Err(e) => tracing::warn!(error = %e, "tarea exchangeInfo abortada"),
            }
        });
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
        tracing::info!(listen = %args.listen, "API: /health /ready /metrics /book/spot /book/perp");
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
