//! Mensajes reales de Kraken v2 (SOL/USD, 2026-09-26): snapshot recortado a 40 niveles
//! por lado + 60 updates con sus checksums originales.

use lu_core::{MarketEvent, Px, Qty, RxStamp};
use lu_kraken::KrakenProtocol;
use lu_net::Protocol;

const FIXTURE: &str = include_str!("kraken_sol_usd.jsonl");

fn run(p: &KrakenProtocol, line: &str) -> Vec<MarketEvent> {
    let mut out = Vec::new();
    p.parse(line, RxStamp::default(), &mut out).unwrap();
    out
}

#[test]
fn todos_los_checksums_reales_coinciden() {
    let p = KrakenProtocol::new("SOL/USD", true, 1000, 2, 8);
    p.on_connect();
    let mut n = 0;
    for line in FIXTURE.lines().filter(|l| !l.is_empty()) {
        n += run(&p, line).len();
    }
    let (ok, bad) = p.checksum_stats();
    assert_eq!(bad, 0, "checksum incorrecto sobre datos reales");
    assert_eq!(ok as usize, n);
    assert!(n >= 50);
    assert!(!p.take_resync());
}

#[test]
fn checksum_alterado_detiene_profundidad_y_pide_resync() {
    let p = KrakenProtocol::new("SOL/USD", true, 1000, 2, 8);
    p.on_connect();
    let mut lines = FIXTURE.lines().filter(|l| !l.is_empty());
    let snap = lines.next().unwrap();
    assert!(matches!(run(&p, snap)[0], MarketEvent::Snapshot(_)));
    let upd = lines.next().unwrap();
    let i = upd.find("\"checksum\":").unwrap() + 11;
    let j = i + upd[i..].find(|c: char| !c.is_ascii_digit()).unwrap();
    let orig: u32 = upd[i..j].parse().unwrap();
    let tampered = format!("{}{}{}", &upd[..i], orig ^ 1, &upd[j..]);
    assert!(
        run(&p, &tampered).is_empty(),
        "no se emite un update no verificado"
    );
    assert!(p.take_resync());
    // Siguientes updates tampoco se emiten hasta un snapshot nuevo.
    assert!(run(&p, lines.next().unwrap()).is_empty());
    let MarketEvent::Snapshot(s) = &run(&p, snap)[0] else {
        panic!()
    };
    assert_eq!(s.last_update_id, 2, "el contador nunca retrocede");
}

#[test]
fn recorte_top_n_emite_bajas_y_verifica_checksum() {
    use lu_kraken::crc::crc32;
    // N = 2. Precio con 2 decimales, cantidad con 8 (como SOL/USD).
    let p = KrakenProtocol::new("SOL/USD", true, 2, 2, 8);
    p.on_connect();
    let c1 = crc32(
        concat!(
            "10100",
            "100000000",
            "10000",
            "100000000",
            "9900",
            "100000000"
        )
        .as_bytes(),
    );
    let snap = format!(
        r#"{{"channel":"book","type":"snapshot","data":[{{"symbol":"SOL/USD","bids":[{{"price":100.0,"qty":1.0}},{{"price":99.0,"qty":1.0}}],"asks":[{{"price":101.0,"qty":1.0}}],"checksum":{c1},"timestamp":"2026-09-26T04:41:10.000000Z"}}]}}"#
    );
    assert!(matches!(run(&p, &snap)[0], MarketEvent::Snapshot(_)));
    // Nuevo mejor bid 100.50: el 99.00 sale del top 2 y debe emitirse como baja.
    let c2 = crc32(
        concat!(
            "10100",
            "100000000",
            "10050",
            "100000000",
            "10000",
            "100000000"
        )
        .as_bytes(),
    );
    let upd = format!(
        r#"{{"channel":"book","type":"update","data":[{{"symbol":"SOL/USD","bids":[{{"price":100.5,"qty":1e0}}],"asks":[],"checksum":{c2},"timestamp":"2026-09-26T04:41:10.100000Z"}}]}}"#
    );
    let MarketEvent::Depth(d) = &run(&p, &upd)[0] else {
        panic!("se esperaba update verificado")
    };
    assert_eq!(d.bids.len(), 2);
    assert!(d.bids.contains(&lu_core::Level {
        px: Px::parse("100.5").unwrap(),
        qty: Qty::from_units(1)
    }));
    assert!(d.bids.contains(&lu_core::Level {
        px: Px::parse("99").unwrap(),
        qty: Qty::ZERO
    }));
    assert_eq!(p.checksum_stats(), (2, 0));
}
