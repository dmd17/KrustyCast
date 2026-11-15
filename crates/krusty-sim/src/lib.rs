//! Deterministic single-node simulation (gap Q5, ARCHITECTURE §11.1).
//!
//! Drives the pure cores through the real codecs on a seeded event loop with
//! virtual time:
//!
//! - clients: `krusty-session` sessions submitting `krusty-wire` SUBMITs;
//! - the node: decodes, applies to `krusty-core` (timestamped with virtual
//!   time), answers with FEEDBACK, publishes native-stream DATA frames, and
//!   serves RECOVER from a bounded history (UNAVAILABLE once aged out);
//! - consumers: `krusty-stream` trackers that recover gaps;
//! - a network that loses, duplicates and delays every datagram.
//!
//! After a faulty phase the network calms down so the run can finish, then
//! the checkers run: G1 (consumers agree with the node's stream), G2 (in
//! order, loss only explicit), G4/G5/G6 on the node's stream, exactly-once
//! outcomes for the clients, and liveness.

pub mod cluster;

use krusty_core::{Config as CoreConfig, Core};
use krusty_session::{Config as SessionConfig, Event, Session, Transmit};
use krusty_stream::{Config as TrackerConfig, Delivered, Frame as TFrame, Kind, Tracker};
use krusty_wire::feedback::Feedback;
use krusty_wire::ingress::{Append, Submit};
use krusty_wire::stream::{self, DataFrameWriter, Frame, MessageMeta};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};

#[derive(Clone, Debug)]
pub struct Params {
    pub clients: u32,
    pub msgs_per_client: u64,
    pub consumers: usize,
    /// Per-mille of datagrams lost during the faulty phase.
    pub loss: u64,
    /// Per-mille of datagrams duplicated during the faulty phase.
    pub dup: u64,
    /// One-way delay is uniform in [min_delay_ns, max_delay_ns].
    pub min_delay_ns: u64,
    pub max_delay_ns: u64,
    /// Per-mille of submissions that are conditional appends.
    pub conditional: u64,
    pub keys: u64,
    /// Messages the node keeps for RECOVER.
    pub retain: usize,
    pub core: CoreConfig,
    pub heartbeat_ns: u64,
    pub tick_ns: u64,
    /// Virtual time limit for the whole run.
    pub limit_ns: u64,
    /// Failed outcomes are acceptable (e.g. evictions cause SESSION_EXPIRED).
    pub allow_failures: bool,
}

impl Default for Params {
    fn default() -> Params {
        Params {
            clients: 4,
            msgs_per_client: 200,
            consumers: 3,
            loss: 100,
            dup: 50,
            min_delay_ns: 5_000,
            max_delay_ns: 200_000,
            conditional: 300,
            keys: 3,
            retain: 100_000,
            core: CoreConfig {
                outcome_ring: 8,
                ..CoreConfig::DEFAULT
            },
            heartbeat_ns: 1_000_000,
            tick_ns: 50_000,
            limit_ns: 60_000_000_000,
            allow_failures: false,
        }
    }
}

/// Named parameter sets for the nightly runner.
pub fn scenario(name: &str) -> Option<Params> {
    let d = Params::default();
    Some(match name {
        "default" => d,
        "lossy" => Params {
            loss: 400,
            dup: 200,
            ..d
        },
        "short-retention" => Params {
            retain: 20,
            loss: 300,
            ..d
        },
        "contention" => Params {
            conditional: 900,
            keys: 1,
            ..d
        },
        // Fewer session slots than clients: newcomers wait (BUSY) or evict
        // idle ones, whose late resends must be SESSION_EXPIRED, never
        // sequenced twice.
        "eviction" => Params {
            clients: 6,
            msgs_per_client: 60,
            loss: 300,
            core: CoreConfig {
                max_clients: 3,
                evict_idle_ns: 300_000,
                ..d.core
            },
            allow_failures: true,
            ..d
        },
        _ => return None,
    })
}

pub const SCENARIOS: [&str; 5] = [
    "default",
    "lossy",
    "short-retention",
    "contention",
    "eviction",
];

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Report {
    pub published: u64,
    pub accepted: u64,
    pub rejected: u64,
    pub failed: u64,
    pub delivered: Vec<u64>,
    pub lost: Vec<u64>,
    pub datagrams: u64,
    pub end_ns: u64,
    pub violations: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum To {
    Node,
    Client(u32),
    Consumer(usize),
}

/// SplitMix64.
pub(crate) struct Rng(pub(crate) u64);

impl Rng {
    pub(crate) fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    pub(crate) fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next() % n }
    }

    pub(crate) fn per_mille(&mut self, p: u64) -> bool {
        self.below(1000) < p
    }
}

/// A datagram in flight: (arrival time, tie-breaker, destination, bytes).
type InFlight = (u64, u64, To, Vec<u8>);

struct Net {
    queue: BinaryHeap<Reverse<InFlight>>,
    counter: u64,
    faulty: bool,
    sent: u64,
}

impl Net {
    fn send(&mut self, rng: &mut Rng, p: &Params, now: u64, to: To, bytes: Vec<u8>) {
        self.sent += 1;
        let copies = if self.faulty {
            if rng.per_mille(p.loss) {
                0
            } else if rng.per_mille(p.dup) {
                2
            } else {
                1
            }
        } else {
            1
        };
        for _ in 0..copies {
            let delay = p.min_delay_ns + rng.below(p.max_delay_ns - p.min_delay_ns + 1);
            self.counter += 1;
            self.queue
                .push(Reverse((now + delay, self.counter, to, bytes.clone())));
        }
    }
}

pub(crate) const STREAM_ID: u64 = 0x5EED;

pub(crate) fn payload(client: u32, handle: u64) -> [u8; 12] {
    let mut b = [0u8; 12];
    b[..4].copy_from_slice(&client.to_le_bytes());
    b[4..].copy_from_slice(&handle.to_le_bytes());
    b
}

pub(crate) fn parse_payload(b: &[u8]) -> (u32, u64) {
    (
        u32::from_le_bytes(b[..4].try_into().unwrap()),
        u64::from_le_bytes(b[4..12].try_into().unwrap()),
    )
}

struct Client {
    session: Session,
    submitted: u64,
    /// Version this client believes each key is at.
    believed: HashMap<u64, u64>,
    /// handle → append mode, for re-encoding transmissions.
    appends: HashMap<u64, Append>,
}

pub(crate) struct Consumer {
    pub(crate) tracker: Tracker,
    pub(crate) stash: HashMap<u64, (MessageMeta, Vec<u8>)>,
    pub(crate) delivered: Vec<(u64, MessageMeta, Vec<u8>)>,
}

/// Runs one simulation. Deterministic in `seed` and `p`.
pub fn run(seed: u64, p: &Params) -> Report {
    let mut rng = Rng(seed);
    let mut net = Net {
        queue: BinaryHeap::new(),
        counter: 0,
        faulty: true,
        sent: 0,
    };
    let mut core = Core::new(p.core);
    let mut log_index = 0u64;
    let mut published: Vec<(MessageMeta, Vec<u8>)> = Vec::new();
    let mut report = Report::default();
    let mut events: HashMap<(u32, u64), Event> = HashMap::new();

    let mut clients: Vec<Client> = (0..p.clients)
        .map(|c| Client {
            session: Session::new(
                SessionConfig {
                    client_id: c,
                    window: p.core.outcome_ring,
                    retry_interval_ns: 2 * p.max_delay_ns + p.tick_ns,
                    max_attempts: 1_000_000,
                },
                1_000 + c as u64,
            ),
            submitted: 0,
            believed: HashMap::new(),
            appends: HashMap::new(),
        })
        .collect();
    let mut consumers: Vec<Consumer> = (0..p.consumers)
        .map(|_| Consumer {
            tracker: Tracker::new(TrackerConfig {
                pinned: Some(STREAM_ID),
                start_seqno: Some(1),
                recover: true,
                retry_interval_ns: 2 * p.max_delay_ns + p.tick_ns,
                max_retries: 1_000_000,
            }),
            stash: HashMap::new(),
            delivered: Vec::new(),
        })
        .collect();

    let total = p.clients as u64 * p.msgs_per_client;
    let mut now = 0u64;
    let mut next_tick = 0u64;
    let mut next_heartbeat = 0u64;
    let mut buf = vec![0u8; 65_536];

    loop {
        let all_done = events.len() as u64 == total;
        let consumers_done = consumers
            .iter()
            .all(|c| c.tracker.next_seqno() == Some(published.len() as u64 + 1));
        if all_done && consumers_done && net.queue.is_empty() {
            break;
        }
        if now > p.limit_ns {
            report.violations.push(format!(
                "liveness: not finished by {} ns ({} of {} outcomes)",
                p.limit_ns,
                events.len(),
                total
            ));
            break;
        }
        if net.faulty && clients.iter().all(|c| c.submitted == p.msgs_per_client) {
            net.faulty = false; // calm down so the run can finish
        }

        // Next event: a datagram or a timer tick.
        let next_dgram = net.queue.peek().map(|Reverse((t, ..))| *t);
        let t = match next_dgram {
            Some(d) if d < next_tick => d,
            _ => next_tick,
        };
        now = t;
        if next_dgram == Some(t) && t < next_tick {
            let Reverse((_, _, to, bytes)) = net.queue.pop().unwrap();
            match to {
                To::Node => node_receive(
                    &bytes,
                    now,
                    &mut core,
                    &mut log_index,
                    &mut published,
                    p,
                    &mut rng,
                    &mut net,
                    &mut buf,
                    &mut report,
                ),
                To::Client(c) => match Feedback::decode(&bytes) {
                    Ok(f) => {
                        let mut ev = Vec::new();
                        let cl = &mut clients[c as usize];
                        cl.session.on_feedback(&f, now, &mut ev);
                        record_events(c, ev, cl, &mut events, &mut report);
                    }
                    Err(e) => report
                        .violations
                        .push(format!("client {c}: bad feedback {e:?}")),
                },
                To::Consumer(k) => consumer_receive(&bytes, &mut consumers[k], k, &mut report),
            }
            continue;
        }

        // Tick: clients submit and transmit, consumers recover, the node heartbeats.
        next_tick = now + p.tick_ns;
        for c in 0..p.clients {
            let cl = &mut clients[c as usize];
            while cl.submitted < p.msgs_per_client {
                let handle = cl.submitted;
                let append = if rng.per_mille(p.conditional) {
                    let key = rng.below(p.keys);
                    let v = *cl.believed.get(&key).unwrap_or(&0);
                    Append::Conditional {
                        key,
                        expected_version: v,
                    }
                } else {
                    Append::Unconditional
                };
                match cl.session.submit(append, handle) {
                    Ok(_) => {
                        cl.appends.insert(handle, append);
                        cl.submitted += 1;
                    }
                    Err(_) => break,
                }
            }
            let (mut tx, mut ev) = (Vec::new(), Vec::new());
            if let Some(min) = cl.session.must_restart() {
                // As the SDK will: a new, larger incarnation.
                let inc = min.max(cl.session.incarnation() + 1);
                cl.session.restart(inc, &mut ev);
            }
            cl.session.poll(now, &mut tx, &mut ev);
            record_events(c, ev, cl, &mut events, &mut report);
            for t in tx {
                send_submit(c, &t, &mut buf, &mut net, &mut rng, p, now);
            }
        }
        for (k, cons) in consumers.iter_mut().enumerate() {
            let mut out = Vec::new();
            if let Some(r) = cons.tracker.poll_recover(now, &mut out) {
                let n = stream::encode_recover(&mut buf, r.stream_id, r.from, r.count).unwrap();
                net.send(&mut rng, p, now, To::Node, buf[..n].to_vec());
            }
            take_deliveries(cons, out, k, &mut report);
        }
        if now >= next_heartbeat {
            next_heartbeat = now + p.heartbeat_ns;
            let next = published.len() as u64 + 1;
            for k in 0..p.consumers {
                let n = stream::encode_heartbeat(&mut buf, STREAM_ID, next).unwrap();
                net.send(&mut rng, p, now, To::Consumer(k), buf[..n].to_vec());
            }
        }
    }

    report.end_ns = now;
    report.datagrams = net.sent;
    report.published = published.len() as u64;
    check(p, &published, &events, &consumers, &mut report);
    report
}

fn send_submit(
    c: u32,
    t: &Transmit,
    buf: &mut [u8],
    net: &mut Net,
    rng: &mut Rng,
    p: &Params,
    now: u64,
) {
    let pl = payload(c, t.handle);
    let s = Submit {
        client_id: c,
        incarnation: t.incarnation,
        client_seqno: t.client_seqno,
        append: t.append,
        payload: &pl,
    };
    let n = s.encode(buf).unwrap();
    net.send(rng, p, now, To::Node, buf[..n].to_vec());
}

fn record_events(
    c: u32,
    ev: Vec<Event>,
    cl: &mut Client,
    events: &mut HashMap<(u32, u64), Event>,
    r: &mut Report,
) {
    for e in ev {
        let handle = match e {
            Event::Accepted { handle, .. } => {
                r.accepted += 1;
                handle
            }
            Event::Rejected {
                handle,
                key,
                current_version,
                ..
            } => {
                r.rejected += 1;
                cl.believed.insert(key, current_version);
                handle
            }
            Event::Failed { handle, .. } => {
                r.failed += 1;
                handle
            }
        };
        if let Some(Append::Conditional {
            key,
            expected_version,
        }) = cl.appends.get(&handle).copied()
            && matches!(e, Event::Accepted { .. })
        {
            cl.believed.insert(key, expected_version + 1);
        }
        if events.insert((c, handle), e).is_some() {
            r.violations
                .push(format!("client {c} handle {handle}: two outcomes"));
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn node_receive(
    bytes: &[u8],
    now: u64,
    core: &mut Core,
    log_index: &mut u64,
    published: &mut Vec<(MessageMeta, Vec<u8>)>,
    p: &Params,
    rng: &mut Rng,
    net: &mut Net,
    buf: &mut [u8],
    r: &mut Report,
) {
    if let Ok(s) = Submit::decode(bytes) {
        *log_index += 1;
        let e = core.apply(*log_index, now, &s).expect("in order");
        if let Some(pb) = e.publish {
            if pb.seqno != published.len() as u64 + 1 {
                r.violations.push(format!(
                    "node: published seqno {} after {}",
                    pb.seqno,
                    published.len()
                ));
            }
            published.push((pb.meta, s.payload.to_vec()));
            let mut w = DataFrameWriter::new(buf, STREAM_ID, pb.seqno, false).unwrap();
            w.push(&pb.meta, s.payload).unwrap();
            let n = w.finish().unwrap();
            let frame = buf[..n].to_vec();
            for k in 0..p.consumers {
                net.send(rng, p, now, To::Consumer(k), frame.clone());
            }
        }
        if let Some(f) = e.reply {
            let n = f.encode(buf).unwrap();
            net.send(rng, p, now, To::Client(f.client_id), buf[..n].to_vec());
        }
        return;
    }
    if let Ok(Frame::Recover { from, count, .. }) = Frame::decode(bytes) {
        // Which consumer asked is not on the wire; answer all (the sim's
        // stand-in for a multicast retransmission).
        let len = published.len() as u64;
        let earliest = (len + 1).saturating_sub(p.retain as u64).max(1);
        let mut frames = Vec::new();
        if from < earliest {
            let n = stream::encode_unavailable(buf, STREAM_ID, from, earliest).unwrap();
            frames.push(buf[..n].to_vec());
        }
        let mut s = from.max(earliest);
        let end = (from + count).min(len + 1);
        while s < end {
            let mut w = DataFrameWriter::new(buf, STREAM_ID, s, true).unwrap();
            while s < end && w.count() < 16 {
                let (meta, pl) = &published[s as usize - 1];
                if w.push(meta, pl).is_err() {
                    break;
                }
                s += 1;
            }
            let n = w.finish().unwrap();
            frames.push(buf[..n].to_vec());
        }
        for f in frames {
            for k in 0..p.consumers {
                net.send(rng, p, now, To::Consumer(k), f.clone());
            }
        }
        return;
    }
    r.violations.push(format!(
        "node: undecodable datagram of {} bytes",
        bytes.len()
    ));
}

pub(crate) fn consumer_receive(bytes: &[u8], cons: &mut Consumer, k: usize, r: &mut Report) {
    let frame = match Frame::decode(bytes) {
        Ok(f) => f,
        Err(e) => {
            r.violations.push(format!("consumer {k}: bad frame {e:?}"));
            return;
        }
    };
    let tf = match frame {
        Frame::Data(d) => {
            for m in d.messages() {
                cons.stash
                    .entry(m.seqno)
                    .or_insert((m.meta, m.payload.to_vec()));
            }
            TFrame {
                stream_id: d.stream_id,
                seqno: d.seqno,
                kind: Kind::Data {
                    count: d.count as u64,
                    first_handle: d.seqno,
                },
            }
        }
        Frame::Heartbeat {
            stream_id,
            next_seqno,
        } => TFrame {
            stream_id,
            seqno: next_seqno,
            kind: Kind::Heartbeat,
        },
        Frame::EndOfSession {
            stream_id,
            next_seqno,
        } => TFrame {
            stream_id,
            seqno: next_seqno,
            kind: Kind::EndOfStream,
        },
        Frame::Unavailable {
            stream_id,
            from,
            earliest_retained,
        } => TFrame {
            stream_id,
            seqno: from,
            kind: Kind::Unavailable { earliest_retained },
        },
        Frame::Recover { .. } => {
            r.violations.push(format!("consumer {k}: got a RECOVER"));
            return;
        }
    };
    let mut out = Vec::new();
    cons.tracker.on_frame(&tf, &mut out);
    take_deliveries(cons, out, k, r);
}

pub(crate) fn take_deliveries(cons: &mut Consumer, out: Vec<Delivered>, k: usize, r: &mut Report) {
    for d in out {
        match cons.stash.remove(&d.handle) {
            Some((meta, pl)) => cons.delivered.push((d.seqno, meta, pl)),
            None => r
                .violations
                .push(format!("consumer {k}: delivered {} without data", d.seqno)),
        }
    }
}

fn check(
    p: &Params,
    published: &[(MessageMeta, Vec<u8>)],
    events: &HashMap<(u32, u64), Event>,
    consumers: &[Consumer],
    r: &mut Report,
) {
    let v = &mut r.violations;
    // G4 / G6 / G5 on the node's stream.
    let mut seen = HashMap::new();
    let mut last_handle: HashMap<u32, u64> = HashMap::new();
    let mut versions: HashMap<u64, u64> = HashMap::new();
    for (i, (meta, pl)) in published.iter().enumerate() {
        let g = i as u64 + 1;
        let (c, h) = parse_payload(pl);
        if meta.client_id != c {
            v.push(format!(
                "seqno {g}: client id {} but payload says {c}",
                meta.client_id
            ));
        }
        if let Some(prev) = seen.insert((c, h), g) {
            v.push(format!("G4: client {c} handle {h} at {prev} and {g}"));
        }
        if let Some(prev) = last_handle.insert(c, h)
            && h <= prev
        {
            v.push(format!("G6: client {c} handle {h} after {prev}"));
        }
        if let Some((key, ver)) = meta.conditional {
            let e = versions.entry(key).or_insert(0);
            if ver != *e + 1 {
                v.push(format!("G5: key {key} reached {ver} after {}", e));
            }
            *e = ver;
        }
    }
    // Exactly once for live clients: every message decided, ACCEPTED names it.
    let mut acked = 0;
    for ((c, h), e) in events {
        match e {
            Event::Accepted { global_seqno, .. } => {
                acked += 1;
                if seen.get(&(*c, *h)) != Some(global_seqno) {
                    v.push(format!(
                        "client {c} handle {h}: ACCEPTED at {global_seqno}, stream has {:?}",
                        seen.get(&(*c, *h))
                    ));
                }
            }
            Event::Rejected { .. } => {
                if seen.contains_key(&(*c, *h)) {
                    v.push(format!("client {c} handle {h}: REJECTED but published"));
                }
            }
            Event::Failed { .. } => {
                if !p.allow_failures {
                    v.push(format!("client {c} handle {h}: failed"));
                }
            }
        }
    }
    // Everything published was answered ACCEPTED, or failed with unknown fate.
    for (c, h) in seen.keys() {
        match events.get(&(*c, *h)) {
            Some(Event::Accepted { .. }) | Some(Event::Failed { .. }) => {}
            other => v.push(format!(
                "client {c} handle {h}: published, but the client got {other:?}"
            )),
        }
    }
    let _ = acked;
    // G1 / G2 per consumer.
    for (k, cons) in consumers.iter().enumerate() {
        let mut prev = 0;
        for (seqno, meta, pl) in &cons.delivered {
            if *seqno <= prev {
                v.push(format!("G2: consumer {k} delivered {seqno} after {prev}"));
            }
            prev = *seqno;
            match published.get(*seqno as usize - 1) {
                Some((m, b)) if m == meta && b == pl => {}
                _ => v.push(format!(
                    "G1: consumer {k} seqno {seqno} differs from the stream"
                )),
            }
        }
        let lost = cons.tracker.lost();
        if cons.delivered.len() as u64 + lost != published.len() as u64 {
            v.push(format!(
                "G2: consumer {k} delivered {} + lost {lost} != {}",
                cons.delivered.len(),
                published.len()
            ));
        }
        if p.retain >= published.len() && lost != 0 {
            v.push(format!(
                "consumer {k} lost {lost} although everything was retained"
            ));
        }
        r.delivered.push(cons.delivered.len() as u64);
        r.lost.push(lost);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_runs_are_clean() {
        for seed in 0..20 {
            let r = run(seed, &Params::default());
            assert!(r.violations.is_empty(), "seed {seed}: {:?}", r.violations);
            assert_eq!(r.accepted + r.rejected, 4 * 200);
        }
    }

    #[test]
    fn short_retention_causes_explicit_loss_only() {
        let p = Params {
            retain: 20,
            loss: 300,
            ..Params::default()
        };
        let mut lost_somewhere = false;
        for seed in 0..20 {
            let r = run(seed, &p);
            assert!(r.violations.is_empty(), "seed {seed}: {:?}", r.violations);
            lost_somewhere |= r.lost.iter().any(|&l| l > 0);
        }
        assert!(lost_somewhere, "the scenario should exercise UNAVAILABLE");
    }

    #[test]
    fn every_scenario_is_clean_on_a_few_seeds() {
        for name in SCENARIOS {
            let p = scenario(name).unwrap();
            for seed in 0..5 {
                let r = run(seed, &p);
                assert!(
                    r.violations.is_empty(),
                    "{name} seed {seed}: {:?}",
                    r.violations
                );
            }
        }
    }

    #[test]
    fn eviction_scenario_actually_expires_sessions() {
        let p = scenario("eviction").unwrap();
        let failed: u64 = (0..10).map(|seed| run(seed, &p).failed).sum();
        assert!(
            failed > 0,
            "no session was ever expired: the scenario tests nothing"
        );
    }

    #[test]
    fn runs_are_deterministic() {
        let p = Params::default();
        assert_eq!(run(7, &p), run(7, &p));
    }
}
