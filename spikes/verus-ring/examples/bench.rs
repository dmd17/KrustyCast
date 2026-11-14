//! Verified ring (vstd atomics: SeqCst) vs. an unverified twin that is
//! identical except for Acquire/Release orderings.
//!
//!   cargo run --release --example bench -- [producer_core consumer_core]
//!
//! Laptop numbers are for comparing the two variants only (PRD §7 targets
//! need the reference servers).
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Same algorithm as the verified ring, with Acquire/Release orderings.
struct RelaxedRing {
    buf: Box<[UnsafeCell<MaybeUninit<u64>>]>,
    head: AtomicU64,
    tail: AtomicU64,
}
unsafe impl Sync for RelaxedRing {}

struct RP(Arc<RelaxedRing>, usize);
struct RC(Arc<RelaxedRing>, usize);

fn relaxed(len: usize) -> (RP, RC) {
    let r = Arc::new(RelaxedRing {
        buf: (0..len).map(|_| UnsafeCell::new(MaybeUninit::uninit())).collect(),
        head: AtomicU64::new(0),
        tail: AtomicU64::new(0),
    });
    (RP(r.clone(), 0), RC(r, 0))
}

impl RP {
    fn try_push(&mut self, v: u64) -> Result<(), u64> {
        let len = self.0.buf.len();
        let next = if self.1 + 1 == len { 0 } else { self.1 + 1 };
        if self.0.head.load(Ordering::Acquire) == next as u64 {
            return Err(v);
        }
        unsafe { (*self.0.buf[self.1].get()).write(v) };
        self.0.tail.store(next as u64, Ordering::Release);
        self.1 = next;
        Ok(())
    }
}

impl RC {
    fn try_pop(&mut self) -> Option<u64> {
        let len = self.0.buf.len();
        if self.0.tail.load(Ordering::Acquire) == self.1 as u64 {
            return None;
        }
        let v = unsafe { (*self.0.buf[self.1].get()).assume_init_read() };
        let next = if self.1 + 1 == len { 0 } else { self.1 + 1 };
        self.0.head.store(next as u64, Ordering::Release);
        self.1 = next;
        Some(v)
    }
}

trait Push: Send + 'static {
    fn push(&mut self, v: u64) -> Result<(), u64>;
}
trait Pop: Send + 'static {
    fn pop(&mut self) -> Option<u64>;
}
impl Push for verus_ring::Producer<u64> {
    fn push(&mut self, v: u64) -> Result<(), u64> {
        self.try_push(v)
    }
}
impl Pop for verus_ring::Consumer<u64> {
    fn pop(&mut self) -> Option<u64> {
        self.try_pop()
    }
}
impl Push for RP {
    fn push(&mut self, v: u64) -> Result<(), u64> {
        self.try_push(v)
    }
}
impl Pop for RC {
    fn pop(&mut self) -> Option<u64> {
        self.try_pop()
    }
}

fn pin(core: usize) {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(core, &mut set);
        assert_eq!(libc::sched_setaffinity(0, std::mem::size_of_val(&set), &set), 0);
    }
}

fn push_spin(p: &mut impl Push, mut v: u64) {
    while let Err(back) = p.push(v) {
        v = back;
        std::hint::spin_loop();
    }
}

fn pop_spin(c: &mut impl Pop) -> u64 {
    loop {
        if let Some(v) = c.pop() {
            return v;
        }
        std::hint::spin_loop();
    }
}

/// One-way streaming: ns per message.
fn throughput<P: Push, C: Pop>(mut p: P, mut c: C, n: u64, cores: (usize, usize)) -> f64 {
    let t = std::thread::spawn(move || {
        pin(cores.0);
        for i in 0..n {
            push_spin(&mut p, i);
        }
    });
    pin(cores.1);
    let start = Instant::now();
    for i in 0..n {
        assert_eq!(pop_spin(&mut c), i);
    }
    let el = start.elapsed();
    t.join().unwrap();
    el.as_nanos() as f64 / n as f64
}

/// Ping-pong over two rings: round-trip times in ns, sorted.
fn round_trip<P: Push, C: Pop>(
    (mut p1, mut c1): (P, C),
    (mut p2, mut c2): (P, C),
    n: usize,
    cores: (usize, usize),
) -> Vec<u64> {
    let t = std::thread::spawn(move || {
        pin(cores.1);
        for _ in 0..n {
            let v = pop_spin(&mut c1);
            push_spin(&mut p2, v);
        }
    });
    pin(cores.0);
    let mut rtts = Vec::with_capacity(n);
    for i in 0..n {
        let start = Instant::now();
        push_spin(&mut p1, i as u64);
        assert_eq!(pop_spin(&mut c2), i as u64);
        rtts.push(start.elapsed().as_nanos() as u64);
    }
    t.join().unwrap();
    rtts.sort_unstable();
    rtts
}

fn pct(v: &[u64], p: f64) -> u64 {
    v[((v.len() - 1) as f64 * p) as usize]
}

fn main() {
    let args: Vec<usize> = std::env::args().skip(1).map(|a| a.parse().unwrap()).collect();
    let cores = if args.len() == 2 { (args[0], args[1]) } else { (4, 5) };
    const N: u64 = 20_000_000;
    const RT: usize = 1_000_000;
    println!("cores {:?}; {} msgs streamed, {} round trips; best of 5 runs", cores, N, RT);
    println!("{:<28}{:>10}{:>10}{:>10}{:>10}", "variant", "ns/msg", "rtt p50", "rtt p99", "p99.99");
    for name in ["verified (SeqCst)", "twin (Acquire/Release)"] {
        let mut best_tp = f64::MAX;
        let mut best_rt: Option<Vec<u64>> = None;
        for _ in 0..5 {
            let (tp, rt) = if name.starts_with("verified") {
                let (p, c) = verus_ring::new_queue::<u64>(1024);
                let tp = throughput(p, c, N, cores);
                let a = verus_ring::new_queue::<u64>(1024);
                let b = verus_ring::new_queue::<u64>(1024);
                (tp, round_trip(a, b, RT, cores))
            } else {
                let (p, c) = relaxed(1024);
                let tp = throughput(p, c, N, cores);
                (tp, round_trip(relaxed(1024), relaxed(1024), RT, cores))
            };
            best_tp = best_tp.min(tp);
            if best_rt.as_ref().is_none_or(|b| pct(&rt, 0.5) < pct(b, 0.5)) {
                best_rt = Some(rt);
            }
        }
        let rt = best_rt.unwrap();
        println!(
            "{:<28}{:>10.1}{:>10}{:>10}{:>10}",
            name,
            best_tp,
            pct(&rt, 0.5),
            pct(&rt, 0.99),
            pct(&rt, 0.9999)
        );
    }
}
