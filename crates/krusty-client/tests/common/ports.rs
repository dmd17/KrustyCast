//! Loopback UDP ports for tests and examples that start nodes.
//!
//! Binding port 0 and closing the socket again (the old `free_port`) races
//! twice: the next `bind(0)` may return the same port, and any process on
//! the machine may take it before the node binds it, or while a node is
//! stopped for a restart. Instead, ports come from below the kernel's
//! ephemeral range (32768 and up by default), so no `bind(0)` elsewhere
//! can take them, and each probe socket stays open until all the ports of a
//! [`Ports`] are chosen, so they are distinct.

use std::net::{Ipv4Addr, SocketAddrV4, UdpSocket};
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

const LOW: u16 = 20_000;
const HIGH: u16 = 32_768;

/// Where the next search starts: different per process, then advancing, so
/// concurrent test binaries rarely probe the same ports.
static NEXT: AtomicU16 = AtomicU16::new(0);

/// Ports reserved by open sockets; [`Ports::take`] hands them out.
pub struct Ports {
    held: Vec<UdpSocket>,
    taken: usize,
}

impl Ports {
    /// Reserves `n` distinct free loopback ports.
    pub fn new(n: usize) -> Ports {
        let span = HIGH - LOW;
        if NEXT.load(Ordering::Relaxed) == 0 {
            let seed = std::process::id()
                ^ SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.subsec_nanos())
                    .unwrap_or(0);
            let _ = NEXT.compare_exchange(
                0,
                1 + (seed % span as u32) as u16,
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
        }
        let mut held = Vec::with_capacity(n);
        let mut tried = 0u32;
        while held.len() < n {
            assert!(
                tried < span as u32,
                "no free loopback UDP port below {HIGH}"
            );
            tried += 1;
            let off = NEXT.fetch_add(1, Ordering::Relaxed) % span;
            if let Ok(s) = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, LOW + off)) {
                held.push(s);
            }
        }
        Ports { held, taken: 0 }
    }

    /// The next reserved port. Its probe socket closes when the `Ports` is
    /// dropped, so drop it before starting what binds them.
    pub fn take(&mut self) -> SocketAddrV4 {
        let s = self
            .held
            .get(self.taken)
            .expect("more ports taken than reserved");
        self.taken += 1;
        match s.local_addr().unwrap() {
            std::net::SocketAddr::V4(a) => a,
            _ => unreachable!(),
        }
    }
}
