//! Snapshots of the core's state (FR-9, ARCHITECTURE §9).
//!
//! A snapshot taken after log index `i` plus the log entries after `i`
//! reproduce the core exactly: [`Core::restore`] of [`Core::snapshot`]
//! behaves like the original for every future input, and snapshots are
//! canonical (equal states give equal bytes).
//!
//! Little-endian layout: a header with the configuration (which must match
//! on restore, since session slots and ring positions depend on it), the
//! scalars, the sessions in slot order with their remembered outcomes, and
//! the key versions in insertion order. `restore.rs` reads them (and is
//! verified; this writer is not, its property test covers it).

use crate::{Core, Decided};

use crate::restore::{MAGIC, VERSION};

fn put(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

impl Core {
    /// Appends a snapshot of the current state to `out`.
    pub fn snapshot(&self, out: &mut Vec<u8>) {
        put(out, MAGIC);
        put(out, VERSION as u64);
        put(out, self.cfg.max_clients as u64);
        put(out, self.cfg.outcome_ring as u64);
        put(out, self.cfg.max_keys as u64);
        put(out, self.cfg.evict_idle_ns);
        put(out, self.applied);
        put(out, self.next_seqno);
        put(out, self.now_ns);
        put(out, self.incarnation_floor);
        put(out, self.unused as u64);
        let live = self.sessions.iter().filter(|s| s.is_some()).count();
        put(out, live as u64);
        for (slot, s) in self.sessions.iter().enumerate() {
            let Some(s) = s else { continue };
            put(out, slot as u64);
            put(out, s.client_id as u64);
            put(out, s.incarnation);
            put(out, s.next);
            put(out, s.last_active_ns);
            let base = slot * self.cfg.outcome_ring;
            let ring = &self.ring[base..base + self.cfg.outcome_ring];
            put(out, ring.iter().filter(|r| r.is_some()).count() as u64);
            for (i, r) in ring.iter().enumerate() {
                let Some(r) = r else { continue };
                put(out, i as u64);
                put(out, r.client_seqno);
                match r.decided {
                    Decided::Accepted { global_seqno } => {
                        put(out, 0);
                        put(out, global_seqno);
                        put(out, 0);
                    }
                    Decided::Rejected {
                        key,
                        current_version,
                    } => {
                        put(out, 1);
                        put(out, key);
                        put(out, current_version);
                    }
                }
            }
        }
        put(out, self.versions.len() as u64);
        for (i, v) in self.versions.iter().enumerate() {
            put(out, self.key_of_slot[i]);
            put(out, *v);
        }
    }
}
