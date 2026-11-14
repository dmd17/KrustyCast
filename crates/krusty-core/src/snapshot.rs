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
//! the key versions in insertion order.

use crate::table::IdMap;
use crate::{Config, Core, Decided, RingSlot, Session};

const MAGIC: u64 = 0x3130_5041_4E53_434B; // "KCSNAP01"
const VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapshotError {
    Truncated,
    BadMagic,
    UnsupportedVersion(u32),
    /// The snapshot was taken with a different configuration.
    ConfigMismatch,
    Corrupt(&'static str),
}

fn put(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

struct Reader<'a> {
    b: &'a [u8],
    at: usize,
}

impl Reader<'_> {
    fn u64(&mut self) -> Result<u64, SnapshotError> {
        let s = self
            .b
            .get(self.at..self.at + 8)
            .ok_or(SnapshotError::Truncated)?;
        self.at += 8;
        Ok(u64::from_le_bytes(s.try_into().unwrap()))
    }
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

    /// Rebuilds a core from a snapshot taken with the same configuration.
    pub fn restore(cfg: Config, bytes: &[u8]) -> Result<Core, SnapshotError> {
        let mut r = Reader { b: bytes, at: 0 };
        if r.u64()? != MAGIC {
            return Err(SnapshotError::BadMagic);
        }
        let version = r.u64()? as u32;
        if version != VERSION {
            return Err(SnapshotError::UnsupportedVersion(version));
        }
        let snap_cfg = Config {
            max_clients: r.u64()? as usize,
            outcome_ring: r.u64()? as usize,
            max_keys: r.u64()? as usize,
            evict_idle_ns: r.u64()?,
        };
        if snap_cfg != cfg {
            return Err(SnapshotError::ConfigMismatch);
        }
        let mut core = Core::new(cfg);
        core.applied = r.u64()?;
        core.next_seqno = r.u64()?;
        core.now_ns = r.u64()?;
        core.incarnation_floor = r.u64()?;
        core.unused = r.u64()? as usize;
        if core.unused > cfg.max_clients || core.next_seqno == 0 {
            return Err(SnapshotError::Corrupt("scalars"));
        }
        let sessions = r.u64()? as usize;
        if sessions > cfg.max_clients {
            return Err(SnapshotError::Corrupt("session count"));
        }
        for _ in 0..sessions {
            let slot = r.u64()? as usize;
            if slot >= core.unused || core.sessions[slot].is_some() {
                return Err(SnapshotError::Corrupt("session slot"));
            }
            let s = Session {
                client_id: r.u64()? as u32,
                incarnation: r.u64()?,
                next: r.u64()?,
                last_active_ns: r.u64()?,
            };
            if core.clients.get(s.client_id as u64).is_some()
                || !core.clients.insert(s.client_id as u64, slot as u32)
            {
                return Err(SnapshotError::Corrupt("duplicate client"));
            }
            core.sessions[slot] = Some(s);
            let n = r.u64()? as usize;
            if n > cfg.outcome_ring {
                return Err(SnapshotError::Corrupt("ring count"));
            }
            for _ in 0..n {
                let i = r.u64()? as usize;
                let client_seqno = r.u64()?;
                let kind = r.u64()?;
                let (a, b) = (r.u64()?, r.u64()?);
                if i >= cfg.outcome_ring || client_seqno % cfg.outcome_ring as u64 != i as u64 {
                    return Err(SnapshotError::Corrupt("ring slot"));
                }
                let decided = match kind {
                    0 => Decided::Accepted { global_seqno: a },
                    1 => Decided::Rejected {
                        key: a,
                        current_version: b,
                    },
                    _ => return Err(SnapshotError::Corrupt("outcome kind")),
                };
                core.ring[slot * cfg.outcome_ring + i] = Some(RingSlot {
                    client_seqno,
                    decided,
                });
            }
        }
        let keys = r.u64()? as usize;
        if keys > cfg.max_keys {
            return Err(SnapshotError::Corrupt("key count"));
        }
        core.keys = IdMap::new(cfg.max_keys);
        for i in 0..keys {
            let (key, version) = (r.u64()?, r.u64()?);
            if core.keys.get(key).is_some() || !core.keys.insert(key, i as u32) {
                return Err(SnapshotError::Corrupt("duplicate key"));
            }
            core.versions.push(version);
            core.key_of_slot.push(key);
        }
        if r.at != bytes.len() {
            return Err(SnapshotError::Corrupt("trailing bytes"));
        }
        Ok(core)
    }
}
