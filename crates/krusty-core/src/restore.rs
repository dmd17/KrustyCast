//! Restoring the core from a snapshot (`snapshot.rs` writes them).
//!
//! In the subset Aeneas translates, so that `spec/lean` can prove a
//! restored core satisfies the invariant `Core::apply` is proved under,
//! whatever the bytes: no early return inside a loop (each loop records
//! its first error and stops), and bytes decoded by indexing.

use crate::{Config, Core, Decided, RingSlot, Session};

pub(crate) const MAGIC: u64 = 0x3130_5041_4E53_434B; // "KCSNAP01"
pub(crate) const VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapshotError {
    Truncated,
    BadMagic,
    UnsupportedVersion(u32),
    /// The snapshot was taken with a different configuration.
    ConfigMismatch,
    Corrupt(Corruption),
}

/// What is wrong with a snapshot that decodes but cannot be a core's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Corruption {
    Scalars,
    SessionCount,
    SessionSlot,
    DuplicateClient,
    RingCount,
    RingSlot,
    OutcomeKind,
    KeyCount,
    DuplicateKey,
    TrailingBytes,
}

/// (The core is `kc` here: a variable named `core` would shadow the `core`
/// namespace in the Lean translation.)
///
/// The little-endian `u64` at `at` in `b`, and the position after it.
/// (Positions are passed along rather than kept in a reader struct: Aeneas
/// does not translate loops over a `&mut` to a struct holding a borrow.)
fn read_u64(b: &[u8], at: usize) -> Result<(u64, usize), SnapshotError> {
    if at > b.len() || b.len() - at < 8 {
        return Err(SnapshotError::Truncated);
    }
    let v = (b[at] as u64)
        | (b[at + 1] as u64) << 8
        | (b[at + 2] as u64) << 16
        | (b[at + 3] as u64) << 24
        | (b[at + 4] as u64) << 32
        | (b[at + 5] as u64) << 40
        | (b[at + 6] as u64) << 48
        | (b[at + 7] as u64) << 56;
    Ok((v, at + 8))
}

/// One remembered outcome of a session's ring.
fn restore_slot(
    kc: &mut Core,
    b: &[u8],
    at: usize,
    cfg: Config,
    slot: usize,
) -> Result<usize, SnapshotError> {
    let (i, at) = read_u64(b, at)?;
    let (client_seqno, at) = read_u64(b, at)?;
    let (kind, at) = read_u64(b, at)?;
    let (x, at) = read_u64(b, at)?;
    let (y, at) = read_u64(b, at)?;
    let ring = cfg.outcome_ring as u64;
    if i >= ring || client_seqno % ring != i {
        return Err(SnapshotError::Corrupt(Corruption::RingSlot));
    }
    let decided = match kind {
        0 => Decided::Accepted { global_seqno: x },
        1 => Decided::Rejected {
            key: x,
            current_version: y,
        },
        _ => return Err(SnapshotError::Corrupt(Corruption::OutcomeKind)),
    };
    kc.ring[slot * cfg.outcome_ring + i as usize] = Some(RingSlot {
        client_seqno,
        decided,
    });
    Ok(at)
}

/// One session and its remembered outcomes.
fn restore_session(
    kc: &mut Core,
    b: &[u8],
    at: usize,
    cfg: Config,
) -> Result<usize, SnapshotError> {
    let (slot, at) = read_u64(b, at)?;
    if slot >= kc.unused as u64 || kc.sessions[slot as usize].is_some() {
        return Err(SnapshotError::Corrupt(Corruption::SessionSlot));
    }
    let slot = slot as usize;
    let (client_id, at) = read_u64(b, at)?;
    let (incarnation, at) = read_u64(b, at)?;
    let (next, at) = read_u64(b, at)?;
    let (last_active_ns, at) = read_u64(b, at)?;
    let s = Session {
        client_id: client_id as u32,
        incarnation,
        next,
        last_active_ns,
    };
    if kc.clients.get(s.client_id as u64).is_some()
        || !kc.clients.insert(s.client_id as u64, slot as u32)
    {
        return Err(SnapshotError::Corrupt(Corruption::DuplicateClient));
    }
    kc.sessions[slot] = Some(s);
    let (n, at) = read_u64(b, at)?;
    if n > cfg.outcome_ring as u64 {
        return Err(SnapshotError::Corrupt(Corruption::RingCount));
    }
    let mut at = at;
    let mut err = None;
    let mut k = 0;
    while k < n && err.is_none() {
        match restore_slot(kc, b, at, cfg, slot) {
            Ok(a) => at = a,
            Err(e) => err = Some(e),
        }
        k += 1;
    }
    match err {
        Some(e) => Err(e),
        None => Ok(at),
    }
}

/// One key and its version, in version slot `i`.
fn restore_key(kc: &mut Core, b: &[u8], at: usize, i: usize) -> Result<usize, SnapshotError> {
    let (key, at) = read_u64(b, at)?;
    let (version, at) = read_u64(b, at)?;
    if kc.keys.get(key).is_some() || !kc.keys.insert(key, i as u32) {
        return Err(SnapshotError::Corrupt(Corruption::DuplicateKey));
    }
    kc.versions.push(version);
    kc.key_of_slot.push(key);
    Ok(at)
}

impl Core {
    /// Rebuilds a core from a snapshot taken with the same configuration.
    /// Any bytes: what is not a snapshot of a reachable core is rejected.
    pub fn restore(cfg: Config, bytes: &[u8]) -> Result<Core, SnapshotError> {
        let b = bytes;
        // (Each check comes after the next read: the Lean translation's
        // proofs step through a read more easily than a bare comparison.)
        let (magic, at) = read_u64(b, 0)?;
        let (version, at) = read_u64(b, at)?;
        if magic != MAGIC {
            return Err(SnapshotError::BadMagic);
        }
        if version != VERSION as u64 {
            return Err(SnapshotError::UnsupportedVersion(version as u32));
        }
        let (max_clients, at) = read_u64(b, at)?;
        let (outcome_ring, at) = read_u64(b, at)?;
        let (max_keys, at) = read_u64(b, at)?;
        let (evict_idle_ns, at) = read_u64(b, at)?;
        if max_clients != cfg.max_clients as u64
            || outcome_ring != cfg.outcome_ring as u64
            || max_keys != cfg.max_keys as u64
            || evict_idle_ns != cfg.evict_idle_ns
        {
            return Err(SnapshotError::ConfigMismatch);
        }
        let mut kc = Core::new(cfg);
        let (applied, at) = read_u64(b, at)?;
        let (next_seqno, at) = read_u64(b, at)?;
        let (now_ns, at) = read_u64(b, at)?;
        let (incarnation_floor, at) = read_u64(b, at)?;
        let (unused, at) = read_u64(b, at)?;
        let (sessions, at) = read_u64(b, at)?;
        if unused > cfg.max_clients as u64 || next_seqno == 0 {
            return Err(SnapshotError::Corrupt(Corruption::Scalars));
        }
        kc.applied = applied;
        kc.next_seqno = next_seqno;
        kc.now_ns = now_ns;
        kc.incarnation_floor = incarnation_floor;
        kc.unused = unused as usize;
        // Slots 0..unused all hold a session (one is reused only by evicting
        // its session); a hole would never be reused.
        if sessions != unused {
            return Err(SnapshotError::Corrupt(Corruption::SessionCount));
        }
        let mut at = at;
        let mut err = None;
        let mut k = 0;
        while k < sessions && err.is_none() {
            match restore_session(&mut kc, b, at, cfg) {
                Ok(a) => at = a,
                Err(e) => err = Some(e),
            }
            k += 1;
        }
        if let Some(e) = err {
            return Err(e);
        }
        let (keys, at) = read_u64(b, at)?;
        if keys > cfg.max_keys as u64 {
            return Err(SnapshotError::Corrupt(Corruption::KeyCount));
        }
        let mut at = at;
        let mut i = 0;
        while (i as u64) < keys && err.is_none() {
            match restore_key(&mut kc, b, at, i) {
                Ok(a) => at = a,
                Err(e) => err = Some(e),
            }
            i += 1;
        }
        if let Some(e) = err {
            return Err(e);
        }
        if at != b.len() {
            return Err(SnapshotError::Corrupt(Corruption::TrailingBytes));
        }
        Ok(kc)
    }
}
