//! The published stream as a ring of fixed-size slots: one writer (the core
//! stage) and any number of readers (recovery servers), without locks.
//!
//! Each slot is a seqlock. The writer marks the slot busy (odd version),
//! writes the bytes, then marks it as holding seqno `s` (version `2s`).
//! A reader checks the version, copies, and checks the version again; a
//! torn copy is detected, never returned. Bytes are stored as `AtomicU64`
//! words with relaxed accesses, so a racing read is merely stale, not
//! undefined behaviour, and the module needs no `unsafe`.

use std::sync::atomic::{AtomicU64, Ordering, fence};

struct Slot {
    version: AtomicU64,
    len: AtomicU64,
    words: Box<[AtomicU64]>,
}

pub struct History {
    slots: Box<[Slot]>,
    mask: u64,
    slot_bytes: usize,
    /// Highest seqno written (first_seqno - 1 = none).
    committed: AtomicU64,
    /// The first seqno this history ever held (after a restart, not 1).
    first_seqno: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadError {
    /// Not published yet.
    NotYet,
    /// Overwritten by a newer message: no longer retained.
    Gone,
}

impl History {
    /// `slots` (a power of two) messages of at most `slot_bytes` each.
    pub fn new(slots: usize, slot_bytes: usize) -> History {
        History::starting_at(slots, slot_bytes, 1)
    }

    /// A history whose first message will be `first_seqno` (≥ 1).
    pub fn starting_at(slots: usize, slot_bytes: usize, first_seqno: u64) -> History {
        assert!(first_seqno >= 1);
        assert!(slots.is_power_of_two());
        let words = slot_bytes.div_ceil(8);
        History {
            slots: (0..slots)
                .map(|_| Slot {
                    version: AtomicU64::new(0),
                    len: AtomicU64::new(0),
                    words: (0..words).map(|_| AtomicU64::new(0)).collect(),
                })
                .collect(),
            mask: slots as u64 - 1,
            slot_bytes,
            committed: AtomicU64::new(first_seqno - 1),
            first_seqno,
        }
    }

    pub fn slot_bytes(&self) -> usize {
        self.slot_bytes
    }

    /// Highest seqno published (`first - 1` = none yet).
    pub fn committed(&self) -> u64 {
        self.committed.load(Ordering::Acquire)
    }

    /// Oldest seqno still retained.
    pub fn earliest(&self) -> u64 {
        let c = self.committed();
        let cap = self.mask + 1;
        let oldest = if c >= cap { c - cap + 1 } else { 1 };
        oldest.max(self.first_seqno)
    }

    /// Appends `seqno` (exactly one more than the last). Writer only: the
    /// type is shared through `&self`, but only the core stage calls this.
    pub fn push(&self, seqno: u64, bytes: &[u8]) {
        assert!(bytes.len() <= self.slot_bytes);
        assert_eq!(seqno, self.committed.load(Ordering::Relaxed) + 1);
        let slot = &self.slots[(seqno & self.mask) as usize];
        slot.version.store(2 * seqno - 1, Ordering::Relaxed);
        fence(Ordering::Release);
        for (i, c) in bytes.chunks(8).enumerate() {
            let mut w = [0u8; 8];
            w[..c.len()].copy_from_slice(c);
            slot.words[i].store(u64::from_le_bytes(w), Ordering::Relaxed);
        }
        slot.len.store(bytes.len() as u64, Ordering::Relaxed);
        slot.version.store(2 * seqno, Ordering::Release);
        self.committed.store(seqno, Ordering::Release);
    }

    /// Copies message `seqno` into `out` and returns its length.
    pub fn read(&self, seqno: u64, out: &mut [u8]) -> Result<usize, ReadError> {
        if seqno > self.committed() {
            return Err(ReadError::NotYet);
        }
        if seqno < self.first_seqno {
            return Err(ReadError::Gone);
        }
        let slot = &self.slots[(seqno & self.mask) as usize];
        let v1 = slot.version.load(Ordering::Acquire);
        if v1 != 2 * seqno {
            return Err(ReadError::Gone);
        }
        let len = slot.len.load(Ordering::Relaxed) as usize;
        if len > self.slot_bytes || len > out.len() {
            return Err(ReadError::Gone); // torn; the version check below would fail too
        }
        let mut i = 0;
        while i * 8 < len {
            let w = slot.words[i].load(Ordering::Relaxed).to_le_bytes();
            let n = (len - i * 8).min(8);
            out[i * 8..i * 8 + n].copy_from_slice(&w[..n]);
            i += 1;
        }
        fence(Ordering::Acquire);
        if slot.version.load(Ordering::Relaxed) != v1 {
            return Err(ReadError::Gone);
        }
        Ok(len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_the_last_n_and_reports_the_rest_gone() {
        let h = History::new(4, 16);
        let mut out = [0u8; 16];
        assert_eq!(h.read(1, &mut out), Err(ReadError::NotYet));
        for s in 1..=6u64 {
            h.push(s, &s.to_le_bytes()[..(s as usize % 8) + 1]);
        }
        assert_eq!(h.earliest(), 3);
        assert_eq!(h.read(2, &mut out), Err(ReadError::Gone));
        assert_eq!(h.read(7, &mut out), Err(ReadError::NotYet));
        for s in 3..=6u64 {
            let n = h.read(s, &mut out).unwrap();
            assert_eq!(&out[..n], &s.to_le_bytes()[..(s as usize % 8) + 1]);
        }
    }
}
