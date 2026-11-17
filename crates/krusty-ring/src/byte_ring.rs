//! SPSC ring of variable-length records.
//!
//! Layout: a power-of-two byte buffer. Each record is an 8-byte header
//! (`len: u32`, `kind: u32`) followed by `len` bytes, padded to a multiple
//! of 8. A record never wraps: if it does not fit before the end, a padding
//! record fills the rest and the record starts at offset 0.
//!
//! Positions (`head`, `tail`) are byte counts that only grow; the offset is
//! `position & mask`. The producer owns `[tail, head + capacity)`, the
//! consumer owns `[head, tail)`; each publishes its position with a release
//! store and reads the other's with an acquire load. That hand-off is what
//! makes the `unsafe` buffer accesses below race-free.

use crate::CachePadded;
use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const HEADER: usize = 8;
const KIND_PADDING: u32 = u32::MAX;

struct Shared {
    buf: Box<[UnsafeCell<u8>]>,
    mask: u64,
    /// Consumer position (bytes consumed).
    head: CachePadded<AtomicU64>,
    /// Producer position (bytes published).
    tail: CachePadded<AtomicU64>,
}

// SAFETY: the buffer is only accessed through `Producer` and `Consumer`,
// each of which exists once and touches only the byte range the protocol
// gives it (see the module docs).
unsafe impl Sync for Shared {}
unsafe impl Send for Shared {}

impl Shared {
    fn cap(&self) -> u64 {
        self.mask + 1
    }

    /// Mutable view of `len` bytes at `offset`.
    ///
    /// # Safety
    /// The caller must own `[offset, offset + len)` under the ring protocol,
    /// so no other reference to these bytes exists.
    #[allow(clippy::mut_from_ref)]
    unsafe fn bytes_mut(&self, offset: usize, len: usize) -> &mut [u8] {
        debug_assert!(offset + len <= self.buf.len());
        let p = self.buf[offset..offset + len].as_ptr() as *mut u8;
        // SAFETY: `UnsafeCell<u8>` has the layout of `u8`; exclusive access is
        // the caller's obligation.
        unsafe { std::slice::from_raw_parts_mut(p, len) }
    }

    /// # Safety
    /// The caller must own `[offset, offset + len)` (no concurrent writer).
    unsafe fn bytes(&self, offset: usize, len: usize) -> &[u8] {
        debug_assert!(offset + len <= self.buf.len());
        let p = self.buf[offset..offset + len].as_ptr() as *const u8;
        // SAFETY: as above, for shared reads.
        unsafe { std::slice::from_raw_parts(p, len) }
    }
}

/// Creates a ring of `capacity` bytes (a power of two, ≥ 64).
pub fn byte_ring(capacity: usize) -> (Producer, Consumer) {
    assert!(capacity.is_power_of_two() && capacity >= 64);
    let buf: Box<[UnsafeCell<u8>]> = (0..capacity).map(|_| UnsafeCell::new(0)).collect();
    let shared = Arc::new(Shared {
        buf,
        mask: capacity as u64 - 1,
        head: CachePadded(AtomicU64::new(0)),
        tail: CachePadded(AtomicU64::new(0)),
    });
    (
        Producer {
            shared: shared.clone(),
            tail: 0,
            cached_head: 0,
        },
        Consumer {
            shared,
            head: 0,
            cached_tail: 0,
        },
    )
}

fn padded(len: usize) -> u64 {
    ((HEADER + len + 7) & !7) as u64
}

pub struct Producer {
    shared: Arc<Shared>,
    /// Local tail: written but maybe not yet published.
    tail: u64,
    cached_head: u64,
}

impl Producer {
    /// Largest record payload that can ever fit.
    pub fn max_record(&self) -> usize {
        (self.shared.cap() / 2) as usize - HEADER
    }

    fn has_room(&mut self, need: u64) -> bool {
        if self.tail + need - self.cached_head <= self.shared.cap() {
            return true;
        }
        self.cached_head = self.shared.head.load(Ordering::Acquire);
        self.tail + need - self.cached_head <= self.shared.cap()
    }

    /// Writes a record of `len` bytes and `kind` with `fill`, which gets the
    /// record's bytes. Returns false (writing nothing) if the ring is full.
    /// The record is visible to the consumer after [`Producer::publish`].
    pub fn try_write(&mut self, kind: u32, len: usize, fill: impl FnOnce(&mut [u8])) -> bool {
        assert!(len <= self.max_record() && kind != KIND_PADDING);
        let need = padded(len);
        let cap = self.shared.cap();
        let offset = self.tail & self.shared.mask;
        let to_end = cap - offset;
        let pad = if need > to_end { to_end } else { 0 };
        if !self.has_room(pad + need) {
            return false;
        }
        if pad > 0 {
            // SAFETY: [tail, tail + pad + need) is free: `has_room` checked it
            // against the consumer's published head, and only we write here.
            let h = unsafe { self.shared.bytes_mut(offset as usize, HEADER) };
            h[..4].copy_from_slice(&((pad as usize - HEADER) as u32).to_le_bytes());
            h[4..].copy_from_slice(&KIND_PADDING.to_le_bytes());
            self.tail += pad;
        }
        let offset = (self.tail & self.shared.mask) as usize;
        // SAFETY: as above; the record lies within the buffer because it does
        // not cross the end (padding was inserted otherwise).
        let rec = unsafe { self.shared.bytes_mut(offset, need as usize) };
        rec[..4].copy_from_slice(&(len as u32).to_le_bytes());
        rec[4..8].copy_from_slice(&kind.to_le_bytes());
        fill(&mut rec[HEADER..HEADER + len]);
        self.tail += need;
        true
    }

    /// Bytes free now (written records count as used until consumed), and
    /// the ring's capacity: for admission control, so a producer can stop
    /// taking new work before it would have to wait.
    pub fn free(&mut self) -> usize {
        self.cached_head = self.shared.head.load(Ordering::Acquire);
        (self.shared.cap() - (self.tail - self.cached_head)) as usize
    }

    pub fn capacity(&self) -> usize {
        self.shared.cap() as usize
    }

    /// Makes everything written so far visible to the consumer.
    pub fn publish(&mut self) {
        self.shared.tail.store(self.tail, Ordering::Release);
    }

    /// Bytes written but not yet published.
    pub fn unpublished(&self) -> bool {
        self.tail != self.shared.tail.load(Ordering::Relaxed)
    }
}

pub struct Consumer {
    shared: Arc<Shared>,
    head: u64,
    cached_tail: u64,
}

impl Consumer {
    /// Calls `f(kind, bytes)` for up to `max` published records, then frees
    /// their space. Returns how many it read.
    pub fn read(&mut self, max: usize, mut f: impl FnMut(u32, &[u8])) -> usize {
        let start = self.head;
        let mut n = 0;
        let mut refreshed = false;
        while n < max {
            if self.head == self.cached_tail {
                if refreshed {
                    break;
                }
                // Out of known records: look once for newly published ones.
                self.cached_tail = self.shared.tail.load(Ordering::Acquire);
                refreshed = true;
                if self.head == self.cached_tail {
                    break;
                }
            }
            let offset = (self.head & self.shared.mask) as usize;
            // SAFETY: [head, cached_tail) was published by the producer (the
            // acquire load above pairs with its release store) and is not
            // reused until we publish a head past it.
            let h = unsafe { self.shared.bytes(offset, HEADER) };
            let len = u32::from_le_bytes(h[..4].try_into().unwrap()) as usize;
            let kind = u32::from_le_bytes(h[4..8].try_into().unwrap());
            if kind != KIND_PADDING {
                // SAFETY: as above; the producer wrote the whole record.
                let body = unsafe { self.shared.bytes(offset + HEADER, len) };
                f(kind, body);
                n += 1;
            }
            self.head += padded(len);
        }
        if self.head != start {
            self.shared.head.store(self.head, Ordering::Release);
        }
        n
    }

    /// True if nothing is published and unread.
    pub fn is_empty(&mut self) -> bool {
        if self.head == self.cached_tail {
            self.cached_tail = self.shared.tail.load(Ordering::Acquire);
        }
        self.head == self.cached_tail
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_round_trip_and_wrap() {
        let (mut p, mut c) = byte_ring(256);
        let mut got = Vec::new();
        for round in 0..200u32 {
            let len = (round as usize * 7) % 50;
            assert!(p.try_write(round, len, |b| b.fill(round as u8)));
            p.publish();
            c.read(10, |k, b| got.push((k, b.to_vec())));
        }
        assert_eq!(got.len(), 200);
        for (i, (k, b)) in got.iter().enumerate() {
            assert_eq!(*k, i as u32);
            assert_eq!(b.len(), (i * 7) % 50);
            assert!(b.iter().all(|&x| x == i as u8));
        }
    }

    #[test]
    fn full_ring_refuses_and_unpublished_records_are_invisible() {
        let (mut p, mut c) = byte_ring(64);
        assert!(p.try_write(1, 8, |_| {})); // 16 bytes
        assert!(p.try_write(2, 8, |_| {}));
        assert!(p.try_write(3, 8, |_| {}));
        assert!(p.try_write(4, 8, |_| {}));
        assert!(!p.try_write(5, 8, |_| {})); // 64 bytes used
        assert_eq!(c.read(10, |_, _| {}), 0); // nothing published yet
        p.publish();
        assert_eq!(c.read(2, |_, _| {}), 2);
        assert!(p.try_write(5, 8, |_| {}));
        p.publish();
        let mut kinds = Vec::new();
        c.read(10, |k, _| kinds.push(k));
        assert_eq!(kinds, [3, 4, 5]);
    }
}
