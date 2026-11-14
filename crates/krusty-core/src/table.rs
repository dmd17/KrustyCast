//! Fixed-capacity open-addressing map from `u64` keys to `u32` slot numbers
//! (linear probing, backward-shift deletion). Written in the subset Aeneas
//! translates: `Vec` indexing, `while` loops, no iterators or closures.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Bucket {
    key: u64,
    slot: u32,
}

#[derive(Clone, Debug)]
pub struct IdMap {
    buckets: Vec<Option<Bucket>>,
    /// `buckets.len() - 1`; the length is a power of two.
    mask: u64,
    len: usize,
    max_len: usize,
}

/// Fibonacci hashing: deterministic and cheap.
fn hash(key: u64) -> u64 {
    key.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 32
}

impl IdMap {
    /// A map that holds up to `max_len` entries at most half full.
    pub fn new(max_len: usize) -> IdMap {
        // Smallest power of two ≥ 2 · max_len (and ≥ 2).
        let mut n: usize = 2;
        while n / 2 < max_len {
            n *= 2;
        }
        IdMap {
            buckets: vec![None; n],
            mask: (n - 1) as u64,
            len: 0,
            max_len,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_full(&self) -> bool {
        self.len >= self.max_len
    }

    fn home(&self, key: u64) -> usize {
        (hash(key) & self.mask) as usize
    }

    fn next(&self, i: usize) -> usize {
        ((i as u64 + 1) & self.mask) as usize
    }

    /// The bucket holding `key`, or the empty bucket where it would go.
    /// Terminates because the table is never more than half full.
    fn probe(&self, key: u64) -> usize {
        let mut i = self.home(key);
        let mut searching = true;
        while searching {
            let b = self.buckets[i];
            match b {
                None => searching = false,
                Some(b) => {
                    if b.key == key {
                        searching = false;
                    } else {
                        i = self.next(i);
                    }
                }
            }
        }
        i
    }

    pub fn get(&self, key: u64) -> Option<u32> {
        let b = self.buckets[self.probe(key)];
        match b {
            Some(b) => Some(b.slot),
            None => None,
        }
    }

    /// Inserts or replaces. Returns false (and changes nothing) if `key` is
    /// new and the map is full.
    pub fn insert(&mut self, key: u64, slot: u32) -> bool {
        let i = self.probe(key);
        let present = self.buckets[i].is_some();
        if !present && self.is_full() {
            return false;
        }
        if !present {
            self.len += 1;
        }
        self.buckets[i] = Some(Bucket { key, slot });
        true
    }

    /// Removes `key` if present, shifting later entries of its probe run
    /// back so lookups never need tombstones.
    pub fn remove(&mut self, key: u64) {
        let mut hole = self.probe(key);
        if self.buckets[hole].is_none() {
            return;
        }
        self.buckets[hole] = None;
        self.len -= 1;
        let mut i = self.next(hole);
        let mut shifting = true;
        while shifting {
            let b = self.buckets[i];
            match b {
                None => shifting = false,
                Some(b) => {
                    // Move `b` into the hole if its home is not in (hole, i].
                    let home = self.home(b.key);
                    let dist_home = (i as u64).wrapping_sub(home as u64) & self.mask;
                    let dist_hole = (i as u64).wrapping_sub(hole as u64) & self.mask;
                    if dist_home >= dist_hole {
                        self.buckets[hole] = Some(b);
                        self.buckets[i] = None;
                        hole = i;
                    }
                    i = self.next(i);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn behaves_like_a_hash_map() {
        let mut m = IdMap::new(64);
        let mut reference = HashMap::new();
        let mut x: u64 = 12345;
        for step in 0..20_000u32 {
            x = x
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            // Few distinct keys so inserts, hits and removes all happen often;
            // multiples of 64 collide on purpose.
            let key = ((x >> 40) % 96) * 64;
            if x & 1 == 0 {
                let ok = m.insert(key, step);
                if reference.contains_key(&key) || reference.len() < 64 {
                    assert!(ok);
                    reference.insert(key, step);
                } else {
                    assert!(!ok);
                }
            } else {
                m.remove(key);
                reference.remove(&key);
            }
            assert_eq!(m.len(), reference.len());
            for k in 0..96u64 {
                assert_eq!(m.get(k * 64), reference.get(&(k * 64)).copied(), "key {k}");
            }
        }
    }
}
