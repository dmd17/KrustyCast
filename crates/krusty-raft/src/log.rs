//! The in-memory window of the Raft log: entries `first..=last` with their
//! terms, plus the term of the entry just before the window. Buffers of
//! removed entries are reused, so steady-state appends do not allocate.

use std::collections::VecDeque;

pub struct RaftLog {
    /// Index of `entries[0]`.
    first: u64,
    /// Term of entry `first - 1` (0 if `first == 1`).
    base_term: u64,
    entries: VecDeque<(u64, Vec<u8>)>,
    free: Vec<Vec<u8>>,
}

impl RaftLog {
    /// An empty log whose next entry is `after + 1`; `after_term` is the
    /// term of entry `after` (e.g. the last one in a snapshot).
    pub fn new(after: u64, after_term: u64) -> RaftLog {
        RaftLog {
            first: after + 1,
            base_term: after_term,
            entries: VecDeque::new(),
            free: Vec::new(),
        }
    }

    pub fn last_index(&self) -> u64 {
        self.first + self.entries.len() as u64 - 1
    }

    pub fn last_term(&self) -> u64 {
        match self.entries.back() {
            Some((t, _)) => *t,
            None => self.base_term,
        }
    }

    /// First index still in memory.
    pub fn first_index(&self) -> u64 {
        self.first
    }

    pub fn term_at(&self, i: u64) -> Option<u64> {
        if i == 0 {
            return Some(0);
        }
        if i + 1 == self.first {
            return Some(self.base_term);
        }
        if i < self.first {
            return None;
        }
        self.entries.get((i - self.first) as usize).map(|(t, _)| *t)
    }

    pub fn get(&self, i: u64) -> Option<(u64, &[u8])> {
        if i < self.first {
            return None;
        }
        self.entries
            .get((i - self.first) as usize)
            .map(|(t, d)| (*t, d.as_slice()))
    }

    /// Appends an entry; returns its index.
    pub fn append(&mut self, term: u64, data: &[u8]) -> u64 {
        let mut buf = self.free.pop().unwrap_or_default();
        buf.clear();
        buf.extend_from_slice(data);
        self.entries.push_back((term, buf));
        self.last_index()
    }

    /// Removes entries `i..`.
    pub fn truncate_from(&mut self, i: u64) {
        assert!(i >= self.first, "cannot truncate below the window");
        while self.last_index() >= i {
            let (_, b) = self.entries.pop_back().expect("non-empty");
            self.free.push(b);
        }
    }

    /// Drops entries so that at most `retain` entries at or below `upto`
    /// stay in memory.
    pub fn compact(&mut self, upto: u64, retain: u64) {
        let keep_from = upto.saturating_sub(retain) + 1;
        while self.first < keep_from && !self.entries.is_empty() {
            let (t, b) = self.entries.pop_front().expect("non-empty");
            self.free.push(b);
            self.base_term = t;
            self.first += 1;
        }
    }

    /// The index just before the run of entries with term `t` that ends at
    /// `i` (for conflict backtracking).
    pub fn first_index_of_term_before(&self, i: u64, t: u64) -> u64 {
        let mut j = i;
        while j >= self.first && self.term_at(j) == Some(t) {
            j -= 1;
        }
        j
    }
}
