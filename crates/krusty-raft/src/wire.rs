//! Raft message encoding between nodes (little-endian, versioned like the
//! rest of the native formats).
//!
//! `magic u16 | version u8 | type u8 | from u32 | term u64 | a u64 | b u64 |
//! c u64 | d u64 | e u64` (56 bytes), then for Append `count` entries of
//! `term u64 | len u32 | pad u32 | data`, each padded to 8 bytes.

use crate::{Entries, Message, NodeId};

pub const MAGIC: u16 = 0x524B; // "KR"
/// 2 (2026-11-23): Append carries the leader's last index (`e`).
pub const VERSION: u8 = 2;
pub const HEADER_LEN: usize = 56;
const ENTRY_HEADER: usize = 16;

const T_PREVOTE: u8 = 1;
const T_PREVOTE_REPLY: u8 = 2;
const T_VOTE: u8 = 3;
const T_VOTE_REPLY: u8 = 4;
const T_APPEND: u8 = 5;
const T_APPEND_REPLY: u8 = 6;
const T_TIMEOUT_NOW: u8 = 7;

fn put(b: &mut [u8], at: usize, v: u64) {
    b[at..at + 8].copy_from_slice(&v.to_le_bytes());
}

fn get(b: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(b[at..at + 8].try_into().unwrap())
}

fn pad8(n: usize) -> usize {
    (n + 7) & !7
}

/// Bytes an entry with `len` bytes of data takes in an Append.
pub fn entry_len(len: usize) -> usize {
    ENTRY_HEADER + pad8(len)
}

/// Writes the header of `m`; for Append, then call [`push_entry`] `count`
/// times. Returns the header length.
pub fn encode(out: &mut [u8], from: NodeId, m: &Message) -> usize {
    let (t, term, a, b, c, d, e) = match *m {
        Message::PreVote {
            term,
            last_index,
            last_term,
        } => (T_PREVOTE, term, last_index, last_term, 0, 0, 0),
        Message::PreVoteReply { term, granted } => {
            (T_PREVOTE_REPLY, term, granted as u64, 0, 0, 0, 0)
        }
        Message::Vote {
            term,
            last_index,
            last_term,
        } => (T_VOTE, term, last_index, last_term, 0, 0, 0),
        Message::VoteReply { term, granted } => (T_VOTE_REPLY, term, granted as u64, 0, 0, 0, 0),
        Message::Append {
            term,
            prev_index,
            prev_term,
            count,
            commit,
            last,
        } => (T_APPEND, term, prev_index, prev_term, count, commit, last),
        Message::AppendReply {
            term,
            success,
            index,
        } => (T_APPEND_REPLY, term, success as u64, index, 0, 0, 0),
        Message::TimeoutNow { term } => (T_TIMEOUT_NOW, term, 0, 0, 0, 0, 0),
    };
    out[..HEADER_LEN].fill(0);
    out[0..2].copy_from_slice(&MAGIC.to_le_bytes());
    out[2] = VERSION;
    out[3] = t;
    out[4..8].copy_from_slice(&from.to_le_bytes());
    put(out, 8, term);
    put(out, 16, a);
    put(out, 24, b);
    put(out, 32, c);
    put(out, 40, d);
    put(out, 48, e);
    HEADER_LEN
}

/// Appends one entry at `at`; returns the new end.
pub fn push_entry(out: &mut [u8], at: usize, term: u64, data: &[u8]) -> usize {
    put(out, at, term);
    out[at + 8..at + 12].copy_from_slice(&(data.len() as u32).to_le_bytes());
    out[at + 12..at + 16].fill(0);
    out[at + ENTRY_HEADER..at + ENTRY_HEADER + data.len()].copy_from_slice(data);
    let end = at + entry_len(data.len());
    out[at + ENTRY_HEADER + data.len()..end].fill(0);
    end
}

/// A decoded Append's entries, as a view over the datagram.
pub struct EntriesView<'a> {
    bytes: &'a [u8],
    /// Offset of each entry (validated at decode); at most `MAX_ENTRIES`.
    offsets: Vec<u32>,
}

impl EntriesView<'_> {
    pub fn empty() -> EntriesView<'static> {
        EntriesView {
            bytes: &[],
            offsets: Vec::new(),
        }
    }
}

impl Entries for EntriesView<'_> {
    fn len(&self) -> u64 {
        self.offsets.len() as u64
    }
    fn term(&self, k: u64) -> u64 {
        get(self.bytes, self.offsets[k as usize] as usize)
    }
    fn data(&self, k: u64) -> &[u8] {
        let at = self.offsets[k as usize] as usize;
        let len = u32::from_le_bytes(self.bytes[at + 8..at + 12].try_into().unwrap()) as usize;
        &self.bytes[at + ENTRY_HEADER..at + ENTRY_HEADER + len]
    }
}

/// Decodes a datagram into (sender, message, entries). `offsets` is reused
/// to avoid allocating per message.
pub fn decode<'a>(
    b: &'a [u8],
    mut offsets: Vec<u32>,
) -> Option<(NodeId, Message, EntriesView<'a>)> {
    if b.len() < HEADER_LEN || u16::from_le_bytes([b[0], b[1]]) != MAGIC || b[2] != VERSION {
        return None;
    }
    let from = u32::from_le_bytes(b[4..8].try_into().unwrap());
    let (term, a, bb, c, d) = (get(b, 8), get(b, 16), get(b, 24), get(b, 32), get(b, 40));
    let e = get(b, 48);
    let flag = |v: u64| match v {
        0 => Some(false),
        1 => Some(true),
        _ => None,
    };
    let m = match b[3] {
        T_PREVOTE => Message::PreVote {
            term,
            last_index: a,
            last_term: bb,
        },
        T_PREVOTE_REPLY => Message::PreVoteReply {
            term,
            granted: flag(a)?,
        },
        T_VOTE => Message::Vote {
            term,
            last_index: a,
            last_term: bb,
        },
        T_VOTE_REPLY => Message::VoteReply {
            term,
            granted: flag(a)?,
        },
        T_APPEND => Message::Append {
            term,
            prev_index: a,
            prev_term: bb,
            count: c,
            commit: d,
            last: e,
        },
        T_APPEND_REPLY => Message::AppendReply {
            term,
            success: flag(a)?,
            index: bb,
        },
        T_TIMEOUT_NOW => Message::TimeoutNow { term },
        _ => return None,
    };
    offsets.clear();
    let mut at = HEADER_LEN;
    if let Message::Append { count, .. } = m {
        for _ in 0..count {
            if b.len() < at + ENTRY_HEADER {
                return None;
            }
            let len = u32::from_le_bytes(b[at + 8..at + 12].try_into().unwrap()) as usize;
            let end = at.checked_add(entry_len(len))?;
            if end > b.len() || at > u32::MAX as usize {
                return None;
            }
            offsets.push(at as u32);
            at = end;
        }
    }
    if at != b.len() {
        return None;
    }
    Some((from, m, EntriesView { bytes: b, offsets }))
}

impl<'a> EntriesView<'a> {
    /// Returns the offsets buffer for reuse.
    pub fn into_offsets(self) -> Vec<u32> {
        self.offsets
    }
}
