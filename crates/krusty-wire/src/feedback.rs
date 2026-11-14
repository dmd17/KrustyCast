//! Cluster → client outcomes (ARCHITECTURE §8.1). Fixed 48 bytes.
//!
//! ```text
//! offset size field
//!  0     2    magic
//!  2     1    version
//!  3     1    type (ACCEPTED, REJECTED, NAK, BUSY, NOT_LEADER, SESSION_EXPIRED)
//!  4     4    reserved, 0
//!  8     4    client_id
//! 12     4    reserved, 0
//! 16     8    incarnation   (the session the outcome belongs to)
//! 24     8    client_seqno  (the message it answers; for NAK, the expected one)
//! 32     8    a             (meaning depends on type, see `Outcome`)
//! 40     8    b
//! ```
//!
//! Carrying the incarnation lets a restarted client ignore outcomes meant
//! for its previous incarnation.

use crate::{WireError, get_u32, get_u64, msg_type, peek_type, put_prefix, put_u32, put_u64};

pub const LEN: usize = 48;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Sequenced as `global_seqno` (a = global_seqno).
    Accepted { global_seqno: u64 },
    /// Conditional append lost: `key` is at `current_version` (a, b).
    Rejected { key: u64, current_version: u64 },
    /// Gap in this session: resend from `client_seqno` (the expected one).
    Nak,
    /// Back pressure; retry after the given delay (a = microseconds).
    Busy { retry_after_us: u64 },
    /// Not the leader; `leader` is the node to try (a; 0 = unknown).
    NotLeader { leader: u64 },
    /// The session's dedup state is gone. Restart with an incarnation of at
    /// least `min_incarnation` (a).
    SessionExpired { min_incarnation: u64 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Feedback {
    pub client_id: u32,
    pub incarnation: u64,
    pub client_seqno: u64,
    pub outcome: Outcome,
}

impl Feedback {
    pub fn decode(b: &[u8]) -> Result<Self, WireError> {
        if b.len() < LEN {
            return Err(WireError::Truncated);
        }
        if b.len() > LEN {
            return Err(WireError::TrailingBytes);
        }
        let t = peek_type(b)?;
        if get_u32(b, 4) != 0 || get_u32(b, 12) != 0 {
            return Err(WireError::BadField("reserved"));
        }
        let (a, bb) = (get_u64(b, 32), get_u64(b, 40));
        let outcome = match t {
            msg_type::ACCEPTED => Outcome::Accepted { global_seqno: a },
            msg_type::REJECTED => Outcome::Rejected {
                key: a,
                current_version: bb,
            },
            msg_type::NAK => Outcome::Nak,
            msg_type::BUSY => Outcome::Busy { retry_after_us: a },
            msg_type::NOT_LEADER => Outcome::NotLeader { leader: a },
            msg_type::SESSION_EXPIRED => Outcome::SessionExpired { min_incarnation: a },
            other => return Err(WireError::UnknownType(other)),
        };
        // Fields a type does not use must be zero, so each value has one encoding.
        let used_b = matches!(outcome, Outcome::Rejected { .. });
        let used_a = !matches!(outcome, Outcome::Nak);
        if (!used_a && a != 0) || (!used_b && bb != 0) {
            return Err(WireError::BadField("unused"));
        }
        Ok(Feedback {
            client_id: get_u32(b, 8),
            incarnation: get_u64(b, 16),
            client_seqno: get_u64(b, 24),
            outcome,
        })
    }

    /// Writes the outcome to the start of `out`; returns `LEN`.
    pub fn encode(&self, out: &mut [u8]) -> Result<usize, WireError> {
        if out.len() < LEN {
            return Err(WireError::BufferTooSmall);
        }
        let out = &mut out[..LEN];
        out.fill(0);
        let (t, a, b) = match self.outcome {
            Outcome::Accepted { global_seqno } => (msg_type::ACCEPTED, global_seqno, 0),
            Outcome::Rejected {
                key,
                current_version,
            } => (msg_type::REJECTED, key, current_version),
            Outcome::Nak => (msg_type::NAK, 0, 0),
            Outcome::Busy { retry_after_us } => (msg_type::BUSY, retry_after_us, 0),
            Outcome::NotLeader { leader } => (msg_type::NOT_LEADER, leader, 0),
            Outcome::SessionExpired { min_incarnation } => {
                (msg_type::SESSION_EXPIRED, min_incarnation, 0)
            }
        };
        put_prefix(out, t);
        put_u32(out, 8, self.client_id);
        put_u64(out, 16, self.incarnation);
        put_u64(out, 24, self.client_seqno);
        put_u64(out, 32, a);
        put_u64(out, 40, b);
        Ok(LEN)
    }
}
