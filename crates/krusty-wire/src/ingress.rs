//! Client → cluster submission (ARCHITECTURE §8.1).
//!
//! ```text
//! offset size field
//!  0     2    magic
//!  2     1    version
//!  3     1    type = SUBMIT
//!  4     2    flags (reserved, 0)
//!  6     1    mode: 0 = unconditional, 1 = conditional
//!  7     1    reserved, 0
//!  8     4    client_id
//! 12     2    payload length
//! 14     2    reserved, 0
//! 16     8    incarnation
//! 24     8    client_seqno (≥ 1)
//! 32     8    key               (conditional; 0 otherwise)
//! 40     8    expected_version  (conditional; 0 otherwise)
//! 48     len  payload
//! ```

use crate::{
    WireError, check_prefix, get_u16, get_u32, get_u64, msg_type, put_prefix, put_u16, put_u32,
    put_u64,
};

pub const HEADER_LEN: usize = 48;
/// Largest payload the format can carry. Nodes enforce a (smaller)
/// configured limit on top of this.
pub const MAX_PAYLOAD: usize = u16::MAX as usize;

const MODE_UNCONDITIONAL: u8 = 0;
const MODE_CONDITIONAL: u8 = 1;

/// How a submission is appended (PRD FR-3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Append {
    /// Always sequenced (unless a duplicate).
    Unconditional,
    /// Sequenced only if `key` is at `expected_version`, which it then
    /// advances by one; otherwise rejected with the current version.
    Conditional { key: u64, expected_version: u64 },
}

/// A decoded submission; `payload` borrows the datagram.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Submit<'a> {
    pub client_id: u32,
    pub incarnation: u64,
    pub client_seqno: u64,
    pub append: Append,
    pub payload: &'a [u8],
}

impl<'a> Submit<'a> {
    pub fn decode(b: &'a [u8]) -> Result<Self, WireError> {
        if b.len() < HEADER_LEN {
            return Err(WireError::Truncated);
        }
        check_prefix(b, msg_type::SUBMIT)?;
        if get_u16(b, 4) != 0 || b[7] != 0 || get_u16(b, 14) != 0 {
            return Err(WireError::BadField("reserved"));
        }
        let len = get_u16(b, 12) as usize;
        if b.len() < HEADER_LEN + len {
            return Err(WireError::Truncated);
        }
        if b.len() > HEADER_LEN + len {
            return Err(WireError::TrailingBytes);
        }
        let client_seqno = get_u64(b, 24);
        if client_seqno == 0 {
            return Err(WireError::BadField("client_seqno"));
        }
        let (key, expected_version) = (get_u64(b, 32), get_u64(b, 40));
        let append = match b[6] {
            MODE_UNCONDITIONAL => {
                if key != 0 || expected_version != 0 {
                    return Err(WireError::BadField("key"));
                }
                Append::Unconditional
            }
            MODE_CONDITIONAL => Append::Conditional {
                key,
                expected_version,
            },
            _ => return Err(WireError::BadField("mode")),
        };
        Ok(Submit {
            client_id: get_u32(b, 8),
            incarnation: get_u64(b, 16),
            client_seqno,
            append,
            payload: &b[HEADER_LEN..],
        })
    }

    /// Bytes needed to encode this submission.
    pub fn encoded_len(&self) -> usize {
        HEADER_LEN + self.payload.len()
    }

    /// Writes the submission to the start of `out`; returns its length.
    pub fn encode(&self, out: &mut [u8]) -> Result<usize, WireError> {
        if self.payload.len() > MAX_PAYLOAD {
            return Err(WireError::TooLarge);
        }
        if self.client_seqno == 0 {
            return Err(WireError::BadField("client_seqno"));
        }
        let n = self.encoded_len();
        if out.len() < n {
            return Err(WireError::BufferTooSmall);
        }
        let out = &mut out[..n];
        out[..HEADER_LEN].fill(0);
        put_prefix(out, msg_type::SUBMIT);
        put_u32(out, 8, self.client_id);
        put_u16(out, 12, self.payload.len() as u16);
        put_u64(out, 16, self.incarnation);
        put_u64(out, 24, self.client_seqno);
        if let Append::Conditional {
            key,
            expected_version,
        } = self.append
        {
            out[6] = MODE_CONDITIONAL;
            put_u64(out, 32, key);
            put_u64(out, 40, expected_version);
        }
        out[HEADER_LEN..].copy_from_slice(self.payload);
        Ok(n)
    }
}
