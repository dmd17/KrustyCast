//! Several messages in one datagram (ARCHITECTURE §8.1): SUBMITs, or
//! outcomes, back to back, each with its own header. There is no batch
//! header: a datagram holding one message is the single-message format,
//! and each message decodes on its own (`Submit::decode`,
//! `Feedback::decode`). Like Aeron's frames in a datagram, a sender packs
//! what it has ready, up to the MTU, without waiting for more.

use crate::feedback::{self, Feedback};
use crate::ingress::HEADER_LEN;
use crate::{WireError, get_u16};

/// The SUBMITs in a datagram, as byte slices for `Submit::decode`, cut at
/// each one's payload length. A cut that does not fit (a truncated or
/// corrupt length) yields an error and ends the iteration.
pub fn submits(datagram: &[u8]) -> Submits<'_> {
    Submits { b: datagram, at: 0 }
}

pub struct Submits<'a> {
    b: &'a [u8],
    at: usize,
}

impl<'a> Iterator for Submits<'a> {
    type Item = Result<&'a [u8], WireError>;

    fn next(&mut self) -> Option<Self::Item> {
        let rest = &self.b[self.at..];
        if rest.is_empty() {
            return None;
        }
        if rest.len() < HEADER_LEN {
            self.at = self.b.len();
            return Some(Err(WireError::Truncated));
        }
        let n = HEADER_LEN + get_u16(rest, 12) as usize;
        if rest.len() < n {
            self.at = self.b.len();
            return Some(Err(WireError::Truncated));
        }
        self.at += n;
        Some(Ok(&rest[..n]))
    }
}

/// The outcomes in a datagram (fixed length each). A trailing piece
/// shorter than one outcome yields `Truncated`.
pub fn feedbacks(datagram: &[u8]) -> impl Iterator<Item = Result<Feedback, WireError>> + '_ {
    datagram.chunks(feedback::LEN).map(Feedback::decode)
}

/// Whether a message of `len` bytes still fits in a datagram holding
/// `used` bytes, under `mtu`.
pub fn fits(used: usize, len: usize, mtu: usize) -> bool {
    used + len <= mtu
}
