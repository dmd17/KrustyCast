//! The native stream (ARCHITECTURE §8.3): cluster → consumers, plus
//! RECOVER requests from consumers.
//!
//! Frame header (24 bytes):
//!
//! ```text
//! offset size field
//!  0     2    magic
//!  2     1    version
//!  3     1    type (DATA, HEARTBEAT, END_OF_SESSION, UNAVAILABLE, RECOVER)
//!  4     2    flags (bit 0: RETRANSMIT, DATA only)
//!  6     2    count (DATA: messages in the frame, ≥ 1; otherwise 0)
//!  8     8    stream_id
//! 16     8    seqno (DATA: first message; HEARTBEAT/END_OF_SESSION: next
//!             seqno; UNAVAILABLE/RECOVER: first seqno of the range)
//! ```
//!
//! DATA: `count` message blocks follow, each padded to a multiple of 8:
//!
//! ```text
//!  0     4    payload length
//!  4     4    flags (bit 0: CONDITIONAL)
//!  8     8    timestamp_ns (assigned by the leader; replicated, so deterministic)
//! 16     4    client_id
//! 20     4    reserved, 0
//! 24     8    key       (conditional; 0 otherwise)
//! 32     8    version   (the version `key` reached with this message; 0 if unconditional)
//! 40     len  payload, then zero padding to a multiple of 8
//! ```
//!
//! UNAVAILABLE: 8-byte body, the earliest seqno still retained.
//! RECOVER: 8-byte body, the number of messages wanted.
//! HEARTBEAT, END_OF_SESSION: no body.

use crate::{
    WireError, get_u16, get_u32, get_u64, msg_type, pad8, peek_type, put_prefix, put_u16, put_u32,
    put_u64,
};

pub const HEADER_LEN: usize = 24;
pub const BLOCK_HEADER_LEN: usize = 40;
/// Most messages in one DATA frame.
pub const MAX_COUNT: usize = u16::MAX as usize;

const FLAG_RETRANSMIT: u16 = 1;
const BLOCK_FLAG_CONDITIONAL: u32 = 1;

/// Metadata of one sequenced message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MessageMeta {
    pub timestamp_ns: u64,
    pub client_id: u32,
    /// `Some((key, version))` for a conditional append: the version `key`
    /// reached with this message.
    pub conditional: Option<(u64, u64)>,
}

/// One message of a DATA frame; `payload` borrows the datagram.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Message<'a> {
    pub seqno: u64,
    pub meta: MessageMeta,
    pub payload: &'a [u8],
}

/// A validated DATA frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DataFrame<'a> {
    pub stream_id: u64,
    pub seqno: u64,
    pub count: u16,
    pub retransmit: bool,
    blocks: &'a [u8],
}

impl<'a> DataFrame<'a> {
    /// The seqno after the last message in the frame.
    pub fn next_seqno(&self) -> u64 {
        // Cannot overflow: checked by `decode`.
        self.seqno + self.count as u64
    }

    pub fn messages(&self) -> Messages<'a> {
        Messages {
            rest: self.blocks,
            seqno: self.seqno,
            left: self.count,
        }
    }
}

/// Iterator over the messages of a validated DATA frame.
pub struct Messages<'a> {
    rest: &'a [u8],
    seqno: u64,
    left: u16,
}

impl<'a> Iterator for Messages<'a> {
    type Item = Message<'a>;

    fn next(&mut self) -> Option<Message<'a>> {
        if self.left == 0 {
            return None;
        }
        // Bounds were validated by `Frame::decode`.
        let (meta, len) = block_meta(self.rest);
        let payload = &self.rest[BLOCK_HEADER_LEN..BLOCK_HEADER_LEN + len];
        self.rest = &self.rest[pad8(BLOCK_HEADER_LEN + len)..];
        let m = Message {
            seqno: self.seqno,
            meta,
            payload,
        };
        self.seqno += 1;
        self.left -= 1;
        Some(m)
    }
}

fn block_meta(b: &[u8]) -> (MessageMeta, usize) {
    let len = get_u32(b, 0) as usize;
    let conditional = if get_u32(b, 4) & BLOCK_FLAG_CONDITIONAL != 0 {
        Some((get_u64(b, 24), get_u64(b, 32)))
    } else {
        None
    };
    (
        MessageMeta {
            timestamp_ns: get_u64(b, 8),
            client_id: get_u32(b, 16),
            conditional,
        },
        len,
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Frame<'a> {
    Data(DataFrame<'a>),
    Heartbeat {
        stream_id: u64,
        next_seqno: u64,
    },
    EndOfSession {
        stream_id: u64,
        next_seqno: u64,
    },
    /// Messages from `from` on are no longer retained before `earliest_retained`.
    Unavailable {
        stream_id: u64,
        from: u64,
        earliest_retained: u64,
    },
    /// Consumer request: resend `count` messages starting at `from`.
    Recover {
        stream_id: u64,
        from: u64,
        count: u64,
    },
}

impl<'a> Frame<'a> {
    pub fn decode(b: &'a [u8]) -> Result<Self, WireError> {
        if b.len() < HEADER_LEN {
            return Err(WireError::Truncated);
        }
        let t = peek_type(b)?;
        let flags = get_u16(b, 4);
        let count = get_u16(b, 6);
        let stream_id = get_u64(b, 8);
        let seqno = get_u64(b, 16);
        if seqno == 0 {
            return Err(WireError::BadField("seqno"));
        }
        if t != msg_type::DATA && (flags != 0 || count != 0) {
            return Err(WireError::BadField("flags"));
        }
        let body = &b[HEADER_LEN..];
        let exact = |n: usize| match body.len() {
            l if l < n => Err(WireError::Truncated),
            l if l > n => Err(WireError::TrailingBytes),
            _ => Ok(()),
        };
        match t {
            msg_type::DATA => {
                if flags & !FLAG_RETRANSMIT != 0 {
                    return Err(WireError::BadField("flags"));
                }
                if count == 0 {
                    return Err(WireError::BadField("count"));
                }
                if seqno.checked_add(count as u64).is_none() {
                    return Err(WireError::BadField("seqno"));
                }
                validate_blocks(body, count)?;
                Ok(Frame::Data(DataFrame {
                    stream_id,
                    seqno,
                    count,
                    retransmit: flags & FLAG_RETRANSMIT != 0,
                    blocks: body,
                }))
            }
            msg_type::HEARTBEAT => {
                exact(0)?;
                Ok(Frame::Heartbeat {
                    stream_id,
                    next_seqno: seqno,
                })
            }
            msg_type::END_OF_SESSION => {
                exact(0)?;
                Ok(Frame::EndOfSession {
                    stream_id,
                    next_seqno: seqno,
                })
            }
            msg_type::UNAVAILABLE => {
                exact(8)?;
                Ok(Frame::Unavailable {
                    stream_id,
                    from: seqno,
                    earliest_retained: get_u64(body, 0),
                })
            }
            msg_type::RECOVER => {
                exact(8)?;
                let want = get_u64(body, 0);
                if want == 0 || seqno.checked_add(want).is_none() {
                    return Err(WireError::BadField("count"));
                }
                Ok(Frame::Recover {
                    stream_id,
                    from: seqno,
                    count: want,
                })
            }
            other => Err(WireError::UnknownType(other)),
        }
    }
}

fn validate_blocks(mut rest: &[u8], count: u16) -> Result<(), WireError> {
    for _ in 0..count {
        if rest.len() < BLOCK_HEADER_LEN {
            return Err(WireError::Truncated);
        }
        let len = get_u32(rest, 0) as usize;
        let flags = get_u32(rest, 4);
        if flags & !BLOCK_FLAG_CONDITIONAL != 0 || get_u32(rest, 20) != 0 {
            return Err(WireError::BadField("block flags"));
        }
        if flags & BLOCK_FLAG_CONDITIONAL == 0 && (get_u64(rest, 24) != 0 || get_u64(rest, 32) != 0)
        {
            return Err(WireError::BadField("key"));
        }
        // `len` is at most u32::MAX, so this cannot overflow on 64-bit
        // targets; on 32-bit, a huge `len` fails the length check below.
        let padded = match BLOCK_HEADER_LEN.checked_add(len) {
            Some(n) if n <= rest.len() => pad8(n),
            _ => return Err(WireError::Truncated),
        };
        if padded > rest.len() {
            return Err(WireError::Truncated);
        }
        if rest[BLOCK_HEADER_LEN + len..padded].iter().any(|&x| x != 0) {
            return Err(WireError::BadField("padding"));
        }
        rest = &rest[padded..];
    }
    if !rest.is_empty() {
        return Err(WireError::TrailingBytes);
    }
    Ok(())
}

/// Builds a DATA frame in a caller-provided buffer (no allocation).
pub struct DataFrameWriter<'b> {
    buf: &'b mut [u8],
    len: usize,
    count: u16,
    seqno: u64,
}

impl<'b> DataFrameWriter<'b> {
    /// Starts a frame whose first message has `seqno` (≥ 1).
    pub fn new(
        buf: &'b mut [u8],
        stream_id: u64,
        seqno: u64,
        retransmit: bool,
    ) -> Result<Self, WireError> {
        if buf.len() < HEADER_LEN {
            return Err(WireError::BufferTooSmall);
        }
        if seqno == 0 {
            return Err(WireError::BadField("seqno"));
        }
        buf[..HEADER_LEN].fill(0);
        put_prefix(buf, msg_type::DATA);
        put_u16(buf, 4, if retransmit { FLAG_RETRANSMIT } else { 0 });
        put_u64(buf, 8, stream_id);
        put_u64(buf, 16, seqno);
        Ok(DataFrameWriter {
            buf,
            len: HEADER_LEN,
            count: 0,
            seqno,
        })
    }

    /// Bytes one message with this payload takes in a frame.
    pub fn block_len(payload_len: usize) -> usize {
        pad8(BLOCK_HEADER_LEN + payload_len)
    }

    pub fn count(&self) -> u16 {
        self.count
    }

    /// Appends the next message. Fails without changing the frame if it
    /// does not fit or the frame is full.
    pub fn push(&mut self, meta: &MessageMeta, payload: &[u8]) -> Result<(), WireError> {
        if payload.len() > u32::MAX as usize {
            return Err(WireError::TooLarge);
        }
        let n = Self::block_len(payload.len());
        if self.count as usize == MAX_COUNT
            || self.seqno.checked_add(self.count as u64 + 1).is_none()
        {
            return Err(WireError::TooLarge);
        }
        if self.buf.len() - self.len < n {
            return Err(WireError::BufferTooSmall);
        }
        let b = &mut self.buf[self.len..self.len + n];
        b.fill(0);
        put_u32(b, 0, payload.len() as u32);
        put_u64(b, 8, meta.timestamp_ns);
        put_u32(b, 16, meta.client_id);
        if let Some((key, version)) = meta.conditional {
            put_u32(b, 4, BLOCK_FLAG_CONDITIONAL);
            put_u64(b, 24, key);
            put_u64(b, 32, version);
        }
        b[BLOCK_HEADER_LEN..BLOCK_HEADER_LEN + payload.len()].copy_from_slice(payload);
        self.len += n;
        self.count += 1;
        Ok(())
    }

    /// Completes the frame; returns its length. A frame needs ≥ 1 message.
    pub fn finish(self) -> Result<usize, WireError> {
        if self.count == 0 {
            return Err(WireError::BadField("count"));
        }
        put_u16(self.buf, 6, self.count);
        Ok(self.len)
    }
}

fn header_only(
    out: &mut [u8],
    t: u8,
    stream_id: u64,
    seqno: u64,
    body: Option<u64>,
) -> Result<usize, WireError> {
    let n = HEADER_LEN + if body.is_some() { 8 } else { 0 };
    if out.len() < n {
        return Err(WireError::BufferTooSmall);
    }
    if seqno == 0 {
        return Err(WireError::BadField("seqno"));
    }
    out[..n].fill(0);
    put_prefix(out, t);
    put_u64(out, 8, stream_id);
    put_u64(out, 16, seqno);
    if let Some(v) = body {
        put_u64(out, HEADER_LEN, v);
    }
    Ok(n)
}

pub fn encode_heartbeat(
    out: &mut [u8],
    stream_id: u64,
    next_seqno: u64,
) -> Result<usize, WireError> {
    header_only(out, msg_type::HEARTBEAT, stream_id, next_seqno, None)
}

pub fn encode_end_of_session(
    out: &mut [u8],
    stream_id: u64,
    next_seqno: u64,
) -> Result<usize, WireError> {
    header_only(out, msg_type::END_OF_SESSION, stream_id, next_seqno, None)
}

pub fn encode_unavailable(
    out: &mut [u8],
    stream_id: u64,
    from: u64,
    earliest_retained: u64,
) -> Result<usize, WireError> {
    header_only(
        out,
        msg_type::UNAVAILABLE,
        stream_id,
        from,
        Some(earliest_retained),
    )
}

pub fn encode_recover(
    out: &mut [u8],
    stream_id: u64,
    from: u64,
    count: u64,
) -> Result<usize, WireError> {
    if count == 0 || from.checked_add(count).is_none() {
        return Err(WireError::BadField("count"));
    }
    header_only(out, msg_type::RECOVER, stream_id, from, Some(count))
}
