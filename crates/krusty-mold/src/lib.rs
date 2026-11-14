//! MoldUDP64 (NASDAQ) codec for the adapter (ARCHITECTURE §8.4, ADR-11).
//!
//! Downstream packet: `session [u8; 10] | seqno u64 | count u16` (big
//! endian), then `count × (len u16 | data)`. `count` 0 is a heartbeat and
//! 0xFFFF is end of session. Request packet (to the re-request server):
//! `session | seqno u64 | count u16`.
//!
//! No allocation: packets are written into caller buffers, decoded as views.
#![no_std]
#![forbid(unsafe_code)]

pub const SESSION_LEN: usize = 10;
pub const HEADER_LEN: usize = 20;
pub const REQUEST_LEN: usize = 20;
const HEARTBEAT: u16 = 0;
const END_OF_SESSION: u16 = 0xFFFF;
/// Most messages in one packet (0xFFFF means end of session).
pub const MAX_COUNT: usize = 0xFFFE;

/// A MoldUDP64 session name: 10 printable ASCII bytes, space padded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Session(pub [u8; SESSION_LEN]);

impl Session {
    pub fn new(name: &str) -> Option<Session> {
        let b = name.as_bytes();
        if b.is_empty() || b.len() > SESSION_LEN || !b.iter().all(|c| c.is_ascii_graphic()) {
            return None;
        }
        let mut s = [b' '; SESSION_LEN];
        s[..b.len()].copy_from_slice(b);
        Some(Session(s))
    }
}

fn header(out: &mut [u8], session: Session, seqno: u64, count: u16) {
    out[..SESSION_LEN].copy_from_slice(&session.0);
    out[10..18].copy_from_slice(&seqno.to_be_bytes());
    out[18..20].copy_from_slice(&count.to_be_bytes());
}

/// Builds a message packet in a caller buffer (at most `buf.len()` bytes).
pub struct PacketWriter<'b> {
    buf: &'b mut [u8],
    len: usize,
    count: usize,
}

impl<'b> PacketWriter<'b> {
    /// Starts a packet whose first message has `seqno`.
    pub fn new(buf: &'b mut [u8], session: Session, seqno: u64) -> Option<PacketWriter<'b>> {
        if buf.len() < HEADER_LEN {
            return None;
        }
        header(buf, session, seqno, 0);
        Some(PacketWriter {
            buf,
            len: HEADER_LEN,
            count: 0,
        })
    }

    pub fn count(&self) -> usize {
        self.count
    }

    /// Appends a message; false if it does not fit (or is over 65535 bytes).
    pub fn push(&mut self, data: &[u8]) -> bool {
        if data.len() > u16::MAX as usize
            || self.count == MAX_COUNT
            || self.buf.len() - self.len < 2 + data.len()
        {
            return false;
        }
        self.buf[self.len..self.len + 2].copy_from_slice(&(data.len() as u16).to_be_bytes());
        self.buf[self.len + 2..self.len + 2 + data.len()].copy_from_slice(data);
        self.len += 2 + data.len();
        self.count += 1;
        true
    }

    /// Writes the count; returns the packet length.
    pub fn finish(self) -> usize {
        self.buf[18..20].copy_from_slice(&(self.count as u16).to_be_bytes());
        self.len
    }
}

pub fn encode_heartbeat(out: &mut [u8], session: Session, next_seqno: u64) -> usize {
    header(out, session, next_seqno, HEARTBEAT);
    HEADER_LEN
}

pub fn encode_end_of_session(out: &mut [u8], session: Session, next_seqno: u64) -> usize {
    header(out, session, next_seqno, END_OF_SESSION);
    HEADER_LEN
}

/// A re-request: resend `count` messages from `seqno`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Request {
    pub session: Session,
    pub seqno: u64,
    pub count: u16,
}

impl Request {
    pub fn decode(b: &[u8]) -> Option<Request> {
        if b.len() != REQUEST_LEN {
            return None;
        }
        let mut s = [0u8; SESSION_LEN];
        s.copy_from_slice(&b[..SESSION_LEN]);
        Some(Request {
            session: Session(s),
            seqno: u64::from_be_bytes([b[10], b[11], b[12], b[13], b[14], b[15], b[16], b[17]]),
            count: u16::from_be_bytes([b[18], b[19]]),
        })
    }

    pub fn encode(&self, out: &mut [u8]) -> usize {
        header(out, self.session, self.seqno, self.count);
        REQUEST_LEN
    }
}

#[cfg(test)]
mod tests {
    extern crate std;
    use super::*;
    use krustycast::protocol::{MoldKind, MoldPacket, RequestPacket};
    use std::vec::Vec;

    #[test]
    fn packets_decode_with_the_interop_tested_codec() {
        let session = Session::new("TEST1").unwrap();
        let mut buf = [0u8; 1400];
        let mut w = PacketWriter::new(&mut buf, session, 42).unwrap();
        let msgs: [&[u8]; 3] = [b"alpha", b"", b"gamma ray"];
        for m in msgs {
            assert!(w.push(m));
        }
        let n = w.finish();
        let p = MoldPacket::decode(&buf[..n]).unwrap();
        assert_eq!(p.session.0, session.0);
        assert_eq!(p.seqno, 42);
        let MoldKind::Messages(got) = p.kind else {
            panic!("not messages")
        };
        assert_eq!(got, msgs.iter().map(|m| m.to_vec()).collect::<Vec<_>>());

        let n = encode_heartbeat(&mut buf, session, 45);
        assert!(matches!(
            MoldPacket::decode(&buf[..n]).unwrap().kind,
            MoldKind::Heartbeat
        ));
        let n = encode_end_of_session(&mut buf, session, 45);
        assert!(matches!(
            MoldPacket::decode(&buf[..n]).unwrap().kind,
            MoldKind::EndOfSession
        ));

        let r = RequestPacket {
            session: krustycast::protocol::Session(session.0),
            seqno: 7,
            count: 3,
        };
        let d = Request::decode(&r.encode()).unwrap();
        assert_eq!((d.session, d.seqno, d.count), (session, 7, 3));
    }

    #[test]
    fn writer_respects_the_buffer_size() {
        let session = Session::new("S").unwrap();
        let mut buf = [0u8; 30];
        let mut w = PacketWriter::new(&mut buf, session, 1).unwrap();
        assert!(w.push(&[1; 8])); // 20 + 10
        assert!(!w.push(&[]));
        assert_eq!(w.finish(), 30);
        assert!(Session::new("ELEVENCHARS").is_none());
        assert!(Session::new("has space").is_none());
    }
}
