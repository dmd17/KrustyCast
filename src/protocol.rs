//! Wire formats shared by the sequencer and the client. All integers are big-endian.
//!
//! ## Client → sequencer (unicast or multicast UDP)
//! ```text
//! sender_id: u32 | incarnation: u64 | sender_seqno: u64 | target_id: u32 | target_seq: u64 | payload...
//! ```
//! `incarnation` is random per client process, so a restarted client's
//! sender seqnos (which start again at 1) are not mistaken for duplicates.
//!
//! ## Sequencer → client feedback (unicast UDP, back to the sending socket)
//! ```text
//! NAK:    0x01 | expected_sender_seqno: u64
//! REJECT:   0x02 | target_id: u32 | rejected_target_seq: u64 | current_target_seq: u64 | sender_seqno: u64
//! ACCEPTED: 0x03 | sender_seqno: u64 | global_seqno: u64
//! ```
//! A client that hears nothing back resends a message unchanged. The
//! sequencer never sequences it twice: it answers a duplicate with the
//! original outcome (REJECT again, or ACCEPTED with the message's seqno).
//!
//! ## Sequencer → all clients: standard MoldUDP64
//! ```text
//! downstream packet: session: [u8; 10] | seqno: u64 | msg_count: u16 | msg_count × (len: u16 | data)
//! request packet:    session: [u8; 10] | seqno: u64 | msg_count: u16          (to the re-request server)
//! ```
//! `seqno` is the sequence number of the first message in the packet (the
//! first message of a session is 1). `msg_count == 0` is a heartbeat and
//! `0xFFFF` is end-of-session; both carry the next sequence number to be used.
//! A re-request is answered with an ordinary downstream packet, unicast back.
//!
//! Each MoldUDP64 message's `data` is a [`SequencedMessage`]:
//! ```text
//! sender_id: u32 | target_id: u32 | target_seq: u64 | payload...
//! ```

use std::convert::TryInto;
use std::fmt;

pub const CLIENT_HEADER_SIZE: usize = 32;
pub const SESSION_LEN: usize = 10;
pub const MOLD_HEADER_SIZE: usize = 20;
pub const REQUEST_PACKET_SIZE: usize = 20;
pub const HEARTBEAT_COUNT: u16 = 0;
pub const END_OF_SESSION_COUNT: u16 = 0xFFFF;
/// Size of the [`SequencedMessage`] header inside a MoldUDP64 message.
pub const SEQUENCED_HEADER_SIZE: usize = 16;
/// Largest payload that fits in one MoldUDP64 message (its length is a u16).
pub const MAX_PAYLOAD: usize = u16::MAX as usize - SEQUENCED_HEADER_SIZE;
/// Most messages one packet can carry (0xFFFF means end-of-session).
pub const MAX_MESSAGES_PER_PACKET: usize = END_OF_SESSION_COUNT as usize - 1;

const FEEDBACK_NAK: u8 = 0x01;
const FEEDBACK_REJECT: u8 = 0x02;
const FEEDBACK_ACCEPTED: u8 = 0x03;

fn u16_at(b: &[u8], at: usize) -> u16 {
    u16::from_be_bytes(b[at..at + 2].try_into().unwrap())
}
fn u32_at(b: &[u8], at: usize) -> u32 {
    u32::from_be_bytes(b[at..at + 4].try_into().unwrap())
}
fn u64_at(b: &[u8], at: usize) -> u64 {
    u64::from_be_bytes(b[at..at + 8].try_into().unwrap())
}

// ---------------------------------------------------------------------------
// Upstream: client → sequencer
// ---------------------------------------------------------------------------

/// A message sent by a client to the sequencer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientMessage {
    pub sender_id: u32,
    pub incarnation: u64,
    pub sender_seqno: u64,
    pub target_id: u32,
    pub target_seq: u64,
    pub payload: Vec<u8>,
}

impl ClientMessage {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(CLIENT_HEADER_SIZE + self.payload.len());
        out.extend_from_slice(&self.sender_id.to_be_bytes());
        out.extend_from_slice(&self.incarnation.to_be_bytes());
        out.extend_from_slice(&self.sender_seqno.to_be_bytes());
        out.extend_from_slice(&self.target_id.to_be_bytes());
        out.extend_from_slice(&self.target_seq.to_be_bytes());
        out.extend_from_slice(&self.payload);
        out
    }

    pub fn decode(b: &[u8]) -> Option<Self> {
        if b.len() < CLIENT_HEADER_SIZE || b.len() - CLIENT_HEADER_SIZE > MAX_PAYLOAD {
            return None;
        }
        Some(ClientMessage {
            sender_id: u32_at(b, 0),
            incarnation: u64_at(b, 4),
            sender_seqno: u64_at(b, 12),
            target_id: u32_at(b, 20),
            target_seq: u64_at(b, 24),
            payload: b[CLIENT_HEADER_SIZE..].to_vec(),
        })
    }
}

/// Unicast feedback from the sequencer to one client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Feedback {
    /// The sequencer saw a gap in this client's sender seqnos; resend from `expected`.
    Nak { expected: u64 },
    /// The message with `sender_seqno` claimed `rejected_target_seq` on `target_id`,
    /// but the target is at `current_target_seq`, so it was not forwarded.
    Reject {
        target_id: u32,
        rejected_target_seq: u64,
        current_target_seq: u64,
        sender_seqno: u64,
    },
    /// The message with `sender_seqno` was sequenced as `global_seqno`
    /// (sent in reply to a resend of an already-sequenced message).
    Accepted { sender_seqno: u64, global_seqno: u64 },
}

impl Feedback {
    pub fn encode(&self) -> Vec<u8> {
        match *self {
            Feedback::Nak { expected } => {
                let mut out = Vec::with_capacity(9);
                out.push(FEEDBACK_NAK);
                out.extend_from_slice(&expected.to_be_bytes());
                out
            }
            Feedback::Reject { target_id, rejected_target_seq, current_target_seq, sender_seqno } => {
                let mut out = Vec::with_capacity(29);
                out.push(FEEDBACK_REJECT);
                out.extend_from_slice(&target_id.to_be_bytes());
                out.extend_from_slice(&rejected_target_seq.to_be_bytes());
                out.extend_from_slice(&current_target_seq.to_be_bytes());
                out.extend_from_slice(&sender_seqno.to_be_bytes());
                out
            }
            Feedback::Accepted { sender_seqno, global_seqno } => {
                let mut out = Vec::with_capacity(17);
                out.push(FEEDBACK_ACCEPTED);
                out.extend_from_slice(&sender_seqno.to_be_bytes());
                out.extend_from_slice(&global_seqno.to_be_bytes());
                out
            }
        }
    }

    pub fn decode(b: &[u8]) -> Option<Self> {
        match b.first()? {
            &FEEDBACK_NAK if b.len() == 9 => Some(Feedback::Nak { expected: u64_at(b, 1) }),
            &FEEDBACK_REJECT if b.len() == 29 => Some(Feedback::Reject {
                target_id: u32_at(b, 1),
                rejected_target_seq: u64_at(b, 5),
                current_target_seq: u64_at(b, 13),
                sender_seqno: u64_at(b, 21),
            }),
            &FEEDBACK_ACCEPTED if b.len() == 17 => {
                Some(Feedback::Accepted { sender_seqno: u64_at(b, 1), global_seqno: u64_at(b, 9) })
            }
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Downstream: MoldUDP64
// ---------------------------------------------------------------------------

/// A MoldUDP64 session identifier: 10 bytes, conventionally ASCII padded with spaces.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Session(pub [u8; SESSION_LEN]);

impl Session {
    /// Builds a session from up to 10 printable ASCII characters, right-padded with spaces.
    pub fn new(s: &str) -> Result<Self, String> {
        if s.is_empty() || s.len() > SESSION_LEN || !s.bytes().all(|c| c.is_ascii_graphic()) {
            return Err(format!("session {s:?} must be 1-10 printable ASCII characters, no spaces"));
        }
        let mut out = [b' '; SESSION_LEN];
        out[..s.len()].copy_from_slice(s.as_bytes());
        Ok(Session(out))
    }

    /// The session with trailing padding removed (lossy for non-UTF-8 bytes).
    pub fn name(&self) -> String {
        String::from_utf8_lossy(&self.0).trim_end().to_string()
    }
}

impl std::str::FromStr for Session {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        Session::new(s)
    }
}

impl fmt::Display for Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.name())
    }
}

impl fmt::Debug for Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Session({:?})", self.name())
    }
}

/// What a MoldUDP64 downstream packet carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MoldKind {
    /// One or more messages (opaque bytes, the `data` of each message block).
    Messages(Vec<Vec<u8>>),
    Heartbeat,
    EndOfSession,
}

/// A decoded MoldUDP64 downstream packet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MoldPacket {
    pub session: Session,
    /// Seqno of the first message, or the next seqno for heartbeat/end-of-session.
    pub seqno: u64,
    pub kind: MoldKind,
}

impl MoldPacket {
    pub fn message_count(&self) -> usize {
        match &self.kind {
            MoldKind::Messages(m) => m.len(),
            _ => 0,
        }
    }

    /// The seqno the packet after this one should start at.
    pub fn next_seqno(&self) -> u64 {
        self.seqno + self.message_count() as u64
    }

    pub fn heartbeat(session: Session, next_seqno: u64) -> Vec<u8> {
        Self::header_only(session, next_seqno, HEARTBEAT_COUNT)
    }

    pub fn end_of_session(session: Session, next_seqno: u64) -> Vec<u8> {
        Self::header_only(session, next_seqno, END_OF_SESSION_COUNT)
    }

    fn header_only(session: Session, seqno: u64, count: u16) -> Vec<u8> {
        let mut out = Vec::with_capacity(MOLD_HEADER_SIZE);
        out.extend_from_slice(&session.0);
        out.extend_from_slice(&seqno.to_be_bytes());
        out.extend_from_slice(&count.to_be_bytes());
        out
    }

    /// Encodes the packet. Panics if a message is over 65535 bytes or there are
    /// more than MAX_MESSAGES_PER_PACKET messages.
    pub fn encode(&self) -> Vec<u8> {
        match &self.kind {
            MoldKind::Heartbeat => Self::heartbeat(self.session, self.seqno),
            MoldKind::EndOfSession => Self::end_of_session(self.session, self.seqno),
            MoldKind::Messages(msgs) => {
                let mut b = PacketBuilder::new(self.session, self.seqno, usize::MAX);
                for m in msgs {
                    assert!(b.try_push(m), "too many messages or message too large");
                }
                b.finish()
            }
        }
    }

    /// Returns None for a truncated or malformed packet (never panics).
    pub fn decode(b: &[u8]) -> Option<Self> {
        if b.len() < MOLD_HEADER_SIZE {
            return None;
        }
        let session = Session(b[..SESSION_LEN].try_into().unwrap());
        let seqno = u64_at(b, 10);
        let count = u16_at(b, 18);
        let kind = match count {
            HEARTBEAT_COUNT | END_OF_SESSION_COUNT => {
                if b.len() != MOLD_HEADER_SIZE {
                    return None;
                }
                if count == HEARTBEAT_COUNT { MoldKind::Heartbeat } else { MoldKind::EndOfSession }
            }
            n => {
                let mut msgs = Vec::with_capacity(n as usize);
                let mut off = MOLD_HEADER_SIZE;
                for _ in 0..n {
                    if b.len() < off + 2 {
                        return None;
                    }
                    let len = u16_at(b, off) as usize;
                    off += 2;
                    if b.len() < off + len {
                        return None;
                    }
                    msgs.push(b[off..off + len].to_vec());
                    off += len;
                }
                if off != b.len() {
                    return None;
                }
                MoldKind::Messages(msgs)
            }
        };
        Some(MoldPacket { session, seqno, kind })
    }
}

/// Incrementally builds a MoldUDP64 message packet up to a size limit, so the
/// sequencer and re-request server can batch without intermediate allocations.
pub struct PacketBuilder {
    buf: Vec<u8>,
    count: usize,
    max_size: usize,
}

impl PacketBuilder {
    /// `max_size` is the datagram size limit; the first message is always
    /// accepted even if it alone exceeds it.
    pub fn new(session: Session, seqno: u64, max_size: usize) -> Self {
        let mut buf = Vec::with_capacity(max_size.min(65536));
        buf.extend_from_slice(&session.0);
        buf.extend_from_slice(&seqno.to_be_bytes());
        buf.extend_from_slice(&0u16.to_be_bytes());
        PacketBuilder { buf, count: 0, max_size }
    }

    /// Appends a message if it fits. Returns false (and leaves the packet
    /// unchanged) if it doesn't.
    pub fn try_push(&mut self, data: &[u8]) -> bool {
        let Ok(len) = u16::try_from(data.len()) else { return false };
        if self.count >= MAX_MESSAGES_PER_PACKET
            || (self.count > 0 && self.buf.len() + 2 + data.len() > self.max_size)
        {
            return false;
        }
        self.buf.extend_from_slice(&len.to_be_bytes());
        self.buf.extend_from_slice(data);
        self.count += 1;
        true
    }

    pub fn count(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// The encoded packet. An empty builder yields a heartbeat.
    pub fn finish(mut self) -> Vec<u8> {
        self.buf[18..20].copy_from_slice(&(self.count as u16).to_be_bytes());
        self.buf
    }
}

/// A MoldUDP64 request packet, asking the re-request server for `count`
/// messages starting at `seqno`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestPacket {
    pub session: Session,
    pub seqno: u64,
    pub count: u16,
}

impl RequestPacket {
    pub fn encode(&self) -> [u8; REQUEST_PACKET_SIZE] {
        let mut out = [0u8; REQUEST_PACKET_SIZE];
        out[..10].copy_from_slice(&self.session.0);
        out[10..18].copy_from_slice(&self.seqno.to_be_bytes());
        out[18..20].copy_from_slice(&self.count.to_be_bytes());
        out
    }

    pub fn decode(b: &[u8]) -> Option<Self> {
        if b.len() != REQUEST_PACKET_SIZE {
            return None;
        }
        Some(RequestPacket {
            session: Session(b[..10].try_into().unwrap()),
            seqno: u64_at(b, 10),
            count: u16_at(b, 18),
        })
    }
}

/// The application message carried in each MoldUDP64 message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequencedMessage {
    pub sender_id: u32,
    pub target_id: u32,
    pub target_seq: u64,
    pub payload: Vec<u8>,
}

impl SequencedMessage {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(SEQUENCED_HEADER_SIZE + self.payload.len());
        out.extend_from_slice(&self.sender_id.to_be_bytes());
        out.extend_from_slice(&self.target_id.to_be_bytes());
        out.extend_from_slice(&self.target_seq.to_be_bytes());
        out.extend_from_slice(&self.payload);
        out
    }

    pub fn decode(b: &[u8]) -> Option<Self> {
        if b.len() < SEQUENCED_HEADER_SIZE {
            return None;
        }
        Some(SequencedMessage {
            sender_id: u32_at(b, 0),
            target_id: u32_at(b, 4),
            target_seq: u64_at(b, 8),
            payload: b[SEQUENCED_HEADER_SIZE..].to_vec(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session() -> Session {
        Session::new("TEST1").unwrap()
    }

    #[test]
    fn client_message_round_trip() {
        let m = ClientMessage {
            sender_id: 7,
            incarnation: 0xfeedface_cafebabe,
            sender_seqno: 0x0102030405060708,
            target_id: 0xdeadbeef,
            target_seq: 42,
            payload: b"hello".to_vec(),
        };
        let bytes = m.encode();
        assert_eq!(bytes.len(), CLIENT_HEADER_SIZE + 5);
        assert_eq!(ClientMessage::decode(&bytes), Some(m));
        assert_eq!(ClientMessage::decode(&bytes[..CLIENT_HEADER_SIZE - 1]), None);
    }

    #[test]
    fn feedback_round_trip() {
        for f in [
            Feedback::Nak { expected: 99 },
            Feedback::Reject { target_id: 3, rejected_target_seq: 5, current_target_seq: 8, sender_seqno: 11 },
            Feedback::Accepted { sender_seqno: 4, global_seqno: 1234 },
        ] {
            assert_eq!(Feedback::decode(&f.encode()), Some(f));
        }
        assert_eq!(Feedback::decode(&[]), None);
        assert_eq!(Feedback::decode(&[0x01, 0, 0]), None);
        assert_eq!(Feedback::decode(&[0x7f; 9]), None);
    }

    #[test]
    fn session_padding_and_validation() {
        assert_eq!(&session().0, b"TEST1     ");
        assert_eq!(session().name(), "TEST1");
        assert!(Session::new("").is_err());
        assert!(Session::new("ELEVENCHARS").is_err());
        assert!(Session::new("has space").is_err());
        assert!(Session::new("TENCHARS10").is_ok());
    }

    #[test]
    fn mold_packet_round_trip() {
        for kind in [
            MoldKind::Messages(vec![b"abc".to_vec(), vec![], b"defg".to_vec()]),
            MoldKind::Heartbeat,
            MoldKind::EndOfSession,
        ] {
            let p = MoldPacket { session: session(), seqno: 10, kind };
            assert_eq!(MoldPacket::decode(&p.encode()), Some(p));
        }
        let p = MoldPacket { session: session(), seqno: 10, kind: MoldKind::Messages(vec![b"abc".to_vec()]) };
        let bytes = p.encode();
        assert_eq!(bytes.len(), 20 + 2 + 3);
        assert_eq!(p.next_seqno(), 11);
        assert_eq!(MoldPacket::heartbeat(session(), 5).len(), MOLD_HEADER_SIZE);
    }

    #[test]
    fn mold_packet_rejects_malformed() {
        let p = MoldPacket { session: session(), seqno: 1, kind: MoldKind::Messages(vec![b"xyz".to_vec()]) };
        let bytes = p.encode();
        for n in 0..bytes.len() {
            assert_eq!(MoldPacket::decode(&bytes[..n]), None, "truncated to {n}");
        }
        let mut extra = bytes.clone();
        extra.push(0);
        assert_eq!(MoldPacket::decode(&extra), None);
        let mut hb = MoldPacket::heartbeat(session(), 1);
        hb.push(0);
        assert_eq!(MoldPacket::decode(&hb), None);
    }

    #[test]
    fn packet_builder_respects_size_limit() {
        // Header 20 + two 10-byte messages with 2-byte lengths = 44.
        let mut b = PacketBuilder::new(session(), 1, 44);
        assert!(b.try_push(&[1; 10]));
        assert!(b.try_push(&[2; 10]));
        assert!(!b.try_push(&[3; 1]));
        assert_eq!(b.count(), 2);
        let p = MoldPacket::decode(&b.finish()).unwrap();
        assert_eq!(p.message_count(), 2);

        // An oversized first message is still accepted.
        let mut b = PacketBuilder::new(session(), 1, 10);
        assert!(b.try_push(&[0; 100]));
        assert!(!b.try_push(&[0; 1]));

        assert_eq!(PacketBuilder::new(session(), 7, 100).finish(), MoldPacket::heartbeat(session(), 7));
    }

    #[test]
    fn request_and_sequenced_round_trip() {
        let r = RequestPacket { session: session(), seqno: 123, count: 45 };
        assert_eq!(RequestPacket::decode(&r.encode()), Some(r));
        assert_eq!(RequestPacket::decode(&r.encode()[..19]), None);

        let m = SequencedMessage { sender_id: 1, target_id: 2, target_seq: 3, payload: b"hi".to_vec() };
        assert_eq!(SequencedMessage::decode(&m.encode()), Some(m));
        assert_eq!(SequencedMessage::decode(&[0; 15]), None);
    }

    /// Our encoding, decoded by an independent MoldUDP64 implementation.
    #[test]
    fn interop_with_moldudp_crate_decoder() {
        use moldudp::{Packet, PacketKind};

        let msgs = vec![
            SequencedMessage { sender_id: 1, target_id: 2, target_seq: 3, payload: b"one".to_vec() }.encode(),
            SequencedMessage { sender_id: 4, target_id: 5, target_seq: 6, payload: b"two!".to_vec() }.encode(),
        ];
        let bytes = MoldPacket { session: session(), seqno: 77, kind: MoldKind::Messages(msgs.clone()) }.encode();
        let theirs = Packet::new(&bytes);
        assert_eq!(theirs.session_ident(), "TEST1     ");
        assert_eq!(theirs.seq_num(), 77);
        assert_eq!(theirs.msg_count(), 2);
        assert!(matches!(theirs.packet_kind(), PacketKind::Standard));
        let data: Vec<Vec<u8>> = theirs.iter().map(|m| m.data().to_vec()).collect();
        assert_eq!(data, msgs);

        let hb = MoldPacket::heartbeat(session(), 9);
        let theirs = Packet::new(&hb);
        assert!(matches!(theirs.packet_kind(), PacketKind::Heartbeat));
        assert_eq!(theirs.seq_num(), 9);

        let eos = MoldPacket::end_of_session(session(), 9);
        assert!(matches!(Packet::new(&eos).packet_kind(), PacketKind::EndOfSession));
    }
}
