//! Streams over time (session roll, gap O3).
//!
//! A roll ends the current stream and starts a new one: a new stream id,
//! seqnos from 1 again, and a fresh core. The history and the egress index
//! never restart: they are keyed by a *global position* that keeps counting
//! across streams. Each stream is an [`Epoch`]: its id, the global position
//! of its seqno 1, and the incarnation floor its fresh core starts with.
//! The table is derived from the log (every node applies the same ROLL
//! entries) and saved in snapshots, so all nodes agree on it.
use std::sync::RwLock;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Epoch {
    pub stream_id: u64,
    /// Global position of this stream's seqno 1.
    pub g0: u64,
    /// Incarnations at or below this are expired (the roll's timestamp:
    /// clients started before the roll must start new incarnations).
    pub floor: u64,
}

impl Epoch {
    /// Global position of stream seqno `s`.
    pub fn global(&self, s: u64) -> u64 {
        self.g0 + s - 1
    }

    /// Stream seqno at global position `g` (≥ `g0`).
    pub fn seqno(&self, g: u64) -> u64 {
        g - self.g0 + 1
    }
}

/// The epochs, oldest first; the last is the current stream.
#[derive(Debug)]
pub struct Streams {
    epochs: RwLock<Vec<Epoch>>,
}

/// Bytes in a saved table: count u64, then (stream, g0, floor) per epoch.
const TAG: u64 = 0x3130_4843_4F50_454B; // "KEPOCH01"

impl Streams {
    pub fn new(first: Epoch) -> Streams {
        Streams {
            epochs: RwLock::new(vec![first]),
        }
    }

    pub fn from_epochs(epochs: Vec<Epoch>) -> Streams {
        assert!(!epochs.is_empty());
        Streams {
            epochs: RwLock::new(epochs),
        }
    }

    pub fn current(&self) -> Epoch {
        *self.epochs.read().unwrap().last().unwrap()
    }

    pub fn all(&self) -> Vec<Epoch> {
        self.epochs.read().unwrap().clone()
    }

    /// Stream `id`'s epoch and the global position its stream ends at
    /// (exclusive; None for the current stream).
    pub fn find(&self, id: u64) -> Option<(Epoch, Option<u64>)> {
        let e = self.epochs.read().unwrap();
        let k = e.iter().position(|x| x.stream_id == id)?;
        Some((e[k], e.get(k + 1).map(|n| n.g0)))
    }

    /// The stream global position `g` belongs to, and where it ends
    /// (exclusive; None for the current stream).
    pub fn at(&self, g: u64) -> (Epoch, Option<u64>) {
        let e = self.epochs.read().unwrap();
        let k = e.partition_point(|x| x.g0 <= g).max(1) - 1;
        (e[k], e.get(k + 1).map(|n| n.g0))
    }

    pub fn contains(&self, id: u64) -> bool {
        self.epochs
            .read()
            .unwrap()
            .iter()
            .any(|x| x.stream_id == id)
    }

    pub fn push(&self, e: Epoch) {
        self.epochs.write().unwrap().push(e);
    }

    /// Appends the table to `out` (for a snapshot).
    pub fn save(&self, out: &mut Vec<u8>) {
        let e = self.epochs.read().unwrap();
        out.extend_from_slice(&TAG.to_le_bytes());
        out.extend_from_slice(&(e.len() as u64).to_le_bytes());
        for x in e.iter() {
            for v in [x.stream_id, x.g0, x.floor] {
                out.extend_from_slice(&v.to_le_bytes());
            }
        }
    }

    /// Reads a table saved by [`Streams::save`] at the start of `b`; returns
    /// it and the bytes after it. None if `b` does not start with one (a
    /// snapshot from before session rolls).
    pub fn load(b: &[u8]) -> Option<(Vec<Epoch>, &[u8])> {
        let u = |i: usize| -> Option<u64> {
            Some(u64::from_le_bytes(b.get(i..i + 8)?.try_into().ok()?))
        };
        if u(0)? != TAG {
            return None;
        }
        let n = u(8)? as usize;
        if n == 0 || n > 1 << 20 {
            return None;
        }
        let mut epochs = Vec::with_capacity(n);
        for k in 0..n {
            let at = 16 + 24 * k;
            epochs.push(Epoch {
                stream_id: u(at)?,
                g0: u(at + 8)?,
                floor: u(at + 16)?,
            });
        }
        Some((epochs, &b[16 + 24 * n..]))
    }
}

/// A ROLL log entry's payload (after the leader's timestamp): this tag,
/// then the new stream id: 16 bytes, shorter than any SUBMIT (whose header
/// alone is 48), so the two cannot be confused.
pub const ROLL_TAG: [u8; 8] = *b"KCROLL01";

pub fn roll_payload(stream_id: u64) -> [u8; 16] {
    let mut p = [0u8; 16];
    p[..8].copy_from_slice(&ROLL_TAG);
    p[8..].copy_from_slice(&stream_id.to_le_bytes());
    p
}

/// The new stream id, if `payload` is a ROLL entry.
pub fn parse_roll(payload: &[u8]) -> Option<u64> {
    if payload.len() == 16 && payload[..8] == ROLL_TAG {
        Some(u64::from_le_bytes(payload[8..].try_into().unwrap()))
    } else {
        None
    }
}

/// The MoldUDP64 session name for a stream: the configured name for the
/// stream the node was configured with, else the stream id in decimal,
/// zero-padded to 10 characters.
pub fn mold_session(
    configured: krusty_mold::Session,
    first_stream: u64,
    stream_id: u64,
) -> krusty_mold::Session {
    if stream_id == first_stream {
        configured
    } else {
        let d = format!("{:0>10}", stream_id % 10_000_000_000);
        krusty_mold::Session::new(&d).expect("ten digits")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_round_trips_and_maps_positions() {
        let s = Streams::new(Epoch {
            stream_id: 7,
            g0: 1,
            floor: 0,
        });
        s.push(Epoch {
            stream_id: 8,
            g0: 101,
            floor: 55,
        });
        let mut b = Vec::new();
        s.save(&mut b);
        b.extend_from_slice(b"rest");
        let (e, rest) = Streams::load(&b).unwrap();
        assert_eq!(e, s.all());
        assert_eq!(rest, b"rest");
        assert!(Streams::load(b"KCSNAP01........").is_none());
        let (old, end) = s.find(7).unwrap();
        assert_eq!((old.global(100), end), (100, Some(101)));
        let (new, end) = s.find(8).unwrap();
        assert_eq!((new.global(1), new.seqno(150), end), (101, 50, None));
        assert_eq!(s.at(100), (old, Some(101)));
        assert_eq!(s.at(101).0, new);
        assert_eq!(parse_roll(&roll_payload(9)), Some(9));
        assert_eq!(parse_roll(b"KCROLL01"), None);
        let cfgd = krusty_mold::Session::new("KC1").unwrap();
        assert_eq!(mold_session(cfgd, 7, 7), cfgd);
        assert_eq!(mold_session(cfgd, 7, 20261117).0, *b"0020261117");
    }
}
