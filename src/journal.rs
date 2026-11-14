//! Append-only sequencer journal, so a restarted sequencer can resume a session.
//!
//! Every decision that changes sequencer state is appended (and, with
//! [`FsyncPolicy::EveryBatch`], synced) before any packet or feedback that
//! reveals it is sent. On restart the records are replayed to rebuild the
//! history, per-target and per-sender state.
//!
//! ```text
//! file:   magic "KRUSTYJ1" | session: [u8; 10] | record*
//! record: len: u32 | crc32(type + body): u32 | type: u8 | body
//! ```
//! A torn or corrupt record (e.g. from a crash mid-write) ends the journal;
//! it and anything after it is truncated on open.

use crate::protocol::Session;
use std::convert::TryInto;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::Path;

const MAGIC: &[u8; 8] = b"KRUSTYJ1";
const FILE_HEADER_SIZE: u64 = 18;
const REC_ACCEPTED: u8 = 1;
const REC_REJECTED: u8 = 2;
const REC_END_OF_SESSION: u8 = 3;
/// Upper bound on a record body, to reject garbage lengths before allocating.
const MAX_RECORD: usize = 1 << 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncPolicy {
    /// Write to the OS on every batch but never fsync: survives a process
    /// crash, not a machine crash. Fastest.
    Never,
    /// fsync (fdatasync) every batch before publishing it: survives power loss.
    EveryBatch,
}

/// Identifies one message from one client process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SenderKey {
    pub sender_id: u32,
    pub incarnation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Record {
    /// A message was sequenced; `data` is its MoldUDP64 message data. Its
    /// global seqno is implied by its position among Accepted records.
    Accepted { key: SenderKey, sender_seqno: u64, data: Vec<u8> },
    /// A message lost its target slot.
    Rejected {
        key: SenderKey,
        sender_seqno: u64,
        target_id: u32,
        rejected_target_seq: u64,
        current_target_seq: u64,
    },
    /// The session was ended; it must not be resumed.
    EndOfSession,
}

impl Record {
    fn encode_into(&self, out: &mut Vec<u8>) {
        let start = out.len();
        out.extend_from_slice(&[0; 8]); // len + crc, filled below
        let key_bytes = |out: &mut Vec<u8>, k: &SenderKey, seqno: u64| {
            out.extend_from_slice(&k.sender_id.to_be_bytes());
            out.extend_from_slice(&k.incarnation.to_be_bytes());
            out.extend_from_slice(&seqno.to_be_bytes());
        };
        match self {
            Record::Accepted { key, sender_seqno, data } => {
                out.push(REC_ACCEPTED);
                key_bytes(out, key, *sender_seqno);
                out.extend_from_slice(data);
            }
            Record::Rejected { key, sender_seqno, target_id, rejected_target_seq, current_target_seq } => {
                out.push(REC_REJECTED);
                key_bytes(out, key, *sender_seqno);
                out.extend_from_slice(&target_id.to_be_bytes());
                out.extend_from_slice(&rejected_target_seq.to_be_bytes());
                out.extend_from_slice(&current_target_seq.to_be_bytes());
            }
            Record::EndOfSession => out.push(REC_END_OF_SESSION),
        }
        let body = &out[start + 8..];
        let len = body.len() as u32;
        let crc = crc32fast::hash(body);
        out[start..start + 4].copy_from_slice(&len.to_be_bytes());
        out[start + 4..start + 8].copy_from_slice(&crc.to_be_bytes());
    }

    fn decode(body: &[u8]) -> Option<Self> {
        let u32_at = |at: usize| u32::from_be_bytes(body[at..at + 4].try_into().unwrap());
        let u64_at = |at: usize| u64::from_be_bytes(body[at..at + 8].try_into().unwrap());
        let key = || SenderKey { sender_id: u32_at(1), incarnation: u64_at(5) };
        match *body.first()? {
            REC_ACCEPTED if body.len() >= 21 => {
                Some(Record::Accepted { key: key(), sender_seqno: u64_at(13), data: body[21..].to_vec() })
            }
            REC_REJECTED if body.len() == 41 => Some(Record::Rejected {
                key: key(),
                sender_seqno: u64_at(13),
                target_id: u32_at(21),
                rejected_target_seq: u64_at(25),
                current_target_seq: u64_at(33),
            }),
            REC_END_OF_SESSION if body.len() == 1 => Some(Record::EndOfSession),
            _ => None,
        }
    }
}

pub struct Journal {
    file: BufWriter<File>,
    fsync: FsyncPolicy,
    buf: Vec<u8>,
}

/// What `Journal::open` found.
pub struct Recovered {
    pub records: Vec<Record>,
    /// Bytes of torn/corrupt tail that were truncated.
    pub truncated_bytes: u64,
}

impl Journal {
    /// Opens (creating if needed) the journal for `session`, returning the
    /// existing records. Fails if the file belongs to a different session.
    pub fn open(path: &Path, session: Session, fsync: FsyncPolicy) -> io::Result<(Journal, Recovered)> {
        let mut file = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path)?;
        let file_len = file.metadata()?.len();
        let mut records = Vec::new();
        let mut good_len = FILE_HEADER_SIZE;

        if file_len == 0 {
            file.write_all(MAGIC)?;
            file.write_all(&session.0)?;
            file.sync_all()?;
        } else {
            let mut r = BufReader::new(&mut file);
            let mut header = [0u8; FILE_HEADER_SIZE as usize];
            r.read_exact(&mut header).map_err(|_| invalid(path, "too short to be a journal"))?;
            if &header[..8] != MAGIC {
                return Err(invalid(path, "not a KrustyCast journal"));
            }
            let found = Session(header[8..].try_into().unwrap());
            if found != session {
                return Err(invalid(path, &format!("belongs to session {found}, not {session}")));
            }
            let mut head = [0u8; 8];
            let mut body = Vec::new();
            loop {
                match r.read_exact(&mut head) {
                    Ok(()) => {}
                    Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(e),
                }
                let len = u32::from_be_bytes(head[..4].try_into().unwrap()) as usize;
                let crc = u32::from_be_bytes(head[4..].try_into().unwrap());
                if len == 0 || len > MAX_RECORD {
                    break;
                }
                body.resize(len, 0);
                if r.read_exact(&mut body).is_err() || crc32fast::hash(&body) != crc {
                    break;
                }
                let Some(rec) = Record::decode(&body) else { break };
                records.push(rec);
                good_len += 8 + len as u64;
            }
        }

        let truncated_bytes = file_len.saturating_sub(good_len);
        if truncated_bytes > 0 {
            file.set_len(good_len)?;
            file.sync_all()?;
        }
        file.seek(SeekFrom::End(0))?;
        let journal = Journal { file: BufWriter::with_capacity(1 << 16, file), fsync, buf: Vec::new() };
        Ok((journal, Recovered { records, truncated_bytes }))
    }

    /// Buffers a record; it is durable only after `commit`.
    pub fn append(&mut self, rec: &Record) -> io::Result<()> {
        self.buf.clear();
        rec.encode_into(&mut self.buf);
        self.file.write_all(&self.buf)
    }

    /// Writes buffered records to the OS and, per the fsync policy, to disk.
    pub fn commit(&mut self) -> io::Result<()> {
        self.file.flush()?;
        if self.fsync == FsyncPolicy::EveryBatch {
            self.file.get_ref().sync_data()?;
        }
        Ok(())
    }
}

fn invalid(path: &Path, why: &str) -> io::Error {
    io::Error::new(ErrorKind::InvalidData, format!("journal {}: {}", path.display(), why))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    struct TempFile(PathBuf);
    impl TempFile {
        fn new(name: &str) -> Self {
            let p = std::env::temp_dir().join(format!("krustycast-{}-{}", name, std::process::id()));
            let _ = std::fs::remove_file(&p);
            TempFile(p)
        }
    }
    impl Drop for TempFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn session() -> Session {
        Session::new("JTEST").unwrap()
    }

    fn sample() -> Vec<Record> {
        let key = SenderKey { sender_id: 1, incarnation: 99 };
        vec![
            Record::Accepted { key, sender_seqno: 1, data: b"hello".to_vec() },
            Record::Rejected { key, sender_seqno: 2, target_id: 3, rejected_target_seq: 4, current_target_seq: 5 },
            Record::Accepted { key, sender_seqno: 3, data: vec![] },
            Record::EndOfSession,
        ]
    }

    #[test]
    fn round_trip_and_append_after_reopen() {
        let f = TempFile::new("roundtrip");
        let (mut j, rec) = Journal::open(&f.0, session(), FsyncPolicy::EveryBatch).unwrap();
        assert!(rec.records.is_empty());
        for r in &sample()[..2] {
            j.append(r).unwrap();
        }
        j.commit().unwrap();
        drop(j);

        let (mut j, rec) = Journal::open(&f.0, session(), FsyncPolicy::Never).unwrap();
        assert_eq!(rec.records, sample()[..2]);
        for r in &sample()[2..] {
            j.append(r).unwrap();
        }
        j.commit().unwrap();
        drop(j);

        let (_, rec) = Journal::open(&f.0, session(), FsyncPolicy::Never).unwrap();
        assert_eq!(rec.records, sample());
        assert_eq!(rec.truncated_bytes, 0);
    }

    #[test]
    fn torn_tail_is_truncated() {
        let f = TempFile::new("torn");
        let (mut j, _) = Journal::open(&f.0, session(), FsyncPolicy::Never).unwrap();
        for r in &sample()[..3] {
            j.append(r).unwrap();
        }
        j.commit().unwrap();
        drop(j);
        let full = std::fs::metadata(&f.0).unwrap().len();

        // Cut the last record in half.
        let file = OpenOptions::new().write(true).open(&f.0).unwrap();
        file.set_len(full - 10).unwrap();
        drop(file);
        let (_, rec) = Journal::open(&f.0, session(), FsyncPolicy::Never).unwrap();
        assert_eq!(rec.records, sample()[..2]);
        assert!(rec.truncated_bytes > 0);
        // The truncation was persisted, so a second open finds a clean file.
        let (_, rec) = Journal::open(&f.0, session(), FsyncPolicy::Never).unwrap();
        assert_eq!((rec.records.len(), rec.truncated_bytes), (2, 0));
    }

    #[test]
    fn corrupt_record_ends_the_journal() {
        let f = TempFile::new("corrupt");
        let (mut j, _) = Journal::open(&f.0, session(), FsyncPolicy::Never).unwrap();
        for r in &sample()[..3] {
            j.append(r).unwrap();
        }
        j.commit().unwrap();
        drop(j);
        // Flip a byte inside the first record's body.
        let mut bytes = std::fs::read(&f.0).unwrap();
        bytes[FILE_HEADER_SIZE as usize + 10] ^= 0xff;
        std::fs::write(&f.0, &bytes).unwrap();
        let (_, rec) = Journal::open(&f.0, session(), FsyncPolicy::Never).unwrap();
        assert!(rec.records.is_empty());
    }

    #[test]
    fn rejects_wrong_session_and_foreign_files() {
        let f = TempFile::new("wrong");
        drop(Journal::open(&f.0, session(), FsyncPolicy::Never).unwrap());
        let err = Journal::open(&f.0, Session::new("OTHER").unwrap(), FsyncPolicy::Never).err().unwrap();
        assert!(err.to_string().contains("belongs to session JTEST"), "{err}");

        std::fs::write(&f.0, b"definitely not a journal").unwrap();
        assert!(Journal::open(&f.0, session(), FsyncPolicy::Never).is_err());
    }
}
