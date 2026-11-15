//! Storage for one stream (ARCHITECTURE §9; gaps S1–S3, R4).
//!
//! - **Log segments** `log-<first index>.seg`: preallocated files of
//!   entries (log index, Raft term, leader timestamp, raw submission). Each
//!   record is `size u32 | crc32c u32 | log_index u64 | term u64 |
//!   timestamp u64 | payload`, padded to 8 bytes, where `size` counts the
//!   32-byte header plus the payload (so it is never 0). A zero size marks
//!   the end. Entries past the commit index may be removed again
//!   ([`Log::truncate_from`], when a new Raft leader overrides them).
//! - **Egress index** `egress-<first seqno>.idx`: fixed 16-byte slots, one
//!   per published stream message, holding the log index and offset of the
//!   entry that produced it, so a recovery read is one lookup plus one read.
//! - **Snapshots** `snap-<log index>.snap`: the sequencer core's state after
//!   that entry (written atomically; the latest two are kept).
//! - **Raft hard state** `raft.state` ([`HardStateFile`]): term and vote.
//!
//! Each segment keeps a sparse in-memory index (the offset of every
//! [`SPARSE`]th entry), so an entry can be found by log index with one short
//! read (Raft followers that lag are served from here).
//!
//! One writer ([`Log`], on the storage thread) and any number of readers
//! ([`LogReader`], e.g. recovery servers) that see everything flushed.
//!
//! Opening scans the last segment and stops at the first record that is
//! torn (bad CRC, wrong index, or cut short), zeroing what follows so a
//! later scan cannot resurrect stale bytes. The egress index may lag the log
//! after a crash; recovery replays from the snapshot and rewrites it.
//!
//! Retention deletes whole segments, oldest first, and only below the latest
//! snapshot, so a restart never needs anything that was deleted.

use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

const SEG_MAGIC: u64 = 0x4745_5347_4F4C_434B; // "KCLOGSEG"
const IDX_MAGIC: u64 = 0x5844_4E49_4745_434B; // "KCEGINDX"
const SNAP_MAGIC: u64 = 0x5041_4E53_4F4C_434B; // "KCLOSNAP"
const FILE_HEADER: u64 = 64;
const FORMAT: u64 = 2;
const REC_HEADER: usize = 32;
/// Every SPARSE-th entry of a segment has its offset kept in memory.
pub const SPARSE: u64 = 64;
const IDX_SLOT: u64 = 16;
/// Bytes zeroed after a recovered end, to cover any torn batch.
const ZERO_AHEAD: u64 = 8 << 20;

#[derive(Clone, Debug)]
pub struct LogConfig {
    pub dir: PathBuf,
    /// Size of each preallocated log segment.
    pub segment_bytes: u64,
    /// Seqnos per egress index file.
    pub index_entries: u64,
    /// Retention budget for log segments (whole segments are deleted).
    pub retain_bytes: u64,
}

impl LogConfig {
    pub fn new(dir: impl Into<PathBuf>) -> LogConfig {
        LogConfig {
            dir: dir.into(),
            segment_bytes: 256 << 20,
            index_entries: 1 << 22,
            retain_bytes: u64::MAX,
        }
    }
}

/// Where an entry is: its log index and its offset in that index's segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Pos {
    pub log_index: u64,
    pub offset: u64,
}

struct Segment {
    first_index: u64,
    path: PathBuf,
    file: Arc<File>,
    /// Offsets of entries `first_index + k * SPARSE`; None until needed
    /// (built by a scan) for segments from before the last open.
    sparse: Arc<Mutex<Option<Vec<u32>>>>,
}

struct IndexFile {
    first_seqno: u64,
    path: PathBuf,
    file: Arc<File>,
}

struct Shared {
    segments: RwLock<Vec<Segment>>,
    indexes: RwLock<Vec<IndexFile>>,
    /// Highest log index readable (flushed).
    flushed_index: AtomicU64,
    /// Highest seqno whose index entry is readable (flushed).
    flushed_seqno: AtomicU64,
    /// Highest seqno whose entry is durable (fsynced).
    synced_seqno: AtomicU64,
    /// Lowest seqno still retained.
    earliest_seqno: AtomicU64,
}

/// The writer. Lives on the storage thread.
pub struct Log {
    cfg: LogConfig,
    shared: Arc<Shared>,
    active: Arc<File>,
    active_first: u64,
    /// Offset of the next record in the active segment (buffered or not).
    write_off: u64,
    buf: Vec<u8>,
    buf_off: u64,
    next_index: u64,
    next_seqno: u64,
    idx_buf: Vec<(u64, Pos)>,
    idx_bytes: Vec<u8>,
    /// Segments rolled since the last sync (they still need an fsync).
    unsynced: Vec<Arc<File>>,
    /// Highest seqno whose egress index slot is durable (`sync`, not
    /// `sync_entries`).
    idx_synced: u64,
    latest_snapshot: u64,
    /// Sparse offsets of the active segment not yet handed to readers.
    sparse_pending: Vec<u32>,
    active_sparse: Arc<Mutex<Option<Vec<u32>>>>,
    /// Log index of the latest published entry (truncation stays above it).
    last_published_index: u64,
}

/// What `open` found.
pub struct Opened {
    pub log: Log,
    /// The latest valid snapshot: (log index it was taken after, bytes).
    pub snapshot: Option<(u64, Vec<u8>)>,
    /// The last log index on disk (0 if none).
    pub last_index: u64,
}

fn name(prefix: &str, n: u64, ext: &str) -> String {
    format!("{prefix}-{n:020}.{ext}")
}

fn parse_name(file: &str, prefix: &str, ext: &str) -> Option<u64> {
    file.strip_prefix(prefix)?
        .strip_prefix('-')?
        .strip_suffix(ext)?
        .strip_suffix('.')?
        .parse()
        .ok()
}

fn preallocate(f: &File, len: u64) -> io::Result<()> {
    // SAFETY: plain system call on an open descriptor.
    let r = unsafe { libc::posix_fallocate(f.as_raw_fd(), 0, len as libc::off_t) };
    if r != 0 {
        Err(io::Error::from_raw_os_error(r))
    } else {
        Ok(())
    }
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

fn header(magic: u64, first: u64, size: u64) -> [u8; FILE_HEADER as usize] {
    let mut h = [0u8; FILE_HEADER as usize];
    h[0..8].copy_from_slice(&magic.to_le_bytes());
    h[8..16].copy_from_slice(&FORMAT.to_le_bytes());
    h[16..24].copy_from_slice(&first.to_le_bytes());
    h[24..32].copy_from_slice(&size.to_le_bytes());
    h
}

fn check_header(f: &File, magic: u64, first: u64) -> io::Result<()> {
    let mut h = [0u8; FILE_HEADER as usize];
    f.read_exact_at(&mut h, 0)?;
    let ok = u64::from_le_bytes(h[0..8].try_into().unwrap()) == magic
        && u64::from_le_bytes(h[8..16].try_into().unwrap()) == FORMAT
        && u64::from_le_bytes(h[16..24].try_into().unwrap()) == first;
    if ok {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bad file header",
        ))
    }
}

fn create_file(
    dir: &Path,
    file: &str,
    magic: u64,
    first: u64,
    size: u64,
) -> io::Result<(PathBuf, File)> {
    let path = dir.join(file);
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)?;
    preallocate(&f, size)?;
    f.write_all_at(&header(magic, first, size), 0)?;
    f.sync_all()?;
    sync_dir(dir)?;
    Ok((path, f))
}

fn record_len(payload: usize) -> u64 {
    ((REC_HEADER + payload + 7) & !7) as u64
}

fn crc(len: u32, log_index: u64, term: u64, ts: u64, payload: &[u8]) -> u32 {
    let c = crc32c::crc32c(&len.to_le_bytes());
    let c = crc32c::crc32c_append(c, &log_index.to_le_bytes());
    let c = crc32c::crc32c_append(c, &term.to_le_bytes());
    let c = crc32c::crc32c_append(c, &ts.to_le_bytes());
    crc32c::crc32c_append(c, payload)
}

/// A record's header fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Header {
    pub term: u64,
    pub timestamp_ns: u64,
}

/// Reads the record at `off` if it is valid and has `expect` as log index.
fn read_record(
    f: &File,
    off: u64,
    expect: u64,
    seg_bytes: u64,
    payload: &mut Vec<u8>,
) -> io::Result<Option<Header>> {
    if off + REC_HEADER as u64 > seg_bytes {
        return Ok(None);
    }
    let mut h = [0u8; REC_HEADER];
    f.read_exact_at(&mut h, off)?;
    let size = u32::from_le_bytes(h[0..4].try_into().unwrap());
    let sum = u32::from_le_bytes(h[4..8].try_into().unwrap());
    let log_index = u64::from_le_bytes(h[8..16].try_into().unwrap());
    let term = u64::from_le_bytes(h[16..24].try_into().unwrap());
    let ts = u64::from_le_bytes(h[24..32].try_into().unwrap());
    if (size as usize) < REC_HEADER || log_index != expect {
        return Ok(None);
    }
    let len = size - REC_HEADER as u32;
    if off + record_len(len as usize) > seg_bytes {
        return Ok(None);
    }
    payload.resize(len as usize, 0);
    f.read_exact_at(payload, off + REC_HEADER as u64)?;
    if crc(len, log_index, term, ts, payload) != sum {
        return Ok(None);
    }
    Ok(Some(Header {
        term,
        timestamp_ns: ts,
    }))
}

/// Reads one segment's records in order through a large buffer (one
/// system call per MiB instead of two per record).
struct Scanner<'f> {
    f: &'f File,
    seg_bytes: u64,
    buf: Vec<u8>,
    /// File offset of `buf[0]`.
    at: u64,
    /// Bytes read at a time.
    chunk: u64,
}

impl<'f> Scanner<'f> {
    fn new(f: &'f File, seg_bytes: u64) -> Scanner<'f> {
        Scanner::with_chunk(f, seg_bytes, 1 << 20)
    }

    fn with_chunk(f: &'f File, seg_bytes: u64, chunk: u64) -> Scanner<'f> {
        Scanner {
            f,
            seg_bytes,
            buf: Vec::new(),
            at: 0,
            chunk,
        }
    }

    /// Makes `[off, off + len)` available in the buffer; false if past the end.
    fn ensure(&mut self, off: u64, len: u64) -> io::Result<bool> {
        if off + len > self.seg_bytes {
            return Ok(false);
        }
        if off >= self.at && off + len <= self.at + self.buf.len() as u64 {
            return Ok(true);
        }
        let n = len.max(self.chunk).min(self.seg_bytes - off) as usize;
        self.buf.resize(n, 0);
        self.f.read_exact_at(&mut self.buf, off)?;
        self.at = off;
        Ok(true)
    }

    /// The record at `off` if it is valid and has index `expect`:
    /// (header, payload, record length).
    fn record(&mut self, off: u64, expect: u64) -> io::Result<Option<(Header, &[u8], u64)>> {
        if !self.ensure(off, REC_HEADER as u64)? {
            return Ok(None);
        }
        let h = &self.buf[(off - self.at) as usize..][..REC_HEADER];
        let size = u32::from_le_bytes(h[0..4].try_into().unwrap());
        let sum = u32::from_le_bytes(h[4..8].try_into().unwrap());
        let log_index = u64::from_le_bytes(h[8..16].try_into().unwrap());
        let term = u64::from_le_bytes(h[16..24].try_into().unwrap());
        let ts = u64::from_le_bytes(h[24..32].try_into().unwrap());
        if (size as usize) < REC_HEADER || log_index != expect {
            return Ok(None);
        }
        let len = (size as usize) - REC_HEADER;
        let rec = record_len(len);
        if !self.ensure(off, rec)? {
            return Ok(None);
        }
        let start = (off - self.at) as usize + REC_HEADER;
        let payload = &self.buf[start..start + len];
        if crc(len as u32, log_index, term, ts, payload) != sum {
            return Ok(None);
        }
        Ok(Some((
            Header {
                term,
                timestamp_ns: ts,
            },
            payload,
            rec,
        )))
    }
}

/// Room for a segment's sparse offsets (allocated once, so appends never
/// allocate).
fn sparse_capacity(seg_bytes: u64) -> usize {
    (seg_bytes / (REC_HEADER as u64 * SPARSE)) as usize + 1
}

/// Scans a segment and returns its sparse offsets and where its data ends.
fn scan_segment(f: &File, first: u64, seg_bytes: u64) -> io::Result<(Vec<u32>, u64, u64)> {
    let mut sparse = Vec::with_capacity(sparse_capacity(seg_bytes));
    let mut off = FILE_HEADER;
    let mut next = first;
    let mut scan = Scanner::new(f, seg_bytes);
    while let Some((_, _, rec)) = scan.record(off, next)? {
        if (next - first).is_multiple_of(SPARSE) {
            sparse.push(off as u32);
        }
        off += rec;
        next += 1;
    }
    Ok((sparse, off, next))
}

impl Log {
    /// Opens (or creates) the log in `cfg.dir`.
    pub fn open(cfg: LogConfig) -> io::Result<Opened> {
        assert!(cfg.segment_bytes > FILE_HEADER + 4096 && cfg.index_entries > 0);
        fs::create_dir_all(&cfg.dir)?;
        let (mut segs, mut idxs, mut snaps) = (Vec::new(), Vec::new(), Vec::new());
        for e in fs::read_dir(&cfg.dir)? {
            let e = e?;
            let n = e.file_name().to_string_lossy().into_owned();
            if let Some(i) = parse_name(&n, "log", "seg") {
                segs.push(i);
            } else if let Some(s) = parse_name(&n, "egress", "idx") {
                idxs.push(s);
            } else if let Some(i) = parse_name(&n, "snap", "snap") {
                snaps.push(i);
            } else if n.ends_with(".tmp") {
                fs::remove_file(e.path())?; // an interrupted snapshot
            }
        }
        segs.sort_unstable();
        idxs.sort_unstable();
        snaps.sort_unstable();

        let mut segments = Vec::new();
        for first in &segs {
            let path = cfg.dir.join(name("log", *first, "seg"));
            let file = OpenOptions::new().read(true).write(true).open(&path)?;
            check_header(&file, SEG_MAGIC, *first)?;
            segments.push(Segment {
                first_index: *first,
                path,
                file: Arc::new(file),
                sparse: Arc::new(Mutex::new(None)),
            });
        }
        if segments.is_empty() {
            let (path, file) = create_file(
                &cfg.dir,
                &name("log", 1, "seg"),
                SEG_MAGIC,
                1,
                cfg.segment_bytes,
            )?;
            segments.push(Segment {
                first_index: 1,
                path,
                file: Arc::new(file),
                sparse: Arc::new(Mutex::new(None)),
            });
        }

        // Find the end of the last segment; earlier ones end where the next begins.
        let last = segments.last().unwrap();
        let (active, active_first) = (last.file.clone(), last.first_index);
        let (sparse, off, next_index) = scan_segment(&active, active_first, cfg.segment_bytes)?;
        let active_sparse = last.sparse.clone();
        *active_sparse.lock().unwrap() = Some(sparse);
        let zero_to = (off + ZERO_AHEAD).min(cfg.segment_bytes);
        if zero_to > off {
            active.write_all_at(&vec![0u8; (zero_to - off) as usize], off)?;
            active.sync_data()?;
        }
        let last_index = next_index - 1;

        // Egress index: drop entries pointing past the end of the log.
        let mut indexes = Vec::new();
        for first in &idxs {
            let path = cfg.dir.join(name("egress", *first, "idx"));
            let file = OpenOptions::new().read(true).write(true).open(&path)?;
            check_header(&file, IDX_MAGIC, *first)?;
            indexes.push(IndexFile {
                first_seqno: *first,
                path,
                file: Arc::new(file),
            });
        }
        let mut next_seqno = 1;
        if let Some(ix) = indexes.last() {
            // Valid slots form a prefix (log indices only grow), so binary
            // search for its end instead of reading every slot.
            let valid = |k: u64| -> io::Result<bool> {
                let mut slot = [0u8; IDX_SLOT as usize];
                ix.file
                    .read_exact_at(&mut slot, FILE_HEADER + k * IDX_SLOT)?;
                let li = u64::from_le_bytes(slot[0..8].try_into().unwrap());
                Ok(li != 0 && li <= last_index)
            };
            let (mut lo, mut hi) = (0, cfg.index_entries);
            while lo < hi {
                let mid = lo + (hi - lo) / 2;
                if valid(mid)? { lo = mid + 1 } else { hi = mid }
            }
            let k = lo;
            next_seqno = ix.first_seqno + k;
            // Zero the stale slots after the end (entries past the log's end
            // from before a crash), up to the first slot that is already zero.
            let mut at = k;
            let mut chunk = vec![0u8; 4096 * IDX_SLOT as usize];
            while at < cfg.index_entries {
                let n = (cfg.index_entries - at).min(4096);
                let bytes = &mut chunk[..(n * IDX_SLOT) as usize];
                ix.file.read_exact_at(bytes, FILE_HEADER + at * IDX_SLOT)?;
                let stale = bytes
                    .chunks(IDX_SLOT as usize)
                    .take_while(|s| s[..8] != [0u8; 8])
                    .count() as u64;
                if stale > 0 {
                    ix.file.write_all_at(
                        &vec![0u8; (stale * IDX_SLOT) as usize],
                        FILE_HEADER + at * IDX_SLOT,
                    )?;
                }
                if stale < n {
                    break;
                }
                at += n;
            }
        }

        // The latest snapshot that is intact and not ahead of the log.
        let mut snapshot = None;
        let mut latest_snapshot = 0;
        for i in snaps.iter().rev() {
            if *i > last_index {
                continue;
            }
            if let Some(bytes) = read_snapshot(&cfg.dir.join(name("snap", *i, "snap")), *i)? {
                snapshot = Some((*i, bytes));
                latest_snapshot = *i;
                break;
            }
        }

        let earliest_seqno = indexes.first().map(|i| i.first_seqno).unwrap_or(1);
        let shared = Arc::new(Shared {
            segments: RwLock::new(segments),
            indexes: RwLock::new(indexes),
            flushed_index: AtomicU64::new(last_index),
            flushed_seqno: AtomicU64::new(next_seqno - 1),
            synced_seqno: AtomicU64::new(next_seqno - 1),
            earliest_seqno: AtomicU64::new(earliest_seqno),
        });
        let log = Log {
            cfg,
            shared,
            active,
            active_first,
            write_off: off,
            buf: Vec::with_capacity(1 << 20),
            buf_off: off,
            next_index,
            next_seqno,
            idx_buf: Vec::with_capacity(4096),
            idx_bytes: Vec::with_capacity(4096 * IDX_SLOT as usize),
            unsynced: Vec::new(),
            idx_synced: next_seqno - 1,
            latest_snapshot,
            sparse_pending: Vec::with_capacity(4096),
            active_sparse,
            last_published_index: 0,
        };
        Ok(Opened {
            log,
            snapshot,
            last_index,
        })
    }

    pub fn reader(&self) -> LogReader {
        LogReader {
            shared: self.shared.clone(),
            segment_bytes: self.cfg.segment_bytes,
            index_entries: self.cfg.index_entries,
        }
    }

    /// Log index the next `append` gets.
    pub fn next_index(&self) -> u64 {
        self.next_index
    }

    /// Seqno the next `index_published` must have.
    pub fn next_seqno(&self) -> u64 {
        self.next_seqno
    }

    /// Calls `f(pos, timestamp, payload)` for every entry from `from` to the
    /// end (for recovery replay). Entries must be contiguous.
    pub fn replay(&self, from: u64, mut f: impl FnMut(Pos, u64, &[u8])) -> io::Result<()> {
        self.replay_entries(from, |pos, h, payload| f(pos, h.timestamp_ns, payload))
    }

    /// Like [`Log::replay`], with each record's term.
    pub fn replay_entries(
        &self,
        from: u64,
        mut f: impl FnMut(Pos, Header, &[u8]),
    ) -> io::Result<()> {
        let segs = self.shared.segments.read().unwrap();
        let start = segs
            .partition_point(|s| s.first_index <= from)
            .saturating_sub(1);
        let mut expect = segs.get(start).map(|s| s.first_index).unwrap_or(from);
        for s in &segs[start..] {
            if s.first_index != expect {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "gap between log segments",
                ));
            }
            let mut off = FILE_HEADER;
            let mut scan = Scanner::new(&s.file, self.cfg.segment_bytes);
            while expect < self.next_index {
                match scan.record(off, expect)? {
                    Some((h, payload, rec)) => {
                        if expect >= from {
                            f(
                                Pos {
                                    log_index: expect,
                                    offset: off,
                                },
                                h,
                                payload,
                            );
                        }
                        off += rec;
                        expect += 1;
                    }
                    None => break, // end of this segment
                }
            }
        }
        if expect != self.next_index {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "log ends early"));
        }
        Ok(())
    }

    /// Buffers entry `next_index()` (term 0); it is readable after `flush`.
    pub fn append(&mut self, timestamp_ns: u64, payload: &[u8]) -> io::Result<Pos> {
        self.append_entry(0, timestamp_ns, payload)
    }

    /// Buffers entry `next_index()` with its Raft term.
    pub fn append_entry(
        &mut self,
        term: u64,
        timestamp_ns: u64,
        payload: &[u8],
    ) -> io::Result<Pos> {
        let n = record_len(payload.len());
        assert!(
            n + FILE_HEADER <= self.cfg.segment_bytes,
            "entry larger than a segment"
        );
        if self.write_off + n > self.cfg.segment_bytes {
            self.roll()?;
        }
        let log_index = self.next_index;
        let len = payload.len() as u32;
        let at = self.buf.len();
        self.buf.resize(at + n as usize, 0);
        let r = &mut self.buf[at..];
        r[0..4].copy_from_slice(&(len + REC_HEADER as u32).to_le_bytes());
        r[4..8].copy_from_slice(&crc(len, log_index, term, timestamp_ns, payload).to_le_bytes());
        r[8..16].copy_from_slice(&log_index.to_le_bytes());
        r[16..24].copy_from_slice(&term.to_le_bytes());
        r[24..32].copy_from_slice(&timestamp_ns.to_le_bytes());
        r[REC_HEADER..REC_HEADER + payload.len()].copy_from_slice(payload);
        let pos = Pos {
            log_index,
            offset: self.write_off,
        };
        if (log_index - self.active_first).is_multiple_of(SPARSE) {
            self.sparse_pending.push(self.write_off as u32);
        }
        self.write_off += n;
        self.next_index += 1;
        Ok(pos)
    }

    /// Records that stream message `seqno` came from the entry at `pos`.
    pub fn index_published(&mut self, seqno: u64, pos: Pos) {
        assert_eq!(
            seqno, self.next_seqno,
            "published seqnos must be contiguous"
        );
        self.idx_buf.push((seqno, pos));
        self.next_seqno += 1;
        self.last_published_index = pos.log_index;
    }

    /// Rewrites the index from `seqno` on (recovery replay regenerates it).
    pub fn reset_index_to(&mut self, seqno: u64) {
        assert!(self.idx_buf.is_empty());
        self.next_seqno = seqno;
        self.idx_synced = self.idx_synced.min(seqno - 1);
        self.shared
            .flushed_seqno
            .store(seqno - 1, Ordering::Release);
        self.shared.synced_seqno.store(seqno - 1, Ordering::Release);
    }

    fn roll(&mut self) -> io::Result<()> {
        self.flush()?;
        let sparse = Arc::new(Mutex::new(Some(Vec::with_capacity(sparse_capacity(
            self.cfg.segment_bytes,
        )))));
        let first = self.next_index;
        let (path, file) = create_file(
            &self.cfg.dir,
            &name("log", first, "seg"),
            SEG_MAGIC,
            first,
            self.cfg.segment_bytes,
        )?;
        let file = Arc::new(file);
        self.unsynced.push(self.active.clone());
        self.active = file.clone();
        self.active_first = first;
        self.write_off = FILE_HEADER;
        self.buf_off = FILE_HEADER;
        self.active_sparse = sparse.clone();
        self.shared.segments.write().unwrap().push(Segment {
            first_index: first,
            path,
            file,
            sparse,
        });
        Ok(())
    }

    /// Where entry `log_index` is (it must exist and be flushed or
    /// buffered).
    pub fn pos_of(&mut self, log_index: u64) -> io::Result<Pos> {
        if log_index >= self.active_first && log_index < self.next_index {
            self.flush()?;
        }
        let r = self.reader();
        r.locate(log_index)?
            .map(|(_, off)| Pos {
                log_index,
                offset: off,
            })
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no such entry"))
    }

    /// Removes entries `from..` (uncommitted entries a new leader
    /// overrides). Entries that were published cannot be removed.
    pub fn truncate_from(&mut self, from: u64) -> io::Result<()> {
        assert!(
            from > self.last_published_index && from >= 1,
            "truncating published entries"
        );
        if from >= self.next_index {
            return Ok(());
        }
        self.flush()?;
        let off = self
            .reader()
            .locate(from)?
            .map(|(_, off)| off)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "entry to cut not found"))?;
        let mut segs = self.shared.segments.write().unwrap();
        let k = segs.partition_point(|s| s.first_index <= from) - 1;
        // Later segments go entirely.
        let removed = segs.len() > k + 1;
        while segs.len() > k + 1 {
            let s = segs.pop().unwrap();
            fs::remove_file(&s.path)?;
        }
        if removed {
            sync_dir(&self.cfg.dir)?;
        }
        let seg = &segs[k];
        let end = if seg.first_index == self.active_first {
            self.write_off
        } else {
            self.cfg.segment_bytes
        };
        // Zero the cut tail so a later scan cannot bring it back.
        let zeros = vec![0u8; 1 << 20];
        let mut at = off;
        while at < end {
            let n = (end - at).min(zeros.len() as u64) as usize;
            seg.file.write_all_at(&zeros[..n], at)?;
            at += n as u64;
        }
        seg.file.sync_data()?;
        let first = seg.first_index;
        {
            let mut sp = seg.sparse.lock().unwrap();
            if let Some(v) = sp.as_mut() {
                v.truncate((from - first).div_ceil(SPARSE) as usize);
            }
        }
        self.active = seg.file.clone();
        self.active_sparse = seg.sparse.clone();
        self.active_first = first;
        drop(segs);
        self.unsynced.clear();
        self.sparse_pending.clear();
        self.write_off = off;
        self.buf_off = off;
        self.next_index = from;
        self.shared.flushed_index.store(from - 1, Ordering::Release);
        Ok(())
    }

    /// Writes everything buffered to the files (not yet durable) and makes
    /// it visible to readers.
    pub fn flush(&mut self) -> io::Result<()> {
        if !self.buf.is_empty() {
            self.active.write_all_at(&self.buf, self.buf_off)?;
            self.buf_off += self.buf.len() as u64;
            self.buf.clear();
        }
        if !self.sparse_pending.is_empty() {
            let mut sp = self.active_sparse.lock().unwrap();
            sp.get_or_insert_with(Vec::new)
                .extend_from_slice(&self.sparse_pending);
            self.sparse_pending.clear();
        }
        let mut i = 0;
        while i < self.idx_buf.len() {
            let (seqno, _) = self.idx_buf[i];
            let file = self.index_file_for(seqno)?;
            let k = seqno - file.0;
            // Write the run of entries that falls in this index file at once.
            let run = (self.cfg.index_entries - k).min((self.idx_buf.len() - i) as u64) as usize;
            self.idx_bytes.clear();
            for (_, p) in &self.idx_buf[i..i + run] {
                self.idx_bytes.extend_from_slice(&p.log_index.to_le_bytes());
                self.idx_bytes.extend_from_slice(&p.offset.to_le_bytes());
            }
            file.1
                .write_all_at(&self.idx_bytes, FILE_HEADER + k * IDX_SLOT)?;
            i += run;
        }
        self.idx_buf.clear();
        self.shared
            .flushed_index
            .store(self.next_index - 1, Ordering::Release);
        self.shared
            .flushed_seqno
            .store(self.next_seqno - 1, Ordering::Release);
        Ok(())
    }

    fn index_file_for(&self, seqno: u64) -> io::Result<(u64, Arc<File>)> {
        {
            // Usually the last file; after a restart, replay may rewrite an
            // earlier one.
            let idx = self.shared.indexes.read().unwrap();
            let i = idx.partition_point(|f| f.first_seqno <= seqno);
            if i > 0 && seqno < idx[i - 1].first_seqno + self.cfg.index_entries {
                return Ok((idx[i - 1].first_seqno, idx[i - 1].file.clone()));
            }
        }
        let first = match self.shared.indexes.read().unwrap().last() {
            Some(f) => f.first_seqno + self.cfg.index_entries,
            None => 1,
        };
        if seqno < first || seqno >= first + self.cfg.index_entries {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "index entry out of order",
            ));
        }
        let size = FILE_HEADER + self.cfg.index_entries * IDX_SLOT;
        let (path, file) = create_file(
            &self.cfg.dir,
            &name("egress", first, "idx"),
            IDX_MAGIC,
            first,
            size,
        )?;
        let file = Arc::new(file);
        self.shared.indexes.write().unwrap().push(IndexFile {
            first_seqno: first,
            path,
            file: file.clone(),
        });
        Ok((first, file))
    }

    /// `flush`, then make everything durable (fdatasync), the egress index
    /// included.
    pub fn sync(&mut self) -> io::Result<()> {
        self.sync_entries()?;
        // Every index file written since the last full sync, not only the
        // last one (the index may have rolled in between).
        let last = self.next_seqno - 1;
        if last > self.idx_synced {
            for ix in self.shared.indexes.read().unwrap().iter() {
                let end = ix.first_seqno + self.cfg.index_entries - 1;
                if end > self.idx_synced && ix.first_seqno <= last {
                    ix.file.sync_data()?;
                }
            }
            self.idx_synced = last;
        }
        Ok(())
    }

    /// `flush`, then make the entries durable (fdatasync of the segments
    /// only). Group commit: call once per batch of appends. The egress index
    /// is written but not synced: after a crash, recovery rewrites it from
    /// the last snapshot (whose [`write_snapshot`](Self::write_snapshot)
    /// syncs it fully), so a published message is durable once its entry
    /// is. One device flush per batch instead of two.
    pub fn sync_entries(&mut self) -> io::Result<()> {
        self.flush()?;
        for f in self.unsynced.drain(..) {
            f.sync_data()?;
        }
        self.active.sync_data()?;
        self.shared
            .synced_seqno
            .store(self.next_seqno - 1, Ordering::Release);
        Ok(())
    }

    /// Stores a snapshot of the core taken after `log_index` (which must be
    /// durable), atomically; keeps the previous one too.
    pub fn write_snapshot(&mut self, log_index: u64, bytes: &[u8]) -> io::Result<()> {
        assert!(log_index < self.next_index);
        self.sync()?;
        let tmp = self.cfg.dir.join(name("snap", log_index, "tmp"));
        let mut h = [0u8; 32];
        h[0..8].copy_from_slice(&SNAP_MAGIC.to_le_bytes());
        h[8..16].copy_from_slice(&log_index.to_le_bytes());
        h[16..24].copy_from_slice(&(bytes.len() as u64).to_le_bytes());
        h[24..28].copy_from_slice(&crc32c::crc32c(bytes).to_le_bytes());
        let f = File::create(&tmp)?;
        f.write_all_at(&h, 0)?;
        f.write_all_at(bytes, 32)?;
        f.sync_all()?;
        fs::rename(&tmp, self.cfg.dir.join(name("snap", log_index, "snap")))?;
        sync_dir(&self.cfg.dir)?;
        let previous = self.latest_snapshot;
        self.latest_snapshot = log_index;
        // Keep this one and the previous one.
        for e in fs::read_dir(&self.cfg.dir)? {
            let e = e?;
            let n = e.file_name().to_string_lossy().into_owned();
            if let Some(i) = parse_name(&n, "snap", "snap")
                && i < previous
            {
                fs::remove_file(e.path())?;
            }
        }
        Ok(())
    }

    /// Deletes the oldest segments while the log exceeds its budget, never
    /// at or above the latest snapshot. Returns the earliest retained seqno.
    pub fn enforce_retention(&mut self) -> io::Result<u64> {
        let mut deleted_below = None;
        {
            let mut segs = self.shared.segments.write().unwrap();
            while segs.len() >= 2
                && segs.len() as u64 * self.cfg.segment_bytes > self.cfg.retain_bytes
                && segs[1].first_index <= self.latest_snapshot + 1
            {
                let s = segs.remove(0);
                fs::remove_file(&s.path)?;
                deleted_below = Some(segs[0].first_index);
            }
        }
        if let Some(first_kept) = deleted_below {
            let reader = self.reader();
            let earliest = reader.first_seqno_at_or_after(first_kept)?;
            self.shared
                .earliest_seqno
                .store(earliest, Ordering::Release);
            let mut idx = self.shared.indexes.write().unwrap();
            while idx.len() >= 2 && idx[1].first_seqno <= earliest {
                let f = idx.remove(0);
                fs::remove_file(&f.path)?;
            }
        }
        Ok(self.shared.earliest_seqno.load(Ordering::Acquire))
    }
}

fn read_snapshot(path: &Path, log_index: u64) -> io::Result<Option<Vec<u8>>> {
    let f = File::open(path)?;
    let mut h = [0u8; 32];
    if f.read_exact_at(&mut h, 0).is_err() {
        return Ok(None);
    }
    let len = u64::from_le_bytes(h[16..24].try_into().unwrap());
    if u64::from_le_bytes(h[0..8].try_into().unwrap()) != SNAP_MAGIC
        || u64::from_le_bytes(h[8..16].try_into().unwrap()) != log_index
        || f.metadata()?.len() != 32 + len
    {
        return Ok(None);
    }
    let mut bytes = vec![0u8; len as usize];
    f.read_exact_at(&mut bytes, 32)?;
    if crc32c::crc32c(&bytes) != u32::from_le_bytes(h[24..28].try_into().unwrap()) {
        return Ok(None);
    }
    Ok(Some(bytes))
}

/// A published message read back from disk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Stored {
    pub log_index: u64,
    pub timestamp_ns: u64,
}

/// Concurrent read access to everything flushed.
#[derive(Clone)]
pub struct LogReader {
    shared: Arc<Shared>,
    segment_bytes: u64,
    index_entries: u64,
}

impl LogReader {
    /// Highest seqno readable.
    pub fn last_seqno(&self) -> u64 {
        self.shared.flushed_seqno.load(Ordering::Acquire)
    }

    /// Highest seqno that is durable (fsynced).
    pub fn durable_seqno(&self) -> u64 {
        self.shared.synced_seqno.load(Ordering::Acquire)
    }

    /// Lowest seqno still retained.
    pub fn earliest_seqno(&self) -> u64 {
        self.shared.earliest_seqno.load(Ordering::Acquire)
    }

    fn slot(&self, seqno: u64) -> io::Result<Option<Pos>> {
        let file = {
            let idx = self.shared.indexes.read().unwrap();
            let i = idx.partition_point(|f| f.first_seqno <= seqno);
            if i == 0 {
                return Ok(None);
            }
            let f = &idx[i - 1];
            if seqno >= f.first_seqno + self.index_entries {
                return Ok(None);
            }
            (f.first_seqno, f.file.clone())
        };
        let mut s = [0u8; IDX_SLOT as usize];
        file.1
            .read_exact_at(&mut s, FILE_HEADER + (seqno - file.0) * IDX_SLOT)?;
        let log_index = u64::from_le_bytes(s[0..8].try_into().unwrap());
        if log_index == 0 {
            return Ok(None);
        }
        Ok(Some(Pos {
            log_index,
            offset: u64::from_le_bytes(s[8..16].try_into().unwrap()),
        }))
    }

    /// Reads the entry that published `seqno` into `payload`. None if it is
    /// not retained or not yet flushed.
    pub fn read_published(&self, seqno: u64, payload: &mut Vec<u8>) -> io::Result<Option<Stored>> {
        if seqno == 0 || seqno > self.last_seqno() || seqno < self.earliest_seqno() {
            return Ok(None);
        }
        let Some(pos) = self.slot(seqno)? else {
            return Ok(None);
        };
        let file = {
            let segs = self.shared.segments.read().unwrap();
            let i = segs.partition_point(|s| s.first_index <= pos.log_index);
            if i == 0 {
                return Ok(None); // deleted by retention meanwhile
            }
            segs[i - 1].file.clone()
        };
        match read_record(
            &file,
            pos.offset,
            pos.log_index,
            self.segment_bytes,
            payload,
        )? {
            Some(h) => Ok(Some(Stored {
                log_index: pos.log_index,
                timestamp_ns: h.timestamp_ns,
            })),
            None => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "egress index points at a bad record",
            )),
        }
    }

    /// Highest log index readable.
    pub fn last_index(&self) -> u64 {
        self.shared.flushed_index.load(Ordering::Acquire)
    }

    /// The segment file and offset of entry `log_index`, if retained and
    /// flushed.
    fn locate(&self, log_index: u64) -> io::Result<Option<(Arc<File>, u64)>> {
        if log_index == 0 || log_index > self.last_index() {
            return Ok(None);
        }
        let (file, first, sparse) = {
            let segs = self.shared.segments.read().unwrap();
            let i = segs.partition_point(|s| s.first_index <= log_index);
            if i == 0 {
                return Ok(None);
            }
            let s = &segs[i - 1];
            (s.file.clone(), s.first_index, s.sparse.clone())
        };
        let k = (log_index - first) / SPARSE;
        let start = {
            let mut sp = sparse.lock().unwrap();
            if sp.is_none() {
                *sp = Some(scan_segment(&file, first, self.segment_bytes)?.0);
            }
            match sp.as_ref().unwrap().get(k as usize) {
                Some(o) => *o as u64,
                None => return Ok(None),
            }
        };
        // Walk forward from the sparse entry (a few KiB for small entries).
        let mut off = start;
        let mut at = first + k * SPARSE;
        let mut scan = Scanner::with_chunk(&file, self.segment_bytes, 16 << 10);
        while at < log_index {
            match scan.record(off, at)? {
                Some((_, _, rec)) => off += rec,
                None => return Ok(None),
            }
            at += 1;
        }
        Ok(Some((file, off)))
    }

    /// Reads entry `log_index` into `payload`. None if it is not retained
    /// or not flushed.
    pub fn read_entry(&self, log_index: u64, payload: &mut Vec<u8>) -> io::Result<Option<Header>> {
        let Some((file, off)) = self.locate(log_index)? else {
            return Ok(None);
        };
        read_record(&file, off, log_index, self.segment_bytes, payload)
    }

    /// The first seqno whose entry is at or after `log_index` (binary search).
    fn first_seqno_at_or_after(&self, log_index: u64) -> io::Result<u64> {
        let (mut lo, mut hi) = (self.earliest_seqno(), self.last_seqno() + 1);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match self.slot(mid)? {
                Some(p) if p.log_index >= log_index => hi = mid,
                _ => lo = mid + 1,
            }
        }
        Ok(lo)
    }
}

/// The Raft hard state (term, vote), in its own small file. Two 32-byte
/// slots are written alternately, each with a generation and a CRC, so a
/// torn write leaves the previous state readable.
pub struct HardStateFile {
    file: File,
    generation: u64,
}

const HS_MAGIC: u32 = 0x5348_434B; // "KCHS"

impl HardStateFile {
    /// Opens (or creates) `dir/raft.state`; returns the latest (term, vote).
    pub fn open(dir: &Path) -> io::Result<(HardStateFile, (u64, u32))> {
        fs::create_dir_all(dir)?;
        let path = dir.join("raft.state");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        let mut best = (0u64, (0u64, 0u32));
        for slot in 0..2u64 {
            let mut b = [0u8; 32];
            if file.read_exact_at(&mut b, slot * 32).is_err() {
                continue;
            }
            let magic = u32::from_le_bytes(b[0..4].try_into().unwrap());
            let sum = u32::from_le_bytes(b[4..8].try_into().unwrap());
            let generation = u64::from_le_bytes(b[8..16].try_into().unwrap());
            if magic != HS_MAGIC || crc32c::crc32c(&b[8..32]) != sum {
                continue;
            }
            let term = u64::from_le_bytes(b[16..24].try_into().unwrap());
            let vote = u32::from_le_bytes(b[24..28].try_into().unwrap());
            if generation > best.0 {
                best = (generation, (term, vote));
            }
        }
        sync_dir(dir)?;
        Ok((
            HardStateFile {
                file,
                generation: best.0,
            },
            best.1,
        ))
    }

    /// Writes (term, vote; 0 = none); with `sync`, also fdatasync.
    pub fn store(&mut self, term: u64, vote: u32, sync: bool) -> io::Result<()> {
        self.generation += 1;
        let mut b = [0u8; 32];
        b[0..4].copy_from_slice(&HS_MAGIC.to_le_bytes());
        b[8..16].copy_from_slice(&self.generation.to_le_bytes());
        b[16..24].copy_from_slice(&term.to_le_bytes());
        b[24..28].copy_from_slice(&vote.to_le_bytes());
        let sum = crc32c::crc32c(&b[8..32]);
        b[4..8].copy_from_slice(&sum.to_le_bytes());
        self.file.write_all_at(&b, (self.generation % 2) * 32)?;
        if sync {
            self.file.sync_data()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
