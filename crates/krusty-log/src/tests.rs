use super::*;

struct Dir(PathBuf);

impl Dir {
    fn new(tag: &str) -> Dir {
        let p = std::env::temp_dir().join(format!("krusty-log-{tag}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&p);
        Dir(p)
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn cfg(d: &Dir) -> LogConfig {
    LogConfig {
        dir: d.0.clone(),
        segment_bytes: 64 << 10,
        index_entries: 256,
        retain_bytes: u64::MAX,
    }
}

fn payload(i: u64) -> Vec<u8> {
    (0..(i % 50) as usize)
        .map(|k| (i as usize + k) as u8)
        .collect()
}

/// Appends entries `from..to`, publishing every even one, then syncs.
fn fill(log: &mut Log, from: u64, to: u64) {
    for i in from..to {
        let pos = log.append(i * 1000, &payload(i)).unwrap();
        assert_eq!(pos.log_index, i);
        if i % 2 == 0 {
            let s = log.next_seqno();
            log.index_published(s, pos).unwrap();
        }
    }
    log.sync().unwrap();
}

fn replayed(log: &Log, from: u64) -> Vec<(u64, u64, Vec<u8>)> {
    let mut v = Vec::new();
    log.replay(from, |pos, ts, p| v.push((pos.log_index, ts, p.to_vec())))
        .unwrap();
    v
}

#[test]
fn survives_reopen_and_rolls_segments() {
    let d = Dir::new("reopen");
    let mut log = Log::open(cfg(&d)).unwrap().log;
    fill(&mut log, 1, 3001);
    drop(log);
    let o = Log::open(cfg(&d)).unwrap();
    assert_eq!(o.last_index, 3000);
    assert!(
        o.log.shared.segments.read().unwrap().len() > 1,
        "small segments must have rolled"
    );
    let all = replayed(&o.log, 1);
    assert_eq!(all.len(), 3000);
    for (k, (i, ts, p)) in all.iter().enumerate() {
        assert_eq!(
            (*i, *ts, p.clone()),
            (k as u64 + 1, (k as u64 + 1) * 1000, payload(k as u64 + 1))
        );
    }
    assert_eq!(replayed(&o.log, 2500).first().map(|e| e.0), Some(2500));
    // The egress index: seqno n came from entry 2n.
    let r = o.log.reader();
    assert_eq!(r.last_seqno(), 1500);
    let mut buf = Vec::new();
    for n in [1u64, 2, 700, 1500] {
        let s = r.read_published(n, &mut buf).unwrap().unwrap();
        assert_eq!((s.log_index, s.timestamp_ns), (2 * n, 2 * n * 1000));
        assert_eq!(buf, payload(2 * n));
    }
    assert_eq!(r.read_published(1501, &mut buf).unwrap(), None);
}

#[test]
fn torn_tail_is_cut_and_never_resurrected() {
    let d = Dir::new("torn");
    let mut log = Log::open(cfg(&d)).unwrap().log;
    fill(&mut log, 1, 101);
    // Corrupt entry 90 in place (as a torn write would leave it).
    let pos90 = {
        let r = log.reader();
        let mut b = Vec::new();
        r.read_published(45, &mut b).unwrap().unwrap(); // seqno 45 = entry 90
        r.slot(45).unwrap().unwrap()
    };
    let seg = log.active.clone();
    seg.write_all_at(&[0xFF; 4], pos90.offset + REC_HEADER as u64)
        .unwrap();
    drop(log);

    let o = Log::open(cfg(&d)).unwrap();
    assert_eq!(
        o.last_index, 89,
        "everything from the torn record on is gone"
    );
    assert_eq!(
        o.log.next_seqno(),
        45,
        "index entries past the end are dropped"
    );
    let mut log = o.log;
    // New entries overwrite the old region; the stale 91..100 must not come back.
    fill(&mut log, 90, 95);
    drop(log);
    let o = Log::open(cfg(&d)).unwrap();
    assert_eq!(o.last_index, 94);
    let all = replayed(&o.log, 1);
    assert_eq!(all.len(), 94);
    assert_eq!(all[93].2, payload(94));
}

#[test]
fn a_cut_short_segment_recovers_to_the_last_whole_record() {
    let d = Dir::new("cut");
    let mut log = Log::open(cfg(&d)).unwrap().log;
    fill(&mut log, 1, 51);
    let end = log.write_off;
    let seg = log.active.clone();
    drop(log);
    // Zero the last 20 bytes, as if the final write never fully landed.
    seg.write_all_at(&[0u8; 20], end - 20).unwrap();
    let o = Log::open(cfg(&d)).unwrap();
    assert_eq!(o.last_index, 49);
}

#[test]
fn snapshots_are_atomic_and_fall_back_when_corrupt() {
    let d = Dir::new("snap");
    let mut log = Log::open(cfg(&d)).unwrap().log;
    fill(&mut log, 1, 11);
    log.write_snapshot(5, b"five").unwrap();
    log.write_snapshot(8, b"eight").unwrap();
    log.write_snapshot(10, b"ten").unwrap();
    // Two slots, overwritten in place: the latest two snapshots.
    let mut held: Vec<u64> = SNAP_SLOTS
        .iter()
        .filter_map(|n| slot_index(&d.0.join(n)))
        .collect();
    held.sort_unstable();
    assert_eq!(held, vec![8, 10]);
    drop(log);
    assert_eq!(
        Log::open(cfg(&d)).unwrap().snapshot,
        Some((10, b"ten".to_vec()))
    );
    // Corrupt the newest (as a torn write would): open falls back.
    let newest = SNAP_SLOTS
        .iter()
        .find(|n| slot_index(&d.0.join(n)) == Some(10))
        .unwrap();
    let f = OpenOptions::new()
        .write(true)
        .open(d.0.join(newest))
        .unwrap();
    f.write_all_at(b"X", 32).unwrap();
    assert_eq!(
        Log::open(cfg(&d)).unwrap().snapshot,
        Some((8, b"eight".to_vec()))
    );
}

#[test]
fn snapshots_of_the_old_format_are_read_then_replaced() {
    let d = Dir::new("snapold");
    let mut log = Log::open(cfg(&d)).unwrap().log;
    fill(&mut log, 1, 11);
    // The old format: one file per snapshot.
    let mut h = [0u8; 32];
    h[0..8].copy_from_slice(&SNAP_MAGIC.to_le_bytes());
    h[8..16].copy_from_slice(&7u64.to_le_bytes());
    h[16..24].copy_from_slice(&5u64.to_le_bytes());
    h[24..28].copy_from_slice(&crc32c::crc32c(b"seven").to_le_bytes());
    let mut file = h.to_vec();
    file.extend_from_slice(b"seven");
    fs::write(d.0.join(name("snap", 7, "snap")), &file).unwrap();
    drop(log);
    let mut o = Log::open(cfg(&d)).unwrap();
    assert_eq!(o.snapshot, Some((7, b"seven".to_vec())));
    o.log.write_snapshot(9, b"nine").unwrap();
    assert!(
        !d.0.join(name("snap", 7, "snap")).exists(),
        "old file removed"
    );
    drop(o);
    assert_eq!(
        Log::open(cfg(&d)).unwrap().snapshot,
        Some((9, b"nine".to_vec()))
    );
}

#[test]
fn retention_deletes_only_below_the_snapshot() {
    let d = Dir::new("retain");
    let c = LogConfig {
        retain_bytes: 3 * (64 << 10),
        ..cfg(&d)
    };
    let mut log = Log::open(c.clone()).unwrap().log;
    fill(&mut log, 1, 6001);
    let segs = log.shared.segments.read().unwrap().len();
    assert!(segs > 4);
    // No snapshot yet: nothing may go.
    assert_eq!(log.enforce_retention().unwrap(), 1);
    assert_eq!(log.shared.segments.read().unwrap().len(), segs);
    log.write_snapshot(5000, b"s").unwrap();
    let earliest = log.enforce_retention().unwrap();
    let kept = log.shared.segments.read().unwrap();
    assert!(kept.len() < segs);
    assert!(kept[0].first_index <= 5001, "the snapshot's suffix is kept");
    drop(kept);
    let r = log.reader();
    let mut b = Vec::new();
    assert!(earliest > 1);
    assert_eq!(r.read_published(earliest - 1, &mut b).unwrap(), None);
    assert!(r.read_published(earliest, &mut b).unwrap().is_some());
    drop(log);
    // A restart can still load the snapshot and replay its suffix.
    let o = Log::open(c).unwrap();
    assert_eq!(o.snapshot.as_ref().map(|s| s.0), Some(5000));
    assert_eq!(replayed(&o.log, 5001).len(), 1000);
}

#[test]
fn a_snapshot_job_stores_off_the_writer_and_retention_waits_for_it() {
    let d = Dir::new("snapjob");
    let c = LogConfig {
        retain_bytes: 3 * (64 << 10),
        ..cfg(&d)
    };
    let mut log = Log::open(c.clone()).unwrap().log;
    fill(&mut log, 1, 6001);
    let segs = log.shared.segments.read().unwrap().len();
    let job = log.snapshot_job(5000, b"s".to_vec()).unwrap();
    // Not yet stored: retention may delete nothing.
    log.enforce_retention().unwrap();
    assert_eq!(log.shared.segments.read().unwrap().len(), segs);
    // Run on another thread while the writer appends more.
    let t = std::thread::spawn(move || job.run().unwrap());
    fill(&mut log, 6001, 6101);
    assert_eq!(t.join().unwrap(), b"s".to_vec());
    log.enforce_retention().unwrap();
    assert!(log.shared.segments.read().unwrap().len() < segs);
    log.sync().unwrap();
    drop(log);
    let o = Log::open(c).unwrap();
    assert_eq!(o.snapshot.as_ref().map(|s| s.0), Some(5000));
    assert_eq!(replayed(&o.log, 5001).len(), 1100);
}

#[test]
fn a_sync_job_makes_written_entries_durable_off_the_writer() {
    let d = Dir::new("syncjob");
    let mut log = Log::open(cfg(&d)).unwrap().log;
    for i in 1..=500u64 {
        let pos = log.append(i, &payload(i)).unwrap();
        let s = log.next_seqno();
        log.index_published(s, pos).unwrap();
    }
    let job = log.take_sync().unwrap();
    // Written out (readable) before the fsync runs; not yet durable.
    assert_eq!(log.reader().last_seqno(), 500);
    assert_eq!(log.shared.synced_seqno.load(Ordering::Acquire), 0);
    std::thread::spawn(move || job.run().unwrap())
        .join()
        .unwrap();
    assert_eq!(log.shared.synced_seqno.load(Ordering::Acquire), 500);
}

#[test]
fn readers_see_flushed_entries_while_the_writer_appends() {
    let d = Dir::new("concurrent");
    let mut log = Log::open(cfg(&d)).unwrap().log;
    let r = log.reader();
    let t = std::thread::spawn(move || {
        let mut b = Vec::new();
        let mut seen = 0;
        while seen < 2000 {
            let last = r.last_seqno();
            for n in seen + 1..=last {
                let s = r
                    .read_published(n, &mut b)
                    .unwrap()
                    .expect("flushed entries are readable");
                assert_eq!(s.log_index, 2 * n);
                assert_eq!(b, payload(2 * n));
            }
            seen = last;
        }
    });
    for chunk in 0..40u64 {
        fill(&mut log, chunk * 100 + 1, chunk * 100 + 101);
    }
    t.join().unwrap();
}

#[test]
fn reindexing_after_restart_can_rewrite_an_earlier_index_file() {
    let d = Dir::new("reindex");
    let mut log = Log::open(cfg(&d)).unwrap().log; // 256 seqnos per index file
    fill(&mut log, 1, 1001); // seqnos 1..500: two index files
    drop(log);
    let mut log = Log::open(cfg(&d)).unwrap().log;
    assert_eq!(log.next_seqno(), 501);
    // As a restart from a snapshot taken at seqno 200 does: rewrite from 201.
    log.reset_index_to(201);
    for n in 201..=500u64 {
        log.index_published(
            n,
            Pos {
                log_index: 2 * n,
                offset: 0,
            },
        )
        .unwrap();
    }
    log.flush().unwrap();
    assert_eq!(log.next_seqno(), 501);
}

#[test]
fn entries_are_found_by_index_and_carry_their_term() {
    let d = Dir::new("byindex");
    let mut log = Log::open(cfg(&d)).unwrap().log;
    for i in 1..3001u64 {
        log.append_entry(i / 100, i * 1000, &payload(i)).unwrap();
    }
    log.sync().unwrap();
    let r = log.reader();
    let mut p = Vec::new();
    for i in [1u64, 2, 63, 64, 65, 128, 1000, 2999, 3000] {
        let h = r.read_entry(i, &mut p).unwrap().unwrap();
        assert_eq!(
            (h.term, h.timestamp_ns, &p[..]),
            (i / 100, i * 1000, &payload(i)[..])
        );
    }
    assert!(r.read_entry(3001, &mut p).unwrap().is_none());
    drop(log);
    // After reopening, older segments' sparse indexes are rebuilt on demand.
    let o = Log::open(cfg(&d)).unwrap();
    let r = o.log.reader();
    for i in [1u64, 777, 3000] {
        let h = r.read_entry(i, &mut p).unwrap().unwrap();
        assert_eq!((h.term, &p[..]), (i / 100, &payload(i)[..]));
    }
    let mut terms = Vec::new();
    o.log
        .replay_entries(2990, |pos, h, _| terms.push((pos.log_index, h.term)))
        .unwrap();
    assert_eq!(terms.len(), 11);
    assert_eq!(terms[0], (2990, 29));
}

#[test]
fn truncation_removes_the_tail_for_good() {
    let d = Dir::new("truncate");
    let mut log = Log::open(cfg(&d)).unwrap().log;
    for i in 1..3001u64 {
        log.append_entry(1, i, &payload(i)).unwrap();
    }
    log.sync().unwrap();
    // Cut in an earlier segment (later segments go too), then write
    // different, shorter entries in a new term.
    log.truncate_from(1500).unwrap();
    assert_eq!(log.next_index(), 1500);
    for i in 1500..1510u64 {
        log.append_entry(2, i, b"x").unwrap();
    }
    log.sync().unwrap();
    // A cut in the active segment, between buffered and flushed entries.
    log.truncate_from(1505).unwrap();
    log.append_entry(3, 1505, b"yy").unwrap();
    log.sync().unwrap();
    drop(log);
    let o = Log::open(cfg(&d)).unwrap();
    assert_eq!(o.last_index, 1505);
    let mut seen = Vec::new();
    o.log
        .replay_entries(1495, |pos, h, p| {
            seen.push((pos.log_index, h.term, p.to_vec()))
        })
        .unwrap();
    assert_eq!(seen.len(), 11);
    assert_eq!(seen[4], (1499, 1, payload(1499)));
    assert_eq!(seen[5], (1500, 2, b"x".to_vec()));
    assert_eq!(seen[10], (1505, 3, b"yy".to_vec()));
    let mut p = Vec::new();
    assert_eq!(
        o.log
            .reader()
            .read_entry(1505, &mut p)
            .unwrap()
            .unwrap()
            .term,
        3
    );
}

#[test]
fn hard_state_survives_and_tolerates_a_torn_slot() {
    let d = Dir::new("hardstate");
    let (mut f, hs) = HardStateFile::open(&d.0).unwrap();
    assert_eq!(hs, (0, 0));
    f.store(3, 2, true).unwrap();
    f.store(4, 0, true).unwrap();
    drop(f);
    let (mut f, hs) = HardStateFile::open(&d.0).unwrap();
    assert_eq!(hs, (4, 0));
    f.store(5, 1, true).unwrap();
    drop(f);
    // Tear the newest slot: the previous state comes back.
    let file = OpenOptions::new()
        .write(true)
        .open(d.0.join("raft.state"))
        .unwrap();
    file.write_all_at(&[0xFF; 4], 3 * 32 % 64 + 20).unwrap();
    let (_, hs) = HardStateFile::open(&d.0).unwrap();
    assert_eq!(hs, (4, 0));
}

/// The next segment and index file are prepared ahead (`*-spare.tmp`), and
/// rolling over renames them into place.
#[test]
fn spare_files_are_prepared_ahead_and_renamed_into_place() {
    let d = Dir::new("spare");
    let mut log = Log::open(cfg(&d)).unwrap().log;
    fill(&mut log, 1, 3001);
    let names = || {
        let mut v: Vec<String> = fs::read_dir(&d.0)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        v.sort();
        v
    };
    // The helper threads prepare the next files in the background.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !(names().contains(&"log-spare.tmp".to_string())
        && names().contains(&"egress-spare.tmp".to_string()))
    {
        assert!(
            std::time::Instant::now() < deadline,
            "no spares: {:?}",
            names()
        );
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    let segs = names().iter().filter(|n| n.ends_with(".seg")).count();
    assert!(segs > 1, "segments must have rolled: {:?}", names());
    drop(log);
    let o = Log::open(cfg(&d)).unwrap();
    assert_eq!(o.last_index, 3000);
    assert_eq!(replayed(&o.log, 1).len(), 3000);
}

/// A crash after a roll's rename but before the next sync can leave tail
/// files whose header never reached the disk; nothing in them was synced,
/// so `open` drops them. An unwritten header before a valid file is
/// corruption.
#[test]
fn tail_files_with_unwritten_headers_are_dropped() {
    let d = Dir::new("unwritten");
    let mut log = Log::open(cfg(&d)).unwrap().log;
    fill(&mut log, 1, 3001);
    let next_seqno = log.next_seqno();
    drop(log);
    let zeros = |name: &str, len: u64| {
        let f = File::create(d.0.join(name)).unwrap();
        f.set_len(len).unwrap();
    };
    let seg = name("log", 3001, "seg");
    let idx_first = {
        let mut v: Vec<u64> = fs::read_dir(&d.0)
            .unwrap()
            .filter_map(|e| parse_name(&e.unwrap().file_name().to_string_lossy(), "egress", "idx"))
            .collect();
        v.sort();
        v.last().unwrap() + 256
    };
    assert!(idx_first > next_seqno - 1);
    let idx = name("egress", idx_first, "idx");
    zeros(&seg, 64 << 10);
    zeros(&idx, FILE_HEADER + 256 * IDX_SLOT);
    let o = Log::open(cfg(&d)).unwrap();
    assert_eq!(o.last_index, 3000);
    assert_eq!(o.log.next_seqno(), next_seqno);
    assert!(!d.0.join(&seg).exists() && !d.0.join(&idx).exists());
    drop(o);

    // In the middle: refuse to open.
    let first = {
        let mut v: Vec<u64> = fs::read_dir(&d.0)
            .unwrap()
            .filter_map(|e| parse_name(&e.unwrap().file_name().to_string_lossy(), "log", "seg"))
            .collect();
        v.sort();
        v[1]
    };
    let f = OpenOptions::new()
        .write(true)
        .open(d.0.join(name("log", first, "seg")))
        .unwrap();
    f.write_all_at(&[0u8; FILE_HEADER as usize], 0).unwrap();
    assert!(Log::open(cfg(&d)).is_err());
}
