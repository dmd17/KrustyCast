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
            log.index_published(s, pos);
        }
    }
    log.sync().unwrap();
}

fn replayed(log: &Log, from: u64) -> Vec<(u64, u64, Vec<u8>)> {
    let mut v = Vec::new();
    log.replay(from, |i, ts, p| v.push((i, ts, p.to_vec())))
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
    let names: Vec<String> = fs::read_dir(&d.0)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.ends_with(".snap"))
        .collect();
    assert_eq!(names.len(), 2, "keeps the latest two: {names:?}");
    drop(log);
    assert_eq!(
        Log::open(cfg(&d)).unwrap().snapshot,
        Some((10, b"ten".to_vec()))
    );
    // Corrupt the newest: open falls back to the previous one.
    let p = d.0.join(name("snap", 10, "snap"));
    let f = OpenOptions::new().write(true).open(&p).unwrap();
    f.write_all_at(b"X", 32).unwrap();
    assert_eq!(
        Log::open(cfg(&d)).unwrap().snapshot,
        Some((8, b"eight".to_vec()))
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
