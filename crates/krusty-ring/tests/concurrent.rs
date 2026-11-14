//! Cross-thread tests (also run under Miri with smaller counts).
use krusty_ring::byte_ring::byte_ring;
use krusty_ring::history::{History, ReadError};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

fn content(i: u64, len: usize) -> impl Iterator<Item = u8> {
    (0..len).map(move |k| (i as usize * 31 + k) as u8)
}

#[test]
fn byte_ring_across_threads_preserves_order_and_content() {
    let n: u64 = if cfg!(miri) { 300 } else { 1_000_000 };
    let (mut p, mut c) = byte_ring(1024);
    let t = std::thread::spawn(move || {
        let mut i = 0;
        while i < n {
            let len = (i % 97) as usize;
            // Publish in batches of up to 8, as the stages do.
            if p.try_write(i as u32, len, |b| {
                for (d, s) in b.iter_mut().zip(content(i, len)) {
                    *d = s;
                }
            }) {
                i += 1;
                if i % 8 == 0 {
                    p.publish();
                }
            } else {
                p.publish();
                std::hint::spin_loop();
            }
        }
        p.publish();
    });
    let mut next = 0u64;
    while next < n {
        let got = c.read(16, |kind, b| {
            assert_eq!(kind, next as u32);
            assert_eq!(b.len(), (next % 97) as usize);
            assert!(b.iter().copied().eq(content(next, b.len())));
            next += 1;
        });
        if got == 0 {
            std::hint::spin_loop();
        }
    }
    t.join().unwrap();
}

#[test]
fn history_readers_never_see_torn_or_wrong_messages() {
    let n: u64 = if cfg!(miri) { 200 } else { 300_000 };
    let h = Arc::new(History::new(8, 64)); // tiny: constant overwriting
    let done = Arc::new(AtomicBool::new(false));
    let readers: Vec<_> = (0..2)
        .map(|r| {
            let (h, done) = (h.clone(), done.clone());
            std::thread::spawn(move || {
                let mut out = [0u8; 64];
                let (mut ok, mut x) = (0u64, r as u64 + 1);
                while !done.load(Ordering::Relaxed) {
                    let c = h.committed();
                    if c == 0 {
                        continue;
                    }
                    x = x.wrapping_mul(6364136223846793005).wrapping_add(1);
                    let s = c.saturating_sub((x >> 33) % 12).max(1);
                    match h.read(s, &mut out) {
                        Ok(len) => {
                            assert_eq!(len, (s % 64) as usize);
                            assert!(out[..len].iter().copied().eq(content(s, len)), "seqno {s}");
                            ok += 1;
                        }
                        Err(ReadError::Gone) | Err(ReadError::NotYet) => {}
                    }
                }
                ok
            })
        })
        .collect();
    let mut buf = [0u8; 64];
    for s in 1..=n {
        let len = (s % 64) as usize;
        for (d, v) in buf[..len].iter_mut().zip(content(s, len)) {
            *d = v;
        }
        h.push(s, &buf[..len]);
    }
    done.store(true, Ordering::Relaxed);
    let reads: u64 = readers.into_iter().map(|t| t.join().unwrap()).sum();
    assert!(cfg!(miri) || reads > 0);
}
