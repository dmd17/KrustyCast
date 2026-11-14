//! Plain Rust tests (built by ordinary cargo; Verus ghost code is erased).
use verus_ring::new_queue;

#[test]
fn push_pop_wraps_and_reports_full_and_empty() {
    let (mut p, mut c) = new_queue::<u64>(4); // holds 3: one slot stays empty
    assert_eq!(c.try_pop(), None);
    for round in 0..10u64 {
        for i in 0..3 {
            assert_eq!(p.try_push(round * 10 + i), Ok(()));
        }
        assert_eq!(p.try_push(99), Err(99));
        for i in 0..3 {
            assert_eq!(c.try_pop(), Some(round * 10 + i));
        }
        assert_eq!(c.try_pop(), None);
    }
}

#[test]
fn two_threads_preserve_order() {
    const N: u64 = if cfg!(miri) { 2_000 } else { 2_000_000 };
    let (mut p, mut c) = new_queue::<u64>(1024);
    let t = std::thread::spawn(move || {
        for i in 0..N {
            let mut v = i;
            while let Err(back) = p.try_push(v) {
                v = back;
                std::hint::spin_loop();
            }
        }
    });
    let mut next = 0;
    while next < N {
        if let Some(v) = c.try_pop() {
            assert_eq!(v, next);
            next += 1;
        } else {
            std::hint::spin_loop();
        }
    }
    t.join().unwrap();
}

// Values still in the ring when it is dropped are leaked (the cells hold
// `MaybeUninit<T>` and there is no verified `Drop`); Miri reports it for
// `String`. KrustyCast rings carry `Copy` handles into preallocated pools, so
// this is a documented restriction, not a fix: see REPORT.md.

mod batched {
    use verus_ring::batched::new_batch_ring;

    #[test]
    fn batches_wrap_and_respect_capacity() {
        let (mut p, mut c) = new_batch_ring::<u64>(8); // holds 7
        let mut out = Vec::new();
        assert_eq!(c.pop_batch(&mut out, 10), 0);
        assert_eq!(p.push_batch(&[1, 2, 3, 4, 5, 6, 7, 8, 9]), 7); // one cell stays empty
        assert_eq!(p.push_batch(&[99]), 0);
        assert_eq!(c.pop_batch(&mut out, 3), 3);
        // Room for 3 more, but a batch stops at the end of the buffer (index 7).
        assert_eq!(p.push_batch(&[8, 9, 10]), 1);
        assert_eq!(p.push_batch(&[9, 10]), 2);
        let mut n = 0;
        while n < 7 {
            n += c.pop_batch(&mut out, 4);
        }
        assert_eq!(out, (1..=10).collect::<Vec<u64>>());
    }

    #[test]
    fn two_threads_preserve_order() {
        const N: u64 = if cfg!(miri) { 3_000 } else { 5_000_000 };
        let (mut p, mut c) = new_batch_ring::<u64>(1024);
        let t = std::thread::spawn(move || {
            let mut next = 0u64;
            let mut buf = [0u64; 64];
            while next < N {
                let k = ((N - next) as usize).min(64);
                for (j, b) in buf[..k].iter_mut().enumerate() {
                    *b = next + j as u64;
                }
                let n = p.push_batch(&buf[..k]);
                next += n as u64;
                if n == 0 {
                    std::hint::spin_loop();
                }
            }
        });
        let mut out = Vec::with_capacity(64);
        let mut expect = 0u64;
        while expect < N {
            out.clear();
            if c.pop_batch(&mut out, 64) == 0 {
                std::hint::spin_loop();
            }
            for v in &out {
                assert_eq!(*v, expect);
                expect += 1;
            }
        }
        t.join().unwrap();
    }
}
