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
