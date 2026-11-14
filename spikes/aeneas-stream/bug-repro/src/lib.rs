pub struct S {
    pub a: [Option<u64>; 4],
}

pub fn clear(s: &mut S) {
    let mut i = 0;
    while i < 4 {
        s.a[i] = None;
        i += 1;
    }
}

pub fn clear_one(s: &mut S, i: usize) {
    s.a[i] = None;
}
