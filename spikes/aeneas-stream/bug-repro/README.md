# Aeneas: enum assigned to an array element inside a loop becomes `()`

Aeneas `0d21a9ec` (Charon `454586e9`). `cargo` crate in this directory.

```rust
pub fn clear(s: &mut S) {          // S { a: [Option<u64>; 4] }
    let mut i = 0;
    while i < 4 { s.a[i] = None; i += 1; }
}
pub fn clear_one(s: &mut S, i: usize) { s.a[i] = None; }
```

```
charon cargo --preset=aeneas
aeneas -backend lean -dest out repro.llbc
```

Generated Lean:

```
-- in the loop body of `clear`
let a ← Array.update s.a i ()        -- ill-typed: expected Option U64
-- in `clear_one`
let a ← Array.update s.a i none      -- correct
```

Lean rejects the first: `Application type mismatch: () has type Unit but is
expected to have type Option Slot`. Workaround: assign the whole array
(`s.a = [None; 4]`).

Not yet reported upstream.
