# M0 spike: Aeneas → Lean on the stream tracker

Date: 2026-11-14. Question (ARCHITECTURE §11.3): does today's `src/stream.rs`
translate, and are the proposed coding rules enough to prove things about it?

**Answer: yes, with a rewrite into the restricted subset, and the rules need
five additions (below).** The first real theorem about the rewritten tracker is
proved, with no `sorry` and no axioms beyond Lean's standard three, and a
mutation check shows the proof fails when the code is wrong.

## Layout

| Path | What |
|---|---|
| `as-is/` | Unmodified copies of `src/stream.rs` + `src/protocol.rs` (stage A) |
| `subset/` | Tracker rewritten in the restricted subset (stage B), unit tests, `tests/differential.rs` (B vs A over 2000 random seeds) |
| `lean-as-is/`, `lean-subset/` | Aeneas output |
| `proofs/` | Lean project: generated `StreamSubset/{Types,Funs,FunsExternal}.lean` + hand-written `StreamSubset/Proofs.lean` |
| `regen.sh` | Charon → Aeneas → copy into `proofs/` |

Reproduce: `./regen.sh && (cd proofs && PATH=~/.elan/bin:$PATH lake build)`.

## Toolchain (all in the user's home, no system changes except `libgmp-dev`)

| Tool | Version |
|---|---|
| Aeneas | git `0d21a9ec` (2026-11-09), OCaml 5.3.0 via opam switch `aeneas` |
| Charon | pinned by Aeneas: `454586e9`, Rust `nightly-2026-09-17`, v0.1.315 |
| Lean | `v4.36.0-rc2` (elan), Mathlib `v4.36.0-rc2` from cache |

Costs: about 1 h of one-off setup, most of it opam builds and the Aeneas
Lean library (3591 modules). The Lean side takes 7.7 GB on disk. After that,
a Charon + Aeneas run on the crate takes about 15 s and a proof rebuild 2–3 s.
`proofs/lakefile.lean` reuses the Aeneas checkout's Mathlib (`packagesDir`)
instead of downloading a second copy; `AENEAS_LEAN` overrides the path.

Setup snags hit: GMP headers (`zarith`), `ppx_deriving_hash` and
`ocamlformat 0.27.0` missing from the documented dependency list, Charon's
`make build-charon-ml` failing only at the `odoc` docs step (harmless), and a
transient DNS failure fetching the Lean toolchain. CI will need all of this
cached; budget for a prebuilt container image.

## Stage A: today's code as-is

- Charon: clean (976 items, 694 of them from std).
- Aeneas: translates everything except `MoldPacket::decode`
  (`Early returns inside of loops are not supported yet`).
- But the result is not provable as is: 54 external axioms with no model,
  among them `BTreeMap::{entry, remove, keys, len}`, `Entry::or_insert`,
  `Instant::duration_since`, `Duration`, and the iterator machinery used by
  `for … in msgs.into_iter().enumerate()`. Proving anything about the
  tracker would first mean writing and proving Lean models for a balanced
  tree map and the iterator protocol. That is the cost the restricted subset
  avoids.

## Stage B: restricted subset

Rewrite rules applied (see the `subset/src/lib.rs` header): fixed 64-slot
reorder window instead of `BTreeMap`; time as `u64` nanoseconds; messages as
`u64` handles; output pushed to a `&mut Vec`; no closures, collections,
trait objects or `unsafe`. One deliberate behaviour change: messages more than
64 ahead of `next` are dropped and recovered by re-request (bounded memory).
The differential test shows B delivers exactly what A delivers whenever
reordering stays within the window.

Translation needed two more source changes, both now in the code:

1. **`match self.window[i] { … }` is rejected** ("Not allowed to expand
   enumerations with several variants"). Copy first: `let cell = self.window[i]; match cell { … }`.
2. **Aeneas bug:** `self.window[i] = None` *inside a loop* is translated to
   `Array.update self.window i ()`, which Lean rejects as ill-typed. The same
   assignment outside a loop is translated correctly. Workaround:
   `self.window = [None; WINDOW]`. Minimal reproduction in `bug-repro/`; not yet reported upstream.

After that, 3 external axioms remain, all from `#[derive(Debug, Clone)]` on
`Option` and none used by the tracker logic.

## What is proved (`proofs/StreamSubset/Proofs.lean`)

- `slot_index_spec`: `slot_index s` returns `s % 64` and is in bounds.
- `take_spec`: `take` either leaves the tracker unchanged or frees exactly one
  window slot; it touches no other field.
- **`deliver_and_drain_spec`**: if
  `next + 1 + occupied_slots ≤ u64::MAX` and
  `out.len() + 1 + occupied_slots < usize::MAX`, then delivering the next
  expected message
  - cannot fail (no overflow, no out-of-bounds, no allocation-size failure),
  - changes only `window` and `next`, and `next` strictly increases,
  - appends `(old next, handle)` followed by deliveries whose seqnos are
    exactly `old next, old next + 1, …, new next − 1`: gap-free, in
    order, no duplicates.

  It covers the loop for any window contents (the measure is
  `2·occupied + more`), not just one iteration. `#print axioms` reports
  only `propext, Classical.choice, Quot.sound`.

**Mutation check:** changing `self.next += 1` to `+= 2` in the drain loop and
regenerating makes the proof fail (as well as one unit test). Restored.

Effort: about 130 lines of Lean, about 12 edit/build rounds. Aeneas's `step` /
`step*` tactics discharged most bounds and array-index obligations
automatically; the hand work was the loop invariant and a `countP` lemma.

## Findings that apply to the production code

- **Overflow on untrusted seqnos.** `p.seqno + i`, `next += 1`,
  `high_water = max(…, seqno + count)` all overflow for a packet whose seqno is
  near `u64::MAX`, both in `src/stream.rs` and in stage B. The
  result is a panic in debug builds and silent wrap-around in release.
  Aeneas models every
  such `+` as a possible failure, so a total-correctness proof of `on_packet`
  forces the issue. Fix: validate the range when decoding
  (`seqno + count ≤ u64::MAX`, reject otherwise) or use checked arithmetic.
  Added to the gap analysis as W8.
- **Unbounded buffering in stage A.** `buffered.entry(s).or_insert(data)` for
  any `s > next` lets a hostile or buggy sender grow memory without bound.
  The 64-slot window in stage B is the fix the design already calls for
  (principle P2). Added to the gap analysis as W9.

## Changes to ARCHITECTURE §11.3

Confirmed: the subset rules work, and owned fixed-capacity structures are what
makes proofs cheap. Additions:

1. No early `return` inside **any** loop (not only nested ones): return a
   value after the loop or carry a flag.
2. Don't `match` on an array or `Vec` element place; copy it to a local first.
3. Avoid writing enum values into array elements inside loops until the
   Aeneas bug is fixed; prefer whole-array assignment.
4. Arithmetic on wire-supplied values is checked or range-validated at decode
   time; proofs of verified functions state `no overflow` preconditions
   explicitly.
5. Keep `#[derive(Debug)]` and other unused std impls out of verified crates,
   or gate them behind a `cfg`, so that CI's `#print axioms` check stays empty. Each
   proof module ends with `#print axioms` for its main theorems, and CI fails
   on `sorryAx` or any axiom not on an allowlist.

Also: the generated Lean is kept in the tree and regenerated by CI;
a diff means the Rust changed and proofs must be re-checked (they already are,
because `lake build` runs in CI). Mutation checks like the one above should be
part of the verification test suite (V-requirements).

## Next proofs worth doing (not done)

- `on_message`: buffered messages are kept or delivered, never lost while in
  the window.
- `on_packet` end to end: with `recover = false`, all deliveries across a
  sequence of packets form one strictly increasing run (needs the overflow
  fix first).
- Window invariant: `window[s % 64]` only ever holds `s` with
  `next < s < next + 64`.
