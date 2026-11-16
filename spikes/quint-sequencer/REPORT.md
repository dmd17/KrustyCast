# Spike: Quint for the protocol specs (2026-11-16)

Question: should KrustyCast's specs move from TLA+ to Quint? This spike
translates `spec/tla/Sequencer.tla` to Quint, checks it with Quint's tools
against the TLA+ results, and drives the real `krusty-core` from traces of
the Quint spec (model-based testing).

| File | What |
|---|---|
| `Sequencer.qnt` | The translation (≈ 270 lines; the TLA+ is 267), with the two CI configs as modules `SequencerRace` and `SequencerRestart` |
| `gen-traces.sh` | Random traces of `SequencerRace` in ITF (JSON) |
| `mbt/tests/replay.rs` | Replays the traces through `krusty_core::Core` and compares decisions and the egress stream with the spec's |

Tool: Quint 0.33.1 (`npm install -g --prefix ~/.local/share/krusty-tools/quint
@informalsystems/quint`), Java 21 for its TLC and Apalache backends.

## Translation

Mechanical: one typecheck error to fix, no semantic surprises. Differences:
- **No state constraint.** TLA+ bounds messages in flight with
  `CONSTRAINT`; in Quint each action that grows `net` or `fb` is guarded.
- **No `CHOOSE`.** The default key became a constant.
- **No symmetry reduction** with the TLC backend: the two-client config
  explores twice the states.
- **Types and effects.** Quint rejected a `match` whose branches updated
  different variables (a mistake TLC would only report at run time, if
  that branch were reached) and replaces the `TypeOK` invariant with static
  types.
- Lists are 0-based, so the egress position is `g - 1`.

## Checking

| | TLA+ (TLC) | Quint, TLC backend | Quint simulator |
|---|---|---|---|
| Race (2 clients) | 449 929 states (with symmetry), depth 18, 8 s | 899 841 states (≈ 2 ×, no symmetry), depth 18, 41 s | 20 000 traces in 3 s, no violation |
| Restart (1 client) | 747 857 states, depth 22, 8 s | **747 857 states**, depth 22, 12 s | 20 000 traces, no violation |
| Restart via Apalache (symbolic, Quint's default backend) | | 8 steps only (the full graph is 22 deep): no violation, **974 s** | |

Times are wall clock on nuc2, TLC with 4 workers; Quint's include
translating to TLA+ and starting Java. The Restart counts and depths are
identical, and Race is just under twice TLA+'s symmetry-reduced count
(states fixed by swapping the clients count once): the translation
explores the same state space.

Apalache is the wrong tool for these specs: they are small, finite and
set-heavy, which suits TLC's enumeration and not SMT. Its strength
(inductive invariants, unbounded parameters) is not something we use.

**Mutations** (the four in spec/README.md, applied to the Quint spec):

| Bug | Quint, TLC backend | Quint simulator |
|---|---|---|
| Dedup keyed by client only | caught (Restart only, as in TLA+) | caught in 52 ms |
| No dedup of resends | caught | caught in 37 ms |
| Conditional append ignores the version | caught | caught in 52 ms |
| Out-of-order seqs accepted | caught (by `TypeOK` in TLA+; Quint has no `TypeOK`, a property invariant catches it) | caught in 60 ms |

The TLC backend reports only that the combined invariant (`q_inv`) failed,
not which conjunct; the simulator's `--invariants` names it.

## Model-based testing

`gen-traces.sh` writes 2 000 random traces of up to 40 steps (33 s). The
test replays each trace's log through `krusty_core::Core` and checks:
- the core publishes exactly the spec's egress stream, with consecutive
  seqnos and each conditional append's next version;
- every message the spec decided, the core decides the same (ACCEPTED at
  the same seqno, or REJECTED at the same version), and resends get the
  same answer.

Result: **2 000 traces, 7 999 log entries, 3 318 accepted and 565 rejected
decisions, all as the spec** (4 s). Bugs planted in `krusty-core`, each
caught at once:

| Bug | Caught by |
|---|---|
| Conditional append ignores the expected version | egress check (wrong version published) |
| Resends of conditional appends decided again | "a resend got a different answer" |
| Publishes the key's old version | egress check |

Scope: `Sequencer.tla` keeps every incarnation's session, while the core
keeps one per client and expires older incarnations (that policy is
`SequencerBounded.tla`). So traces come from the one-incarnation config;
traces with restarts would need `SequencerBounded` translated too.

How this differs from what we have:
- The Lean refinement proof covers the same ground for *all* inputs, so
  this test adds little over it for `krusty-core`.
- Trace validation goes the other way: code traces checked against the
  spec.
- The value is for code without a proof: `krusty-stream`'s tracker (from
  `Egress.tla`), the node's session-roll filter, `krusty-raft` (driven from
  a `RaftKC` model instead of the randomized harness).

## Verdict

- **Keep the TLA+ specs.** Quint checks them equally well through TLC,
  with the same state counts, but not better, and moving would mean
  re-validating every spec, config and mutation, rebuilding trace
  validation (`TraceRaftKC` replays code traces with RaftKC's own actions
  in TLC), and running two toolchains.
- **Quint is good for model-based testing**, which TLA+ tooling does not
  give us cheaply: random traces in a JSON format, a fast simulator, and
  typed specs. Worth adopting where there is no proof: a Quint version of
  `Egress.tla` driving `krusty-stream`, and of `RaftKC` driving
  `krusty-raft`.
- **For new specs** either language works. Quint's types and effect checks
  caught one mistake here that TLC would have found only at run time.
