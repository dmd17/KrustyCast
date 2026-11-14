# Specifications

ARCHITECTURE §11 describes the verification layers. This directory holds
the protocol-level specs.

## `tla/Sequencer.tla`

The sequencer core and client sessions (§8.1, ADR-4). The replicated log is
one atomic log for now; the Raft spec will refine it. Clients pipeline,
resend, and restart with new incarnations, over a network that loses,
duplicates and reorders in both directions.

Invariants (PRD §5):

| Invariant | Guarantee |
|---|---|
| `AtMostOnce` | G4: one global seqno per (client, incarnation, seq) |
| `ConditionalAppend` | G5: per key, accepted versions are 0, 1, 2, … in order |
| `SessionFifo` | G6: a session's messages are sequenced in seq order |
| `OutcomesTrue` | ACCEPTED(g) names the message at g; REJECTED never reaches egress |
| `OutcomesStable` | A resend never gets a different answer |
| `NoInvention` | Only logged client messages reach egress |
| `VersionsMatch` | Key versions agree with the egress stream |

| Config | Bounds | States | Time (laptop) |
|---|---|---|---|
| `SequencerRace.cfg` (CI) | 2 clients, 1 incarnation, 2 msgs, log 4, ≤2 in flight | 450 k | ~20 s |
| `SequencerRestart.cfg` (CI) | 1 client, 2 incarnations, 2 msgs, log 4 | 748 k | ~20 s |
| `SequencerLarge.cfg` | 2 clients, 2 incarnations, 2 versions, log 5, ≤3 in flight | not yet run to completion | hours |

Run: `spec/tla/check.sh` (add `--large` for the big configs). It needs Java 11+
and `tla2tools.jar` v1.8.0, which goes in `~/.local/share/krusty-tools/tla/`
(or set `TLA2TOOLS`).

**Mutation checks.** Each of these deliberate bugs makes TLC fail:

| Bug introduced | Caught by |
|---|---|
| Dedup keyed by client only, not (client, incarnation) | `OutcomesTrue` (Restart) |
| No dedup of resends | `AtMostOnce` (Race) |
| Conditional append ignores the version | `ConditionalAppend` (Race) |
| Out-of-order seqs accepted | `TypeOK` (Race) |

Not modelled yet: NAK, BUSY, NOT_LEADER, the bounded outcome ring and
SESSION_EXPIRED, Raft, consumers, liveness.

## `tla/SequencerBounded.tla`

The same model with **bounded dedup state** (ADR-12, as in `krusty-core`):
one session per client, a table of `MaxClients` sessions, eviction of *any*
session when full (a superset of the code's idle-LRU policy), an incarnation
floor, and an outcome ring of `Ring` per session with a client window to
match. Same invariants as above, plus `Bounded` (table never over capacity).

| Config | Bounds | States | Time (laptop) |
|---|---|---|---|
| `SequencerBoundedEvict.cfg` (CI) | 2 clients, room for 1, 2 incarnations, 1 msg each, log 4 | 725 k | ~36 s |
| `SequencerBoundedRing.cfg` (CI) | 1 client, ring 1, 3 msgs, 2 incarnations, log 5 | 1.3 M | ~49 s |
| `SequencerBoundedEvictLarge.cfg` | as Evict with 2 msgs and ring 2 | > 25 M, not finished | hours |

Mutation checks: evicting without raising the floor, and accepting an
incarnation equal to the floor, are both caught by `OutcomesTrue`.

## `lean/`

Lean proofs about `krusty-core`, against its Aeneas translation
(`lean/regen.sh`). CI fails if the translation is stale (`source-hash.sh`)
or if any theorem uses `sorry` or an unexpected axiom.

## `../spikes/aeneas-stream/proofs`

Lean proofs about the stream tracker, generated with Aeneas
(see `spikes/aeneas-stream/REPORT.md`). They move here once the verified
crates exist.
