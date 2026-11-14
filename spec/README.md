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

Run: `spec/tla/check.sh` (add `--large` for the big config). It needs Java 11+
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

## `../spikes/aeneas-stream/proofs`

Lean proofs about the stream tracker, generated with Aeneas
(see `spikes/aeneas-stream/REPORT.md`). They move here once the verified
crates exist.
