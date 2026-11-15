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
| `SequencerLarge.cfg` | 2 clients, 2 incarnations, 2 versions, log 5, ≤3 in flight | 157 M explored in 3 h without a violation, not exhausted | > 3 h |

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

## `tla/RaftKC.tla`

Raft as `krusty-raft` implements it (based on Ongaro's spec): a new
leader's no-op, appends of the leader's log after any prefix, followers
treating entries at or below their commit index as matching, a lossy and
reordering network, process crash-restarts (term, vote and log survive),
and leaders stepping down at any time (which covers check-quorum; pre-vote
only decides when a node times out). Invariants: `ElectionSafety`,
`LogMatching`, `StateMachineSafety`, `LeaderCompleteness`.

**Why two specs compose.** `Sequencer.tla` and `SequencerBounded.tla` show
the sequencer's guarantees hold over *any* single growing log of inputs
(duplicated, reordered, from restarting clients). `RaftKC.tla` shows the
committed entries form exactly that: one log, agreed by every node, that
never changes once committed. Every node applies that log to the same
deterministic core, so G1 (agreement) and G7 (determinism) hold for the
egress stream. Not modelled: power loss in durability mode (a), which is
outside G3's assumptions.

| Config | Bounds | States | Time (laptop) |
|---|---|---|---|
| `MCRaftKC.cfg` (CI) | all behaviours up to 11 steps; 3 servers, 2 terms, logs of 2, ≤3 messages, restarts and step-downs | 141 k | ~3 min (2 workers) |
| `RaftKC.cfg` | exhaustive; 2 terms, logs of 2, ≤2 messages, no restarts or step-downs | > 2.4 M, see below | > 20 min |
| `RaftKCLarge.cfg` | exhaustive; 2 terms, logs of 3, ≤3 messages, restarts and step-downs | > 10 M explored without a violation in 20 min, not exhausted | hours |

What finds which bug (mutations of the spec, and of `krusty-raft`'s code):

| Bug | Bounded TLC (CI) | `krusty-raft` harness |
|---|---|---|
| Double vote in a term | `ElectionSafety` | CI tests |
| Vote ignores log freshness | not within 11 steps | CI tests |
| Follower skips the prev-term check | not within 11 steps | CI tests |
| Commit of an old-term entry by count (Figure 8) | not within 11 steps | nightly sweep (found at seed 12500) |

## `lean/`

Lean proofs about `krusty-core`, against its Aeneas translation
(`lean/regen.sh`). CI fails if the translation is stale (`source-hash.sh`)
or if any theorem uses `sorry` or an unexpected axiom.

## `../spikes/aeneas-stream/proofs`

Lean proofs about the stream tracker, generated with Aeneas
(see `spikes/aeneas-stream/REPORT.md`). They move here once the verified
crates exist.
