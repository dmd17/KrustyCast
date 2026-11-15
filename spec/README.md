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
leader's no-op, appends of any range of the leader's log (the code sends
at most `max_append_entries` at a time), followers
treating entries at or below their commit index as matching, a lossy and
reordering network, process crash-restarts (term, vote and log survive),
and leaders stepping down at any time (which covers check-quorum; pre-vote
only decides when a node times out). Invariants: `ElectionSafety`,
`LogMatching`, `StateMachineSafety`, `CommittedStable` (what a node has
committed is the one committed log, which never changes) and
`LeaderCompleteness` (an entry committed in term T is in the log of every
leader of term T or later). The last two read a ghost variable,
`committed`: each committed entry with the term it was committed in.

`LeaderCompleteness` used to guard on the entry's own term, which is too
strong. A stale leader of term 2 need not hold a term-1 entry that a
term-3 leader committed after the stale leader's election. Two terms
cannot reach that, so the 2-term configs never reported it; with three,
TLC does (23 steps). The randomized harness in `krusty-raft` had the same
over-strict check and failed on nightly seed 415446889 (fixed 2026-11-15).

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
| `MCRaftKC.cfg` (CI) | all behaviours up to 11 steps; 3 servers, 2 terms, logs of 2, ≤3 messages, restarts and step-downs | 176 k | ~3 min (2 workers) |
| `RaftKC.cfg` | exhaustive; 2 terms, logs of 2, ≤2 messages, no restarts or step-downs | 9.41 M (depth 39), no violation | 43 min (3 workers) |
| `RaftKCLarge.cfg` | exhaustive; 2 terms, logs of 3, ≤3 messages, restarts and step-downs | > 10 M explored without a violation in 20 min, not exhausted | hours |
| `RaftKCTerms3.cfg` | exhaustive; 3 terms, logs of 2, ≤2 messages, no restarts or step-downs | > 18.6 M explored without a violation in 12 min, not exhausted | hours |
| `RaftKCTerms4.cfg` | exhaustive; 4 terms, logs of 2, 1 message, no client values, restarts or step-downs | > 21.2 M explored to depth 41 without a violation in 30 min, not exhausted | about an hour |

The figures for the configs above `RaftKCTerms4` were measured before
2026-11-15, when the model sent only whole suffixes of the leader's log.
Partial ranges make each state space somewhat larger.

**Figure 8 needs four terms and partial batches.** A leader commits an
old-term entry by count, and a later leader, elected without that entry
because its log ends in a newer term, overwrites it. With three servers
that takes a stale term-2 entry, a term-3 leader committing the term-1
entry, and a term-4 leader. A follower must also hold the old entry but
*not* the term-3 leader's no-op after it. The model used to send only
whole suffixes of the leader's log, which cannot produce that state, so
no bound could reach the bug. `krusty-raft` sends at most
`max_append_entries` at a time, so the code can reach it, and the model
now can too.

What finds which bug (mutations of the spec, and of `krusty-raft`'s code):

| Bug | Bounded TLC (CI) | `krusty-raft` harness | Trace validation |
|---|---|---|---|
| Double vote in a term | `ElectionSafety` | CI tests | 2 of 6 traces |
| Vote ignores log freshness | not within 11 steps; `RaftKCTerms3`: `LeaderCompleteness` at 23 steps (6 min) | CI tests | 2 of 6 traces |
| Follower skips the prev-term check | not within 11 steps | CI tests | 4 of 6 traces |
| Commit of an old-term entry by count (Figure 8) | not within 11 steps; `RaftKCTerms4`: `LeaderCompleteness` at 32 steps (15 min) | nightly sweep (found at seed 12500) | 5 of 6 traces |

## `tla/TraceRaftKC.tla`: trace validation (gap Q3)

Checks that runs of the real `krusty-raft` code are behaviours of
`RaftKC`. A randomized harness (`crates/krusty-raft/tests/trace.rs`: loss,
delay, reordering, partitions, crashes and restarts) logs one record per
call into the core: the received message and the sent messages in RaftKC's
form, the node's state afterwards, and a *plan*, the RaftKC actions that
explain the call (for example `inject, receive, advanceCommit(n),
append(t, prev, last)`). `TraceRaftKC` replays each plan with RaftKC's own
actions; TLC checks every action is enabled and that the node's term,
role, vote, log, commit index and sent messages match the code exactly.
The plan only picks *which* spec actions to try; whether they fit is
TLC's verdict.

Along each trace TLC also checks `ElectionSafety`, `LogMatching`,
`StateMachineSafety`, `CommittedStable` and `LeaderCompleteness`
(`TraceSafety`).

Abstractions: the network (a delivered message is injected; every message
was itself checked when it was sent), pre-vote (no RaftKC state; a
pre-candidate's role is not compared), and duplicate deliveries (the
generator does not duplicate). Calls RaftKC cannot see are left out as
stuttering steps.

```
KRUSTY_RAFT_TRACE_DIR=/tmp/traces KRUSTY_RAFT_TRACE_SEEDS=6 \
    cargo test -p krusty-raft --test trace -- --ignored
spec/tla/check-traces.sh /tmp/traces      # accepted / REJECTED per trace
```

A trace of ~250–1000 records (2–5 elections, conflicts truncated) takes
about 10 s. CI checks 3 traces; the nightly workflow 20 new ones.

Mutations of `krusty-raft`, 6 traces each:

| Bug | Traces rejected |
|---|---|
| Vote ignores log freshness | 2 of 6 |
| Double vote in a term | 2 of 6 |
| Follower skips the prev-term check | 4 of 6 |
| Follower commits past what it matched | 4 of 6 (the code's own assertion stopped the other runs) |
| Commit of an old-term entry by count (Figure 8) | 5 of 6 |

The bounded model check misses three of these within its CI depth, and the
randomized harness needs a long sweep for Figure 8; trace validation
catches all five in a minute. Unchanged code: every trace accepted.

## `lean/`

Lean proofs about `krusty-core`, against its Aeneas translation
(`lean/regen.sh`). CI fails if the translation is stale (`source-hash.sh`)
or if any theorem uses `sorry` or an unexpected axiom.

## `../spikes/aeneas-stream/proofs`

Lean proofs about the stream tracker, generated with Aeneas
(see `spikes/aeneas-stream/REPORT.md`). They move here once the verified
crates exist.
