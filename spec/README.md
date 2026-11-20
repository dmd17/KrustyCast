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
deterministic core, so every node's egress stream is a prefix of one
stream (G7). `Egress.tla` takes it from there to the consumers (G1, G2).
`Cluster.tla` checks the glue between them: how a node turns SUBMITs into
proposals and committed entries back into outcomes. Not modelled: power
loss in durability mode (a), which is outside G3's assumptions.

The action property `LeaderAppendOnly` (a leader's log only grows while it
leads in one term) is checked too: the cluster shell's resend filter
relies on it. A mutation that lets a leader drop its last entry fails it.

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

Abstractions:
- The network: a delivered message is injected, and every message was
  itself checked when it was sent.
- Pre-vote has no RaftKC state, and a pre-candidate's role is not compared.
- Leadership transfer: the leader's TimeoutNow is not a RaftKC message,
  and the election it starts is RaftKC's `timeout`. The leader refusing
  proposals meanwhile is invisible to RaftKC. Transfers are mixed into the
  runs, so handovers are validated too.
- Duplicate deliveries: the generator does not duplicate. Calls RaftKC cannot see are left out as
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

## `tla/Egress.tla`: the native stream and its consumers (gap Q1)

Nodes publish (live or retransmitted) and answer RECOVER requests, and
consumers run `krusty-stream`'s tracker. The network loses, duplicates
and reorders frames. Several nodes may publish at once, as during a leader
handover.

- **Raft is abstracted** to what `RaftKC` proves: every node's log agrees
  with one committed log up to its commit index. Beyond that a node may
  hold anything, and may lose or replace it.
- **Nodes** append, truncate and commit entries, and age out retained
  data. They publish DATA and heartbeats up to their commit index. They
  answer RECOVER as the recovery stage does: UNAVAILABLE if the start has
  aged out, then DATA from the earliest retained seqno. The answer goes to
  the requester, or onto the feed when several consumers asked
  (suppression).
- **The tracker** follows `krusty-stream`: a reorder window of W slots,
  delivery and draining in seqno order, high-water marks from DATA and
  heartbeats, `skip_until` on UNAVAILABLE, and RECOVER for the gap at
  `next`.

| Invariant | Guarantee |
|---|---|
| `Agreement` | G1 (with G7): a delivered message is the committed one at its seqno |
| `PrefixDelivery` | G2: deliveries in seqno order; every seqno below `next` delivered or reported lost, never both |
| `LossOnlyAgedOut` | G2: loss is reported only for data some node no longer retains (gap policy `Wait`) |
| `CommittedAgree` | the Raft abstraction holds |

| Config | Bounds | States | Time |
|---|---|---|---|
| `Egress.cfg` (CI) | gap policy `Wait`; 2 nodes, 1 consumer, 2 values, logs of 2, single-message frames, window 2 | 2.26 M, exhaustive | 1 min (4 workers) |
| `EgressGiveUp.cfg` (CI) | gap policy `GiveUpAfter`, same bounds, without `LossOnlyAgedOut` | 2.48 M, exhaustive | 1 min (4 workers) |
| `EgressLarge.cfg` | 2 consumers, logs of 3, frames of up to 2 | > 123 M explored to depth 17 without a violation in 20 min, not exhausted | hours (6 workers) |

Mutation checks (each flag in the spec turns on one bug):

| Bug | Caught by |
|---|---|
| Nodes publish or serve entries they have not committed | `Agreement` (6 steps) |
| UNAVAILABLE skips to the high-water mark, not to the earliest retained seqno | `LossOnlyAgedOut` (11 steps) |
| Skipping a gap does not report it lost | `PrefixDelivery` (10 steps) |

**The gap policy is the consumer's choice** (`krusty_stream::GapPolicy`,
`ConsumerConfig::gap_policy`), and both are checked:
- `Wait` keeps asking until the data arrives or a node answers
  UNAVAILABLE. Loss is then only ever for aged-out data
  (`LossOnlyAgedOut`), but delivery stalls while no node answers.
- `GiveUpAfter(n)`, the SDK default with n = 50, reports a gap lost after
  n unanswered requests and keeps going. Loss stays explicit and ordered
  (`PrefixDelivery`), but it may cover data the cluster still holds.
  `LossOnlyAgedOut` fails for it within 8 steps, as it should.

One check is redundant. `take` checks a window slot's seqno, but the
model shows it can't matter: the buffer only holds seqnos in
`(next, next + W)`, so `next`'s slot never holds another seqno.

Not modelled: stream switches after END_OF_STREAM, foreign streams,
unicast fan-out, the MoldUDP64 adapter, timers and rate limits.

## `tla/RaftKCInd.tla`: towards an inductive invariant for Raft

Bounded checks cover the states a few steps reach. An *inductive*
invariant holds in every state that satisfies it, and so in every
reachable state at any size; checked for inductiveness (one step from any
state satisfying it), it would make RaftKC's safety unbounded. This
module builds one in layers, on top of the unchanged RaftKC (and its trace
validation), with two history variables updated by observing each step:
`votedIn[t][T]`, whom t voted for in term T (`votedFor` forgets it on a
new term), and `hist[T]`, the log of term T's leader while it led.

| Layer | Invariants | Implies |
|---|---|---|
| Elections | votes held are votes cast in the term; granted replies in flight likewise; `votedFor` matches the history; leaders hold a quorum | ElectionSafety (checked as an implication) |
| Logs | an entry of term T, with everything before it, is term T's history, in every log, in every history and in every append in flight; a leader's log is its history | LogMatching (checked as an implication) |
| Commits | every term at or above an entry's commit term that had a leader has it; what a server knows is committed is in the history of every term from its own; an append's commit index covers committed entries only | (LeaderCompleteness, StateMachineSafety: to be derived) |

Status (2026-11-20): the three layers hold on reachable states:
`RaftKCIndMC.cfg` (CI, depth 11, 192 k states) and `RaftKCIndTerms3.cfg`
(three terms: 20 M states to depth 23 in 30 min, no violation, not
exhausted). Not yet checked for inductiveness: that needs Apalache
(symbolic, from any state satisfying the invariant), and RaftKC's message
set mixes record shapes, which Apalache does not type; a typed variant,
shown equivalent to RaftKC by TLC on small bounds, comes next. The commit
layer is where the follower rule (entries at or below its commit index
count as matching) and in-flight replies from earlier terms make the
argument hard, and where strengthening is expected.

## `tla/Cluster.tla`: the cluster shell (`krusty-node/src/cluster.rs`)

How a node turns client SUBMITs into Raft proposals and committed entries
back into outcomes, with Raft abstracted to what `RaftKC` checks: terms,
at most one current leader (elected in a fresh term with every committed
entry), `LeaderAppendOnly`, stale leaders that still think they lead, one
committed log every node agrees with up to its commit index, and crashes
that keep the log and term but lose the shell's memory. The core is a
function of the committed log, as in `Sequencer.tla`.

The shell as the code has it: a node that thinks it leads proposes each
SUBMIT and queues (index, term, client) in proposal order; applying entry
i it answers only if the queue's front is (i, t) with t the entry's term;
others answer NOT_LEADER. **The resend filter** drops a resend of a
message proposed in the leader's term and not yet applied. Clients go
back N (on a timeout or NAK, every unanswered message from the oldest
on), with a retry timer longer than a round trip; their routing (follow
NOT_LEADER, rotate after silence) is abstracted to "a resend eventually
reaches the current leader" (strong fairness).

| Property | |
|---|---|
| `OutcomesTrue` | Every outcome a client hears, or that is in flight, is true |
| `CommittedAgree` | The Raft abstraction holds |
| `CopiesBounded` | No message has more than `MaxCopies` copies in any log: a term change, a loss, a crash, or a resend crossing its outcome each allow one more; without the filter there is no bound (the resend storm of BASELINE E3) |
| `Liveness` | Every message sent gets an outcome (under fairness, with finitely many losses, crashes and elections) |
| `ProposalsInLog`, `ProposalsOrdered` | What the code keeps true of its queue of proposals: each names an entry still in the node's log (its term and client), in index order |

| Config | Bounds | States | Time |
|---|---|---|---|
| `ClusterLive1.cfg` (CI) | 2 nodes, 1 client, 1 message, 2 terms, 1 loss; liveness | 306 k | 30 s |
| `ClusterLive2.cfg` (CI) | as above with 1 crash and no loss | 153 k | 15 s |
| `ClusterTwo.cfg` (large) | 2 messages (gaps, NAKs, go-back-N, reordering), 1 loss; safety and `CopiesBounded` | > 223 M explored to depth 43 without a violation, not exhausted (the per-session filters fail at 4.5 M and 102 M) | 90 min (6 workers) |

Liveness over two messages is out of reach for TLC here (`ClusterTwo`'s
state space without the liveness check is already that size).

**What it found** (2026-11-19). The filter as first written
(2026-11-17) kept, per session, the highest seq proposed and that
proposal's index. After a NAK the client goes back to a lower seq, which
the leader proposes after the higher one was applied, and every resend of
it passed: `krusty-client`'s test `resends_after_a_nak_are_not_logged_again`
logged seq 1 201 times for 201 sends. Tracking the index of the session's
*latest* proposal instead fixed that and broke liveness: a lower seq that
was never proposed was dropped while a higher one was pending, and with
reordering it never got into the log. Both are per-session shortcuts for
what the filter means; keyed by message, it is exact. Mutations:

| Bug | Caught by |
|---|---|
| No filter | `CopiesBounded` (`ClusterLive1`, 2 s) |
| The filter drops resends after the entry was applied | `Liveness` (`ClusterLive1`, 4 s) |
| Per session, the highest seq's index (`SessionFilter = "highest"`) | `CopiesBounded` (`ClusterTwo`, 2 min) |
| Per session, the latest proposal's index (`"latest"`) | `CopiesBounded` (`ClusterTwo`, 37 min: the never-proposed seq is starved while copies of the other pile up) |
| The filter ignores the term | not caught: an entry from an earlier term only delays a resend until its index is applied |
| Answering without checking the entry's term | not caught: the reply goes to the wrong client, whose session discards it (it names another message) |

The last two checks in the code are defensive: the model shows they are
not needed for these properties. `MaxLog` bounds the log, so the model
can tell bounded copies (a finite log absorbs them) from unbounded ones.

**Trace validation** (`TraceCluster.tla`, 2026-11-20). Each node of a
test cluster writes a trace of its shell (`ClusterConfig::trace`; the
`krusty-client` test `cluster_shell_traces`: a client retrying every
50 µs, a NAK and go-back-N, duplicates of decided messages, the leader
stopping, and its restart from a snapshot). `check-cluster-traces.sh`
replays each with Cluster.tla's own `Receive` and `Apply`. Each step must
then match the code: the code proposed at the same index, or dropped the
resend, or answered NOT_LEADER, and it sent the same outcome for each
applied entry. The outcome is the model's sequencer against krusty-core's
decision. What the shell sees of Raft is injected, since Raft is validated
on its own (`TraceRaftKC`); the abstraction's assumptions are checked
against it: terms never go back, a leader only appends in its term, and
committed entries never change. The first run accepted all three traces
(4.4 k records, 2.5 k resends filtered; 1 min of TLC). It runs nightly.
One mismatch was fixed in the model beforehand: when Raft truncates the
log, the code forgets its own proposals from there on, so `Replicate` now
does too (`KeepProposals`). The evidence that this was the right fix:
the model before it breaks `ProposalsInLog`, the code's own invariant,
within 600 states. A stale leader proposes at index 2, a newer leader
replaces its log, and its queue still names index 2 of an empty log. The
model after it keeps both invariants in every configuration, and it still
catches every filter mutation it caught before (the table above).
Stale entries were harmless to safety (the term check makes them "not
mine"), but they left the queue out of order after a re-election. The
model then withheld answers that the code sends, so its `Liveness` was
about a shell slightly worse than the real one. The traces so far never
drop a leader's own unapplied proposals: that path needs a leader
deposed while alive (a partition), which the test does not yet stage.

| Mutation | Result |
|---|---|
| A trace record changed (an ACCEPTED's seqno, an ACCEPTED dropped, a filtered resend as NOT_LEADER, a proposal dropped or at another index) | rejected (deadlock at that record) |
| The code's filter keyed by session, not message | rejected (the leader's trace) |
| The code's filter without its "not yet applied" check | accepted, rightly: the code also removes an entry when it is applied, so the check never decides anything |

## `lean/`: the core proof (ARCHITECTURE §11.2, gap Q2)

Lean proofs about `krusty-core`, against its Aeneas translation
(`lean/regen.sh`). CI fails if the translation is stale (`source-hash.sh`)
or if any theorem uses `sorry` or an axiom beyond Lean's standard three.

| File | What it proves |
|---|---|
| `IdMap.lean` | The client and key tables (`table.rs`: linear probing, backward-shift deletion) are finite maps. A well-formedness invariant (power-of-two size, at most half full, distinct keys, no empty bucket inside a probe run) holds after `new` and is kept by `insert` and `remove`; `get`, `insert` and `remove` are lookup, update and erase on the map the buckets hold. Probing terminates because a half-empty table always has an empty bucket. `remove`'s shift loop is proved with Knuth's invariant, plus a ghost empty bucket that bounds the scan. |
| `Spec.lean` | The abstract sequencer: sessions, outcome rings, key versions, a bounded client table from which any idle session may be evicted, and a bounded key table, all over maps and naturals. Proved on it: **G4** (`step_hinv`), every step keeps a history invariant, so a message is decided at most once and remembered outcomes are the decisions taken; **G5** (`step_pub`), a publication takes the next seqno, and a conditional append publishes its key's next version and changes no other version. |
| `Refine.lean` | **Refinement** (`apply_refines`): under an invariant on `Core` (table invariants, slots and sessions in one-to-one correspondence, ring and version bounds), `Core::apply` never panics, keeps the invariant, and its effects and new state are one `spec.Step` of the abstraction (the eviction choice it makes is one the spec allows). `new_inv`: `Core::new` establishes the invariant. `apply_at_most_once`: G4 for the code itself. |
| `Restore.lean` | **Restoring a snapshot** (`restore_inv`): whatever the bytes, `Core::restore` never panics and either rejects them or returns a core satisfying the invariant (with the configuration asked for), so `apply_refines` holds from the first entry after a restart. The proof found one gap: `restore` accepted fewer sessions than slots in use, a core no run reaches, whose empty slots would never be reused; it now rejects them (2026-11-19). |

G7 (determinism) holds by construction: `Core.apply` is a function of the
state and the entry.

Not covered: the snapshot *writer* (`snapshot.rs`, left out of the
translation; its property test checks that what it writes restores to an
indistinguishable core). The reader, `restore.rs`, is translated and
proved; it is written for Aeneas (no early return inside a loop, bytes
decoded by indexing), and the core variable is `kc` because one named
`core` would shadow Lean's `core` namespace.

`Option::clone` is modelled with its standard semantics instead of being
left an axiom (`regen.sh` reapplies the model).

```
PATH=$HOME/.elan/bin:$PATH scripts/check-axioms.sh spec/lean
```

## `../spikes/aeneas-stream/proofs`

Lean proofs about the stream tracker, generated with Aeneas
(see `spikes/aeneas-stream/REPORT.md`). They move here once the verified
crates exist.
