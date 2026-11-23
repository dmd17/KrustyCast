# Verifying the Raft implementation against a formal model

Backlog item Q9 (docs/GAP-ANALYSIS.md). This is how to get from today's
checks to a machine-checked statement that `krusty-raft`, as compiled,
is safe. Written 2026-11-22.

## Where things stand

| Layer | What exists | What it gives |
|---|---|---|
| Protocol model | `tla/RaftKC.tla`: Raft as krusty-raft implements it (multi-entry Appends, a leader's no-op, followers taking entries at or below their commit index as matching, restarts). TLC checks ElectionSafety, LogMatching, StateMachineSafety, CommittedStable, LeaderCompleteness and LeaderAppendOnly | Safety for small bounds only (3 servers, 2–3 terms, short logs) |
| Inductive invariant | `tla/RaftKCInd.tla`: a candidate in three layers (elections, logs, commits), holding on every state TLC reaches | Not yet shown inductive, so not yet unbounded |
| Code against model | `tla/TraceRaftKC.tla`: traces of `krusty-raft` under its randomized harness, replayed with RaftKC's actions (3 per CI run, 20 nightly) | Evidence for the tested runs, from the harness rather than the node |
| Durability | `tla/RaftKCPower.tla` (2026-11-22): power loss in mode (a) | Two counterexamples, below |
| Not modelled | Pre-vote, check-quorum and leadership transfer (left to Timeout and StepDown, which RaftKC allows at any time), liveness, InstallSnapshot and membership changes (neither implemented yet) | |

**Mode (a) and power loss.** In mode (a), term, vote and entries reach
the disk only when the OS flushes them, and followers acknowledge entries
on receipt. One node losing power then breaks safety. That is within PRD
G3's failure budget, which assumes only that not every node loses power
at once.
- `RaftKCPower.cfg` finds two leaders in one term. A node forgets a vote
  it cast and votes again (ElectionSafety, depth 12).
- `RaftKCPowerLog.cfg` makes term and vote durable and loses only
  unsynced entries. A leader is then elected without a committed entry
  (LeaderCompleteness, depth 17).

The fix is a design decision. The options:
- fsync the hard state before acting on it;
- have a node that may have lost power stay out of votes and
  acknowledgements until it has caught up (protocol-aware recovery,
  Alagappan et al., FAST '18);
- or narrow G3 to "no node loses unsynced data".

Whichever is chosen goes into the model before the code.

### Decision (2026-11-22): cautious restart

Dan chose option 1. The design under check is below. It goes into the code
only once the model passes.

1. **Term and vote are always fsynced,** in both modes, before any message
   that depends on them. They change only in elections, so this is cheap.
   It removes the double-vote counterexample on its own.
2. **Detecting a possible power loss.** The node records the kernel's boot
   id (`/proc/sys/kernel/random/boot_id`) in its data directory. A process
   crash keeps the page cache and the boot id. In mode (a), a different
   boot id at start means unsynced log writes may be gone. Mode (b) does
   not need this, because it acknowledges only fsynced entries.
3. **Recovering.** Such a node restarts recovering, remembering its term
   at restart. It refuses every vote and starts no election (no pre-vote
   either). It still accepts entries and acknowledges what it really
   holds.
4. **Leaving recovery.** Appends carry the leader's last index (`last`, a
   new field in the replication protocol). The node leaves recovery when
   it accepts an Append, from a leader of its restart term or later, that
   leaves it holding everything up to that `last`. Catching up to the
   leader's commit index is not enough. A leader may still count
   acknowledgements the node gave before it lost power. If that leader
   later commits an entry the node no longer holds, the node must have
   received it before it votes again.

**Costs.**
- While one node is recovering and another is down, no leader can be
  elected until one of them comes back. That is within f = 1.
- If every node loses power at once, which G3 already excludes for mode
  (a), every node is recovering and no leader can be elected. An operator
  then needs an explicit override: a `krusty-admin` command that ends
  recovery, documented as possibly losing acknowledged messages.

**Model check** (`tla/RaftKCRecover.tla`, an exploration copy of RaftKC).
It adds power loss in mode (a), with term and vote durable and the log
durable up to the last `Sync`. It also adds the `last` field and the
recovery rule, selected by `Rule`:
- `"none"`: today's code. Expected to violate (LeaderCompleteness).
- `"any"`: leave recovery on any accepted Append from a leader of the
  restart term or later. Expected to violate: the stale-acknowledgement
  case.
- `"last"`: the design. Expected to hold.

`RaftKCRecover-{none,any,last}.cfg` use 3 servers, 2 terms, logs of 1
entry (enough for both counterexamples), one power loss, and depth 17.
Results (2026-11-23):

| Rule | Result |
|---|---|
| `"none"` | LeaderCompleteness violated (5.8 M states, 7 min) |
| `"any"` | LeaderCompleteness violated (14 M states, 21 min) |
| `"last"` | **No violation: exhaustive to depth 17, 15.3 M states, 32 min** |

The `"any"` counterexample is the stale-acknowledgement case:
1. The no-op commits on s2 and s3, and s3 loses power without it.
2. s3 leaves recovery on an Append from s2 that matched only up to index
   0, while s2's log ran to 1.
3. s3 then votes for s1, whose log is empty, and s1 leads without the
   committed entry.

Under `"last"`, s3 must hold s2's whole log before voting again.

A first attempt with logs of 2 entries ran 40 minutes past 23 M states
without finishing; logs of 1 is what completes. Still to do: a third term
(stale leaders) and a deeper random simulation of `"last"`.

**Implemented (2026-11-23).**
- *krusty-raft:*
  - Appends carry `last` (replication wire version 2);
  - `recover_after_power_loss`, `recovering`, `end_recovery`;
  - recovering nodes grant no votes or pre-votes, and ignore election
    timeouts and TimeoutNow;
  - recovery ends on an accepted Append, from a leader of the restart
    term or later, matching up to its `last`.

  `tests/recovery.rs` covers these. Two of its tests fail if the rule is
  weakened to "any accepted Append".
- *krusty-node:*
  - the hard state is always fsynced;
  - `must_recover`: in mode (a), a recorded boot id that differs, or none
    recorded with state on disk, or the `recovering` marker (created
    fsynced, removed once recovered);
  - the `raft_recovering` and `raft_recoveries` metrics;
  - `krusty-admin end-recovery`.

  The krusty-client test `a_node_back_from_a_power_loss_recovers_before_voting`
  rewrites a follower's boot id, restarts it, and sees it recover, catch
  up and vote again. A plain restart in the same boot does not recover.
- *Spec:* RaftKC's Appends carry `last`, so the nightly Raft trace
  validation checks the field the code sends (three traces accepted).
  The recovery rule itself stays in `RaftKCRecover.tla` until the
  randomized Raft harness has a power-loss fault.

**Plan as written before implementing.**
- *krusty-raft:* the `last` field on Append (wire); a `recovering` state
  (no votes granted, no election or pre-vote timeouts, left by the rule
  above); power-loss faults in the randomized harness, losing the
  unsynced suffix.
- *krusty-node:* the hard state always fsynced; the boot id file and the
  restart decision (mode (a) only); a `recovering` gauge in metrics and
  `krusty-admin status`; the operator override.
- *Spec:* fold `last` and the recovery rule into RaftKC, so the nightly
  trace validation checks the code's recovery steps, and add the
  power-loss configurations to the checked set.
- *Docs:* PRD G3 stands as written once this ships. ARCHITECTURE §8.2 and
  the user guide describe recovering nodes and the override.

## Against published specifications (2026-11-23)

Three published TLA+ specifications, compared with RaftKC:
- Ongaro's `raft.tla` (ongardie/raft.tla);
- etcd-io/raft's `etcdraft.tla`, trace-validated against its Go code;
- CCF's `ccfraft.tla`, release 6.0.0, trace-validated against its C++.

CCF's main branch has since moved to a Lean model of its consensus protocol
(`lean/consensus`). Its safety theorems (election safety, comparable
committed logs) are proved for unbounded executions, and a replayer checks
traces of the C++ code against the model. That is the end state this plan
describes for step 1.2 and route B. It is a worked example in the tool we
already use for `krusty-core`.

| | raft.tla | etcdraft.tla | ccfraft.tla (6.0.0) | RaftKC |
|---|---|---|---|---|
| Entries per Append | 1 | a range | a range | a range (after any prefix) |
| Leader's no-op on election | no | yes | signatures instead | yes |
| Follower matching at its commit index | no | no | no | yes (code's shortcut) |
| Crash and restart | log kept | reverts to durable state | none | log kept; power loss in RaftKCPower / RaftKCRecover |
| Pre-vote | no | no | no | no (Timeout covers it) |
| Check-quorum, step-down | no | no | yes | yes (StepDown) |
| Leadership transfer | no | no | ProposeVote | no (Timeout covers it) |
| Membership changes | no | joint, learners | yes | not implemented |
| Snapshots | no | yes | yes | not implemented |
| Liveness / fairness | no | no | Fairness defined | no |

**Invariants RaftKC now checks from these** (`RaftKC.tla`; all hold on the
CI configuration, 176 k states):
- from etcd: `QuorumLogInv` (every quorum holds what any node has
  committed) and `MoreUpToDateCorrectInv` (a log at least as up to date as
  another holds what the other has committed);
- from CCF: `CandidateTermNotInLogInv`, `MonoTermInv`,
  `NeverCommitEntryPrevTermsProp` (a leader commits only entries of its
  own term), `MonotonicMatchIndexProp` and `CommittedLogAppendOnlyProp`.
  The last two exempt restarts, which CCF does not model.

`CommittedAppendOnly` restates CCF's refinement to its abstract
replicated log (`abs.tla`): one committed log that only grows. In RaftKC
it holds by construction of the ghost. The substance is
`CommittedStable`, which RaftKC already checked.

**Refinement to `raft.tla` is not attempted.** It would need an
intermediate specification:
- `raft.tla` takes several atomic steps for what RaftKC does in one
  Append: it raises the term, removes one conflicting entry per step,
  appends one entry per step, and replies in yet another step;
- it also has state RaftKC lacks (`nextIndex`, `votesResponded`,
  `voterLog`, messages as a bag).

The safety properties are compared directly instead, above.

**Do the new properties catch bugs?** Removing the quorum check from
commit is caught by `QuorumLogInv` at once (1,185 states). Removing the
rule that a leader commits only entries of its own term is *not* caught
within 20 minutes on the CI bounds (2 terms, depth 15, 2.9 M states): the
scenario needs a third term and an Append range that stops short of the
new leader's no-op. So `NeverCommitEntryPrevTermsProp` is not yet shown to
catch anything; that check belongs with the three-term configuration.

## The plan

The goal is one chain of checked steps:

**node code ⊑ per-node protocol actions → protocol model → safety for
any number of terms, entries and messages.**

### 1. Protocol: make the model's safety unbounded

1. **Compare with published specs first.** That is cheap, and it shows
   which deviations need arguing.
   - Check that RaftKC refines Ongaro's `raft.tla` with TLC: a mapping of
     variables, with `Raft!Spec` as the property. It is expected to stop
     at the known deviations (several entries per Append, the no-op,
     followers matching at their commit index). Each needs a stuttering
     or step-splitting mapping, or a separate argument.
   - Tabulate the actions and invariants in etcd-io/raft's TLA+ spec and
     CCF's (both trace-validated against their code) that RaftKC lacks,
     such as pre-vote, check-quorum and configuration changes, and add
     what applies.
   - Use Verdi's Coq proof of Raft's safety as a checklist of invariants.
2. **Inductive invariant.**
   - Make a typed copy of RaftKC that Apalache accepts (its message set
     mixes record shapes). Show it equivalent to RaftKC with TLC on small
     bounds.
   - Check `Init => Inv` and `Inv /\ Next => Inv'` with Apalache, then
     strengthen `RaftKCInd` from each counterexample. The commit layer
     (followers matching at their commit index, and stale replies) is
     where the work is.
   - Apalache checks inductiveness for fixed sizes (servers, terms,
     lengths). For a proof without bounds, carry the invariant into a
     proof assistant: TLAPS on the TLA+ model, or Lean on a transcription
     of RaftKC as a transition system (see 3).
3. **Durability in the model.** Extend RaftKCPower with each mode's real
   rules: mode (b) acknowledges only fsynced entries, and the hard state
   is fsynced before dependent messages. Check that mode (b) is safe
   under any number of power losses within f, and that the chosen fix
   for mode (a) is too.
4. **Liveness.** Add pre-vote, check-quorum and leadership transfer, and
   check that a quorum that stays connected (with partitions that end)
   eventually has a stable leader that commits.

### 2. Implementation: check the code against the protocol's actions

Two routes, which can be combined.

**A. Trace validation everywhere (pragmatic, now).**
- Have `krusty-node` itself log Raft traces, as `ClusterConfig::trace`
  does for the shell, rather than only the `krusty-raft` harness.
- Run them under fault injection of the real processes: partitions
  (full, partial, one-way), delay, reordering, duplication, SIGSTOP
  pauses, kills, and lost unsynced writes. Validate every trace against
  RaftKC (and the shell's against Cluster.tla).
- This covers what a proof of `krusty-raft` alone would not: the
  threads, rings, storage modes and held replies around it.

**B. Deductive refinement of `krusty-raft` (the full proof).**
- *Per-node simulation.* For each public step (`on_message`, `tick`,
  `propose`, `replicate`, `persisted`, `applied`, `transfer_leadership`)
  prove that the node's state afterwards, and the messages in its
  `Outbox`, are what some sequence of RaftKC actions of that node
  produces. TraceRaftKC's per-record *plans* already name these
  sequences: each proof obligation is a plan, generalized. An
  abstraction function maps `Raft` to RaftKC's per-node variables, and
  the `Outbox` to messages added to `msgs`.
- *Global composition.* Nodes interact only through messages, and RaftKC
  lets the network lose, reorder and duplicate them. So per-node
  simulation plus the protocol's invariant gives the implementation's
  safety. That is the structure of IronFleet (Dafny) and Verdi (Coq).
- *Tool.* Aeneas → Lean, as for `krusty-core` (toolchain, regen and
  axiom checks already in CI). To fit its subset:
  - replace `&dyn Entries` in `on_message` with a generic parameter or a
    slice;
  - keep loops free of early returns;
  - check `RaftLog` and `Outbox` (Vec-based) translate.

  Transcribe RaftKC's actions into Lean as a relation. If the protocol
  invariant is also proved in Lean (step 1.2), the whole chain is one
  Lean theorem. With TLAPS for the protocol instead, the link between
  the two is the transcription, which must be reviewed. Verus is the
  alternative: proofs in the Rust source, already used for the ring
  spike. It suits a code-first proof, but has no TLA+ link either.
- *Scope.* Prove the safety-relevant steps first: votes, Append
  handling, commit advance and the hard state. Clocks and timers only
  decide when actions happen, and RaftKC allows them at any time.

### 3. Order and size

| Step | Size | Depends on |
|---|---|---|
| Fix or restate mode (a), checked in RaftKCPower | S–M | a decision on G3 |
| Refinement to `raft.tla`; feature table against etcd and CCF | M | — |
| Real-node traces under fault injection (A) | M | a fault harness (Q8) |
| Liveness model | M | — |
| Inductive invariant (Apalache), then unbounded (TLAPS or Lean) | L–XL | — |
| Deductive refinement of `krusty-raft` in Lean (B) | XL | the protocol invariant, Aeneas-ready code |

Until B is done, the claim is: the protocol is checked for small bounds
(unbounded once step 1.2 is done), and the code is validated on every
trace we can produce.
