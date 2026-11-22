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
