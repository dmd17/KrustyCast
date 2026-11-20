---------------------------- MODULE TraceCluster ----------------------------
(***************************************************************************)
(* Trace validation of the cluster shell (krusty-node/src/cluster.rs)      *)
(* against Cluster.tla: the shell's decisions, replayed with Cluster.tla's *)
(* own actions.                                                            *)
(*                                                                         *)
(* Each node writes a trace (ClusterConfig::trace; the krusty-client test  *)
(* cluster_shell_traces), validated on its own as the one node of the     *)
(* model:                                                                  *)
(*  - raft: what the shell sees of Raft (term, whether it leads, commit    *)
(*    index, its log from an index on). Injected: Raft is validated        *)
(*    against RaftKC on its own (TraceRaftKC). What Cluster.tla's Raft     *)
(*    abstraction assumes is checked: terms do not go back, a leader only  *)
(*    appends in its term, committed entries do not change. C is the       *)
(*    node's committed log; the shell forgets its own proposals the log no *)
(*    longer holds (KeepProposals, as Cluster.tla's Replicate).            *)
(*  - submit: Receive, which must propose at the index the code did, or    *)
(*    drop the resend (filter) or answer NOT_LEADER as the code did.       *)
(*  - apply: Apply, which must send the outcome the code sent (the model's *)
(*    sequencer against the krusty-core decision), or none if none.        *)
(*  - crash: as Crash, with the core restored from a snapshot at `applied`.*)
(* A trace is accepted when the last record is reached (the invariant      *)
(* Unfinished is then violated, as hoped); rejected on a deadlock (a step  *)
(* of the code that is not one of the model's) or a TLC error.             *)
(***************************************************************************)
EXTENDS Cluster, TraceClusterData   \* Trace, TraceClients, TraceMaxSeq

VARIABLES l,     \* the record being replayed
          k,     \* the step of its plan
          core   \* St(applied[N]), kept as entries are applied

N == "n"
TraceNodes == {N}
E == Trace[l]
Min2(a, b) == IF a < b THEN a ELSE b

\* The core after C[1..j]: from `core` for the next entry (each Apply);
\* from scratch otherwise (after a crash).
RECURSIVE SlowSt(_)
SlowSt(j) == IF j = 0 THEN Core0 ELSE CoreStep(SlowSt(j - 1), C[j].m)
TraceSt(j) == IF j = applied[N] + 1 THEN CoreStep(core, C[j].m) ELSE SlowSt(j)

traceVars == <<vars, l, k, core>>

TraceInit == Init /\ l = 1 /\ k = 1 /\ core = Core0

Plan(r) ==
    CASE r.k = "submit" -> <<"inject", "receive">>
      [] r.k = "apply"  -> <<"apply">>
      [] r.k = "raft"   -> <<"raft">>
      [] r.k = "crash"  -> <<"crash">>

X == [to |-> N, m |-> E.m]

RaftStep(r) ==
    LET new == SubSeq(log[N], 1, r.from - 1) \o r.entries IN
    /\ r.from <= Len(log[N]) + 1
    /\ r.commit <= Len(new)
    \* Cluster.tla's abstraction of Raft:
    /\ r.term >= term[N]
    /\ (lead[N] /\ r.lead /\ r.term = term[N]) => r.from = Len(log[N]) + 1
    /\ \A i \in 1..Min2(r.commit, Len(C)) : new[i] = C[i]
    /\ log' = [log EXCEPT ![N] = new]
    /\ pending' = [pending EXCEPT ![N] = KeepProposals(@, log[N], new)]
    /\ term' = [term EXCEPT ![N] = r.term]
    /\ lead' = [lead EXCEPT ![N] = r.lead]
    /\ commit' = [commit EXCEPT ![N] = r.commit]
    /\ C' = IF r.commit > Len(C) THEN SubSeq(new, 1, r.commit) ELSE C
    /\ UNCHANGED <<applied, proposed, sessf, net, clients, faults, core>>

\* Crash, but the core comes back from its snapshot, at `a`: as if it had
\* applied C[1..a] answering no one (Crash then Apply with nothing pending).
TraceCrash(a) ==
    /\ a <= Len(C)
    /\ lead' = [lead EXCEPT ![N] = FALSE]
    /\ commit' = [commit EXCEPT ![N] = 0]
    /\ applied' = [applied EXCEPT ![N] = a]
    /\ pending' = [pending EXCEPT ![N] = <<>>]
    /\ proposed' = [proposed EXCEPT ![N] = [m \in Msgs |-> NoProposal]]
    /\ sessf' = [sessf EXCEPT ![N] = [c \in Clients |-> NoFilterEntry]]
    /\ core' = SlowSt(a)
    /\ UNCHANGED <<term, log, C, net, clients, faults>>

Do(a) ==
    CASE a = "inject" ->
            /\ toNode' = {X}
            /\ UNCHANGED <<term, lead, log, C, commit, shell, toClient, clients, faults, core>>
      [] a = "receive" ->
            /\ Receive(X)
            /\ CASE E.res = "proposed" ->
                      lead[N] /\ Len(log'[N]) = Len(log[N]) + 1 /\ Len(log'[N]) = E.i
                 [] E.res = "filtered" -> lead[N] /\ log' = log
                 [] E.res = "notLeader" -> ~lead[N] /\ log' = log
            /\ UNCHANGED core
      [] a = "apply" ->
            /\ Apply(N)
            /\ applied'[N] = E.i
            /\ IF E.out.k = "none" THEN toClient' = {}
               ELSE \E y \in toClient' : toClient' = {y} /\ y.out = E.out
            /\ core' = CoreStep(core, C[E.i].m)
      [] a = "raft" -> RaftStep(E)
      [] a = "crash" -> TraceCrash(E.applied)

TraceNext ==
    \/ /\ k <= Len(Plan(E))
       /\ Do(Plan(E)[k])
       /\ k' = k + 1
       /\ UNCHANGED l
    \/ /\ k = Len(Plan(E)) + 1
       /\ l < Len(Trace)
       /\ l' = l + 1
       /\ k' = 1
       /\ toNode' = {}
       /\ toClient' = {}
       /\ UNCHANGED <<term, lead, log, C, commit, shell, clients, faults, core>>

\* Violated (as hoped) once the last record has been replayed.
Unfinished == ~(l = Len(Trace) /\ k = Len(Plan(E)) + 1)
=============================================================================
