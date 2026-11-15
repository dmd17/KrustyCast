----------------------------- MODULE TraceRaftKC -----------------------------
(***************************************************************************)
(* Trace validation of krusty-raft against RaftKC (gap Q3).                *)
(*                                                                         *)
(* The krusty-raft test harness logs one record per call into the Raft     *)
(* core (a tick, a message, a proposal, a restart): the node, the message  *)
(* it received and the messages it sent (both in RaftKC's form), its state *)
(* afterwards, and a plan, the RaftKC actions that explain the call.  This  *)
(* module replays every plan with RaftKC's own actions; TLC checks that    *)
(* each action is enabled and that, at the end of each record, the node's  *)
(* state and the messages sent are exactly what the code produced.  A      *)
(* trace is accepted when the last record is reached: the invariant        *)
(* `Unfinished` is then violated, which is the expected outcome.           *)
(*                                                                         *)
(* The network is abstracted: before a node receives a message, the        *)
(* message is injected (msgs' = {m}).  That is sound because every message  *)
(* was checked against RaftKC when it was sent (`sends` of an earlier      *)
(* record), and the harness only delivers messages it sent.  Pre-vote is   *)
(* not in RaftKC: pre-vote steps leave RaftKC's state unchanged, and a     *)
(* pre-candidate's role is not compared.                                   *)
(***************************************************************************)
EXTENDS RaftKC, TraceData

VARIABLES l,   \* the record being replayed
          k    \* the step of its plan

traceVars == <<vars, l, k>>

TraceInit ==
    /\ Init
    /\ l = 1
    /\ k = 1

E == Trace[l]
Step == E.plan[k]
S == E.node

Do(a) ==
    CASE a.a = "inject"        -> msgs' = {E.msg} /\ UNCHANGED <<currentTerm, role, votedFor, log, commitIndex, votes, matchIndex>>
      [] a.a = "receive"       -> Receive(E.msg)
      [] a.a = "timeout"       -> Timeout(S)
      [] a.a = "requestVote"   -> RequestVote(S, a.t)
      [] a.a = "becomeLeader"  -> BecomeLeader(S)
      [] a.a = "stepDown"      -> StepDown(S)
      [] a.a = "clientRequest" -> ClientRequest(S, a.v)
      [] a.a = "append"        -> AppendEntries(S, a.t, a.prev, a.last)
      \* RaftKC lets the leader pick any eligible index; the code's choice:
      [] a.a = "advanceCommit" -> AdvanceCommit(S) /\ commitIndex'[S] = a.n
      [] a.a = "restart"       -> Restart(S)

\* The node's state and what it sent match the record.
Matches ==
    /\ currentTerm[S] = E.term
    /\ (E.role = "precandidate" \/ role[S] = E.role)
    /\ votedFor[S] = E.votedFor
    /\ log[S] = E.log
    /\ commitIndex[S] = E.commit
    /\ msgs = E.sends

TraceNext ==
    /\ \/ /\ k <= Len(E.plan)
          /\ Do(Step)
          /\ k' = k + 1
          /\ UNCHANGED l
       \/ /\ k = Len(E.plan) + 1
          /\ Matches
          /\ l < Len(Trace)
          /\ l' = l + 1
          /\ k' = 1
          /\ msgs' = {}
          /\ UNCHANGED <<currentTerm, role, votedFor, log, commitIndex, votes, matchIndex>>
    /\ RecordCommits   \* last: it reads the primed state

TraceSpec == TraceInit /\ [][TraceNext]_traceVars

\* Violated (as hoped) once the last record has been replayed and matched.
Unfinished == ~(l = Len(Trace) /\ k = Len(E.plan) + 1 /\ Matches)

\* RaftKC's safety properties hold along the trace too.
TraceSafety == ElectionSafety /\ LogMatching /\ StateMachineSafety /\ CommittedStable /\ LeaderCompleteness
=============================================================================
