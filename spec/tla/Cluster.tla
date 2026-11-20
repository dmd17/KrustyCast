----------------------------- MODULE Cluster -----------------------------
(***************************************************************************)
(* The cluster shell (krusty-node/src/cluster.rs): how a node turns client *)
(* SUBMITs into Raft proposals and committed entries back into outcomes.   *)
(*                                                                         *)
(* Raft is abstracted to what RaftKC.tla checks: terms, at most one        *)
(* current leader (elected in a fresh term with every committed entry,     *)
(* LeaderCompleteness), leaders that only append within their term         *)
(* (LeaderAppendOnly), stale leaders that still think they lead, one       *)
(* committed log `C` that every node agrees with up to its commit index,   *)
(* and crashes that keep the log and the term but lose the shell's memory. *)
(* The sequencer core is a function of the committed log (G7), as in       *)
(* Sequencer.tla: it accepts a session's next seq, repeats the stored      *)
(* outcome for an older one, and NAKs a gap.                               *)
(*                                                                         *)
(* The shell as the code has it:                                           *)
(* - A node that thinks it leads proposes each SUBMIT, and remembers       *)
(*   (index, term, client) in `pending`, a queue in proposal order.        *)
(* - The resend filter: a resend is dropped if that very message was       *)
(*   proposed in the leader's term and is not yet applied (its outcome     *)
(*   follows). Per-session versions of it are kept as mutations.          *)
(* - Applying entry i, a node answers only if the front of its queue is    *)
(*   (i, t) with t the entry's term: an entry it proposed itself.          *)
(* - Others answer NOT_LEADER; the client's routing (follow NOT_LEADER,    *)
(*   rotate on silence) is abstracted to "a resend eventually reaches the  *)
(*   current leader".                                                      *)
(*                                                                         *)
(* Checked: every outcome a client hears is true, and (Liveness) every     *)
(* message a client sends eventually gets one, under fairness, with       *)
(* finitely many losses, crashes and elections. The log is bounded, so     *)
(* resends that fill it with copies fail Liveness: the resend storm of     *)
(* BASELINE E3.                                                            *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    Nodes, Clients,
    MaxSeq,         \* messages per client
    MaxTerm,        \* elections
    MaxLog,         \* log capacity (copies of resends count against it)
    MaxLoss,        \* datagrams lost, in all
    MaxCrash,       \* node crashes, in all
    MaxCopies,      \* the CopiesBounded invariant's bound
    \* Deliberate bugs, for mutation checks (all FALSE in the real model):
    NoFilter,             \* every resend is proposed again
    FilterIgnoresApplied, \* drop resends even after the entry was applied
    FilterIgnoresTerm,    \* use a filter entry from an earlier term
    MineIgnoresTerm,      \* answer for an index without checking its term
    \* Per-session filters, as first written ("highest", 2026-11-17: the
    \* highest seq proposed and that proposal's index) and as first fixed
    \* ("latest": the highest seq and the index of the latest proposal);
    \* "none" is the per-message filter. "highest": after a NAK the client
    \* goes back to a lower seq, proposed after the higher one was applied,
    \* and its resends are logged again without bound. "latest": a lower
    \* seq that was never proposed is dropped while a higher one is pending,
    \* and with reordering it never gets in (both fail CopiesBounded).
    SessionFilter

NONE == [k |-> "none"]
NOOP == [c |-> "noop", seq |-> 0]
Acc(g) == [k |-> "acc", g |-> g]
Nak(s) == [k |-> "nak", expect |-> s]

Seqs == 1..MaxSeq
Msgs == [c : Clients, seq : Seqs]
Outs == {Acc(g) : g \in 1..MaxLog} \cup {Nak(s) : s \in Seqs} \cup {NONE}
Replies == [c : Clients, seq : Seqs, about : Msgs, out : Outs]

VARIABLES
    term,       \* each node's term
    lead,       \* whether it thinks it leads (in its term)
    log,        \* its log: entries [t, m]
    C,          \* the committed log
    commit,     \* its commit index
    applied,    \* how far its core has applied the committed log
    pending,    \* its proposals, in order: [i, t, c]
    proposed,   \* the resend filter: [Msgs -> [i, t]] (i 0: none)
    sessf,      \* the per-session filters (SessionFilter): [Clients -> [seq, i, t]]
    toNode,     \* SUBMITs in flight: [to, m] (a set: a resend may coincide)
    toClient,   \* outcomes in flight: [c, seq, out]
    sent,       \* each client's highest seq sent
    got,        \* the outcome each client heard for each seq
    lost, crashed

vars == <<term, lead, log, C, commit, applied, pending, proposed, sessf,
          toNode, toClient, sent, got, lost, crashed>>

Max(S) == CHOOSE x \in S : \A y \in S : y <= x
MaxT == Max({term[n] : n \in Nodes})
CurLeader(n) == lead[n] /\ term[n] = MaxT
NoLeader == \A n \in Nodes : ~CurLeader(n)
IsPrefix(a, b) == Len(a) <= Len(b) /\ SubSeq(b, 1, Len(a)) = a
NoFilterEntry == [seq |-> 0, i |-> 0, t |-> 0]
NoProposal == [i |-> 0, t |-> 0]

(***************************************************************************)
(* The sequencer core over the committed log: its state after C[1..j],    *)
(* with `out` the outcome of entry j.                                      *)
(***************************************************************************)
Core0 == [nx |-> [c \in Clients |-> 1],
          res |-> [c \in Clients |-> [s \in Seqs |-> NONE]],
          stream |-> <<>>, out |-> NONE]

CoreStep(p, m) ==
    IF m = NOOP THEN [p EXCEPT !.out = NONE]
    ELSE IF m.seq < p.nx[m.c] THEN [p EXCEPT !.out = p.res[m.c][m.seq]]
    ELSE IF m.seq > p.nx[m.c] THEN [p EXCEPT !.out = Nak(p.nx[m.c])]
    ELSE LET o == Acc(Len(p.stream) + 1)
         IN [nx |-> [p.nx EXCEPT ![m.c] = @ + 1],
             res |-> [p.res EXCEPT ![m.c][m.seq] = o],
             stream |-> Append(p.stream, m),
             out |-> o]

RECURSIVE St(_)
St(j) == IF j = 0 THEN Core0 ELSE CoreStep(St(j - 1), C[j].m)

Core == St(Len(C))

Init ==
    /\ term = [n \in Nodes |-> 0]
    /\ lead = [n \in Nodes |-> FALSE]
    /\ log = [n \in Nodes |-> <<>>]
    /\ C = <<>>
    /\ commit = [n \in Nodes |-> 0]
    /\ applied = [n \in Nodes |-> 0]
    /\ pending = [n \in Nodes |-> <<>>]
    /\ proposed = [n \in Nodes |-> [m \in Msgs |-> NoProposal]]
    /\ sessf = [n \in Nodes |-> [c \in Clients |-> NoFilterEntry]]
    /\ toNode = {}
    /\ toClient = {}
    /\ sent = [c \in Clients |-> 0]
    /\ got = [c \in Clients |-> [s \in Seqs |-> NONE]]
    /\ lost = 0
    /\ crashed = 0

shell == <<applied, pending, proposed, sessf>>
net == <<toNode, toClient>>
clients == <<sent, got>>
faults == <<lost, crashed>>

(***************************************************************************)
(* Raft, abstracted                                                        *)
(***************************************************************************)

\* Elected in a fresh term, holding every committed entry; appends a no-op.
\* Leaders of older terms may still think they lead.
Elect(n) ==
    /\ MaxT < MaxTerm
    /\ IsPrefix(C, log[n])
    /\ term' = [term EXCEPT ![n] = MaxT + 1]
    /\ lead' = [lead EXCEPT ![n] = TRUE]
    /\ log' = IF Len(log[n]) < MaxLog
              THEN [log EXCEPT ![n] = Append(@, [t |-> MaxT + 1, m |-> NOOP])]
              ELSE log
    /\ UNCHANGED <<C, commit, shell, net, clients, faults>>

\* The first index at which log `new` no longer holds what `old` held
\* (Len(old) + 1 if it holds all of it).
FirstChange(old, new) ==
    LET D == {i \in 1..Len(old) : i > Len(new) \/ old[i] # new[i]}
    IN IF D = {} THEN Len(old) + 1 ELSE CHOOSE i \in D : \A j \in D : i <= j

\* When a node's log changes from `old` to `new`, its own proposals from the
\* first changed index on are gone; the shell forgets them (carry_out).
KeepProposals(q, old, new) ==
    LET k == FirstChange(old, new) IN SelectSeq(q, LAMBDA p : p.i < k)

\* A leader (possibly stale) brings a follower in line: a prefix of its log,
\* keeping what the follower knows is committed. The follower adopts the
\* term; a stale leader it reaches steps down.
Replicate(f, l) ==
    /\ f # l /\ lead[l] /\ term[f] <= term[l]
    /\ IsPrefix(SubSeq(log[f], 1, commit[f]), log[l])
    /\ \E k \in commit[f]..Len(log[l]) :
          LET new == SubSeq(log[l], 1, k) IN
          /\ log' = [log EXCEPT ![f] = new]
          /\ pending' = [pending EXCEPT ![f] = KeepProposals(@, log[f], new)]
    /\ term' = [term EXCEPT ![f] = term[l]]
    /\ lead' = [lead EXCEPT ![f] = FALSE]
    /\ UNCHANGED <<C, commit, applied, proposed, sessf, net, clients, faults>>

\* The current leader's next entry reaches a majority and commits.
AdvanceCommit(l) ==
    /\ CurLeader(l) /\ Len(C) < Len(log[l])
    /\ C' = Append(C, log[l][Len(C) + 1])
    /\ UNCHANGED <<term, lead, log, commit, shell, net, clients, faults>>

\* A node learns its next entry is committed.
LearnCommit(n) ==
    /\ commit[n] < Len(C) /\ commit[n] < Len(log[n])
    /\ log[n][commit[n] + 1] = C[commit[n] + 1]
    /\ commit' = [commit EXCEPT ![n] = @ + 1]
    /\ UNCHANGED <<term, lead, log, C, shell, net, clients, faults>>

\* Check-quorum, a transfer, or a partition; another election can follow.
StepDown(n) ==
    /\ lead[n] /\ MaxT < MaxTerm
    /\ lead' = [lead EXCEPT ![n] = FALSE]
    /\ UNCHANGED <<term, log, C, commit, shell, net, clients, faults>>

\* A crash: the term and log survive; the shell's memory does not (the core
\* is rebuilt by applying the committed log again, answering no one).
Crash(n) ==
    /\ crashed < MaxCrash /\ MaxT < MaxTerm
    /\ crashed' = crashed + 1
    /\ lead' = [lead EXCEPT ![n] = FALSE]
    /\ commit' = [commit EXCEPT ![n] = 0]
    /\ applied' = [applied EXCEPT ![n] = 0]
    /\ pending' = [pending EXCEPT ![n] = <<>>]
    /\ proposed' = [proposed EXCEPT ![n] = [m \in Msgs |-> NoProposal]]
    /\ sessf' = [sessf EXCEPT ![n] = [c \in Clients |-> NoFilterEntry]]
    /\ UNCHANGED <<term, log, C, net, clients, lost>>

(***************************************************************************)
(* The shell                                                               *)
(***************************************************************************)

\* A resend is dropped if this very message was proposed in this term and
\* is not yet applied: it is in the log, and its outcome follows.
Filtered(n, m) ==
    /\ ~NoFilter
    /\ IF SessionFilter = "none"
       THEN LET p == proposed[n][m]
            IN /\ p.i > 0
               /\ (FilterIgnoresTerm \/ p.t = term[n])
               /\ (FilterIgnoresApplied \/ p.i > applied[n])
       ELSE LET p == sessf[n][m.c]
            IN /\ p.seq > 0 /\ p.t = term[n] /\ p.i > applied[n]
               /\ m.seq <= p.seq

\* on_submit: a node that thinks it leads proposes (unless the filter drops
\* the resend, or the log is full: BUSY); others answer NOT_LEADER, which the
\* abstracted routing makes a drop.
Receive(x) ==
    /\ x \in toNode
    /\ toNode' = toNode \ {x}
    /\ LET n == x.to
           m == x.m
           i == Len(log[n]) + 1
           p == sessf[n][m.c]
       IN IF lead[n] /\ ~Filtered(n, m) /\ Len(log[n]) < MaxLog
          THEN /\ log' = [log EXCEPT ![n] = Append(@, [t |-> term[n], m |-> m])]
               /\ pending' = [pending EXCEPT ![n] = Append(@, [i |-> i, t |-> term[n], c |-> m.c])]
               /\ proposed' = [proposed EXCEPT ![n][m] = [i |-> i, t |-> term[n]]]
               /\ sessf' = [sessf EXCEPT ![n][m.c] =
                      IF p.t # term[n]
                      THEN [seq |-> m.seq, i |-> i, t |-> term[n]]
                      ELSE IF SessionFilter = "highest"
                      THEN (IF m.seq >= p.seq
                            THEN [seq |-> m.seq, i |-> i, t |-> term[n]] ELSE p)
                      ELSE [seq |-> IF m.seq > p.seq THEN m.seq ELSE p.seq,
                            i |-> i, t |-> term[n]]]
               /\ UNCHANGED <<term, lead, C, commit, applied, toClient, clients, faults>>
          ELSE UNCHANGED <<term, lead, log, C, commit, shell, toClient, clients, faults>>

\* apply_committed: drop proposals below i from the front of the queue; if
\* the front is (i, t) and the entry's term is t, the entry is this node's
\* and its outcome goes to the client.
Apply(n) ==
    /\ applied[n] < commit[n]
    /\ LET i == applied[n] + 1
           e == C[i]
           RECURSIVE Drop(_)
           Drop(q) == IF q # <<>> /\ Head(q).i < i THEN Drop(Tail(q)) ELSE q
           q == Drop(pending[n])
           front == q # <<>> /\ Head(q).i = i
           mine == front /\ (MineIgnoresTerm \/ Head(q).t = e.t)
       IN /\ applied' = [applied EXCEPT ![n] = i]
          /\ pending' = [pending EXCEPT ![n] = IF front THEN Tail(q) ELSE q]
          /\ toClient' =
                IF mine /\ e.m # NOOP
                THEN toClient \cup {[c |-> Head(q).c, seq |-> e.m.seq,
                                     about |-> e.m, out |-> St(i).out]}
                ELSE toClient
    /\ UNCHANGED <<term, lead, log, C, commit, proposed, sessf, toNode, clients, faults>>

(***************************************************************************)
(* Clients and the network                                                 *)
(***************************************************************************)

Send(c, n) ==
    /\ sent[c] < MaxSeq
    /\ sent' = [sent EXCEPT ![c] = @ + 1]
    /\ toNode' = toNode \cup {[to |-> n, m |-> [c |-> c, seq |-> sent[c] + 1]]}
    /\ UNCHANGED <<term, lead, log, C, commit, shell, toClient, got, faults>>

\* Go back N (krusty-session): on a timeout or a NAK the client resends
\* every unanswered message from the oldest on (the network may reorder
\* them). The retry timer is longer than a round trip: it fires only when
\* nothing of the oldest unanswered message is in flight, neither its
\* SUBMIT nor an outcome (retry_interval, 20 ms in the SDK, against round
\* trips of tens of microseconds). It still fires while the entry waits to
\* commit, which is what filled the log without the filter.
Unanswered(c) == {s \in 1..sent[c] : got[c][s] = NONE}
Resend(c, n) ==
    /\ Unanswered(c) # {}
    /\ LET o == CHOOSE s \in Unanswered(c) : \A t \in Unanswered(c) : s <= t
       IN /\ ~\E x \in toNode : x.m = [c |-> c, seq |-> o]
          /\ ~\E y \in toClient : y.c = c /\ y.seq = o
    /\ toNode' = toNode \cup {[to |-> n, m |-> [c |-> c, seq |-> s]] : s \in Unanswered(c)}
    /\ UNCHANGED <<term, lead, log, C, commit, shell, toClient, clients, faults>>

\* A client takes an outcome about its own message (session checks); a NAK
\* only prompts the go-back-N resend, which Resend covers.
Hear(x) ==
    /\ x \in toClient
    /\ toClient' = toClient \ {x}
    /\ got' = IF x.about = [c |-> x.c, seq |-> x.seq] /\ x.out.k = "acc"
                 /\ got[x.c][x.seq] = NONE
              THEN [got EXCEPT ![x.c][x.seq] = x.out] ELSE got
    /\ UNCHANGED <<term, lead, log, C, commit, shell, toNode, sent, faults>>

Lose ==
    /\ lost < MaxLoss
    /\ lost' = lost + 1
    /\ \/ \E x \in toNode : toNode' = toNode \ {x} /\ UNCHANGED toClient
       \/ \E x \in toClient : toClient' = toClient \ {x} /\ UNCHANGED toNode
    /\ UNCHANGED <<term, lead, log, C, commit, shell, clients, crashed>>

Next ==
    \/ \E n \in Nodes : Elect(n) \/ AdvanceCommit(n) \/ LearnCommit(n)
                        \/ StepDown(n) \/ Crash(n) \/ Apply(n)
    \/ \E f, l \in Nodes : Replicate(f, l)
    \/ \E x \in toNode : Receive(x)
    \/ \E x \in toClient : Hear(x)
    \/ \E c \in Clients, n \in Nodes : Send(c, n)
    \/ \E c \in Clients, n \in Nodes : Resend(c, n)
    \/ Lose

(***************************************************************************)
(* Fairness: the current leader commits; nodes learn and apply; datagrams  *)
(* in flight are delivered (losses are finite); an unanswered message is   *)
(* eventually resent to the current leader; with no leader, one is         *)
(* elected. Nothing else is assumed.                                       *)
(***************************************************************************)
Fairness ==
    /\ \A n \in Nodes : WF_vars(AdvanceCommit(n)) /\ WF_vars(LearnCommit(n))
                        /\ WF_vars(Apply(n))
    /\ \A x \in [to : Nodes, m : Msgs] : WF_vars(Receive(x))
    \* Strong: resending is enabled only while nothing is in flight, so a
    \* client could otherwise keep missing the leader (the SDK follows
    \* NOT_LEADER to it, or rotates after silence).
    /\ \A c \in Clients :
          SF_vars(\E n \in Nodes : CurLeader(n) /\ Resend(c, n))
    /\ \A x \in Replies : WF_vars(Hear(x))
    /\ WF_vars(NoLeader /\ \E n \in Nodes : Elect(n))

Spec == Init /\ [][Next]_vars /\ Fairness

(***************************************************************************)
(* Properties                                                              *)
(***************************************************************************)

\* Every outcome a client hears, or that is on its way, is true: ACCEPTED(g)
\* names the message at g of the one stream.
OutcomesTrue ==
    /\ \A c \in Clients, s \in Seqs :
          got[c][s] # NONE =>
              got[c][s].g <= Len(Core.stream)
              /\ Core.stream[got[c][s].g] = [c |-> c, seq |-> s]
    /\ \A x \in toClient :
          x.out.k = "acc" =>
              x.out.g <= Len(Core.stream) /\ Core.stream[x.out.g] = x.about

\* The abstraction holds: nodes agree with C up to their commit index, and
\* the current leader holds all of C.
CommittedAgree ==
    /\ \A n \in Nodes : commit[n] <= Len(log[n]) /\ applied[n] <= commit[n]
    /\ \A n \in Nodes : SubSeq(log[n], 1, commit[n]) = SubSeq(C, 1, commit[n])
    /\ \A n \in Nodes : CurLeader(n) => IsPrefix(C, log[n])

\* Resends do not multiply the log: no message has more than MaxCopies
\* copies in any log (a term change, a loss, or a resend crossing its
\* outcome each allow one more). Without the filter there is no bound.
Copies(m, l) == Cardinality({i \in 1..Len(l) : l[i].m = m})
CopiesBounded == \A n \in Nodes, m \in Msgs : Copies(m, log[n]) <= MaxCopies

\* Every message sent gets an outcome.
Liveness ==
    \A c \in Clients, s \in Seqs : (sent[c] >= s) ~> (got[c][s] # NONE)

\* Sanity (expected to be violated): the model reaches answered messages.
NothingHeard == \A c \in Clients, s \in Seqs : got[c][s] = NONE
=============================================================================
