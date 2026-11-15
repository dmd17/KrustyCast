------------------------------- MODULE RaftKC -------------------------------
(***************************************************************************)
(* Raft as krusty-raft implements it (ADR-3), for safety model checking    *)
(* and as the target of trace validation (gap Q3).                         *)
(*                                                                         *)
(* Based on Ongaro's Raft specification. Differences, following the code:  *)
(*  - A new leader appends a no-op entry in its term.                      *)
(*  - The leader sends its log after any prefix (covers pipelining and     *)
(*    backtracking; batches are cut at a limit in the code).               *)
(*  - Followers treat entries at or below their commit index as matching.  *)
(*  - Pre-vote and check-quorum are not modelled: they only decide when a  *)
(*    node times out or a leader steps down, which the spec allows at any  *)
(*    time (Timeout, StepDown), so they cannot break safety.               *)
(*  - The network loses and reorders; receiving consumes a message.        *)
(*    Duplicate requests are covered because leaders and candidates may    *)
(*    resend at any time; duplicate replies are not modelled (the          *)
(*    randomized harness in krusty-raft covers them). Restart models a     *)
(*    process crash: term, vote and log are on disk; role and commit index *)
(*    are lost.                                                            *)
(*                                                                         *)
(* Safety: ElectionSafety, LogMatching, StateMachineSafety (committed      *)
(* entries agree on every node), LeaderCompleteness.                       *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS Servers, Values, MaxTerm, MaxLog, MaxMsgs,
          WithRestart,   \* include process crash-restarts
          WithStepDown   \* include leaders stepping down (check-quorum)

Noop == "noop"
Nil == "nil"
Quorum(S) == 2 * Cardinality(S) > Cardinality(Servers)

VARIABLES currentTerm, role, votedFor, log, commitIndex, votes, matchIndex, msgs

vars == <<currentTerm, role, votedFor, log, commitIndex, votes, matchIndex, msgs>>

LastTerm(l) == IF Len(l) = 0 THEN 0 ELSE l[Len(l)].term
Min(a, b) == IF a < b THEN a ELSE b
Max(a, b) == IF a > b THEN a ELSE b

Init ==
    /\ currentTerm = [s \in Servers |-> 0]
    /\ role = [s \in Servers |-> "follower"]
    /\ votedFor = [s \in Servers |-> Nil]
    /\ log = [s \in Servers |-> << >>]
    /\ commitIndex = [s \in Servers |-> 0]
    /\ votes = [s \in Servers |-> {}]
    /\ matchIndex = [s \in Servers |-> [t \in Servers |-> 0]]
    /\ msgs = {}

Send(m) == msgs' = msgs \cup {m}

\* Receiving consumes the message (and may add a reply).
Reply(m, r) == msgs' = (msgs \ {m}) \cup {r}
Consume(m) == msgs' = msgs \ {m}

-----------------------------------------------------------------------------
(* Elections.                                                              *)

Timeout(s) ==
    /\ role[s] \in {"follower", "candidate"}
    /\ currentTerm[s] < MaxTerm
    /\ role' = [role EXCEPT ![s] = "candidate"]
    /\ currentTerm' = [currentTerm EXCEPT ![s] = @ + 1]
    /\ votedFor' = [votedFor EXCEPT ![s] = s]
    /\ votes' = [votes EXCEPT ![s] = {s}]
    /\ UNCHANGED <<log, commitIndex, matchIndex, msgs>>

RequestVote(s, t) ==
    /\ role[s] = "candidate"
    /\ s # t
    /\ Send([type |-> "vote", term |-> currentTerm[s], from |-> s, to |-> t,
             lastTerm |-> LastTerm(log[s]), lastIndex |-> Len(log[s])])
    /\ UNCHANGED <<currentTerm, role, votedFor, log, commitIndex, votes, matchIndex>>

BecomeLeader(s) ==
    /\ role[s] = "candidate"
    /\ Quorum(votes[s])
    /\ Len(log[s]) < MaxLog
    /\ role' = [role EXCEPT ![s] = "leader"]
    /\ matchIndex' = [matchIndex EXCEPT ![s] = [t \in Servers |-> 0]]
    \* The no-op that lets the new leader commit earlier entries.
    /\ log' = [log EXCEPT ![s] = Append(@, [term |-> currentTerm[s], value |-> Noop])]
    /\ UNCHANGED <<currentTerm, votedFor, commitIndex, votes, msgs>>

\* Check-quorum (or anything else) may make a leader step down.
StepDown(s) ==
    /\ WithStepDown
    /\ role[s] = "leader"
    /\ role' = [role EXCEPT ![s] = "follower"]
    /\ UNCHANGED <<currentTerm, votedFor, log, commitIndex, votes, matchIndex, msgs>>

-----------------------------------------------------------------------------
(* Replication.                                                            *)

ClientRequest(s, v) ==
    /\ role[s] = "leader"
    /\ Len(log[s]) < MaxLog
    /\ log' = [log EXCEPT ![s] = Append(@, [term |-> currentTerm[s], value |-> v])]
    /\ UNCHANGED <<currentTerm, role, votedFor, commitIndex, votes, matchIndex, msgs>>

\* Entries prev+1..last of the leader's log, after any prefix.
AppendEntries(s, t, prev, last) ==
    /\ role[s] = "leader"
    /\ s # t
    /\ prev <= last /\ last <= Len(log[s])
    /\ Send([type |-> "append", term |-> currentTerm[s], from |-> s, to |-> t,
             prev |-> prev, prevTerm |-> IF prev = 0 THEN 0 ELSE log[s][prev].term,
             entries |-> SubSeq(log[s], prev + 1, last),
             commit |-> Min(commitIndex[s], last)])
    /\ UNCHANGED <<currentTerm, role, votedFor, log, commitIndex, votes, matchIndex>>

\* The leader commits the largest index a quorum holds, if from its term.
AdvanceCommit(s) ==
    /\ role[s] = "leader"
    /\ \E n \in (commitIndex[s] + 1)..Len(log[s]) :
        /\ log[s][n].term = currentTerm[s]
        /\ Quorum({s} \cup {t \in Servers : matchIndex[s][t] >= n})
        /\ commitIndex' = [commitIndex EXCEPT ![s] = n]
    /\ UNCHANGED <<currentTerm, role, votedFor, log, votes, matchIndex, msgs>>

-----------------------------------------------------------------------------
(* Receiving.                                                              *)

\* A higher term makes anyone a follower first (applied when receiving).
NewTerm(s, term) ==
    IF term > currentTerm[s] THEN [ct |-> term, r |-> "follower", v |-> Nil]
    ELSE [ct |-> currentTerm[s], r |-> role[s], v |-> votedFor[s]]

UpToDate(m, s) ==
    \/ m.lastTerm > LastTerm(log[s])
    \/ m.lastTerm = LastTerm(log[s]) /\ m.lastIndex >= Len(log[s])

HandleVote(m) ==
    LET s == m.to
        n == NewTerm(s, m.term)
        grant == m.term = n.ct /\ n.v \in {Nil, m.from} /\ UpToDate(m, s)
    IN
    /\ currentTerm' = [currentTerm EXCEPT ![s] = n.ct]
    /\ role' = [role EXCEPT ![s] = n.r]
    /\ votedFor' = [votedFor EXCEPT ![s] = IF grant THEN m.from ELSE n.v]
    /\ Reply(m, [type |-> "voteReply", term |-> n.ct, from |-> s, to |-> m.from, granted |-> grant])
    /\ UNCHANGED <<log, commitIndex, votes, matchIndex>>

HandleVoteReply(m) ==
    LET s == m.to IN
    IF m.term > currentTerm[s] THEN
        /\ currentTerm' = [currentTerm EXCEPT ![s] = m.term]
        /\ role' = [role EXCEPT ![s] = "follower"]
        /\ votedFor' = [votedFor EXCEPT ![s] = Nil]
        /\ Consume(m)
        /\ UNCHANGED <<log, commitIndex, votes, matchIndex>>
    ELSE
        /\ votes' = IF m.term = currentTerm[s] /\ role[s] = "candidate" /\ m.granted
                    THEN [votes EXCEPT ![s] = @ \cup {m.from}] ELSE votes
        /\ Consume(m)
        /\ UNCHANGED <<currentTerm, role, votedFor, log, commitIndex, matchIndex>>

\* The follower's log after accepting `entries` after `prev`: keep what
\* matches, truncate at the first conflict, append the rest.
Merge(l, prev, entries) ==
    LET n == Len(entries)
        conflict(k) == prev + k <= Len(l) /\ l[prev + k].term # entries[k].term
        firstNew == CHOOSE k \in 1..(n + 1) :
                        /\ \A j \in 1..(k - 1) : prev + j <= Len(l) /\ ~conflict(j)
                        /\ (k = n + 1 \/ prev + k > Len(l) \/ conflict(k))
    IN IF firstNew = n + 1 THEN l
       ELSE SubSeq(l, 1, prev + firstNew - 1) \o SubSeq(entries, firstNew, n)

HandleAppend(m) ==
    LET s == m.to
        n == NewTerm(s, m.term)
    IN
    IF m.term < currentTerm[s] THEN
        /\ Reply(m, [type |-> "appendReply", term |-> currentTerm[s], from |-> s, to |-> m.from,
                 success |-> FALSE, match |-> 0])
        /\ UNCHANGED <<currentTerm, role, votedFor, log, commitIndex, votes, matchIndex>>
    ELSE
        LET ok == m.prev <= Len(log[s])
                  /\ (m.prev <= commitIndex[s] \/ m.prev = 0 \/ log[s][m.prev].term = m.prevTerm)
            newLog == Merge(log[s], m.prev, m.entries)
            matched == m.prev + Len(m.entries)
        IN
        /\ currentTerm' = [currentTerm EXCEPT ![s] = n.ct]
        /\ role' = [role EXCEPT ![s] = "follower"]
        /\ votedFor' = [votedFor EXCEPT ![s] = n.v]
        /\ IF ok THEN
              /\ Len(newLog) <= MaxLog
              /\ log' = [log EXCEPT ![s] = newLog]
              /\ commitIndex' = [commitIndex EXCEPT ![s] = Max(@, Min(m.commit, matched))]
              /\ Reply(m, [type |-> "appendReply", term |-> n.ct, from |-> s, to |-> m.from,
                       success |-> TRUE, match |-> matched])
           ELSE
              /\ Reply(m, [type |-> "appendReply", term |-> n.ct, from |-> s, to |-> m.from,
                       success |-> FALSE, match |-> 0])
              /\ UNCHANGED <<log, commitIndex>>
        /\ UNCHANGED <<votes, matchIndex>>

HandleAppendReply(m) ==
    LET s == m.to IN
    IF m.term > currentTerm[s] THEN
        /\ currentTerm' = [currentTerm EXCEPT ![s] = m.term]
        /\ role' = [role EXCEPT ![s] = "follower"]
        /\ votedFor' = [votedFor EXCEPT ![s] = Nil]
        /\ Consume(m)
        /\ UNCHANGED <<log, commitIndex, votes, matchIndex>>
    ELSE
        /\ matchIndex' = IF m.term = currentTerm[s] /\ role[s] = "leader" /\ m.success
                         THEN [matchIndex EXCEPT ![s][m.from] = Max(@, m.match)] ELSE matchIndex
        /\ Consume(m)
        /\ UNCHANGED <<currentTerm, role, votedFor, log, commitIndex, votes>>

Receive(m) ==
    /\ m \in msgs
    /\ \/ m.type = "vote" /\ HandleVote(m)
       \/ m.type = "voteReply" /\ HandleVoteReply(m)
       \/ m.type = "append" /\ HandleAppend(m)
       \/ m.type = "appendReply" /\ HandleAppendReply(m)

Drop(m) == m \in msgs /\ msgs' = msgs \ {m} /\ UNCHANGED <<currentTerm, role, votedFor, log, commitIndex, votes, matchIndex>>

\* Process crash: what is on disk survives.
Restart(s) ==
    /\ WithRestart
    /\ role' = [role EXCEPT ![s] = "follower"]
    /\ commitIndex' = [commitIndex EXCEPT ![s] = 0]
    /\ votes' = [votes EXCEPT ![s] = {}]
    /\ matchIndex' = [matchIndex EXCEPT ![s] = [t \in Servers |-> 0]]
    /\ UNCHANGED <<currentTerm, votedFor, log, msgs>>

Next ==
    \/ \E s \in Servers : Timeout(s) \/ BecomeLeader(s) \/ StepDown(s) \/ AdvanceCommit(s) \/ Restart(s)
    \/ \E s, t \in Servers : RequestVote(s, t)
    \/ \E s \in Servers, v \in Values : ClientRequest(s, v)
    \* The leader sends everything after any prefix (krusty-raft sends up to
    \* its batch limit; a shorter batch is a prefix of this one's effect).
    \/ \E s, t \in Servers : \E prev \in 0..MaxLog : AppendEntries(s, t, prev, Len(log[s]))
    \/ \E m \in msgs : Receive(m) \/ Drop(m)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Properties.                                                             *)

ElectionSafety ==
    \A s, t \in Servers :
        (role[s] = "leader" /\ role[t] = "leader" /\ currentTerm[s] = currentTerm[t]) => s = t

LogMatching ==
    \A s, t \in Servers : \A n \in 1..Min(Len(log[s]), Len(log[t])) :
        log[s][n].term = log[t][n].term => SubSeq(log[s], 1, n) = SubSeq(log[t], 1, n)

\* What any two nodes have committed agrees (the committed log is one log).
StateMachineSafety ==
    \A s, t \in Servers :
        LET c == Min(commitIndex[s], commitIndex[t]) IN
        c <= Len(log[s]) /\ c <= Len(log[t]) /\ SubSeq(log[s], 1, c) = SubSeq(log[t], 1, c)

\* A leader holds every entry committed anywhere in its term or earlier.
LeaderCompleteness ==
    \A s, t \in Servers :
        role[s] = "leader" =>
            \A n \in 1..Min(commitIndex[t], Len(log[t])) :
                log[t][n].term <= currentTerm[s] => (n <= Len(log[s]) /\ log[s][n] = log[t][n])

TypeOK ==
    /\ \A s \in Servers : Len(log[s]) <= MaxLog /\ commitIndex[s] <= Len(log[s])

MsgBound == Cardinality(msgs) <= MaxMsgs

Symmetry == Permutations(Servers)

\* Sanity (expected to be violated): the model reaches leaders and commits.
NoLeader == \A s \in Servers : role[s] # "leader"
NothingCommitted == \A s \in Servers : commitIndex[s] = 0

=============================================================================
