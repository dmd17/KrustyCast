------------------------------ MODULE RaftKCInd ------------------------------
(***************************************************************************)
(* Towards an inductive invariant for RaftKC (spec/README.md, step 3 of    *)
(* the verification plan): one that holds in every state satisfying it,    *)
(* not only in the states a bounded search reaches, and implies RaftKC's   *)
(* safety properties.                                                      *)
(*                                                                         *)
(* RaftKC forgets what an inductive argument needs, so two history         *)
(* variables are kept alongside it, updated by observing each step         *)
(* (RaftKC itself is unchanged, as is its trace validation):               *)
(*  - votedIn[t][T]: whom t voted for in term T (votedFor is reset when a  *)
(*    server moves to a higher term).                                      *)
(*  - hist[T]: the log of term T's leader, as long as it led (a leader's   *)
(*    log only grows in its term, LeaderAppendOnly). An entry of term T    *)
(*    anywhere, and everything before it, is what that leader had.         *)
(***************************************************************************)
EXTENDS RaftKC

CONSTANT MaxSteps
VARIABLES votedIn, hist,
          steps    \* only for the depth-bounded check (MCIndSpec)

Terms == 1..MaxTerm

indVars == <<vars, votedIn, hist>>

IndInit ==
    /\ Init
    /\ votedIn = [t \in Servers |-> [T \in Terms |-> Nil]]
    /\ hist = [T \in Terms |-> << >>]
    /\ steps = 0

\* The history, from each step's new state.
Observe ==
    /\ votedIn' = [t \in Servers |-> [T \in Terms |->
          IF votedIn[t][T] = Nil /\ T = currentTerm'[t] /\ votedFor'[t] # Nil
          THEN votedFor'[t] ELSE votedIn[t][T]]]
    /\ hist' = [T \in Terms |->
          IF \E s \in Servers : role'[s] = "leader" /\ currentTerm'[s] = T
          THEN log'[CHOOSE s \in Servers : role'[s] = "leader" /\ currentTerm'[s] = T]
          ELSE hist[T]]

IndNext == Next /\ Observe /\ UNCHANGED steps
IndSpec == IndInit /\ [][IndNext]_<<indVars, steps>>

\* Every behaviour of up to MaxSteps steps (CI).
MCIndNext == Next /\ Observe /\ steps' = steps + 1
MCIndSpec == IndInit /\ [][MCIndNext]_<<indVars, steps>>
DepthBound == steps <= MaxSteps

-----------------------------------------------------------------------------
(* Elections.                                                              *)

\* What a server's vote state says is what it voted.
VotedForInv ==
    \A t \in Servers :
        /\ votedFor[t] # Nil => currentTerm[t] \in Terms /\ votedIn[t][currentTerm[t]] = votedFor[t]
        /\ (currentTerm[t] \in Terms /\ votedIn[t][currentTerm[t]] # Nil)
              => votedFor[t] = votedIn[t][currentTerm[t]]

\* Nobody has voted in a term it has not reached.
VotedInPast ==
    \A t \in Servers : \A T \in Terms : votedIn[t][T] # Nil => T <= currentTerm[t]

\* A candidate's or leader's votes, and granted replies in flight, are votes
\* cast for it in its term.
VotesInv ==
    \A s \in Servers : role[s] \in {"candidate", "leader"} =>
        /\ currentTerm[s] \in Terms
        /\ \A t \in votes[s] : votedIn[t][currentTerm[s]] = s

VoteReplyInv ==
    \A m \in msgs : (m.type = "voteReply" /\ m.granted) =>
        m.term \in Terms /\ votedIn[m.from][m.term] = m.to

LeaderQuorum == \A s \in Servers : role[s] = "leader" => Quorum(votes[s])

ElectionInv == VotedForInv /\ VotedInPast /\ VotesInv /\ VoteReplyInv /\ LeaderQuorum

\* ElectionSafety follows: two leaders of a term have quorums of votes cast
\* for each in it, and the quorums share a voter.
ElectionInvImpliesSafety == ElectionInv => ElectionSafety
-----------------------------------------------------------------------------
(* Logs.                                                                   *)

\* A leader's log is its term's history.
HistLeader ==
    \A s \in Servers : role[s] = "leader" => hist[currentTerm[s]] = log[s]

\* Entry terms are real terms, at most the holder's (or history's) term.
EntryTerms ==
    /\ \A s \in Servers : \A i \in 1..Len(log[s]) :
          log[s][i].term \in Terms /\ log[s][i].term <= currentTerm[s]
    /\ \A T \in Terms : \A i \in 1..Len(hist[T]) :
          hist[T][i].term \in Terms /\ hist[T][i].term <= T

\* An entry of term T, and everything before it, is what T's leader had.
InHist(l, i) ==
    LET T == l[i].term IN i <= Len(hist[T]) /\ SubSeq(l, 1, i) = SubSeq(hist[T], 1, i)

HistInHist == \A T \in Terms : \A i \in 1..Len(hist[T]) : InHist(hist[T], i)
LogInHist == \A s \in Servers : \A i \in 1..Len(log[s]) : InHist(log[s], i)

\* An append message carries a piece of its term's history.
MsgInHist ==
    \A m \in msgs : m.type = "append" =>
        /\ m.term \in Terms
        /\ m.prev + Len(m.entries) <= Len(hist[m.term])
        /\ m.entries = SubSeq(hist[m.term], m.prev + 1, m.prev + Len(m.entries))
        /\ m.prev > 0 => hist[m.term][m.prev].term = m.prevTerm

LogInv == HistLeader /\ EntryTerms /\ HistInHist /\ LogInHist /\ MsgInHist

\* LogMatching follows: two entries of one term at one index both carry the
\* prefix of that term's history.
LogInvImpliesMatching == LogInv => LogMatching

-----------------------------------------------------------------------------
(* Commits.                                                                *)

HadLeader(T) == hist[T] # << >>

\* Every term at or above an entry's commit term that had a leader has it.
HistComplete ==
    \A i \in 1..Len(committed) : \A T \in Terms :
        (committed[i].term <= T /\ HadLeader(T)) =>
            i <= Len(hist[T]) /\ hist[T][i] = committed[i].entry

\* What a server knows is committed is in the history of every term from its
\* own on (what lets a follower take entries at or below its commit index as
\* matching).
KnownCommitInHist ==
    \A s \in Servers : \A i \in 1..commitIndex[s] : \A T \in Terms :
        (currentTerm[s] <= T /\ HadLeader(T)) =>
            i <= Len(hist[T]) /\ hist[T][i] = log[s][i]

\* An append's commit index covers only committed entries of its history.
MsgCommit ==
    \A m \in msgs : m.type = "append" =>
        /\ m.commit <= m.prev + Len(m.entries)
        /\ m.commit <= Len(committed)
        /\ \A i \in 1..m.commit : hist[m.term][i] = committed[i].entry

CommitInv == HistComplete /\ KnownCommitInHist /\ MsgCommit
=============================================================================
