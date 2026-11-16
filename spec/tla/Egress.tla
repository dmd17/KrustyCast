------------------------------ MODULE Egress ------------------------------
(***************************************************************************)
(* The native stream and its consumers (ARCHITECTURE §8.3, §8.5; gap Q1).  *)
(*                                                                         *)
(* Nodes publish what they have committed and answer RECOVER requests from *)
(* what they retain; consumers run krusty-stream's tracker over a network  *)
(* that loses, duplicates and reorders frames. Checked: every message a    *)
(* consumer delivers is the committed one at its seqno (G1, and G7 for     *)
(* the stream), consumers deliver in seqno order with every gap either     *)
(* filled or explicitly reported lost (G2), and loss is reported only for  *)
(* data some node had aged out.                                            *)
(*                                                                         *)
(* Raft is abstracted to what RaftKC.tla proves (StateMachineSafety,       *)
(* CommittedStable): every node's log agrees with one committed log `C` up *)
(* to its commit index; beyond it a node may hold anything, and may lose   *)
(* or replace it. Several nodes may publish at once (leader handover).     *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    Nodes, Consumers, Values,
    MaxLog,         \* bound on log length (and so on seqnos)
    W,              \* the tracker's reorder window
    MaxFrame,       \* most messages in one DATA frame
    \* The consumer's gap policy: TRUE for GapPolicy::GiveUpAfter (report a
    \* gap lost after unanswered RECOVERs), FALSE for GapPolicy::Wait.
    GiveUp,
    \* Deliberate bugs, for mutation checks (all FALSE in the real model):
    PubUncommitted, \* nodes publish entries they have not committed
    SilentSkip,     \* skipping a gap does not report it lost
    UnavailToHw     \* UNAVAILABLE skips to the high-water mark, not to earliest

LOST == "lost"

VARIABLES
    C,          \* the committed log (one stream message per entry)
    log,        \* each node's log
    commit,     \* each node's commit index
    ret,        \* earliest seqno each node retains
    net,        \* frames in flight to each consumer (a set: resent at will)
    reqs,       \* RECOVER requests in flight
    \* krusty-stream tracker state:
    next, hw, buf,
    \* ghost: what each consumer delivered (in order) and reported lost
    delivered, lost

vars == <<C, log, commit, ret, net, reqs, next, hw, buf, delivered, lost>>

Seqnos == 1..MaxLog
Min(a, b) == IF a < b THEN a ELSE b
Max(a, b) == IF a > b THEN a ELSE b
Range(a, b) == a..b

DataFrame(s, vs) == [type |-> "data", seqno |-> s, vals |-> vs]
HbFrame(s) == [type |-> "hb", seqno |-> s]
UnavailFrame(s, e) == [type |-> "unavail", seqno |-> s, earliest |-> e]

Frames ==
    {DataFrame(s, vs) : s \in Seqnos,
        vs \in UNION {[1..k -> Values] : k \in 1..MaxFrame}}
    \cup {HbFrame(s) : s \in 1..(MaxLog + 1)}
    \cup {UnavailFrame(s, e) : s \in Seqnos, e \in 1..(MaxLog + 1)}

TypeOK ==
    /\ C \in Seq(Values) /\ Len(C) <= MaxLog
    /\ \A n \in Nodes : log[n] \in Seq(Values) /\ Len(log[n]) <= MaxLog
    /\ commit \in [Nodes -> 0..MaxLog]
    /\ ret \in [Nodes -> 1..(MaxLog + 1)]
    /\ net \in [Consumers -> SUBSET Frames]
    /\ next \in [Consumers -> 1..(MaxLog + 1)]
    /\ hw \in [Consumers -> 1..(MaxLog + 1)]

Init ==
    /\ C = <<>>
    /\ log = [n \in Nodes |-> <<>>]
    /\ commit = [n \in Nodes |-> 0]
    /\ ret = [n \in Nodes |-> 1]
    /\ net = [c \in Consumers |-> {}]
    /\ reqs = {}
    /\ next = [c \in Consumers |-> 1]
    /\ hw = [c \in Consumers |-> 1]
    /\ buf = [c \in Consumers |-> {}]
    /\ delivered = [c \in Consumers |-> <<>>]
    /\ lost = [c \in Consumers |-> {}]

(***************************************************************************)
(* Nodes                                                                   *)
(***************************************************************************)

\* A leader (or a node that thinks it is one) appends an entry.
AppendEntry(n, v) ==
    /\ Len(log[n]) < MaxLog
    /\ log' = [log EXCEPT ![n] = Append(@, v)]
    /\ UNCHANGED <<C, commit, ret, net, reqs, next, hw, buf, delivered, lost>>

\* An uncommitted entry is overwritten or lost (conflict, crash).
Truncate(n) ==
    /\ Len(log[n]) > commit[n]
    /\ log' = [log EXCEPT ![n] = SubSeq(@, 1, Len(@) - 1)]
    /\ UNCHANGED <<C, commit, ret, net, reqs, next, hw, buf, delivered, lost>>

\* A node learns its next entry is committed. The first such entry fixes
\* the committed log; later ones must agree with it (Raft's safety).
Commit(n) ==
    /\ commit[n] < Len(log[n])
    /\ commit[n] < Len(C) => log[n][commit[n] + 1] = C[commit[n] + 1]
    /\ commit' = [commit EXCEPT ![n] = @ + 1]
    /\ C' = IF commit[n] = Len(C) THEN Append(C, log[n][commit[n] + 1]) ELSE C
    /\ UNCHANGED <<log, ret, net, reqs, next, hw, buf, delivered, lost>>

\* Retention: committed data ages out (below the snapshot, past retain_gib).
Age(n) ==
    /\ ret[n] <= commit[n]
    /\ ret' = [ret EXCEPT ![n] = @ + 1]
    /\ UNCHANGED <<C, log, commit, net, reqs, next, hw, buf, delivered, lost>>

\* How far a node may publish and serve.
Limit(n) == IF PubUncommitted THEN Len(log[n]) ELSE commit[n]

ToAll(f) == [c \in Consumers |-> net[c] \cup {f}]

\* Multicast DATA (live or retransmitted) and heartbeats.
Publish(n, s, k) ==
    /\ s + k - 1 <= Limit(n)
    /\ net' = ToAll(DataFrame(s, [i \in 1..k |-> log[n][s + i - 1]]))
    /\ UNCHANGED <<C, log, commit, ret, reqs, next, hw, buf, delivered, lost>>

Heartbeat(n) ==
    /\ net' = ToAll(HbFrame(Limit(n) + 1))
    /\ UNCHANGED <<C, log, commit, ret, reqs, next, hw, buf, delivered, lost>>

\* A RECOVER answer (stages.rs, recovery): UNAVAILABLE if the start has aged
\* out, then DATA from the earliest retained seqno; to the requester, or on
\* the feed when several consumers asked (suppression).
Answer(n, r, k, toFeed) ==
    /\ r \in reqs
    /\ LET from == Max(r.from, ret[n])
           last == Min(r.from + r.count - 1, Limit(n))
           ua == IF r.from < ret[n] THEN {UnavailFrame(r.from, ret[n])} ELSE {}
           da == IF from <= last /\ k <= last - from + 1
                 THEN {DataFrame(from, [i \in 1..k |-> log[n][from + i - 1]])}
                 ELSE {}
           out == ua \cup da
       IN /\ out # {}
          /\ net' = IF toFeed THEN [c \in Consumers |-> net[c] \cup out]
                    ELSE [net EXCEPT ![r.c] = @ \cup out]
    /\ reqs' = reqs \ {r}
    /\ UNCHANGED <<C, log, commit, ret, next, hw, buf, delivered, lost>>

(***************************************************************************)
(* The tracker (krusty-stream). State as a record for the recursive steps. *)
(***************************************************************************)

St(c) == [next |-> next[c], hw |-> hw[c], buf |-> buf[c],
          delivered |-> delivered[c], lost |-> lost[c]]

SlotOf(s) == s % W

\* `take`: the buffered message for seqno s, if its slot holds it.
Taken(st, s) == {e \in st.buf : SlotOf(e.s) = SlotOf(s) /\ e.s = s}

Deliver(st, s, v) ==
    [st EXCEPT !.delivered = Append(@, <<s, v>>), !.next = s + 1]

RECURSIVE Drain(_)
Drain(st) ==
    IF Taken(st, st.next) = {} THEN st
    ELSE LET e == CHOOSE x \in Taken(st, st.next) : TRUE
         IN Drain(Deliver([st EXCEPT !.buf = @ \ {e}], st.next, e.v))

OnMessage(st, s, v) ==
    IF s = st.next THEN Drain(Deliver(st, s, v))
    ELSE IF s > st.next /\ s - st.next < W
            /\ ~\E e \in st.buf : SlotOf(e.s) = SlotOf(s)
         THEN [st EXCEPT !.buf = @ \cup {[s |-> s, v |-> v]}]
         ELSE st

RECURSIVE OnData(_, _, _, _)
OnData(st, s, vs, i) ==
    IF i > Len(vs) THEN st ELSE OnData(OnMessage(st, s + i - 1, vs[i]), s, vs, i + 1)

GapEnd(st) ==
    LET later == {e.s : e \in {x \in st.buf : x.s > st.next}}
    IN IF later = {} THEN st.hw
       ELSE Min(st.hw, CHOOSE m \in later : \A x \in later : m <= x)

\* `skip_gap`: report next..resume-1 lost, then deliver what follows.
SkipGap(st, limit) ==
    LET resume == Min(GapEnd(st), limit)
    IN IF resume <= st.next THEN st
       ELSE LET st1 == [st EXCEPT !.lost = IF SilentSkip THEN @ ELSE @ \cup Range(st.next, resume - 1),
                                  !.next = resume]
            IN IF Taken(st1, resume) = {} THEN st1
               ELSE LET e == CHOOSE x \in Taken(st1, resume) : TRUE
                    IN Drain(Deliver([st1 EXCEPT !.buf = @ \ {e}], resume, e.v))

RECURSIVE SkipUntil(_, _)
SkipUntil(st, target) ==
    IF st.next >= target THEN st ELSE SkipUntil(SkipGap(st, target), target)

OnFrame(st, f) ==
    CASE f.type = "data" ->
            OnData([st EXCEPT !.hw = Max(@, f.seqno + Len(f.vals))], f.seqno, f.vals, 1)
      [] f.type = "hb" -> [st EXCEPT !.hw = Max(@, f.seqno)]
      [] f.type = "unavail" ->
            LET st1 == [st EXCEPT !.hw = Max(@, f.earliest)]
            IN SkipUntil(st1, IF UnavailToHw THEN st1.hw ELSE f.earliest)

SetSt(c, st) ==
    /\ next' = [next EXCEPT ![c] = st.next]
    /\ hw' = [hw EXCEPT ![c] = st.hw]
    /\ buf' = [buf EXCEPT ![c] = st.buf]
    /\ delivered' = [delivered EXCEPT ![c] = st.delivered]
    /\ lost' = [lost EXCEPT ![c] = st.lost]

Receive(c, f) ==
    /\ f \in net[c]
    /\ SetSt(c, OnFrame(St(c), f))
    /\ UNCHANGED <<C, log, commit, ret, net, reqs>>

\* The network loses a frame (it may also deliver one any number of times).
Drop(c, f) ==
    /\ f \in net[c]
    /\ net' = [net EXCEPT ![c] = @ \ {f}]
    /\ UNCHANGED <<C, log, commit, ret, reqs, next, hw, buf, delivered, lost>>

\* `poll_recover`: ask any node for the gap at `next`.
Recover(c) ==
    /\ next[c] < hw[c]
    /\ reqs' = reqs \cup {[c |-> c, from |-> next[c], count |-> GapEnd(St(c)) - next[c]]}
    /\ UNCHANGED <<C, log, commit, ret, net, next, hw, buf, delivered, lost>>

\* `poll_recover` under GapPolicy::GiveUpAfter, after unanswered requests.
Abandon(c) ==
    /\ GiveUp
    /\ next[c] < hw[c]
    /\ SetSt(c, SkipGap(St(c), hw[c]))
    /\ UNCHANGED <<C, log, commit, ret, net, reqs>>

Next ==
    \/ \E n \in Nodes, v \in Values : AppendEntry(n, v)
    \/ \E n \in Nodes : Truncate(n) \/ Commit(n) \/ Age(n) \/ Heartbeat(n)
    \/ \E n \in Nodes, s \in Seqnos, k \in 1..MaxFrame : Publish(n, s, k)
    \/ \E n \in Nodes, r \in reqs, k \in 1..MaxFrame, t \in BOOLEAN : Answer(n, r, k, t)
    \/ \E c \in Consumers : \E f \in net[c] : Receive(c, f) \/ Drop(c, f)
    \/ \E c \in Consumers : Recover(c) \/ Abandon(c)

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Properties                                                              *)
(***************************************************************************)

\* G1 (with G7): what a consumer delivers at seqno s is the committed
\* message at s, so all consumers agree.
Agreement ==
    \A c \in Consumers : \A i \in 1..Len(delivered[c]) :
        LET d == delivered[c][i] IN d[1] <= Len(C) /\ d[2] = C[d[1]]

\* G2: deliveries are in seqno order, and every seqno below `next` was
\* delivered or explicitly reported lost, never both.
PrefixDelivery ==
    \A c \in Consumers :
        LET ds == {delivered[c][i][1] : i \in 1..Len(delivered[c])}
        IN /\ \A i \in 1..(Len(delivered[c]) - 1) : delivered[c][i][1] < delivered[c][i + 1][1]
           /\ ds \cup lost[c] = Range(1, next[c] - 1)
           /\ ds \cap lost[c] = {}

\* G2: loss is reported only for data a node no longer retains (under
\* GapPolicy::Wait; GiveUpAfter trades this for progress).
LossOnlyAgedOut ==
    \A c \in Consumers : \A s \in lost[c] : \E n \in Nodes : s < ret[n]

\* The abstraction of Raft holds (RaftKC proves it for the real protocol).
CommittedAgree ==
    \A n \in Nodes : commit[n] <= Len(C) /\ \A i \in 1..commit[n] : log[n][i] = C[i]

=============================================================================
