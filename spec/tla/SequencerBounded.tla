-------------------------- MODULE SequencerBounded --------------------------
(***************************************************************************)
(* The sequencer core with BOUNDED dedup state (ADR-12, krusty-core).      *)
(*                                                                         *)
(* Differences from Sequencer.tla (the unbounded reference):              *)
(*  - One session per client: its latest incarnation. Clients pick        *)
(*    strictly increasing incarnations when they restart.                  *)
(*  - At most MaxClients sessions. When the table is full, a new client    *)
(*    either gets BUSY or ANY existing session is evicted (a superset of   *)
(*    the code's "least recently active idle client" policy), and the      *)
(*    incarnation floor rises to the evicted incarnation.                  *)
(*  - A message from a client without a session whose incarnation is at   *)
(*    or below the floor is answered SESSION_EXPIRED; so is one from an    *)
(*    older incarnation than the client's session.                         *)
(*  - Each session answers resends only for its last Ring seqnos; clients  *)
(*    keep at most Ring messages unanswered.                               *)
(*                                                                         *)
(* The invariants are the same as Sequencer.tla's: if they hold here,      *)
(* bounded memory costs no duplicates and no wrong answers.               *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS Clients, Keys, MaxSeq, MaxInc, MaxVer, MaxLog, MaxInFlight,
          MaxClients,   \* session table capacity
          Ring          \* outcomes remembered per session (client window)

Incs == 1..MaxInc
Seqs == 1..MaxSeq
Vers == 0..MaxVer

Msgs == [c : Clients, inc : Incs, seq : Seqs, mode : {"u", "c"}, key : Keys, ver : Vers]

Accepted(g) == [type |-> "acc", g |-> g]
Rejected(cur) == [type |-> "rej", cur |-> cur]
Busy == [type |-> "busy"]
Expired(min) == [type |-> "exp", min |-> min]
None == [type |-> "none"]

VARIABLES log, sess, floor, ver, out, net, fb, inc, sent, msg, got

sm == <<log, sess, floor, ver, out>>
clients == <<inc, sent, msg, got>>
vars == <<log, sess, floor, ver, out, net, fb, inc, sent, msg, got>>

NoSession == [inc |-> 0]
Fresh(i) == [inc |-> i, next |-> 1, res |-> [s \in Seqs |-> None]]
HasSession(S, c) == S[c] # NoSession
Count(S) == Cardinality({c \in Clients : HasSession(S, c)})

Init ==
    /\ log = << >>
    /\ sess = [c \in Clients |-> NoSession]
    /\ floor = 0
    /\ ver = [k \in Keys |-> 0]
    /\ out = << >>
    /\ net = {}
    /\ fb = {}
    /\ inc = [c \in Clients |-> 1]
    /\ sent = [c \in Clients |-> 0]
    /\ msg = [c \in Clients |-> [s \in Seqs |-> None]]
    /\ got = [c \in Clients |-> [s \in Seqs |-> None]]

-----------------------------------------------------------------------------
(* The state machine. `Decide(e, S)` handles e inside its session, given  *)
(* the session table S in which e.c's session is for e.inc.                *)

Decide(e, S) ==
    LET s == S[e.c] IN
    IF e.seq < s.next THEN
        \* Resend: repeat the outcome if it is still within the ring.
        [S |-> S, ver |-> ver, out |-> out,
         reply |-> IF e.seq + Ring > s.next - 1 THEN s.res[e.seq] ELSE None]
    ELSE IF e.seq > s.next THEN
        [S |-> S, ver |-> ver, out |-> out, reply |-> None]   \* NAK
    ELSE IF e.mode = "u" \/ ver[e.key] = e.ver THEN
        LET res == Accepted(Len(out) + 1) IN
        [S |-> [S EXCEPT ![e.c].next = @ + 1, ![e.c].res[e.seq] = res],
         ver |-> IF e.mode = "c" THEN [ver EXCEPT ![e.key] = @ + 1] ELSE ver,
         out |-> Append(out, e),
         reply |-> res]
    ELSE
        LET res == Rejected(ver[e.key]) IN
        [S |-> [S EXCEPT ![e.c].next = @ + 1, ![e.c].res[e.seq] = res],
         ver |-> ver, out |-> out, reply |-> res]

Reply(e, res) ==
    fb' = IF res = None THEN fb
          ELSE fb \cup {[c |-> e.c, inc |-> e.inc, seq |-> e.seq, res |-> res]}

Commit(e, R, newFloor) ==
    /\ sess' = R.S
    /\ ver' = R.ver
    /\ out' = R.out
    /\ floor' = newFloor
    /\ Reply(e, R.reply)

Only(e, res) == Reply(e, res) /\ UNCHANGED <<sess, floor, ver, out>>

Apply(e) ==
    LET c == e.c IN
    IF HasSession(sess, c) THEN
        IF e.inc < sess[c].inc THEN Only(e, Expired(sess[c].inc + 1))
        ELSE IF e.inc = sess[c].inc THEN Commit(e, Decide(e, sess), floor)
        ELSE IF e.seq = 1 THEN
            \* A newer incarnation replaces the session.
            Commit(e, Decide(e, [sess EXCEPT ![c] = Fresh(e.inc)]), floor)
        ELSE Only(e, None)                                   \* NAK
    ELSE IF e.inc <= floor THEN Only(e, Expired(floor + 1))
    ELSE IF e.seq # 1 THEN Only(e, None)                     \* NAK
    ELSE IF Count(sess) < MaxClients THEN
        Commit(e, Decide(e, [sess EXCEPT ![c] = Fresh(e.inc)]), floor)
    ELSE
        \* Table full: BUSY, or evict any session and raise the floor.
        \/ Only(e, Busy)
        \/ \E v \in Clients :
              /\ HasSession(sess, v)
              /\ Commit(e,
                        Decide(e, [sess EXCEPT ![v] = NoSession, ![c] = Fresh(e.inc)]),
                        IF sess[v].inc > floor THEN sess[v].inc ELSE floor)

Sequence(e) ==
    /\ e \in net
    /\ Len(log) < MaxLog
    /\ log' = Append(log, e)
    /\ Apply(e)
    /\ UNCHANGED <<net, clients>>

-----------------------------------------------------------------------------
(* Clients.                                                                *)

DefaultKey == CHOOSE k \in Keys : TRUE

Unanswered(c) == {s \in 1..sent[c] : got[c][s] = None}

\* The window: a new message may be at most Ring - 1 ahead of the oldest
\* unanswered one, so every resend stays inside the session's ring.
InWindow(c) ==
    \A s \in Unanswered(c) : sent[c] + 1 - s < Ring

Send(c, mode, key, v) ==
    /\ sent[c] < MaxSeq
    /\ InWindow(c)
    /\ mode = "u" => (key = DefaultKey /\ v = 0)
    /\ LET m == [c |-> c, inc |-> inc[c], seq |-> sent[c] + 1,
                 mode |-> mode, key |-> key, ver |-> v]
       IN /\ net' = net \cup {m}
          /\ sent' = [sent EXCEPT ![c] = @ + 1]
          /\ msg' = [msg EXCEPT ![c][m.seq] = m]
    /\ UNCHANGED <<sm, fb, inc, got>>

Resend(c, s) ==
    /\ s <= sent[c]
    /\ got[c][s] = None
    /\ msg[c][s] \notin net
    /\ net' = net \cup {msg[c][s]}
    /\ UNCHANGED <<sm, fb, clients>>

\* Only decided outcomes count as answers; BUSY and SESSION_EXPIRED leave
\* the message unanswered (the client retries or restarts).
Receive(f) ==
    /\ f \in fb
    /\ fb' = fb \ {f}
    /\ got' = IF f.inc = inc[f.c] /\ f.res.type \in {"acc", "rej"}
              THEN [got EXCEPT ![f.c][f.seq] = f.res] ELSE got
    /\ UNCHANGED <<sm, net, inc, sent, msg>>

\* Restart with any strictly larger incarnation (covers the reaction to
\* SESSION_EXPIRED{min}).
Restart(c, i) ==
    /\ i > inc[c]
    /\ inc' = [inc EXCEPT ![c] = i]
    /\ sent' = [sent EXCEPT ![c] = 0]
    /\ msg' = [msg EXCEPT ![c] = [s \in Seqs |-> None]]
    /\ got' = [got EXCEPT ![c] = [s \in Seqs |-> None]]
    /\ UNCHANGED <<sm, net, fb>>

DropMsg(m) == m \in net /\ net' = net \ {m} /\ UNCHANGED <<sm, fb, clients>>
DropFeedback(f) == f \in fb /\ fb' = fb \ {f} /\ UNCHANGED <<sm, net, clients>>

Next ==
    \/ \E e \in net : Sequence(e)
    \/ \E c \in Clients, mode \in {"u", "c"}, key \in Keys, v \in Vers : Send(c, mode, key, v)
    \/ \E c \in Clients, s \in Seqs : Resend(c, s)
    \/ \E f \in fb : Receive(f)
    \/ \E c \in Clients, i \in Incs : Restart(c, i)
    \/ \E m \in net : DropMsg(m)
    \/ \E f \in fb : DropFeedback(f)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Properties: the same as Sequencer.tla.                                 *)

Id(e) == <<e.c, e.inc, e.seq>>

Heard ==
    {f \in fb : f.res.type \in {"acc", "rej"}}
    \cup UNION {{[c |-> c, inc |-> inc[c], seq |-> s, res |-> got[c][s]] :
                  s \in {x \in Seqs : got[c][x] # None}} : c \in Clients}

AtMostOnce == \A i, j \in 1..Len(out) : i # j => Id(out[i]) # Id(out[j])

ConditionalAppend ==
    \A i \in 1..Len(out) :
        out[i].mode = "c" =>
            out[i].ver = Cardinality({j \in 1..(i - 1) :
                                        out[j].mode = "c" /\ out[j].key = out[i].key})

SessionFifo ==
    \A i, j \in 1..Len(out) :
        (i < j /\ out[i].c = out[j].c /\ out[i].inc = out[j].inc) => out[i].seq < out[j].seq

OutcomesTrue ==
    \A f \in Heard :
        LET same(e) == Id(e) = <<f.c, f.inc, f.seq>> IN
        /\ f.res.type = "acc" => (f.res.g \in 1..Len(out) /\ same(out[f.res.g]))
        /\ f.res.type = "rej" => \A i \in 1..Len(out) : ~same(out[i])

OutcomesStable ==
    \A f1, f2 \in Heard :
        <<f1.c, f1.inc, f1.seq>> = <<f2.c, f2.inc, f2.seq>> => f1.res = f2.res

\* The table never exceeds its capacity.
Bounded == Count(sess) <= MaxClients

Symmetry == Permutations(Clients)

InFlightBound == Cardinality(net) <= MaxInFlight /\ Cardinality(fb) <= MaxInFlight

=============================================================================
