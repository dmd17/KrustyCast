----------------------------- MODULE Sequencer -----------------------------
(***************************************************************************)
(* The sequencer core and client sessions (ARCHITECTURE §8.1, ADR-4).      *)
(*                                                                         *)
(* The replicated log is abstracted as one atomic log: appending an entry  *)
(* commits it, and the state machine applies it at once. Raft refines this *)
(* later (§11.1); nothing here depends on which node is the leader.        *)
(*                                                                         *)
(* The log holds raw ingress entries, duplicates included. A deterministic *)
(* state machine turns each entry into an outcome: rejected entries take a *)
(* log index but no global seqno (§8.2), and only accepted entries reach   *)
(* the egress stream `out`, whose index is the global seqno.               *)
(*                                                                         *)
(* The network between clients and sequencer loses, duplicates and        *)
(* reorders in both directions (`net` and `fb` are sets; entries stay      *)
(* until dropped or received). Clients pipeline, resend anything           *)
(* unanswered, and restart with a fresh incarnation while old messages are *)
(* still in flight.                                                        *)
(*                                                                         *)
(* Not modelled yet: NAK (it only prompts a resend, which clients may do   *)
(* at any time here), BUSY, NOT_LEADER, the bounded outcome ring and       *)
(* SESSION_EXPIRED, consumers (egress delivery is covered by the stream    *)
(* tracker proof), liveness.                                               *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Clients,   \* client ids
    Keys,      \* keys for conditional appends
    MaxSeq,    \* messages per incarnation
    MaxInc,    \* incarnations per client
    MaxVer,    \* largest expected_version a client may claim
    MaxLog     \* bound on the log (state-space limit)

Incs == 1..MaxInc
Seqs == 1..MaxSeq
Vers == 0..MaxVer

\* An ingress message. `mode` is "u" (unconditional) or "c" (conditional:
\* accepted only if `key` is at version `ver`, which it then advances).
Msgs == [c : Clients, inc : Incs, seq : Seqs, mode : {"u", "c"}, key : Keys, ver : Vers]

Accepted(g) == [type |-> "acc", g |-> g]
Rejected(cur) == [type |-> "rej", cur |-> cur]
Outcomes == {Accepted(g) : g \in 1..MaxLog} \cup {Rejected(v) : v \in 0..MaxLog}

None == [type |-> "none"]

\* Feedback to a client: the outcome of its message (c, inc, seq).
Feedback == [c : Clients, inc : Incs, seq : Seqs, res : Outcomes]

VARIABLES
    log,    \* every entry the sequencer appended, in order (the replicated log)
    sess,   \* dedup state per (client, incarnation): next expected seq, outcomes
    ver,    \* current version of each key
    out,    \* egress stream: accepted entries; index = global seqno
    net,    \* messages in flight to the sequencer
    fb,     \* feedback in flight to clients
    inc,    \* each client's current incarnation
    sent,   \* highest seq sent by the current incarnation
    msg,    \* what the current incarnation sent, by seq (for resends)
    got     \* outcomes the current incarnation has received, by seq

sm == <<log, sess, ver, out>>
clients == <<inc, sent, msg, got>>
vars == <<log, sess, ver, out, net, fb, inc, sent, msg, got>>

NoSession == [next |-> 1, res |-> [s \in Seqs |-> None]]

Init ==
    /\ log = << >>
    /\ sess = [k \in Clients \X Incs |-> NoSession]
    /\ ver = [k \in Keys |-> 0]
    /\ out = << >>
    /\ net = {}
    /\ fb = {}
    /\ inc = [c \in Clients |-> 1]
    /\ sent = [c \in Clients |-> 0]
    /\ msg = [c \in Clients |-> [s \in Seqs |-> None]]
    /\ got = [c \in Clients |-> [s \in Seqs |-> None]]

-----------------------------------------------------------------------------
(* The state machine: apply one log entry.                                 *)

Reply(e, res) == fb' = fb \cup {[c |-> e.c, inc |-> e.inc, seq |-> e.seq, res |-> res]}

Apply(e) ==
    LET k == <<e.c, e.inc>>
        s == sess[k]
    IN
    IF e.seq < s.next THEN
        \* Already decided: repeat the stored outcome, change nothing.
        /\ Reply(e, s.res[e.seq])
        /\ UNCHANGED <<sess, ver, out>>
    ELSE IF e.seq > s.next THEN
        \* A gap in this session (the real system NAKs): no state change.
        /\ UNCHANGED <<sess, ver, out, fb>>
    ELSE IF e.mode = "u" \/ ver[e.key] = e.ver THEN
        LET res == Accepted(Len(out) + 1) IN
        /\ out' = Append(out, e)
        /\ ver' = IF e.mode = "c" THEN [ver EXCEPT ![e.key] = @ + 1] ELSE ver
        /\ sess' = [sess EXCEPT ![k].next = @ + 1, ![k].res[e.seq] = res]
        /\ Reply(e, res)
    ELSE
        LET res == Rejected(ver[e.key]) IN
        /\ sess' = [sess EXCEPT ![k].next = @ + 1, ![k].res[e.seq] = res]
        /\ Reply(e, res)
        /\ UNCHANGED <<ver, out>>

\* The sequencer takes any message in flight (possibly again) and commits it.
Sequence(e) ==
    /\ e \in net
    /\ Len(log) < MaxLog
    /\ log' = Append(log, e)
    /\ Apply(e)
    /\ UNCHANGED <<net, clients>>

-----------------------------------------------------------------------------
(* Clients.                                                                *)

DefaultKey == CHOOSE k \in Keys : TRUE

Send(c, mode, key, v) ==
    /\ sent[c] < MaxSeq
    /\ mode = "u" => (key = DefaultKey /\ v = 0)   \* fields unused when unconditional
    /\ LET m == [c |-> c, inc |-> inc[c], seq |-> sent[c] + 1,
                 mode |-> mode, key |-> key, ver |-> v]
       IN /\ net' = net \cup {m}
          /\ sent' = [sent EXCEPT ![c] = @ + 1]
          /\ msg' = [msg EXCEPT ![c][m.seq] = m]
    /\ UNCHANGED <<sm, fb, inc, got>>

\* Resend an unanswered message that is no longer in flight.
Resend(c, s) ==
    /\ s <= sent[c]
    /\ got[c][s] = None
    /\ msg[c][s] \notin net
    /\ net' = net \cup {msg[c][s]}
    /\ UNCHANGED <<sm, fb, clients>>

Receive(f) ==
    /\ f \in fb
    /\ fb' = fb \ {f}
    /\ got' = IF f.inc = inc[f.c] THEN [got EXCEPT ![f.c][f.seq] = f.res] ELSE got
    /\ UNCHANGED <<sm, net, inc, sent, msg>>

\* Crash and restart: a new incarnation, nothing remembered. Old messages
\* and feedback stay in flight.
Restart(c) ==
    /\ inc[c] < MaxInc
    /\ inc' = [inc EXCEPT ![c] = @ + 1]
    /\ sent' = [sent EXCEPT ![c] = 0]
    /\ msg' = [msg EXCEPT ![c] = [s \in Seqs |-> None]]
    /\ got' = [got EXCEPT ![c] = [s \in Seqs |-> None]]
    /\ UNCHANGED <<sm, net, fb>>

-----------------------------------------------------------------------------
(* The network.                                                            *)

DropMsg(m) ==
    /\ m \in net
    /\ net' = net \ {m}
    /\ UNCHANGED <<sm, fb, clients>>

DropFeedback(f) ==
    /\ f \in fb
    /\ fb' = fb \ {f}
    /\ UNCHANGED <<sm, net, clients>>

Next ==
    \/ \E e \in net : Sequence(e)
    \/ \E c \in Clients, mode \in {"u", "c"}, key \in Keys, v \in Vers : Send(c, mode, key, v)
    \/ \E c \in Clients, s \in Seqs : Resend(c, s)
    \/ \E f \in fb : Receive(f)
    \/ \E c \in Clients : Restart(c)
    \/ \E m \in net : DropMsg(m)
    \/ \E f \in fb : DropFeedback(f)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Properties (PRD §5).                                                    *)

Id(e) == <<e.c, e.inc, e.seq>>

\* Every outcome a client can hear: feedback in flight, and what each
\* client recorded. Invariants hold in every state, so each feedback is
\* checked while it is in flight; no history variable is needed.
Heard ==
    fb \cup UNION {{[c |-> c, inc |-> inc[c], seq |-> s, res |-> got[c][s]] :
                      s \in {x \in Seqs : got[c][x] # None}} : c \in Clients}

TypeOK ==
    /\ log \in Seq(Msgs)
    /\ out \in Seq(Msgs)
    /\ ver \in [Keys -> Nat]
    /\ net \subseteq Msgs
    /\ fb \subseteq Feedback
    /\ inc \in [Clients -> Incs]
    /\ sent \in [Clients -> 0..MaxSeq]

\* G4: at most one global seqno per (client, incarnation, seq).
AtMostOnce ==
    \A i, j \in 1..Len(out) : i # j => Id(out[i]) # Id(out[j])

\* G5: per key, accepted conditional appends claim versions 0, 1, 2, ...
\* in egress order, so at most one wins each version and they are contiguous.
ConditionalAppend ==
    \A i \in 1..Len(out) :
        out[i].mode = "c" =>
            out[i].ver = Cardinality({j \in 1..(i - 1) :
                                        out[j].mode = "c" /\ out[j].key = out[i].key})

\* G6: one session's messages are sequenced in seq order.
SessionFifo ==
    \A i, j \in 1..Len(out) :
        (i < j /\ out[i].c = out[j].c /\ out[i].inc = out[j].inc) => out[i].seq < out[j].seq

\* Every outcome a client hears is true: ACCEPTED(g) means the egress
\* stream holds exactly that message at g, and REJECTED means it never
\* reaches the egress stream.
OutcomesTrue ==
    \A f \in Heard :
        LET same(e) == Id(e) = <<f.c, f.inc, f.seq>> IN
        /\ f.res.type = "acc" =>
              /\ f.res.g \in 1..Len(out)
              /\ same(out[f.res.g])
        /\ f.res.type = "rej" =>
              \A i \in 1..Len(out) : ~same(out[i])

\* Outcomes are idempotent: a resend never gets a different answer.
OutcomesStable ==
    \A f1, f2 \in Heard :
        <<f1.c, f1.inc, f1.seq>> = <<f2.c, f2.inc, f2.seq>> => f1.res = f2.res

\* Only messages a client really sent are sequenced (no invention).
NoInvention ==
    \A i \in 1..Len(out) : out[i] \in {log[j] : j \in 1..Len(log)}

\* G7, in the abstract: the egress stream is a function of the log. It
\* holds by construction here (Apply is deterministic); the refinement
\* proof of the Rust core is what makes it a statement about the code.

\* The key versions agree with the egress stream.
VersionsMatch ==
    \A k \in Keys :
        ver[k] = Cardinality({i \in 1..Len(out) : out[i].mode = "c" /\ out[i].key = k})

Safety ==
    /\ TypeOK
    /\ AtMostOnce
    /\ ConditionalAppend
    /\ SessionFifo
    /\ OutcomesTrue
    /\ OutcomesStable
    /\ NoInvention
    /\ VersionsMatch

Symmetry == Permutations(Clients)

\* State constraint for bounded runs: at most MaxInFlight messages and
\* feedback in flight at once (the network still loses, duplicates and
\* reorders within that bound).
CONSTANT MaxInFlight
InFlightBound == Cardinality(net) <= MaxInFlight /\ Cardinality(fb) <= MaxInFlight

=============================================================================
