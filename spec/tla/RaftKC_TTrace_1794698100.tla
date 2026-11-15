---- MODULE RaftKC_TTrace_1794698100 ----
EXTENDS RaftKC, Sequences, TLCExt, RaftKC_TEConstants, Toolbox, Naturals, TLC

_expression ==
    LET RaftKC_TEExpression == INSTANCE RaftKC_TEExpression
    IN RaftKC_TEExpression!expression
----

_trace ==
    LET RaftKC_TETrace == INSTANCE RaftKC_TETrace
    IN RaftKC_TETrace!trace
----

_inv ==
    ~(
        TLCGet("level") = Len(_TETrace)
        /\
        msgs = ({[term |-> 1, type |-> "voteReply", from |-> s2, to |-> s1, granted |-> TRUE], [term |-> 1, type |-> "vote", from |-> s1, to |-> s2, lastTerm |-> 0, lastIndex |-> 0]})
        /\
        votedFor = ((s1 :> s1 @@ s2 :> s1 @@ s3 :> "nil"))
        /\
        currentTerm = ((s1 :> 1 @@ s2 :> 1 @@ s3 :> 0))
        /\
        role = ((s1 :> "leader" @@ s2 :> "follower" @@ s3 :> "follower"))
        /\
        log = ((s1 :> <<[term |-> 1, value |-> "noop"]>> @@ s2 :> <<>> @@ s3 :> <<>>))
        /\
        matchIndex = ((s1 :> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0) @@ s2 :> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0) @@ s3 :> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0)))
        /\
        votes = ((s1 :> {s1, s2} @@ s2 :> {} @@ s3 :> {}))
        /\
        commitIndex = ((s1 :> 0 @@ s2 :> 0 @@ s3 :> 0))
    )
----

_init ==
    /\ matchIndex = _TETrace[1].matchIndex
    /\ log = _TETrace[1].log
    /\ msgs = _TETrace[1].msgs
    /\ commitIndex = _TETrace[1].commitIndex
    /\ currentTerm = _TETrace[1].currentTerm
    /\ votes = _TETrace[1].votes
    /\ role = _TETrace[1].role
    /\ votedFor = _TETrace[1].votedFor
----

_next ==
    /\ \E i,j \in DOMAIN _TETrace:
        /\ \/ /\ j = i + 1
              /\ i = TLCGet("level")
        /\ matchIndex  = _TETrace[i].matchIndex
        /\ matchIndex' = _TETrace[j].matchIndex
        /\ log  = _TETrace[i].log
        /\ log' = _TETrace[j].log
        /\ msgs  = _TETrace[i].msgs
        /\ msgs' = _TETrace[j].msgs
        /\ commitIndex  = _TETrace[i].commitIndex
        /\ commitIndex' = _TETrace[j].commitIndex
        /\ currentTerm  = _TETrace[i].currentTerm
        /\ currentTerm' = _TETrace[j].currentTerm
        /\ votes  = _TETrace[i].votes
        /\ votes' = _TETrace[j].votes
        /\ role  = _TETrace[i].role
        /\ role' = _TETrace[j].role
        /\ votedFor  = _TETrace[i].votedFor
        /\ votedFor' = _TETrace[j].votedFor

\* Uncomment the ASSUME below to write the states of the error trace
\* to the given file in Json format. Note that you can pass any tuple
\* to `JsonSerialize`. For example, a sub-sequence of _TETrace.
    \* ASSUME
    \*     LET J == INSTANCE Json
    \*         IN J!JsonSerialize("RaftKC_TTrace_1794698100.json", _TETrace)

=============================================================================

 Note that you can extract this module `RaftKC_TEExpression`
  to a dedicated file to reuse `expression` (the module in the 
  dedicated `RaftKC_TEExpression.tla` file takes precedence 
  over the module `RaftKC_TEExpression` below).

---- MODULE RaftKC_TEExpression ----
EXTENDS RaftKC, Sequences, TLCExt, RaftKC_TEConstants, Toolbox, Naturals, TLC

expression == 
    [
        \* To hide variables of the `RaftKC` spec from the error trace,
        \* remove the variables below.  The trace will be written in the order
        \* of the fields of this record.
        matchIndex |-> matchIndex
        ,log |-> log
        ,msgs |-> msgs
        ,commitIndex |-> commitIndex
        ,currentTerm |-> currentTerm
        ,votes |-> votes
        ,role |-> role
        ,votedFor |-> votedFor
        
        \* Put additional constant-, state-, and action-level expressions here:
        \* ,_stateNumber |-> _TEPosition
        \* ,_matchIndexUnchanged |-> matchIndex = matchIndex'
        
        \* Format the `matchIndex` variable as Json value.
        \* ,_matchIndexJson |->
        \*     LET J == INSTANCE Json
        \*     IN J!ToJson(matchIndex)
        
        \* Lastly, you may build expressions over arbitrary sets of states by
        \* leveraging the _TETrace operator.  For example, this is how to
        \* count the number of times a spec variable changed up to the current
        \* state in the trace.
        \* ,_matchIndexModCount |->
        \*     LET F[s \in DOMAIN _TETrace] ==
        \*         IF s = 1 THEN 0
        \*         ELSE IF _TETrace[s].matchIndex # _TETrace[s-1].matchIndex
        \*             THEN 1 + F[s-1] ELSE F[s-1]
        \*     IN F[_TEPosition - 1]
    ]

=============================================================================



Parsing and semantic processing can take forever if the trace below is long.
 In this case, it is advised to uncomment the module below to deserialize the
 trace from a generated binary file.

\*
\*---- MODULE RaftKC_TETrace ----
\*EXTENDS RaftKC, IOUtils, RaftKC_TEConstants, TLC
\*
\*trace == IODeserialize("RaftKC_TTrace_1794698100.bin", TRUE)
\*
\*=============================================================================
\*

---- MODULE RaftKC_TETrace ----
EXTENDS RaftKC, RaftKC_TEConstants, TLC

trace == 
    <<
    ([msgs |-> {},votedFor |-> (s1 :> "nil" @@ s2 :> "nil" @@ s3 :> "nil"),currentTerm |-> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0),role |-> (s1 :> "follower" @@ s2 :> "follower" @@ s3 :> "follower"),log |-> (s1 :> <<>> @@ s2 :> <<>> @@ s3 :> <<>>),matchIndex |-> (s1 :> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0) @@ s2 :> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0) @@ s3 :> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0)),votes |-> (s1 :> {} @@ s2 :> {} @@ s3 :> {}),commitIndex |-> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0)]),
    ([msgs |-> {},votedFor |-> (s1 :> s1 @@ s2 :> "nil" @@ s3 :> "nil"),currentTerm |-> (s1 :> 1 @@ s2 :> 0 @@ s3 :> 0),role |-> (s1 :> "candidate" @@ s2 :> "follower" @@ s3 :> "follower"),log |-> (s1 :> <<>> @@ s2 :> <<>> @@ s3 :> <<>>),matchIndex |-> (s1 :> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0) @@ s2 :> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0) @@ s3 :> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0)),votes |-> (s1 :> {s1} @@ s2 :> {} @@ s3 :> {}),commitIndex |-> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0)]),
    ([msgs |-> {[term |-> 1, type |-> "vote", from |-> s1, to |-> s2, lastTerm |-> 0, lastIndex |-> 0]},votedFor |-> (s1 :> s1 @@ s2 :> "nil" @@ s3 :> "nil"),currentTerm |-> (s1 :> 1 @@ s2 :> 0 @@ s3 :> 0),role |-> (s1 :> "candidate" @@ s2 :> "follower" @@ s3 :> "follower"),log |-> (s1 :> <<>> @@ s2 :> <<>> @@ s3 :> <<>>),matchIndex |-> (s1 :> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0) @@ s2 :> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0) @@ s3 :> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0)),votes |-> (s1 :> {s1} @@ s2 :> {} @@ s3 :> {}),commitIndex |-> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0)]),
    ([msgs |-> {[term |-> 1, type |-> "voteReply", from |-> s2, to |-> s1, granted |-> TRUE], [term |-> 1, type |-> "vote", from |-> s1, to |-> s2, lastTerm |-> 0, lastIndex |-> 0]},votedFor |-> (s1 :> s1 @@ s2 :> s1 @@ s3 :> "nil"),currentTerm |-> (s1 :> 1 @@ s2 :> 1 @@ s3 :> 0),role |-> (s1 :> "candidate" @@ s2 :> "follower" @@ s3 :> "follower"),log |-> (s1 :> <<>> @@ s2 :> <<>> @@ s3 :> <<>>),matchIndex |-> (s1 :> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0) @@ s2 :> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0) @@ s3 :> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0)),votes |-> (s1 :> {s1} @@ s2 :> {} @@ s3 :> {}),commitIndex |-> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0)]),
    ([msgs |-> {[term |-> 1, type |-> "voteReply", from |-> s2, to |-> s1, granted |-> TRUE], [term |-> 1, type |-> "vote", from |-> s1, to |-> s2, lastTerm |-> 0, lastIndex |-> 0]},votedFor |-> (s1 :> s1 @@ s2 :> s1 @@ s3 :> "nil"),currentTerm |-> (s1 :> 1 @@ s2 :> 1 @@ s3 :> 0),role |-> (s1 :> "candidate" @@ s2 :> "follower" @@ s3 :> "follower"),log |-> (s1 :> <<>> @@ s2 :> <<>> @@ s3 :> <<>>),matchIndex |-> (s1 :> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0) @@ s2 :> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0) @@ s3 :> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0)),votes |-> (s1 :> {s1, s2} @@ s2 :> {} @@ s3 :> {}),commitIndex |-> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0)]),
    ([msgs |-> {[term |-> 1, type |-> "voteReply", from |-> s2, to |-> s1, granted |-> TRUE], [term |-> 1, type |-> "vote", from |-> s1, to |-> s2, lastTerm |-> 0, lastIndex |-> 0]},votedFor |-> (s1 :> s1 @@ s2 :> s1 @@ s3 :> "nil"),currentTerm |-> (s1 :> 1 @@ s2 :> 1 @@ s3 :> 0),role |-> (s1 :> "leader" @@ s2 :> "follower" @@ s3 :> "follower"),log |-> (s1 :> <<[term |-> 1, value |-> "noop"]>> @@ s2 :> <<>> @@ s3 :> <<>>),matchIndex |-> (s1 :> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0) @@ s2 :> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0) @@ s3 :> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0)),votes |-> (s1 :> {s1, s2} @@ s2 :> {} @@ s3 :> {}),commitIndex |-> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0)])
    >>
----


=============================================================================

---- MODULE RaftKC_TEConstants ----
EXTENDS RaftKC

CONSTANTS s1, s2, s3, v1

=============================================================================

---- CONFIG RaftKC_TTrace_1794698100 ----
CONSTANTS
    Servers = { s1 , s2 , s3 }
    Values = { v1 }
    MaxTerm = 2
    MaxLog = 2
    MaxMsgs = 2
    s1 = s1
    s2 = s2
    s3 = s3
    v1 = v1

INVARIANT
    _inv

CHECK_DEADLOCK
    \* CHECK_DEADLOCK off because of PROPERTY or INVARIANT above.
    FALSE

INIT
    _init

NEXT
    _next

CONSTANT
    _TETrace <- _trace

ALIAS
    _expression
=============================================================================
\* Generated on Sun Nov 15 00:15:01 CET 2026