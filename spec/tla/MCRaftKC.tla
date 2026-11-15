------------------------------ MODULE MCRaftKC ------------------------------
(***************************************************************************)
(* Bounded-depth exhaustive check of RaftKC for CI: every behaviour of up  *)
(* to MaxSteps steps is explored. The full state space at these bounds    *)
(* takes too long per push; the full runs are on demand (check.sh --large).*)
(***************************************************************************)
EXTENDS RaftKC

CONSTANT MaxSteps
VARIABLE steps

MCInit == Init /\ steps = 0
MCNext == Next /\ steps' = steps + 1
MCSpec == MCInit /\ [][MCNext]_<<vars, steps>>

DepthBound == steps <= MaxSteps
=============================================================================
