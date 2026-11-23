---------------------------- MODULE MCRaftKCPower ----------------------------
(***************************************************************************)
(* RaftKC's own recovery rule under power loss in mode (a): what a node    *)
(* has on disk (`disk`) is its log as of its last Sync (the OS writes back *)
(* when it likes), and a power loss leaves it that (PowerLossTo), with     *)
(* term and vote kept (fsynced before use). At most one node is recovering *)
(* or down from a power loss at a time (f = 1 of 3). RaftKCRecover.tla was *)
(* the exploration of the rule; this checks the rule as folded into RaftKC  *)
(* (with LearnTerm and everything else RaftKC has).                        *)
(***************************************************************************)
EXTENDS RaftKC

CONSTANTS MaxPowerLoss, MaxSteps
VARIABLES disk, lost, steps

pvars == <<vars, disk, lost, steps>>

PInit == Init /\ disk = [s \in Servers |-> << >>] /\ lost = 0 /\ steps = 0

Sync(s) ==
    /\ disk' = [disk EXCEPT ![s] = log[s]]
    /\ UNCHANGED <<vars, lost>>

PowerLoss(s) ==
    /\ lost < MaxPowerLoss
    /\ \A t \in Servers : ~recovering[t]
    /\ lost' = lost + 1
    /\ PowerLossTo(s, disk[s])
    /\ UNCHANGED disk

PNext ==
    /\ \/ Next /\ UNCHANGED <<disk, lost>>
       \/ \E s \in Servers : Sync(s) \/ PowerLoss(s)
    /\ steps' = steps + 1

PSpec == PInit /\ [][PNext]_pvars
DepthBound == steps <= MaxSteps
=============================================================================
