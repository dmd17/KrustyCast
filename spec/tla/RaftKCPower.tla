----------------------------- MODULE RaftKCPower -----------------------------
(***************************************************************************)
(* RaftKC with power loss in mode (a): what a node has on disk can lag     *)
(* what it acted on. In mode (a) term, vote and log are written before the *)
(* messages that depend on them but not fsynced, and followers acknowledge *)
(* entries on receipt. `disk` is what a power loss leaves: the node's      *)
(* state as of its last Sync (any earlier moment: the OS flushes when it   *)
(* likes). PowerLoss reverts the node to it, and loses what Restart loses. *)
(* One node losing power is one failure (PRD G3: f = 1 of 3).              *)
(***************************************************************************)
EXTENDS RaftKC

CONSTANTS MaxPowerLoss, MaxSteps,
          SyncHardState  \* term and vote fsynced before use (only the log is lost)
VARIABLES disk, lost, steps

pvars == <<vars, disk, lost, steps>>

DiskOf(s) == [term |-> currentTerm[s], vote |-> votedFor[s], log |-> log[s]]

PInit == Init /\ disk = [s \in Servers |-> DiskOf(s)] /\ lost = 0 /\ steps = 0

\* The OS writes a node's state back (at any time).
Sync(s) ==
    /\ disk' = [disk EXCEPT ![s] = DiskOf(s)]
    /\ UNCHANGED <<vars, lost>>

\* Power loss: the node comes back with what reached its disk.
PowerLoss(s) ==
    /\ lost < MaxPowerLoss
    /\ lost' = lost + 1
    /\ currentTerm' = IF SyncHardState THEN currentTerm
                      ELSE [currentTerm EXCEPT ![s] = disk[s].term]
    /\ votedFor' = IF SyncHardState THEN votedFor
                   ELSE [votedFor EXCEPT ![s] = disk[s].vote]
    /\ log' = [log EXCEPT ![s] = disk[s].log]
    /\ role' = [role EXCEPT ![s] = "follower"]
    /\ commitIndex' = [commitIndex EXCEPT ![s] = 0]
    /\ votes' = [votes EXCEPT ![s] = {}]
    /\ matchIndex' = [matchIndex EXCEPT ![s] = [t \in Servers |-> 0]]
    /\ UNCHANGED <<msgs, committed, disk>>

PNext ==
    /\ \/ Next /\ UNCHANGED <<disk, lost>>
       \/ \E s \in Servers : Sync(s) \/ PowerLoss(s)
    /\ steps' = steps + 1

PSpec == PInit /\ [][PNext]_pvars

DepthBound == steps <= MaxSteps
=============================================================================
