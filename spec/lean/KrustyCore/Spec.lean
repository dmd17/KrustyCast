/-
  The abstract sequencer (ARCHITECTURE §11.2): `krusty-core`'s state machine
  over plain maps and natural numbers, mirroring the TLA+ actions of
  `SequencerBounded.tla`. `Refine.lean` proves the code refines it;
  the guarantees below are proved here, on the spec.
-/
import KrustyCore.Funs

open Aeneas Aeneas.Std

namespace krusty_core.spec

/-- `u64::MAX`. -/
def MAX : Nat := 2 ^ 64 - 1

/-- `x.saturating_add(1)` on a `u64`. -/
def sat1 (x : Nat) : Nat := min (x + 1) MAX

/-- A decided outcome, as remembered for resends. -/
inductive Dec where
  | acc (seqno : Nat)
  | rej (key cur : Nat)
  deriving DecidableEq

inductive Out where
  | acc (seqno : Nat)
  | rej (key cur : Nat)
  | nak
  | busy
  | expired (min : Nat)
  /-- Anything the core never produces (`NotLeader`, `Busy` with a delay). -/
  | other
  deriving DecidableEq

def Dec.out : Dec → Out
  | .acc g => .acc g
  | .rej k c => .rej k c

structure Reply where
  client : Nat
  inc : Nat
  seq : Nat
  out : Out
  deriving DecidableEq

structure Pub where
  seqno : Nat
  ts : Nat
  client : Nat
  cond : Option (Nat × Nat)
  deriving DecidableEq

structure Eff where
  pub : Option Pub
  reply : Option Reply
  deriving DecidableEq

structure Sess where
  inc : Nat
  next : Nat
  active : Nat
  deriving DecidableEq

structure Cfg where
  maxClients : Nat
  ring : Nat
  maxKeys : Nat
  idle : Nat

/-- A submission as the core sees it. `cond`: a conditional append's
    (key, expected version). -/
structure Req where
  client : U64
  inc : Nat
  seq : Nat
  cond : Option (U64 × Nat)

structure St where
  applied : Nat
  now : Nat
  floor : Nat
  /-- Next stream seqno. -/
  seq : Nat
  sess : U64 → Option Sess
  nclients : Nat
  /-- Each session's outcome ring: index ↦ (client seqno, decision). -/
  mem : U64 → Nat → Option (Nat × Dec)
  ver : U64 → Nat
  /-- Keys holding a version slot. -/
  known : U64 → Bool
  nkeys : Nat

def upd {α β : Type} [DecidableEq α] (f : α → β) (a : α) (b : β) : α → β :=
  fun x => if x = a then b else f x

def reply (r : Req) (q : Nat) (o : Out) : Eff :=
  ⟨none, some ⟨r.client.val, r.inc, q, o⟩⟩

def noEff : Eff := ⟨none, none⟩

/-- The next message of a session: its decision, what to publish, and the
    new key versions. `none`: undecided because the key table is full. -/
def decideAppend (cfg : Cfg) (s : St) (r : Req) : Option Dec × Option Pub × St :=
  match r.cond with
  | none => (some (.acc s.seq), some ⟨s.seq, s.now, r.client.val, none⟩, s)
  | some (key, ev) =>
    let cur := s.ver key
    if cur ≠ ev ∨ cur = MAX then (some (.rej key.val cur), none, s)
    else if s.known key ∨ s.nkeys < cfg.maxKeys then
      (some (.acc s.seq), some ⟨s.seq, s.now, r.client.val, some (key.val, cur + 1)⟩,
        { s with ver := upd s.ver key (cur + 1), known := upd s.known key true,
                 nkeys := if s.known key then s.nkeys else s.nkeys + 1 })
    else (none, none, s)

/-- A message of the client's current session `S`. The third component
    (ghost) is the decision taken for this message, if one was. -/
def inSession (cfg : Cfg) (s : St) (r : Req) (S : Sess) : Eff × St × Option Dec :=
  if r.seq < S.next then
    match s.mem r.client (r.seq % cfg.ring) with
    | some (q, d) => if q = r.seq then (reply r r.seq d.out, s, none) else (noEff, s, none)
    | none => (noEff, s, none)
  else if S.next < r.seq then (reply r S.next .nak, s, none)
  else if r.seq = MAX then (reply r r.seq (.expired (sat1 r.inc)), s, none)
  else if s.seq = MAX then (reply r r.seq .busy, s, none)
  else
    match decideAppend cfg s r with
    | (none, _, s1) => (reply r r.seq .busy, s1, none)
    | (some d, pub, s1) =>
      let s2 := { s1 with
        mem := upd s1.mem r.client (upd (s1.mem r.client) (r.seq % cfg.ring) (some (r.seq, d))),
        sess := upd s1.sess r.client (some { S with next := S.next + 1, active := s1.now }) }
      match pub with
      | none => (reply r r.seq d.out, s2, some d)
      | some p => (⟨some p, (reply r r.seq d.out).reply⟩, { s2 with seq := s2.seq + 1 }, some d)

def startSession (s : St) (c : U64) (inc : Nat) : St :=
  { s with sess := upd s.sess c (some ⟨inc, 1, s.now⟩), mem := upd s.mem c (fun _ => none) }

/-- Sessions that may be evicted. -/
def Idle (cfg : Cfg) (S : Sess) (now : Nat) : Prop := cfg.idle ≤ now - S.active

/-- The core's decision on one request; `victim` is the session to evict if
    the client table is full. -/
def decide (cfg : Cfg) (s : St) (r : Req) (victim : Option U64) : Eff × St × Option Dec :=
  match s.sess r.client with
  | some S =>
    if r.inc < S.inc then (reply r r.seq (.expired (sat1 S.inc)), s, none)
    else if r.inc = S.inc then inSession cfg s r S
    else if r.seq = 1 then inSession cfg (startSession s r.client r.inc) r ⟨r.inc, 1, s.now⟩
    else (reply r 1 .nak, s, none)
  | none =>
    if r.inc ≤ s.floor then (reply r r.seq (.expired (sat1 s.floor)), s, none)
    else if r.seq ≠ 1 then (reply r 1 .nak, s, none)
    else if s.nclients < cfg.maxClients then
      inSession cfg (startSession { s with nclients := s.nclients + 1 } r.client r.inc) r
        ⟨r.inc, 1, s.now⟩
    else
      match victim with
      | none => (reply r r.seq .busy, s, none)
      | some v =>
        match s.sess v with
        | none => (reply r r.seq .busy, s, none)
        | some V =>
          inSession cfg
            (startSession { s with sess := upd s.sess v none, mem := upd s.mem v (fun _ => none),
                                   floor := max s.floor V.inc } r.client r.inc)
            r ⟨r.inc, 1, s.now⟩

/-- A legal eviction choice: an idle session, or none if no session is idle. -/
def ValidVictim (cfg : Cfg) (s : St) : Option U64 → Prop
  | none => ∀ c S, s.sess c = some S → ¬ Idle cfg S s.now
  | some v => ∃ V, s.sess v = some V ∧ Idle cfg V s.now

/-- Applying committed entry `li`, timestamped `ts`. `none`: out of order. -/
def apply (cfg : Cfg) (s : St) (li ts : Nat) (r : Req) (victim : Option U64) :
    Option Eff × St × Option Dec :=
  if s.applied = MAX ∨ li ≠ s.applied + 1 then (none, s, none)
  else
    let s0 := { s with applied := li, now := max s.now ts }
    let (e, s1, g) := decide cfg s0 r victim
    (some e, s1, g)

/-- One step of the abstract sequencer; `g` (ghost) is the decision it took
    for `r`'s message, if any. -/
def Step (cfg : Cfg) (s : St) (li ts : Nat) (r : Req) (e : Option Eff) (s' : St)
    (g : Option Dec) : Prop :=
  ∃ v, ValidVictim cfg { s with applied := li, now := max s.now ts } v ∧
    apply cfg s li ts r v = (e, s', g)

end krusty_core.spec

/-! ## Guarantees, on the spec -/

namespace krusty_core.spec

/-- Ghost history: the decision taken for each (client, incarnation, seq). -/
abbrev Hist := U64 → Nat → Nat → Option Dec

def record (h : Hist) (r : Req) (d : Dec) : Hist :=
  upd h r.client (upd (h r.client) r.inc (upd (h r.client r.inc) r.seq (some d)))

def Hist.after (h : Hist) (r : Req) : Option Dec → Hist
  | none => h
  | some d => record h r d

/-- What the history and the state agree on. -/
structure HInv (cfg : Cfg) (s : St) (h : Hist) : Prop where
  /-- A decided message is behind its client's session, or below the floor. -/
  owned : ∀ c inc q d, h c inc q = some d →
    match s.sess c with
    | some S => inc < S.inc ∨ (inc = S.inc ∧ q < S.next)
    | none => inc ≤ s.floor
  /-- Remembered outcomes are the decisions taken. -/
  mem : ∀ c S idx q d, s.sess c = some S → s.mem c idx = some (q, d) → h c S.inc q = some d
  /-- Accepted messages got seqnos below the next one. -/
  seqs : ∀ c inc q g, h c inc q = some (.acc g) → g < s.seq

theorem upd_same {α β : Type} [DecidableEq α] (f : α → β) (a : α) (b : β) : upd f a b a = b := by
  simp [upd]

theorem upd_other {α β : Type} [DecidableEq α] (f : α → β) {a x : α} (b : β) (h : x ≠ a) :
    upd f a b x = f x := by
  simp [upd, h]

/-- Facts about `decideAppend`: only versions change. -/
theorem decideAppend_frame (cfg : Cfg) (s : St) (r : Req) :
    let s1 := (decideAppend cfg s r).2.2
    s1.sess = s.sess ∧ s1.mem = s.mem ∧ s1.seq = s.seq ∧ s1.floor = s.floor ∧
      s1.now = s.now ∧ s1.nclients = s.nclients ∧ s1.applied = s.applied := by
  unfold decideAppend
  rcases h : r.cond with _ | ⟨key, ev⟩ <;> simp only [h]
  · simp
  · split_ifs <;> simp

/-- An accepted decision is the next seqno, and comes with a publication. -/
theorem decideAppend_acc (cfg : Cfg) (s : St) (r : Req) :
    match decideAppend cfg s r with
    | (some (.acc g), pub, _) => g = s.seq ∧ ∃ p, pub = some p ∧ p.seqno = s.seq
    | (some (.rej _ _), pub, _) => pub = none
    | (none, pub, _) => pub = none := by
  unfold decideAppend
  rcases h : r.cond with _ | ⟨key, ev⟩ <;> simp only [h]
  · simp
  · split_ifs <;> simp

theorem record_get (h : Hist) (r : Req) (d : Dec) (c : U64) (inc q : Nat) :
    record h r d c inc q = if c = r.client ∧ inc = r.inc ∧ q = r.seq then some d else h c inc q := by
  unfold record upd
  by_cases h1 : c = r.client <;> by_cases h2 : inc = r.inc <;> by_cases h3 : q = r.seq <;> simp [*]

/-- The history only grows. -/
theorem record_mono {h : Hist} {r : Req} {d : Dec} (hfresh : h r.client r.inc r.seq = none)
    {c : U64} {inc q : Nat} {d' : Dec} (h' : h c inc q = some d') : record h r d c inc q = some d' := by
  rw [record_get]
  split
  · rename_i e; obtain ⟨rfl, rfl, rfl⟩ := e; rw [hfresh] at h'; cases h'
  · exact h'

/-- What a session's message does to the history: it is decided at most once. -/
theorem inSession_hinv {cfg : Cfg} {s : St} {r : Req} {S : Sess} {h : Hist}
    (hS : s.sess r.client = some S) (hinc : S.inc = r.inc) (hI : HInv cfg s h) :
    let x := inSession cfg s r S
    (∀ d, x.2.2 = some d → h r.client r.inc r.seq = none) ∧ HInv cfg x.2.1 (h.after r x.2.2) := by
  intro x
  unfold x inSession
  split_ifs with h1 h2 h3 h4
  · split
    · split <;> exact ⟨by simp, hI⟩
    · exact ⟨by simp, hI⟩
  · exact ⟨by simp, hI⟩
  · exact ⟨by simp, hI⟩
  · exact ⟨by simp, hI⟩
  · have hq : r.seq = S.next := by omega
    have hfr := decideAppend_frame cfg s r
    have hac := decideAppend_acc cfg s r
    rcases hd : decideAppend cfg s r with ⟨dd, pub, s1⟩
    rw [hd] at hfr hac
    simp only at hfr
    obtain ⟨fsess, fmem, fseq, ffloor, fnow, -, -⟩ := hfr
    have hfresh : h r.client r.inc r.seq = none := by
      cases e : h r.client r.inc r.seq with
      | none => rfl
      | some d' =>
        have := hI.owned _ _ _ _ e; rw [hS] at this; omega
    cases dd with
    | none =>
      simp only [Hist.after]
      refine ⟨by simp, ⟨?_, ?_, ?_⟩⟩
      · intro c inc q d e; rw [fsess, ffloor]; exact hI.owned c inc q d e
      · intro c S' idx q d e1 e2; rw [fsess] at e1; rw [fmem] at e2; exact hI.mem c S' idx q d e1 e2
      · intro c inc q g e; rw [fseq]; exact hI.seqs c inc q g e
    | some d =>
      -- The decision, and the state after it (with or without a publication).
      have key : ∀ s' : St, s'.sess = upd s1.sess r.client (some { S with next := S.next + 1, active := s1.now }) →
          s'.mem = upd s1.mem r.client (upd (s1.mem r.client) (r.seq % cfg.ring) (some (r.seq, d))) →
          s'.floor = s1.floor →
          (∀ g, d = .acc g → g < s'.seq) → s.seq ≤ s'.seq →
          HInv cfg s' (record h r d) := by
        intro s' es em ef eacc eseq
        refine ⟨?_, ?_, ?_⟩
        · intro c inc q d' e
          rw [record_get] at e
          rw [es, ef, ffloor]
          split at e
          · rename_i e'; obtain ⟨rfl, rfl, rfl⟩ := e'
            rw [upd_same]; right; refine ⟨hinc.symm, ?_⟩; simp only; omega
          · have := hI.owned c inc q d' e
            by_cases hc : c = r.client
            · subst hc; rw [upd_same]; rw [hS] at this
              simp only at this ⊢; omega
            · rw [upd_other _ _ hc, fsess]; exact this
        · intro c S' idx q d' e1 e2
          rw [es] at e1; rw [em] at e2
          by_cases hc : c = r.client
          · subst hc
            rw [upd_same] at e1 e2; cases e1
            by_cases hi : idx = r.seq % cfg.ring
            · subst hi; rw [upd_same] at e2; cases e2
              rw [record_get]; simp [hinc]
            · rw [upd_other _ _ hi, fmem] at e2
              exact record_mono hfresh (by have := hI.mem _ S idx q d' hS e2; simpa using this)
          · rw [upd_other _ _ hc, fsess] at e1; rw [upd_other _ _ hc, fmem] at e2
            exact record_mono hfresh (hI.mem c S' idx q d' e1 e2)
        · intro c inc q g e
          rw [record_get] at e
          split at e
          · cases e; exact eacc g rfl
          · have := hI.seqs c inc q g e; omega
      simp only [Hist.after]
      refine ⟨fun _ _ => hfresh, ?_⟩
      cases pub with
      | none =>
        simp only
        apply key
        · rfl
        · rfl
        · rfl
        · intro g e; subst e; simp at hac
        · simp only [fseq]; omega
      | some p =>
        simp only
        apply key
        · rfl
        · rfl
        · rfl
        · intro g e; subst e; simp at hac; simp [hac.1, fseq]
        · simp only [fseq]; omega

theorem HInv.congr {cfg : Cfg} {s s' : St} {h : Hist} (hI : HInv cfg s h)
    (e1 : s'.sess = s.sess) (e2 : s'.mem = s.mem) (e3 : s'.floor = s.floor) (e4 : s'.seq = s.seq) :
    HInv cfg s' h :=
  ⟨by rw [e1, e3]; exact hI.owned, by rw [e1, e2]; exact hI.mem, by rw [e4]; exact hI.seqs⟩

/-- A new session for `c`, newer than all of `c`'s decided messages. -/
theorem startSession_hinv {cfg : Cfg} {s : St} {h : Hist} (hI : HInv cfg s h) (c : U64) (inc : Nat)
    (hnew : ∀ i q d, h c i q = some d → i < inc) : HInv cfg (startSession s c inc) h := by
  refine ⟨?_, ?_, hI.seqs⟩
  · intro c' i q d e
    simp only [startSession]
    by_cases hc : c' = c
    · subst hc; rw [upd_same]; left; exact hnew i q d e
    · rw [upd_other _ _ hc]; exact hI.owned c' i q d e
  · intro c' S idx q d e1 e2
    simp only [startSession] at e1 e2
    by_cases hc : c' = c
    · subst hc; rw [upd_same] at e2; cases e2
    · rw [upd_other _ _ hc] at e1 e2; exact hI.mem c' S idx q d e1 e2

theorem decide_hinv {cfg : Cfg} {s0 : St} {r : Req} {v : Option U64} {h : Hist}
    (hI0 : HInv cfg s0 h) :
    let x := decide cfg s0 r v
    (∀ d, x.2.2 = some d → h r.client r.inc r.seq = none) ∧ HInv cfg x.2.1 (h.after r x.2.2) := by
  intro x
  unfold x
  unfold decide
  split
  · rename_i S hS
    split_ifs with h1 h2 h3
    · exact ⟨by simp, hI0⟩
    · exact inSession_hinv hS h2.symm hI0
    · apply inSession_hinv (by simp [startSession, upd_same]) rfl
      apply startSession_hinv hI0
      intro i q d e'
      have := hI0.owned _ _ _ _ e'; rw [hS] at this; simp only at this; omega
    · exact ⟨by simp, hI0⟩
  · rename_i hS
    split_ifs with h1 h2 h3
    · exact ⟨by simp, hI0⟩
    · exact ⟨by simp, hI0⟩
    · apply inSession_hinv (by simp [startSession, upd_same]) rfl
      apply startSession_hinv (hI0.congr (s' := { s0 with nclients := s0.nclients + 1 }) rfl rfl rfl rfl)
      intro i q d e'
      have := hI0.owned _ _ _ _ e'; rw [hS] at this; simp only at this; omega
    · split
      · exact ⟨by simp, hI0⟩
      · rename_i w
        split
        · exact ⟨by simp, hI0⟩
        · rename_i V hV
          have hwc : w ≠ r.client := by rintro rfl; rw [hS] at hV; cases hV
          apply inSession_hinv (by simp [startSession, upd_same]) rfl
          apply startSession_hinv
          · refine ⟨?_, ?_, hI0.seqs⟩
            · intro c' i q d e'
              have := hI0.owned c' i q d e'
              simp only
              by_cases hc : c' = w
              · subst hc; rw [upd_same]; rw [hV] at this; simp only at this; omega
              · rw [upd_other _ _ hc]
                rcases hsc : s0.sess c' with _ | S' <;> rw [hsc] at this <;> simp only at this ⊢ <;> omega
            · intro c' S idx q d e1 e2
              simp only at e1 e2
              by_cases hc : c' = w
              · subst hc; rw [upd_same] at e1; cases e1
              · rw [upd_other _ _ hc] at e1 e2; exact hI0.mem c' S idx q d e1 e2
          · intro i q d e'
            have := hI0.owned _ _ _ _ e'; rw [hS] at this; simp only at this; omega

/-- **G4 (at most once)** and the consistency of remembered outcomes: every
    step keeps the history invariant, and a decision is only ever taken for
    a message that had none. -/
theorem step_hinv {cfg : Cfg} {s s' : St} {li ts : Nat} {r : Req} {e : Option Eff} {g : Option Dec}
    {h : Hist} (hI : HInv cfg s h) (hs : Step cfg s li ts r e s' g) :
    (∀ d, g = some d → h r.client r.inc r.seq = none) ∧ HInv cfg s' (h.after r g) := by
  obtain ⟨v, -, happ⟩ := hs
  unfold apply at happ
  split at happ
  · cases happ; exact ⟨by simp, hI⟩
  · have hI0 : HInv cfg { s with applied := li, now := max s.now ts } h := hI.congr rfl rfl rfl rfl
    have := decide_hinv (r := r) (v := v) hI0
    simp only at happ this
    rcases hd : decide cfg { s with applied := li, now := max s.now ts } r v with ⟨e', s'', g'⟩
    rw [hd] at happ this
    simp only [Prod.mk.injEq] at happ
    obtain ⟨-, rfl, rfl⟩ := happ
    exact this

/-- What one message may publish: the next seqno, and for a conditional
    append, the key's next version (the only version that changes). -/
def PubOK (s : St) (e : Eff) (s' : St) : Prop :=
  match e.pub with
  | some p => p.seqno = s.seq ∧ s'.seq = s.seq + 1 ∧
      match p.cond with
      | none => ∀ k, s'.ver k = s.ver k
      | some (kv, v) => ∃ k : U64, k.val = kv ∧ v = s.ver k + 1 ∧ s'.ver k = v ∧
          ∀ k', k' ≠ k → s'.ver k' = s.ver k'
  | none => s'.seq = s.seq ∧ ∀ k, s'.ver k = s.ver k

theorem inSession_pub (cfg : Cfg) (s : St) (r : Req) (S : Sess) :
    PubOK s (inSession cfg s r S).1 (inSession cfg s r S).2.1 := by
  unfold inSession
  split_ifs
  · split
    · split <;> simp [PubOK, reply, noEff]
    · simp [PubOK, noEff]
  · simp [PubOK, reply]
  · simp [PubOK, reply]
  · simp [PubOK, reply]
  · unfold decideAppend
    rcases hc : r.cond with _ | ⟨key, ev⟩
    · simp [PubOK]
    · simp only
      split_ifs with h1 h2
      all_goals first
        | (simp [PubOK, reply]; done)
        | (simp only [PubOK, true_and]
           exact ⟨key, rfl, rfl, by simp [upd_same], fun k' hk => upd_other _ _ (fun e => hk (by rw [e]))⟩)

theorem PubOK.congr {s s0 : St} {e : Eff} {s' : St} (h : PubOK s0 e s') (e1 : s0.seq = s.seq)
    (e2 : s0.ver = s.ver) : PubOK s e s' := by
  unfold PubOK at h ⊢; rw [e1, e2] at h; exact h

theorem decide_pub (cfg : Cfg) (s0 : St) (r : Req) (v : Option U64) :
    PubOK s0 (decide cfg s0 r v).1 (decide cfg s0 r v).2.1 := by
  unfold decide
  split
  · split_ifs
    · simp [PubOK, reply]
    · exact inSession_pub _ _ _ _
    · exact (inSession_pub _ _ _ _).congr rfl rfl
    · simp [PubOK, reply]
  · split_ifs
    · simp [PubOK, reply]
    · simp [PubOK, reply]
    · exact (inSession_pub _ _ _ _).congr rfl rfl
    · split
      · simp [PubOK, reply]
      · split
        · simp [PubOK, reply]
        · exact (inSession_pub _ _ _ _).congr rfl rfl

/-- **G5 (conditional append)**, and consecutive stream seqnos: a step that
    publishes uses the next seqno; a conditional append publishes the key's
    next version, and no other version changes; nothing else changes the
    seqno or any version. -/
theorem step_pub {cfg : Cfg} {s s' : St} {li ts : Nat} {r : Req} {e : Option Eff} {g : Option Dec}
    (hs : Step cfg s li ts r e s' g) :
    match e with
    | some E => PubOK s E s'
    | none => s' = s := by
  obtain ⟨v, -, happ⟩ := hs
  unfold apply at happ
  split at happ
  · cases happ; rfl
  · have := decide_pub cfg { s with applied := li, now := max s.now ts } r v
    simp only at happ this
    rcases hd : decide cfg { s with applied := li, now := max s.now ts } r v with ⟨e', s'', g'⟩
    rw [hd] at happ this
    simp only [Prod.mk.injEq] at happ
    obtain ⟨rfl, rfl, -⟩ := happ
    exact this.congr rfl rfl

#print axioms step_hinv
#print axioms step_pub

end krusty_core.spec
