/-
  `Core::restore` (crates/krusty-core/src/restore.rs): whatever the bytes,
  it either rejects them or returns a core satisfying `refine.Inv`, the
  invariant `Core::apply` is proved under (`Refine.lean`). So a restored
  core behaves as the abstract sequencer does from then on.
-/
import KrustyCore.Refine

open Aeneas Aeneas.Std Result
open krusty_core krusty_core.idmap krusty_core.refine

namespace krusty_core.restore_proof

/-- `read_u64` never panics, and on success moves 8 bytes on, staying
    within the slice. -/
@[step] theorem read_u64_spec (b : Slice U8) (p : Usize) :
    restore.read_u64 b p ⦃ r => match r with
      | .Ok (_, a) => a.val = p.val + 8 ∧ a.val ≤ b.length
      | .Err _ => True ⦄ := by
  unfold restore.read_u64
  step*

/-- `?` on a `Result`: `Ok v` continues with `v`, `Err e` returns `Err e`. -/
@[simp] theorem branch_ok {T E : Type} (v : T) :
    core.result.Result.Insts.CoreOpsTry.branch (T := T) (E := E) (.Ok v) = ok (.Continue v) := rfl
@[simp] theorem branch_err {T E : Type} (e : E) :
    core.result.Result.Insts.CoreOpsTry.branch (T := T) (E := E) (.Err e) = ok (.Break (.Err e)) := rfl
@[simp] theorem residual_err {T E : Type} (e : E) :
    core.result.Result.Insts.CoreOpsTry_traitFromResidualResult.from_residual T
      (core.convert.FromSame E) (.Err e) = ok (.Err e) := by
  simp [core.result.Result.Insts.CoreOpsTry_traitFromResidualResult.from_residual,
    core.convert.FromSame.from]

/-- One `read_u64(...)?`: on `Err` the rest is skipped. -/
macro "rd" : tactic => `(tactic| (
  try dsimp only
  step with read_u64_spec as ⟨r, hr⟩
  rcases r with ⟨⟨v, a⟩⟩ | ⟨e⟩ <;> simp only [branch_ok, branch_err, residual_err, bind_ok] at * <;>
    try dsimp only))

/-- `rd`, then reduce the `let (x, at) := (v, a)` that follows (Aeneas's
    `uncurry`), for when no monadic step comes next. -/
macro "rdu" : tactic => `(tactic| (rd <;> try simp only [Std.uncurry_apply_pair]))

/-- Close a branch that returns `Err` (the claim is about `Ok` only). -/
macro "rej" : tactic => `(tactic| first
  | (simp only [WP.spec_ok]; intro _ h; simp at h; done)
  | (step; (try simp only [WP.spec_ok]); intro _ h; simp at h; done))

/-! ## The invariant while restoring

`Inv` without `clients_len` (slots `0..unused` fill up one by one). -/

structure PInv (c : Core) : Prop where
  clients_pos : 1 ≤ c.cfg.max_clients.val
  clients_u32 : c.cfg.max_clients.val ≤ U32.max
  ring_pos : 1 ≤ c.cfg.outcome_ring.val
  ring_cap : c.cfg.max_clients.val * c.cfg.outcome_ring.val ≤ Usize.max
  keys_u32 : c.cfg.max_keys.val ≤ U32.max
  clients_wf : Wf c.clients
  clients_max : c.clients.max_len = c.cfg.max_clients
  keys_wf : Wf c.keys
  keys_max : c.keys.max_len = c.cfg.max_keys
  sess_len : c.sessions.val.length = c.cfg.max_clients.val
  ring_len : c.ring.val.length = c.cfg.max_clients.val * c.cfg.outcome_ring.val
  unused_le : c.unused.val ≤ c.cfg.max_clients.val
  slot_of : ∀ k slot, slotOf c k = some slot →
    slot.val < c.unused.val ∧ ∃ S, c.sessions.val[slot.val]? = some (some S) ∧
      UScalar.cast .U64 S.client_id = k
  client_of : ∀ (j : Nat) S, c.sessions.val[j]? = some (some S) →
    ∃ slot : U32, slot.val = j ∧ slotOf c (UScalar.cast .U64 S.client_id) = some slot
  versions_len : c.versions.val.length = c.keys.len.val
  key_of_slot_len : c.key_of_slot.val.length = c.keys.len.val
  key_slot : ∀ k (i : U32), lookup c.keys.buckets.val k = some i → i.val < c.versions.val.length
  key_of : ∀ k (i : U32), lookup c.keys.buckets.val k = some i → c.key_of_slot.val[i.val]? = some k

theorem PInv.toInv {c : Core} (h : PInv c) (hl : c.clients.len.val = c.unused.val) : Inv c :=
  ⟨h.clients_pos, h.clients_u32, h.ring_pos, h.ring_cap, h.keys_u32, h.clients_wf, h.clients_max,
   h.keys_wf, h.keys_max, h.sess_len, h.ring_len, h.unused_le, hl, h.slot_of, h.client_of,
   h.versions_len, h.key_of_slot_len, h.key_slot, h.key_of⟩

/-- Writing the ring keeps `PInv` if its length stays. -/
theorem PInv.with_ring {c : Core} (h : PInv c) (r : alloc.vec.Vec (Option RingSlot))
    (hr : r.val.length = c.ring.val.length) : PInv { c with ring := r } :=
  { h with ring_len := by rw [hr]; exact h.ring_len }

/-! ## One ring slot -/

theorem restore_slot_spec (kc : Core) (b : Slice U8) (p slot : Usize) (hI : PInv kc)
    (hslot : slot.val < kc.cfg.max_clients.val) :
    restore.restore_slot kc b p kc.cfg slot ⦃ r =>
      (∃ ring : alloc.vec.Vec (Option RingSlot), r.2 = { kc with ring := ring } ∧
        ring.val.length = kc.ring.val.length) ∧
      ∀ a, r.1 = core.result.Result.Ok a → a.val ≤ b.length ⦄ := by
  have hR := hI.ring_pos
  have hcap := hI.ring_cap
  have hrl := hI.ring_len
  have hRmax : kc.cfg.outcome_ring.val ≤ Usize.max := by
    have := Nat.le_mul_of_pos_left kc.cfg.outcome_ring.val (by omega : 0 < kc.cfg.max_clients.val)
    omega
  -- Errors leave the core as it was.
  have same : (∃ ring : alloc.vec.Vec (Option RingSlot), kc = { kc with ring := ring } ∧
      ring.val.length = kc.ring.val.length) := ⟨kc.ring, rfl, rfl⟩
  unfold restore.restore_slot
  repeat rd
  all_goals (try step)
  all_goals (try (simp only [WP.spec_ok]; exact ⟨same, fun _ h => by cases h⟩))
  rename_i i a1 h1 cs a2 h2 kind a3 h3 x a4 h4 y a5 h5
  have hr' : r.val = kc.cfg.outcome_ring.val := by
    rw [r_post, UScalar.cast_val_eq]; apply Nat.mod_eq_of_lt
    have : Usize.max < 2 ^ 64 := by
      have := System.Platform.numBits_eq; rcases this with h | h <;> simp [Usize.max, Usize.numBits, h]
    simp only [UScalarTy.numBits]; omega
  have hsl : (slot.val + 1) * kc.cfg.outcome_ring.val ≤ kc.cfg.max_clients.val * kc.cfg.outcome_ring.val :=
    Nat.mul_le_mul_right _ (by omega)
  rw [Nat.add_mul, Nat.one_mul] at hsl
  split
  · simp only [WP.spec_ok]; exact ⟨same, fun _ h => by cases h⟩
  rename_i hlt
  simp only [GE.ge, UScalar.le_equiv, not_le] at hlt
  step
  split
  · simp only [WP.spec_ok]; exact ⟨same, fun _ h => by cases h⟩
  split
  all_goals (try (simp only [WP.spec_ok]; exact ⟨same, fun _ h => by cases h⟩))
  all_goals
    have hi : i.val ≤ Usize.max := by omega
    have hlen : kc.ring.length = kc.ring.val.length := rfl
    step*
    all_goals (try (
      have hc : i3.val = i.val := by
        rw [i3_post, UScalar.cast_val_eq]; exact Nat.mod_eq_of_lt (usize_lt _ hi)
      omega))
    all_goals (try (
      subst index_mut_back_post
      exact ⟨⟨_, rfl, by simp [alloc.vec.Vec.set_val_eq]⟩, fun a h => by cases h; exact h5.2⟩))

/-! ## A session's ring -/

/-- What the ring loop keeps, from `kc0`: only the ring changes (its length
    stays), and the position stays within the slice. -/
def RingFrame (kc0 kc : Core) (b : Slice U8) (p : Usize) : Prop :=
  (∃ ring : alloc.vec.Vec (Option RingSlot), kc = { kc0 with ring := ring } ∧
    ring.val.length = kc0.ring.val.length) ∧ p.val ≤ b.length

theorem ring_frame_trans {kc0 kc kc' : Core} {b : Slice U8} {p p' : Usize}
    (h : RingFrame kc0 kc b p)
    (h' : (∃ ring : alloc.vec.Vec (Option RingSlot), kc' = { kc with ring := ring } ∧
      ring.val.length = kc.ring.val.length)) (hp : p'.val ≤ b.length) :
    RingFrame kc0 kc' b p' := by
  obtain ⟨⟨r, rfl, hr⟩, -⟩ := h
  obtain ⟨r', rfl, hr'⟩ := h'
  exact ⟨⟨r', rfl, by simp at hr'; rw [hr', hr]⟩, hp⟩

theorem session_loop_spec (kc0 : Core) (b : Slice U8) (slot : Usize) (n : U64)
    (hI : PInv kc0) (hslot : slot.val < kc0.cfg.max_clients.val)
    (kc : Core) (p : Usize) (k : U64) (hk : k.val ≤ n.val) (hF : RingFrame kc0 kc b p) :
    restore.restore_session_loop kc b kc0.cfg.max_clients kc0.cfg.outcome_ring kc0.cfg.max_keys
      kc0.cfg.evict_idle_ns slot n p none k ⦃ r =>
        r.2.2 = none → RingFrame kc0 r.1 b r.2.1 ⦄ := by
  unfold restore.restore_session_loop
  apply loop.spec_decr_nat
    (fun (x : Core × Usize × Option restore.SnapshotError × U64) =>
      n.val - x.2.2.2.val + (if x.2.2.1.isSome then 0 else 1))
    (fun x => x.2.2.2.val ≤ n.val ∧ (x.2.2.1 = none → RingFrame kc0 x.1 b x.2.1))
  · rintro ⟨kc1, p1, err1, k1⟩ ⟨hk1, hF1⟩
    simp only at hk1 hF1
    unfold restore.restore_session_loop.body
    have hcfg : Config.mk kc0.cfg.max_clients kc0.cfg.outcome_ring kc0.cfg.max_keys
        kc0.cfg.evict_idle_ns = kc0.cfg := rfl
    simp only [hcfg]
    split
    · rename_i hlt
      simp only [UScalar.lt_equiv] at hlt
      cases err1 with
      | some e =>
        simp only [core.option.Option.is_none, Option.isNone_some, Bool.false_eq_true, ite_false, WP.spec_ok]
        intro h; cases h
      | none =>
        simp only [core.option.Option.is_none, Option.isNone_none, ite_true]
        obtain ⟨⟨ring1, rfl, hring1⟩, hp1⟩ := hF1 rfl
        have hP1 : PInv { kc0 with ring := ring1 } := hI.with_ring ring1 hring1
        step with restore_slot_spec { kc0 with ring := ring1 } b p1 slot hP1 hslot
        have hrl2 := r_post
        have hpos := r_post1
        rcases r with ⟨a⟩ | ⟨e⟩ <;> simp only [bind_ok]
        · step
          simp only [Option.isSome_none]
          refine ⟨by omega, fun _ => ⟨⟨ring, kc1_post, by rw [hrl2, hring1]⟩, hpos a rfl⟩, by omega⟩
        · step
          simp only [Option.isSome_some, Option.isSome_none]
          refine ⟨by omega, ?_, ?_⟩
          · first | trivial | (intro h; cases h) | simp
          · simp only [ite_true, Bool.false_eq_true, ite_false]; omega
    · simp only [WP.spec_ok]; exact hF1
  · simp only; exact ⟨hk, fun _ => hF⟩

/-! ## Placing one session -/

/-- Mapping a new client to an empty slot below `unused`, and storing its
    session there, keeps `PInv`. -/
theorem PInv.add_session {kc : Core} (h : PInv kc) (slot : Usize) (s32 : U32)
    (hs32 : s32.val = slot.val) (hlt : slot.val < kc.unused.val)
    (hnone : kc.sessions.val[slot.val]? = some none) (S : Session)
    (im : table.IdMap) (hw : Wf im) (hmax : im.max_len = kc.clients.max_len)
    (hlk : lookup kc.clients.buckets.val (UScalar.cast .U64 S.client_id) = none)
    (hmap : ∀ k', lookup im.buckets.val k' =
      if k' = UScalar.cast .U64 S.client_id then some s32 else lookup kc.clients.buckets.val k') :
    PInv { kc with clients := im, sessions := kc.sessions.set slot (some S) } := by
  have hsl : slot.val < kc.sessions.val.length := by
    have := h.sess_len; have := h.unused_le; omega
  refine { h with clients_wf := hw, clients_max := by rw [hmax]; exact h.clients_max,
                  sess_len := by simp [alloc.vec.Vec.set_val_eq, h.sess_len],
                  slot_of := ?_, client_of := ?_ }
  · intro k s' hk
    simp only [slotOf, hmap] at hk
    split at hk
    · rename_i e; cases hk
      refine ⟨by simpa [hs32] using hlt, S, ?_, e.symm⟩
      simp [alloc.vec.Vec.set_val_eq, hs32, List.getElem?_set_self hsl]
    · obtain ⟨hlt', S', hS', hk'⟩ := h.slot_of k s' hk
      have hne : slot.val ≠ s'.val := by
        intro e; rw [← e, hnone] at hS'; cases hS'
      refine ⟨hlt', S', ?_, hk'⟩
      simp [alloc.vec.Vec.set_val_eq, List.getElem?_set_ne hne, hS']
  · intro j S' hS'
    simp only [alloc.vec.Vec.set_val_eq] at hS'
    by_cases e : slot.val = j
    · subst e
      rw [List.getElem?_set_self hsl] at hS'
      cases hS'
      exact ⟨s32, hs32, by simp [slotOf, hmap]⟩
    · rw [List.getElem?_set_ne e] at hS'
      obtain ⟨s', hs', hk'⟩ := h.client_of j S' hS'
      refine ⟨s', hs', ?_⟩
      have hne : UScalar.cast .U64 S'.client_id ≠ UScalar.cast .U64 S.client_id := by
        intro e'; simp only [slotOf] at hk'; rw [e', hlk] at hk'; cases hk'
      simp only [slotOf, hmap, ite_eq_right_iff, hne, ite_false]; exact hk'

/-! ## One session -/

/-- What restoring a session keeps, apart from the clients, sessions and ring. -/
def SessFrame (kc kc' : Core) : Prop :=
  kc'.cfg = kc.cfg ∧ kc'.unused = kc.unused ∧ kc'.keys = kc.keys ∧
    kc'.versions = kc.versions ∧ kc'.key_of_slot = kc.key_of_slot

theorem usize_u64_lt (x : Nat) (h : x ≤ Usize.max) : x < 2 ^ 64 := by
  have := usize_le_u64; have : U64.max = 2 ^ 64 - 1 := by simp [U64.max_def, U64.numBits]
  omega

theorem restore_session_spec (kc : Core) (b : Slice U8) (p : Usize) (hI : PInv kc) :
    restore.restore_session kc b p kc.cfg ⦃ r => ∀ a, r.1 = .Ok a →
      PInv r.2 ∧ r.2.clients.len.val = kc.clients.len.val + 1 ∧ SessFrame kc r.2 ∧
        a.val ≤ b.length ⦄ := by
  have hum := hI.unused_le
  have hmx := hI.clients_u32
  have hU32 : U32.max ≤ Usize.max := u32_le_usize
  have hU64 : Usize.max ≤ U64.max := usize_le_u64
  have hsl := hI.sess_len
  unfold restore.restore_session
  rd
  all_goals (try (simp only [WP.spec_ok]; intro _ h; cases h))
  step
  split
  · simp only [WP.spec_ok]; intro _ h; cases h
  rename_i slotv pa hpa hslt
  have hr : r.val = kc.unused.val := by
    rw [r_post, UScalar.cast_val_eq]; exact Nat.mod_eq_of_lt (usize_u64_lt _ (by omega))
  simp only [GE.ge, UScalar.le_equiv, not_le] at hslt
  step
  have hi1 : i1.val = slotv.val := by
    rw [i1_post, UScalar.cast_val_eq]; exact Nat.mod_eq_of_lt (usize_lt _ (by omega))
  step
  split
  · simp only [WP.spec_ok]; intro _ h; cases h
  rename_i hnone
  step
  repeat rd
  all_goals (try (simp only [WP.spec_ok]; intro _ h; cases h))
  rename_i cid a2 h2 inc a3 h3 nxt a4 h4 lan a5 h5
  step as ⟨ cid32, hcid32 ⟩
  step
  step with get_spec kc.clients i3 hI.clients_wf
  split
  · simp only [WP.spec_ok]; intro _ h; cases h
  rename_i hget
  step
  step
  step with insert_spec kc.clients i4 i5 hI.clients_wf as ⟨ b3, im, hins ⟩
  have hlk : lookup kc.clients.buckets.val i4 = none := by
    rw [i4_post, ← i3_post]; simp only [o1_post, Option.isSome_iff_ne_none, not_not] at hget; exact hget
  obtain ⟨hw, hmx', hrest⟩ := hins
  split
  swap
  · simp only [WP.spec_ok]; intro _ h; cases h
  rename_i hb3
  split at hrest
  · simp only [Prod.mk.injEq] at hrest; rw [hb3] at hrest; cases hrest.1
  obtain ⟨-, hmap, hlen⟩ := hrest
  simp only [ite_true] at hlen
  simp only at hmap
  step
  rd
  all_goals (try (simp only [WP.spec_ok]; intro _ h; cases h))
  rename_i nv a6 h6
  step
  split
  · simp only [WP.spec_ok]; intro _ h; cases h
  subst index_mut_back_post
  have hs1 : slot1.val = slotv.val := by
    rw [slot1_post, UScalar.cast_val_eq]; exact Nat.mod_eq_of_lt (usize_lt _ (by omega))
  have hs32 : i5.val = slot1.val := by
    rw [i5_post, UScalar.cast_val_eq]; apply Nat.mod_eq_of_lt
    have : U32.max = 2 ^ 32 - 1 := by simp [U32.max_def, U32.numBits]
    simp only [UScalarTy.numBits]; omega
  have hlt1 : slot1.val < kc.sessions.val.length := by omega
  have hnone' : kc.sessions.val[slot1.val]? = some none := by
    rw [List.getElem?_eq_getElem hlt1]
    have e : o = kc.sessions.val[slot1.val] := by rw [o_post]; simp [hi1, hs1]
    rw [← e]; cases o with
    | none => rfl
    | some _ => simp at hnone
  let S : Session := { client_id := cid32, incarnation := inc, next := nxt, last_active_ns := lan }
  have hP' := hI.add_session slot1 i5 hs32 (by omega) hnone' S im hw hmx'
    (by rw [i4_post] at hlk; exact hlk) (by intro k'; rw [hmap, i4_post])
  step with session_loop_spec { kc with clients := im, sessions := kc.sessions.set slot1 (some S) }
    b slot1 nv hP' (by simp; omega) _ a6 0#u64 (by simp) ⟨⟨_, rfl, rfl⟩, h6.2⟩
  cases err with
  | some e => simp only [WP.spec_ok]; intro _ h; cases h
  | none =>
    obtain ⟨⟨ring, rfl, hring⟩, hp7⟩ := kc1_post rfl
    simp only [WP.spec_ok]
    intro a h; cases h
    exact ⟨hP'.with_ring ring hring, hlen, ⟨rfl, rfl, rfl, rfl, rfl⟩, hp7⟩

/-! ## One key -/

/-- What restoring a key keeps, apart from the key tables. -/
def KeyFrame (kc kc' : Core) : Prop :=
  kc'.cfg = kc.cfg ∧ kc'.unused = kc.unused ∧ kc'.clients = kc.clients ∧
    kc'.sessions = kc.sessions ∧ kc'.ring = kc.ring

theorem restore_key_spec (kc : Core) (b : Slice U8) (p i : Usize) (hI : PInv kc)
    (hi : i.val = kc.keys.len.val) (himax : i.val < kc.cfg.max_keys.val) :
    restore.restore_key kc b p i ⦃ r => ∀ a, r.1 = .Ok a →
      PInv r.2 ∧ r.2.keys.len.val = kc.keys.len.val + 1 ∧ KeyFrame kc r.2 ∧ a.val ≤ b.length ⦄ := by
  have hk32 := hI.keys_u32
  have hU32 : U32.max ≤ Usize.max := u32_le_usize
  have hvl := hI.versions_len
  have hkl := hI.key_of_slot_len
  unfold restore.restore_key
  repeat rd
  all_goals (try (simp only [WP.spec_ok]; intro _ h; cases h))
  rename_i key a1 h1 ver a2 h2
  step with get_spec kc.keys key hI.keys_wf as ⟨ o, o_post ⟩
  cases o with
  | some _ => simp only [Option.isSome_some, ite_true, WP.spec_ok]; intro _ h; cases h
  | none =>
  have hlk : lookup kc.keys.buckets.val key = none := o_post.symm
  simp only [Option.isSome_none, Bool.false_eq_true, ite_false]
  step
  have hi1 : i1.val = i.val := by
    rw [i1_post, UScalar.cast_val_eq]; apply Nat.mod_eq_of_lt
    have : U32.max = 2 ^ 32 - 1 := by simp [U32.max_def, U32.numBits]
    simp only [UScalarTy.numBits]; omega
  step with insert_spec kc.keys key i1 hI.keys_wf as ⟨ b2, im, hins ⟩
  obtain ⟨hw, hmx', hrest⟩ := hins
  split
  swap
  · simp only [WP.spec_ok]; intro _ h; cases h
  rename_i hb2
  split at hrest
  · simp only [Prod.mk.injEq] at hrest; rw [hb2] at hrest; cases hrest.1
  obtain ⟨-, hmap, hlen⟩ := hrest
  simp only [ite_true] at hlen hmap
  step
  step
  try simp only [WP.spec_ok]
  intro a h; cases h
  refine ⟨{ hI with keys_wf := hw, keys_max := by rw [hmx']; exact hI.keys_max,
                    versions_len := ?_, key_of_slot_len := ?_, key_slot := ?_, key_of := ?_ },
          hlen, ⟨rfl, rfl, rfl, rfl, rfl⟩, h2.2⟩
  · simp [v_post, hlen, hvl]
  · simp [v1_post, hlen, hkl]
  · intro k j hk
    rw [hmap] at hk
    simp only [v_post, List.length_append, List.length_singleton]
    split at hk
    · cases hk; omega
    · have := hI.key_slot k j hk; omega
  · intro k j hk
    rw [hmap] at hk
    simp only [v1_post]
    split at hk
    · rename_i e; cases hk; subst e
      rw [List.getElem?_append_right (by omega)]; simp [hi1, hi, hkl]
    · have hj := hI.key_slot k j hk
      rw [List.getElem?_append_left (by omega)]; exact hI.key_of k j hk

/-! ## The loops -/

theorem sess_frame_trans {a b c : Core} (h : SessFrame a b) (h' : SessFrame b c) : SessFrame a c := by
  obtain ⟨h1, h2, h3, h4, h5⟩ := h; obtain ⟨h1', h2', h3', h4', h5'⟩ := h'
  exact ⟨h1'.trans h1, h2'.trans h2, h3'.trans h3, h4'.trans h4, h5'.trans h5⟩

theorem key_frame_trans {a b c : Core} (h : KeyFrame a b) (h' : KeyFrame b c) : KeyFrame a c := by
  obtain ⟨h1, h2, h3, h4, h5⟩ := h; obtain ⟨h1', h2', h3', h4', h5'⟩ := h'
  exact ⟨h1'.trans h1, h2'.trans h2, h3'.trans h3, h4'.trans h4, h5'.trans h5⟩

theorem sessions_loop_spec (kc0 : Core) (b : Slice U8) (sessions : U64)
    (kc : Core) (p : Usize) (k : U64) (hk : k.val ≤ sessions.val)
    (hP : PInv kc) (hlen : kc.clients.len.val = k.val) (hF : SessFrame kc0 kc) (hp : p.val ≤ b.length) :
    restore.Core.restore_loop0 kc0.cfg.max_clients kc0.cfg.outcome_ring kc0.cfg.max_keys
      kc0.cfg.evict_idle_ns b kc sessions p none k ⦃ r =>
        r.2.2 = none → PInv r.1 ∧ r.1.clients.len.val = sessions.val ∧ SessFrame kc0 r.1 ∧
          r.2.1.val ≤ b.length ⦄ := by
  unfold restore.Core.restore_loop0
  apply loop.spec_decr_nat
    (fun (x : Core × Usize × Option restore.SnapshotError × U64) =>
      sessions.val - x.2.2.2.val + (if x.2.2.1.isSome then 0 else 1))
    (fun x => x.2.2.2.val ≤ sessions.val ∧ (x.2.2.1 = none →
      PInv x.1 ∧ x.1.clients.len.val = x.2.2.2.val ∧ SessFrame kc0 x.1 ∧ x.2.1.val ≤ b.length))
  · rintro ⟨kc1, p1, err1, k1⟩ ⟨hk1, hI1⟩
    simp only at hk1 hI1
    unfold restore.Core.restore_loop0.body
    dsimp only
    split
    · rename_i hlt
      simp only [UScalar.lt_equiv] at hlt
      cases err1 with
      | some e =>
        simp only [core.option.Option.is_none, Option.isNone_some, Bool.false_eq_true, ite_false, WP.spec_ok]
        intro h; cases h
      | none =>
        simp only [core.option.Option.is_none, Option.isNone_none, ite_true]
        obtain ⟨hP1, hl1, hF1, hp1⟩ := hI1 rfl
        have hcfg : Config.mk kc0.cfg.max_clients kc0.cfg.outcome_ring kc0.cfg.max_keys
            kc0.cfg.evict_idle_ns = kc1.cfg := by rw [hF1.1]
        rw [hcfg]
        step with restore_session_spec kc1 b p1 hP1
        have hok := r_post
        rcases r with ⟨a⟩ | ⟨e⟩ <;> simp only [bind_ok]
        · obtain ⟨hP2, hl2, hF2, hp2⟩ := hok a rfl
          step
          simp only [Option.isSome_none]
          refine ⟨by omega, fun _ => ⟨hP2, by omega, sess_frame_trans hF1 hF2, hp2⟩, by omega⟩
        · step
          simp only [Option.isSome_some, Option.isSome_none]
          refine ⟨by omega, ?_, ?_⟩
          · first | trivial | (intro h; cases h) | simp
          · simp only [ite_true, Bool.false_eq_true, ite_false]; omega
    · rename_i hge
      simp only [UScalar.lt_equiv, not_lt] at hge
      simp only [WP.spec_ok]
      intro he
      obtain ⟨hP1, hl1, hF1, hp1⟩ := hI1 he
      exact ⟨hP1, by omega, hF1, hp1⟩
  · simp only; exact ⟨hk, fun _ => ⟨hP, hlen, hF, hp⟩⟩

theorem keys_loop_spec (kc0 : Core) (b : Slice U8) (keys : U64)
    (hkm : keys.val ≤ kc0.cfg.max_keys.val)
    (kc : Core) (p i : Usize) (hi : i.val ≤ keys.val)
    (hP : PInv kc) (hlen : kc.keys.len.val = i.val) (hF : KeyFrame kc0 kc) (hp : p.val ≤ b.length) :
    restore.Core.restore_loop1 b kc none keys p i ⦃ r =>
        r.2.1 = none → PInv r.1 ∧ r.1.keys.len.val = keys.val ∧ KeyFrame kc0 r.1 ∧
          r.2.2.val ≤ b.length ⦄ := by
  have hU32 : U32.max ≤ Usize.max := u32_le_usize
  have hU64 : Usize.max ≤ U64.max := usize_le_u64
  have hk32 : kc0.cfg.max_keys.val ≤ U32.max := by rw [← hF.1]; exact hP.keys_u32
  unfold restore.Core.restore_loop1
  apply loop.spec_decr_nat
    (fun (x : Core × Option restore.SnapshotError × Usize × Usize) =>
      keys.val - x.2.2.2.val + (if x.2.1.isSome then 0 else 1))
    (fun x => x.2.2.2.val ≤ keys.val ∧ (x.2.1 = none →
      PInv x.1 ∧ x.1.keys.len.val = x.2.2.2.val ∧ KeyFrame kc0 x.1 ∧ x.2.2.1.val ≤ b.length))
  · rintro ⟨kc1, err1, p1, i1⟩ ⟨hi1, hI1⟩
    simp only at hi1 hI1
    unfold restore.Core.restore_loop1.body
    dsimp only
    step as ⟨ ic, ic_post ⟩
    have hc : ic.val = i1.val := by
      rw [ic_post, UScalar.cast_val_eq]; exact Nat.mod_eq_of_lt (by
        have := usize_u64_lt i1.val (by omega); simpa [UScalarTy.numBits] using this)
    split
    · rename_i hlt
      simp only [UScalar.lt_equiv, hc] at hlt
      cases err1 with
      | some e =>
        simp only [core.option.Option.is_none, Option.isNone_some, Bool.false_eq_true, ite_false, WP.spec_ok]
        intro h; cases h
      | none =>
        simp only [core.option.Option.is_none, Option.isNone_none, ite_true]
        obtain ⟨hP1, hl1, hF1, hp1⟩ := hI1 rfl
        have hmax1 : i1.val < kc1.cfg.max_keys.val := by rw [hF1.1]; omega
        step with restore_key_spec kc1 b p1 i1 hP1 hl1.symm hmax1
        have hok := r_post
        rcases r with ⟨a⟩ | ⟨e⟩ <;> simp only [bind_ok]
        · obtain ⟨hP2, hl2, hF2, hp2⟩ := hok a rfl
          step
          simp only [Option.isSome_none]
          refine ⟨by omega, fun _ => ⟨hP2, by omega, key_frame_trans hF1 hF2, hp2⟩, by omega⟩
        · step
          simp only [Option.isSome_some, Option.isSome_none]
          refine ⟨by omega, ?_, ?_⟩
          · first | trivial | (intro h; cases h) | simp
          · simp only [ite_true, Bool.false_eq_true, ite_false]; omega
    · rename_i hge
      simp only [UScalar.lt_equiv, not_lt, hc] at hge
      simp only [WP.spec_ok]
      intro he
      obtain ⟨hP1, hl1, hF1, hp1⟩ := hI1 he
      exact ⟨hP1, by omega, hF1, hp1⟩
  · simp only; exact ⟨hi, fun _ => ⟨hP, hlen, hF, hp⟩⟩

/-! ## The whole snapshot -/

theorem lookup_empty {m : table.IdMap} (hw : Wf m) (h0 : m.len.val = 0) (k : U64) :
    lookup m.buckets.val k = none := by
  cases hl : lookup m.buckets.val k with
  | none => rfl
  | some s =>
    obtain ⟨j, b, hj, -, -⟩ := lookup_some_mem hl
    have hc := hw.len_eq; rw [h0] at hc
    have hmem : some b ∈ m.buckets.val := List.mem_of_getElem? hj
    have : 0 < m.buckets.val.countP Option.isSome := List.countP_pos_iff.mpr ⟨_, hmem, rfl⟩
    omega

/-- A fresh core (no clients) with its scalars set and `unused` at most the
    table size: what restoring starts from. -/
theorem PInv.reset {kc : Core} (hI : Inv kc) (hu0 : kc.unused.val = 0)
    (applied next now floor : U64) (u : Usize) (hu : u.val ≤ kc.cfg.max_clients.val) :
    PInv { kc with applied := applied, next_seqno := next, now_ns := now,
                   incarnation_floor := floor, unused := u } := by
  have hc0 : kc.clients.len.val = 0 := by rw [hI.clients_len, hu0]
  have hnone : ∀ k, slotOf kc k = none := fun k => lookup_empty hI.clients_wf hc0 k
  refine
    { clients_pos := hI.clients_pos, clients_u32 := hI.clients_u32, ring_pos := hI.ring_pos,
      ring_cap := hI.ring_cap, keys_u32 := hI.keys_u32, clients_wf := hI.clients_wf,
      clients_max := hI.clients_max, keys_wf := hI.keys_wf, keys_max := hI.keys_max,
      sess_len := hI.sess_len, ring_len := hI.ring_len, unused_le := hu,
      slot_of := ?_, client_of := ?_,
      versions_len := hI.versions_len, key_of_slot_len := hI.key_of_slot_len,
      key_slot := hI.key_slot, key_of := hI.key_of }
  · intro k slot hk
    have := hnone k; simp only [slotOf] at hk this; rw [this] at hk; cases hk
  · intro j S hS
    obtain ⟨slot, -, hk⟩ := hI.client_of j S hS
    rw [hnone] at hk; cases hk

/-- **Restoring a snapshot.** Whatever the bytes, `Core::restore` never
    panics and either rejects them or returns a core satisfying `Inv`, with
    the configuration it was asked for: from there `Core::apply` refines the
    abstract sequencer (`apply_refines`). The configuration conditions are
    `new_inv`'s. -/
theorem restore_inv (cfg : Config) (h1 : 1 ≤ cfg.max_clients.val) (h2 : cfg.max_clients.val ≤ U32.max)
    (h3 : 1 ≤ cfg.outcome_ring.val) (h4 : cfg.max_keys.val ≤ U32.max)
    (h5 : cfg.max_clients.val * cfg.outcome_ring.val ≤ Usize.max)
    (h6 : 4 * cfg.max_clients.val ≤ Usize.max) (h7 : 4 * cfg.max_keys.val ≤ Usize.max)
    (bytes : Slice U8) :
    restore.Core.restore cfg bytes ⦃ r => ∀ c, r = .Ok c → Inv c ∧ c.cfg = cfg ⦄ := by
  have hU32 : U32.max ≤ Usize.max := u32_le_usize
  have hU64 : Usize.max ≤ U64.max := usize_le_u64
  have hu64 : ∀ x : Usize, (UScalar.cast .U64 x).val = x.val := fun x => by
    rw [UScalar.cast_val_eq]; exact Nat.mod_eq_of_lt (by
      have := usize_u64_lt x.val (by scalar_tac)
      simpa [UScalarTy.numBits] using this)
  unfold restore.Core.restore
  -- Header: magic, version, configuration.
  rd
  all_goals (try rej)
  rdu
  all_goals (try rej)
  spec_split
  · rej
  step
  spec_split
  · rej
  repeat rd
  all_goals (try rej)
  step
  spec_split
  · rej
  step
  spec_split
  · rej
  step
  spec_split
  · rej
  spec_split
  · rej
  -- A fresh core, then the scalars.
  step with new_inv cfg h1 h2 h3 h4 h5 h6 h7 as ⟨ kc, hInv, hcfg, happ, hnext, hun0, hkl0 ⟩
  repeat rd
  all_goals (try rej)
  rename_i ap a6 hr6 nx a7 hr7 nw a8 hr8 fl a9 hr9 un a10 hr10 ss a11 hr11
  step as ⟨ i4, i4_post ⟩
  have hi4 : i4.val = cfg.max_clients.val := by rw [i4_post]; exact hu64 _
  spec_split
  · rej
  rename_i hunle
  simp only [GT.gt, UScalar.lt_equiv, not_lt, hi4] at hunle
  spec_split
  · rej
  step as ⟨ i5, i5_post ⟩
  have hi5 : i5.val = un.val := by
    rw [i5_post, UScalar.cast_val_eq]; exact Nat.mod_eq_of_lt (usize_lt _ (by
      have := h2; have := hU32; omega))
  spec_split
  · rej
  rename_i hss
  have hss' : ss = un := by rw [UScalar.eq_equiv]; simpa using hss
  subst hss'
  -- The sessions.
  have hP0 := PInv.reset hInv hun0 ap nx nw fl i5 (by rw [hcfg]; omega)
  have hc0 : kc.clients.len.val = 0 := by rw [hInv.clients_len, hun0]
  rw [← hcfg]
  step with sessions_loop_spec
      { kc with applied := ap, next_seqno := nx, now_ns := nw, incarnation_floor := fl, unused := i5 }
      bytes ss _ a11 0#u64 (by simp) hP0 (by simpa using hc0) ⟨rfl, rfl, rfl, rfl, rfl⟩ hr11.2
      as ⟨ kc1, at12, err, hloop ⟩
  cases err with
  | some e => rej
  | none =>
  obtain ⟨hP1, hl1, hF1, hp1⟩ := hloop rfl
  -- The keys.
  rd
  all_goals (try rej)
  rename_i ks a13 hr13
  step as ⟨ i6, i6_post ⟩
  have hi6 : i6.val = kc.cfg.max_keys.val := by rw [i6_post]; exact hu64 _
  spec_split
  · rej
  rename_i hksle
  simp only [GT.gt, UScalar.lt_equiv, not_lt, hi6] at hksle
  have hcfg1 : kc1.cfg = kc.cfg := hF1.1
  have hkl1 : kc1.keys.len.val = 0 := by rw [hF1.2.2.1]; exact hkl0
  step with keys_loop_spec kc1 bytes ks (by rw [hcfg1]; exact hksle) kc1 a13 0#usize (by simp)
      hP1 (by simpa using hkl1) ⟨rfl, rfl, rfl, rfl, rfl⟩ hr13.2 as ⟨ kc2, err1, at14, hloop2 ⟩
  cases err1 with
  | some e => rej
  | none =>
  obtain ⟨hP2, hl2, hF2, hp2⟩ := hloop2 rfl
  simp only []
  spec_split
  · rej
  simp only [WP.spec_ok]
  intro c h
  cases h
  have hcl : kc2.clients.len.val = kc2.unused.val := by
    rw [hF2.2.2.1, hF2.2.1, hl1, hF1.2.1]; simp [hi5]
  exact ⟨hP2.toInv hcl, by rw [hF2.1, hcfg1, hcfg]⟩

#print axioms restore_inv

end krusty_core.restore_proof
