/-
  `krusty-core` refines the abstract sequencer (`Spec.lean`): under an
  invariant on `Core`, `Core::apply` never panics, keeps the invariant, and
  its effects and new state are those of a `spec.Step`.
-/
import KrustyCore.IdMap
import KrustyCore.Spec

open Aeneas Aeneas.Std Result
open krusty_core krusty_core.idmap

namespace krusty_core.refine

/-! ## Abstraction -/

def αcfg (c : Config) : spec.Cfg :=
  ⟨c.max_clients.val, c.outcome_ring.val, c.max_keys.val, c.evict_idle_ns.val⟩

def αdec : Decided → spec.Dec
  | .Accepted g => .acc g.val
  | .Rejected k v => .rej k.val v.val

def αout : krusty_wire.feedback.Outcome → spec.Out
  | .Accepted g => .acc g.val
  | .Rejected k v => .rej k.val v.val
  | .Nak => .nak
  | .Busy us => if us.val = 0 then .busy else .other
  | .NotLeader _ => .other
  | .SessionExpired m => .expired m.val

def αeff (e : Effects) : spec.Eff :=
  ⟨e.publish.map fun p => ⟨p.seqno.val, p.meta.timestamp_ns.val, p.meta.client_id.val,
      p.meta.conditional.map fun kv => (kv.1.val, kv.2.val)⟩,
   e.reply.map fun f => ⟨f.client_id.val, f.incarnation.val, f.client_seqno.val, αout f.outcome⟩⟩

def αreq (r : Request) : spec.Req :=
  ⟨UScalar.cast .U64 r.client_id, r.incarnation.val, r.client_seqno.val,
   match r.append with
   | .Unconditional => none
   | .Conditional k v => some (k, v.val)⟩

def αsess (S : Session) : spec.Sess := ⟨S.incarnation.val, S.next.val, S.last_active_ns.val⟩

/-- The session slot the client table maps `k` to. -/
def slotOf (c : Core) (k : U64) : Option U32 := lookup c.clients.buckets.val k

def sessA (c : Core) (k : U64) : Option spec.Sess :=
  match slotOf c k with
  | some slot =>
    match c.sessions.val[slot.val]? with
    | some (some S) => some (αsess S)
    | _ => none
  | none => none

def memA (c : Core) (k : U64) (idx : Nat) : Option (Nat × spec.Dec) :=
  match slotOf c k with
  | some slot =>
    if idx < c.cfg.outcome_ring.val then
      match c.ring.val[slot.val * c.cfg.outcome_ring.val + idx]? with
      | some (some rs) => some (rs.client_seqno.val, αdec rs.decided)
      | _ => none
    else none
  | none => none

def verA (c : Core) (k : U64) : Nat :=
  match lookup c.keys.buckets.val k with
  | some i => match c.versions.val[i.val]? with
    | some v => v.val
    | none => 0
  | none => 0

def α (c : Core) : spec.St where
  applied := c.applied.val
  now := c.now_ns.val
  floor := c.incarnation_floor.val
  seq := c.next_seqno.val
  sess := sessA c
  nclients := c.clients.len.val
  mem := memA c
  ver := verA c
  known := fun k => (lookup c.keys.buckets.val k).isSome
  nkeys := c.keys.len.val

/-! ## The invariant -/

structure Inv (c : Core) : Prop where
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
  clients_len : c.clients.len.val = c.unused.val
  /-- A mapped client's slot is in use and holds its session. -/
  slot_of : ∀ k slot, slotOf c k = some slot →
    slot.val < c.unused.val ∧ ∃ S, c.sessions.val[slot.val]? = some (some S) ∧
      UScalar.cast .U64 S.client_id = k
  /-- Every session is mapped from its client. -/
  client_of : ∀ (j : Nat) S, c.sessions.val[j]? = some (some S) →
    ∃ slot : U32, slot.val = j ∧ slotOf c (UScalar.cast .U64 S.client_id) = some slot
  versions_len : c.versions.val.length = c.keys.len.val
  key_of_slot_len : c.key_of_slot.val.length = c.keys.len.val
  key_slot : ∀ k (i : U32), lookup c.keys.buckets.val k = some i → i.val < c.versions.val.length
  /-- `key_of_slot` names each version slot's key (so slots are not shared). -/
  key_of : ∀ k (i : U32), lookup c.keys.buckets.val k = some i → c.key_of_slot.val[i.val]? = some k

/-! ## Small pieces -/

theorem version_spec (c : Core) (key : U64) (hI : Inv c) :
    Core.version c key ⦃ v => v.val = verA c key ⦄ := by
  unfold Core.version
  apply WP.spec_bind (get_spec c.keys key hI.keys_wf)
  rintro o rfl
  unfold verA
  cases hk : lookup c.keys.buckets.val key with
  | none => simp
  | some i =>
    have hi := hI.key_slot key i hk
    have hlt : i.val < 2 ^ System.Platform.numBits := by
      have := hI.keys_u32; have := usize_lt i.val (by
        have := alloc.vec.Vec.len_ineq c.versions; omega); exact this
    have hc : (UScalar.cast .Usize i).val = i.val := by
      rw [UScalar.cast_val_eq]; exact Nat.mod_eq_of_lt hlt
    simp only [lift, bind_ok]
    rw [vidx _ _ (by rw [hc]; exact hi)]
    simp only [WP.spec_ok, hc, List.getElem?_eq_getElem hi]

theorem ring_index_spec (c : Core) (slot : U32) (q : U64) (hI : Inv c)
    (hslot : slot.val < c.cfg.max_clients.val) :
    Core.ring_index c slot q ⦃ i => i.val = slot.val * c.cfg.outcome_ring.val +
      q.val % c.cfg.outcome_ring.val ∧ i.val < c.ring.val.length ⦄ := by
  unfold Core.ring_index
  have hcap := hI.ring_cap
  have hpos := hI.ring_pos
  have hu := usize_le_u64
  have hR : c.cfg.outcome_ring.val ≤ Usize.max := by
    have := Nat.le_mul_of_pos_left c.cfg.outcome_ring.val (n := c.cfg.max_clients.val) (by have := hI.clients_pos; omega)
    omega
  have hs : (UScalar.cast .Usize slot).val = slot.val := by
    rw [UScalar.cast_val_eq]; exact Nat.mod_eq_of_lt (usize_lt _ (by
      have := hI.clients_u32; have := hI.clients_pos
      have : c.cfg.max_clients.val ≤ Usize.max := by
        have := Nat.le_mul_of_pos_right c.cfg.max_clients.val (m := c.cfg.outcome_ring.val) (by omega)
        omega
      omega))
  have hmul : slot.val * c.cfg.outcome_ring.val + c.cfg.outcome_ring.val ≤ Usize.max := by
    have : (slot.val + 1) * c.cfg.outcome_ring.val ≤ c.cfg.max_clients.val * c.cfg.outcome_ring.val :=
      Nat.mul_le_mul_right _ (by omega)
    rw [Nat.add_mul, Nat.one_mul] at this; omega
  step*
  subst_vars
  have h2 : (UScalar.cast UScalarTy.U64 c.cfg.outcome_ring).val = c.cfg.outcome_ring.val := by
    rw [UScalar.cast_val_eq]; exact Nat.mod_eq_of_lt (by
      have : Usize.max ≤ 2 ^ 64 - 1 := by simpa [U64.max_def, U64.numBits] using hu
      simp; omega)
  have h3 : i3.val < c.cfg.outcome_ring.val := by rw [i3_post, h2]; exact Nat.mod_lt _ (by omega)
  have h4 : (UScalar.cast UScalarTy.Usize i3).val = i3.val := by
    rw [UScalar.cast_val_eq]; exact Nat.mod_eq_of_lt (usize_lt _ (by omega))
  have h1 : i1.val = slot.val * c.cfg.outcome_ring.val := by rw [i1_post, hs]
  refine ⟨by rw [i_post, h1, h4, i3_post, h2], ?_⟩
  rw [i_post, h1, h4, hI.ring_len]
  have : (slot.val + 1) * c.cfg.outcome_ring.val ≤ c.cfg.max_clients.val * c.cfg.outcome_ring.val :=
    Nat.mul_le_mul_right _ (by omega)
  rw [Nat.add_mul, Nat.one_mul] at this; omega

theorem u64max : U64.max = 2 ^ 64 - 1 := by simp [U64.max_def, U64.numBits]

theorem u64_lt (x : U64) : x.val < 2 ^ 64 := by
  have := x.hBounds; simpa [UScalarTy.numBits] using this

theorem vidxu {α : Type} (v : alloc.vec.Vec α) (i : Usize) (h : i.val < v.val.length) :
    alloc.vec.Vec.index_usize v i = ok v.val[i.val] := by
  simp [alloc.vec.Vec.index_usize, List.getElem?_eq_getElem h]

theorem getElem?_some_lt' {α : Type} {l : List α} {j : Nat} {o : α} (h : l[j]? = some o) :
    j < l.length := by
  rcases Nat.lt_or_ge j l.length with h' | h'
  · exact h'
  · rw [List.getElem?_eq_none h'] at h; cases h

theorem Inv.key_inj {c : Core} (hI : Inv c) {k k' : U64} {i : U32}
    (h : lookup c.keys.buckets.val k = some i) (h' : lookup c.keys.buckets.val k' = some i) : k = k' := by
  have := hI.key_of k i h; rw [hI.key_of k' i h'] at this; cases this; rfl

theorem u32_lt_usize (x : Nat) (h : x ≤ U32.max) : x < 2 ^ System.Platform.numBits := by
  have : U32.max = 2 ^ 32 - 1 := by simp [U32.max_def, U32.numBits]
  have := System.Platform.numBits_eq
  have : 2 ^ 32 ≤ 2 ^ System.Platform.numBits := by
    rcases this with h | h <;> rw [h] <;> decide
  omega

theorem u32_le_usize : U32.max ≤ Usize.max := by
  have := u32_lt_usize U32.max (le_refl _)
  have : Usize.max = 2 ^ System.Platform.numBits - 1 := by simp [Usize.max_def, Usize.numBits]
  omega

theorem Inv.with_versions {c : Core} (hI : Inv c) (vs : alloc.vec.Vec U64)
    (h : vs.val.length = c.versions.val.length) : Inv { c with versions := vs } := by
  obtain ⟨a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19⟩ := hI
  exact ⟨a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, h ▸ a16, a17,
    fun k i hk => h ▸ a18 k i hk, a19⟩

/-- What `set_version` does: a version slot for a known key, or a new slot if
    the key table has room. Only the key table and versions change. -/
def SetVersionPost (c : Core) (key version : U64) (b : Bool) (c1 : Core) : Prop :=
  b = ((lookup c.keys.buckets.val key).isSome || decide (c.keys.len.val < c.cfg.max_keys.val)) ∧
  (∃ ks vs kos, c1 = { c with keys := ks, versions := vs, key_of_slot := kos }) ∧
  Inv c1 ∧
  if b then
    verA c1 = spec.upd (verA c) key version.val ∧
    (fun k => (lookup c1.keys.buckets.val k).isSome) =
      spec.upd (fun k => (lookup c.keys.buckets.val k).isSome) key true ∧
    c1.keys.len.val = if (lookup c.keys.buckets.val key).isSome then c.keys.len.val else c.keys.len.val + 1
  else c1 = c

theorem set_version_spec (c : Core) (key version : U64) (hI : Inv c) :
    Core.set_version c key version ⦃ r => SetVersionPost c key version r.1 r.2 ⦄ := by
  unfold Core.set_version
  apply WP.spec_bind (get_spec c.keys key hI.keys_wf)
  rintro o rfl
  cases hk : lookup c.keys.buckets.val key with
  | some i =>
    have hi := hI.key_slot key i hk
    have hc : (UScalar.cast .Usize i).val = i.val := by
      rw [UScalar.cast_val_eq]; exact Nat.mod_eq_of_lt (u32_lt_usize _ (by
        have := alloc.vec.Vec.len_ineq c.versions; have := hI.keys_u32
        exact Nat.le_trans (Nat.le_of_lt_succ (Nat.lt_succ_of_le (le_refl _))) (by
          have : i.val ≤ U32.max := by have := i.hBounds; simp [U32.max_def, U32.numBits] at *; omega
          exact this)))
    step*
    subst_vars
    have hset : (c.versions.set (UScalar.cast .Usize i) version).val.length = c.versions.val.length := by
      simp
    refine ⟨by simp [hk], ⟨_, _, _, rfl⟩, hI.with_versions _ hset, ?_⟩
    simp only [ite_true]
    refine ⟨?_, ?_, by simp [hk]⟩
    · funext k
      unfold verA spec.upd
      simp only
      by_cases e : k = key
      · subst e; rw [hk]; simp [alloc.vec.Vec.set_val_eq, hc, List.getElem?_set_self hi]
      · simp only [e, ite_false]
        cases hk' : lookup c.keys.buckets.val k with
        | none => rfl
        | some j =>
          have hji : i.val ≠ j.val := by
            intro hij
            have : i = j := (UScalar.eq_equiv i j).mpr hij
            subst this; exact e (hI.key_inj hk' hk)
          simp [alloc.vec.Vec.set_val_eq, hc, List.getElem?_set_ne hji]
    · funext k; unfold spec.upd; by_cases e : k = key
      · subst e; simp [hk]
      · simp [e]
  | none =>
    simp only [table.IdMap.is_full, bind_ok]
    split
    · rename_i hfull
      simp only [WP.spec_ok, SetVersionPost, hk, Option.isSome_none, Bool.false_or, ite_false]
      have := hI.keys_max
      refine ⟨by simp at hfull ⊢; rw [this] at hfull; omega, ⟨_, _, _, rfl⟩, hI, ?_⟩
      split <;> simp_all
    · rename_i hfull
      have hkm := hI.keys_max
      have hlt : c.keys.len.val < c.cfg.max_keys.val := by simp at hfull; rw [hkm] at hfull; omega
      have hu := u32_le_usize
      have hku := hI.keys_u32
      have hvl := hI.versions_len
      have hkl := hI.key_of_slot_len
      have h1 : c.versions.val.length < Usize.max := by omega
      have h2 : c.key_of_slot.val.length < Usize.max := by omega
      have hcast : (UScalar.cast .U32 (alloc.vec.Vec.len c.versions)).val = c.versions.val.length := by
        rw [UScalar.cast_val_eq]; simp only [alloc.vec.Vec.len_val]
        exact Nat.mod_eq_of_lt (by have : U32.max = 2 ^ 32 - 1 := by simp [U32.max_def, U32.numBits]
                                   simp; omega)
      step*
      step with insert_spec c.keys key i1 hI.keys_wf as ⟨ b1, im, hpost ⟩
      obtain ⟨hwf, hmax, hrest⟩ := hpost
      rw [ite_eq_right_of_eq_false _ _ (eq_false (by rw [hkm] at *; simp only at *; omega))] at hrest
      simp only at hwf hmax hrest
      obtain ⟨rfl, hlk, hlen⟩ := hrest
      rw [hk] at hlen; simp only [ite_true] at hlen
      have hi1 : i1.val = c.versions.val.length := by rw [i1_post]; exact hcast
      have hlk' : ∀ k, lookup im.buckets.val k = if k = key then some i1 else lookup c.keys.buckets.val k :=
        hlk
      refine ⟨by simp [hk, hlt], ⟨_, _, _, rfl⟩, ?_, ?_⟩
      · obtain ⟨a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19⟩ := hI
        refine ⟨a1, a2, a3, a4, a5, a6, a7, hwf, by rw [hmax, a9], a10, a11, a12, a13, a14, a15,
          by simp only; rw [v_post, hlen]; simp; omega, by simp only; rw [v1_post, hlen]; simp; omega,
          ?_, ?_⟩
        · intro k j hj; simp only at hj ⊢; rw [hlk'] at hj; rw [v_post]; simp only [List.length_append]
          split at hj
          · cases hj; rw [hi1]; simp
          · have := a18 k j hj; omega
        · intro k j hj; simp only at hj ⊢; rw [hlk'] at hj; rw [v1_post]
          split at hj
          · rename_i e; subst e; cases hj; rw [hi1, hvl, ← hkl]
            simp
          · have := a19 k j hj
            have hj' : j.val < c.key_of_slot.val.length := by
              have := a18 k j hj; omega
            rw [List.getElem?_append_left hj']; exact this
      · refine ⟨?_, ?_, by simp only; rw [hlen]; simp [hk]⟩
        · funext k
          unfold verA spec.upd
          simp only [hlk', v_post]
          by_cases e : k = key
          · subst e; simp [hi1]
          · simp only [e, ite_false]
            cases hk' : lookup c.keys.buckets.val k with
            | none => rfl
            | some j =>
              have := hI.key_slot k j hk'
              simp [List.getElem?_append_left this]
        · funext k; unfold spec.upd; simp only [hlk']
          by_cases e : k = key <;> simp [e]

theorem cast_u32_u64 (x : U32) : (UScalar.cast .U64 x).val = x.val := by
  rw [UScalar.cast_val_eq]; exact Nat.mod_eq_of_lt (by have := x.hBounds; simp [U32.numBits] at *; omega)

/-- `α` after a change to the key table only. -/
theorem α_keys (c : Core) (ks : table.IdMap) (vs kos : alloc.vec.Vec U64) :
    α { c with keys := ks, versions := vs, key_of_slot := kos } =
      { α c with ver := verA { c with keys := ks, versions := vs, key_of_slot := kos },
                 known := fun k => (lookup ks.buckets.val k).isSome, nkeys := ks.len.val } := rfl

def αpub (seqno : U64) (m : krusty_wire.stream.MessageMeta) : spec.Pub :=
  ⟨seqno.val, m.timestamp_ns.val, m.client_id.val, m.conditional.map fun kv => (kv.1.val, kv.2.val)⟩

/-- `decide_append` is the spec's `decideAppend`. -/
def DecideAppendPost (c : Core) (r : Request)
    (x : (Option Decided × Option krusty_wire.stream.MessageMeta) × Core) : Prop :=
  let sp := spec.decideAppend (αcfg c.cfg) (α c) (αreq r)
  x.1.1.map αdec = sp.1 ∧ x.1.2.map (αpub c.next_seqno) = sp.2.1 ∧ α x.2 = sp.2.2 ∧ Inv x.2 ∧
  ∃ ks vs kos, x.2 = { c with keys := ks, versions := vs, key_of_slot := kos }

theorem decide_append_spec (c : Core) (r : Request) (hI : Inv c) :
    Core.decide_append c r ⦃ x => DecideAppendPost c r x ⦄ := by
  unfold Core.decide_append
  have hmax : (core.num.U64.MAX).val = spec.MAX := by simp [core.num.U64.MAX, U64.rMax, spec.MAX]
  rcases ha : r.append with _ | ⟨key, ev⟩
  · simp only [WP.spec_ok, DecideAppendPost]
    refine ⟨?_, ?_, ?_, hI, c.keys, c.versions, c.key_of_slot, rfl⟩ <;>
      simp [spec.decideAppend, αreq, ha, αdec, αpub, α, cast_u32_u64]
  · simp only
    apply WP.spec_bind (version_spec c key hI)
    intro cur hcur
    have hd : spec.decideAppend (αcfg c.cfg) (α c) (αreq r) =
        let cur' := verA c key
        if cur' ≠ ev.val ∨ cur' = spec.MAX then (some (.rej key.val cur'), none, α c)
        else if (lookup c.keys.buckets.val key).isSome ∨ c.keys.len.val < c.cfg.max_keys.val then
          (some (.acc c.next_seqno.val),
            some ⟨c.next_seqno.val, c.now_ns.val, r.client_id.val, some (key.val, cur' + 1)⟩,
            { α c with ver := spec.upd (α c).ver key (cur' + 1),
                       known := spec.upd (α c).known key true,
                       nkeys := if (lookup c.keys.buckets.val key).isSome then c.keys.len.val
                                else c.keys.len.val + 1 })
        else (none, none, α c) := by
      simp [spec.decideAppend, αreq, ha, α, αcfg, cast_u32_u64]
      split_ifs <;> simp_all
    unfold DecideAppendPost
    rw [hd, ← hcur]
    by_cases hne : cur.val = ev.val
    · have hce : cur = ev := (UScalar.eq_equiv _ _).mpr hne
      have hb : (cur != ev) = false := by simp [hce]
      simp only [hb, Bool.false_eq_true, ite_false]
      by_cases hm : cur.val = spec.MAX
      · have hcm : cur = core.num.U64.MAX := (UScalar.eq_equiv _ _).mpr (by rw [hmax]; exact hm)
        simp only [hcm, ite_true, WP.spec_ok]
        rw [← hcm]
        simp only [hm, or_true, ite_true]
        refine ⟨?_, ?_, ?_, hI, c.keys, c.versions, c.key_of_slot, rfl⟩ <;> simp [αdec, hm]
      · have hcm : cur ≠ core.num.U64.MAX := fun e => hm (by rw [e, hmax])
        simp only [hcm, ite_false, hne, hm, not_true, false_or, ite_false]
        have hlt : cur.val + 1 ≤ U64.max := by
          have : cur.val ≤ U64.max := by scalar_tac
          simp [spec.MAX, U64.max_def, U64.numBits] at hm this ⊢; omega
        step*
        step with set_version_spec c key i hI as ⟨ b, c1, hpost ⟩
        obtain ⟨hb1, ⟨ks, vs, kos, rfl⟩, hI1, hpost⟩ := hpost
        have hev : ¬ (ev.val ≠ ev.val ∨ ev.val = spec.MAX) := by simp; simpa [hce] using hm
        simp only [hev, ite_false]
        cases b with
        | true =>
          simp only [ite_true] at hpost ⊢
          obtain ⟨hver, hknown, hlen⟩ := hpost
          have hcond : (lookup c.keys.buckets.val key).isSome = true ∨ c.keys.len.val < c.cfg.max_keys.val := by
            simpa using hb1.symm
          simp only [hcond, ite_true, WP.spec_ok]
          refine ⟨by simp [αdec], by simp [αpub, i_post, hne], ?_, hI1, ks, vs, kos, rfl⟩
          rw [α_keys]
          simp only [α] at hknown ⊢
          rw [hver, hknown, hlen, i_post, hne]
        | false =>
          simp only [Bool.false_eq_true, ite_false] at hpost ⊢
          have hcond : ¬ ((lookup c.keys.buckets.val key).isSome = true ∨ c.keys.len.val < c.cfg.max_keys.val) := by
            simpa using hb1.symm
          simp only [hcond, ite_false, WP.spec_ok]
          rw [hpost]
          exact ⟨rfl, rfl, rfl, hI, c.keys, c.versions, c.key_of_slot, rfl⟩
    · have hce : cur ≠ ev := fun e => hne (by rw [e])
      have hb : (cur != ev) = true := by simp [hce]
      simp only [hb, ite_true, WP.spec_ok, hne, not_false_eq_true, true_or]
      refine ⟨?_, ?_, ?_, hI, c.keys, c.versions, c.key_of_slot, rfl⟩ <;> simp [αdec, hne]

/-! ## Session and ring updates -/

theorem u32_eq {a b : U32} (h : a.val = b.val) : a = b := (UScalar.eq_equiv a b).mpr h

theorem Inv.slot_inj {c : Core} (hI : Inv c) {k k' : U64} {slot : U32}
    (h : slotOf c k = some slot) (h' : slotOf c k' = some slot) : k = k' := by
  obtain ⟨-, S, hS, hk⟩ := hI.slot_of k slot h
  obtain ⟨-, S', hS', hk'⟩ := hI.slot_of k' slot h'
  rw [hS] at hS'; cases hS'; rw [← hk, ← hk']

theorem block_ne {R s s' i j : Nat} (hs : s ≠ s') (hi : i < R) (hj : j < R) : s * R + i ≠ s' * R + j := by
  intro e
  have h1 : (s * R + i) / R = s := by
    rw [Nat.add_comm, Nat.add_mul_div_right _ _ (by omega), Nat.div_eq_of_lt hi, Nat.zero_add]
  have h2 : (s' * R + j) / R = s' := by
    rw [Nat.add_comm, Nat.add_mul_div_right _ _ (by omega), Nat.div_eq_of_lt hj, Nat.zero_add]
  rw [e, h2] at h1; exact hs h1.symm

theorem block_lt {R n s i : Nat} (hs : s < n) (hi : i < R) : s * R + i < n * R := by
  have : (s + 1) * R ≤ n * R := Nat.mul_le_mul_right _ hs
  rw [Nat.add_mul, Nat.one_mul] at this; omega

/-- Writing client `k`'s session (in its own slot). -/
theorem sessA_set {c : Core} (hI : Inv c) {k : U64} {slot : U32} (hk : slotOf c k = some slot)
    (S : Session) :
    sessA { c with sessions := c.sessions.set (UScalar.cast .Usize slot) (some S) } =
      spec.upd (sessA c) k (some (αsess S)) := by
  have hslot := (hI.slot_of k slot hk).1
  have hcs : (UScalar.cast .Usize slot).val = slot.val := by
    rw [UScalar.cast_val_eq]; exact Nat.mod_eq_of_lt (u32_lt_usize _ (by
      have := hI.unused_le; have := hI.clients_u32; omega))
  have hlt : slot.val < c.sessions.val.length := by have := hI.unused_le; rw [hI.sess_len]; omega
  have hk2 : lookup c.clients.buckets.val k = some slot := hk
  funext k'
  unfold sessA slotOf spec.upd
  simp only
  by_cases e : k' = k
  · subst e; rw [hk2]; simp [alloc.vec.Vec.set_val_eq, hcs, List.getElem?_set_self hlt]
  · simp only [e, ite_false]
    cases hk' : lookup c.clients.buckets.val k' with
    | none => rfl
    | some s' =>
      have hne : slot.val ≠ s'.val := fun h => e (hI.slot_inj hk' (by rw [hk, u32_eq h]))
      simp [alloc.vec.Vec.set_val_eq, hcs, List.getElem?_set_ne hne]

/-- Writing one entry of client `k`'s outcome ring. -/
theorem memA_set {c : Core} (hI : Inv c) {k : U64} {slot : U32} (hk : slotOf c k = some slot)
    {idx : Nat} (hidx : idx < c.cfg.outcome_ring.val) (i : Usize)
    (hi : i.val = slot.val * c.cfg.outcome_ring.val + idx) (rs : RingSlot) :
    memA { c with ring := c.ring.set i (some rs) } =
      spec.upd (memA c) k (spec.upd (memA c k) idx (some (rs.client_seqno.val, αdec rs.decided))) := by
  have hslot := (hI.slot_of k slot hk).1
  have hlt : i.val < c.ring.val.length := by
    rw [hi, hI.ring_len]; exact block_lt (by have := hI.unused_le; omega) hidx
  have hk2 : lookup c.clients.buckets.val k = some slot := hk
  funext k' idx'
  unfold memA spec.upd slotOf
  simp only
  by_cases e : k' = k
  · subst e
    rw [hk2]
    simp only [ite_true]
    by_cases ei : idx' = idx
    · subst ei; simp [hidx, alloc.vec.Vec.set_val_eq, ← hi, List.getElem?_set_self hlt]
    · simp only [ei, ite_false]
      split
      · rename_i hidx'
        have : i.val ≠ slot.val * c.cfg.outcome_ring.val + idx' := by rw [hi]; omega
        simp [alloc.vec.Vec.set_val_eq, List.getElem?_set_ne this]
      · rfl
  · simp only [e, ite_false]
    cases hk' : lookup c.clients.buckets.val k' with
    | none => rfl
    | some s' =>
      simp only
      split
      · rename_i hidx'
        have hne : slot.val ≠ s'.val := fun h => e (hI.slot_inj hk' (by rw [hk, u32_eq h]))
        have : i.val ≠ s'.val * c.cfg.outcome_ring.val + idx' := by
          rw [hi]; exact block_ne hne hidx hidx'
        simp [alloc.vec.Vec.set_val_eq, List.getElem?_set_ne this]
      · rfl

theorem sat_add_val (x y : U64) :
    (core.num.U64.saturating_add x y).val = min U64.max (x.val + y.val) := by
  have hm : U64.max = 2 ^ 64 - 1 := by simp [U64.max_def, U64.numBits]
  have hm2 : UScalar.max .U64 = 2 ^ 64 - 1 := by simp [UScalar.max_def, UScalarTy.numBits]
  show (BitVec.ofNat _ (min (UScalar.max .U64) (x.val + y.val))).toNat = _
  rw [BitVec.toNat_ofNat, hm2, hm]
  apply Nat.mod_eq_of_lt
  simp [UScalarTy.numBits]

theorem sat_sub_val (x y : U64) : (core.num.U64.saturating_sub x y).val = x.val - y.val := by
  show (BitVec.ofNat _ (max 0 (x.val - y.val))).toNat = _
  rw [BitVec.toNat_ofNat, Nat.zero_max]
  apply Nat.mod_eq_of_lt
  have : x.val < 2 ^ 64 := by scalar_tac
  simp [UScalarTy.numBits]; omega

theorem sat1_eq (x : U64) : spec.sat1 x.val = (core.num.U64.saturating_add x 1#u64).val := by
  rw [sat_add_val]; simp [spec.sat1, spec.MAX, U64.max_def, U64.numBits]; omega

theorem αout_outcome (d : Decided) : Decided.outcome d = ok (match d with
    | .Accepted g => .Accepted g | .Rejected k v => .Rejected k v) := by
  cases d <;> rfl

theorem αeff_reply (r : Request) (q : U64) (o : krusty_wire.feedback.Outcome) :
    αeff { publish := none, reply := some ⟨r.client_id, r.incarnation, q, o⟩ } =
      spec.reply (αreq r) q.val (αout o) := by
  simp [αeff, spec.reply, αreq, cast_u32_u64]

theorem Inv.slot_lt {c : Core} (hI : Inv c) {k : U64} {slot : U32} (hk : slotOf c k = some slot) :
    slot.val < c.cfg.max_clients.val := by
  have := (hI.slot_of k slot hk).1; have := hI.unused_le; omega

theorem Inv.cast_slot {c : Core} (hI : Inv c) {k : U64} {slot : U32} (hk : slotOf c k = some slot) :
    (UScalar.cast .Usize slot).val = slot.val := by
  rw [UScalar.cast_val_eq]; exact Nat.mod_eq_of_lt (u32_lt_usize _ (by
    have := hI.slot_lt hk; have := hI.clients_u32; omega))

/-- A session update in its own slot, and a write anywhere in the ring,
    keep the invariant. -/
theorem Inv.with_sess_ring {c : Core} (hI : Inv c) {k : U64} {slot : U32} (hk : slotOf c k = some slot)
    (S : Session) (hS : UScalar.cast .U64 S.client_id = k) (ring : alloc.vec.Vec (Option RingSlot))
    (hring : ring.val.length = c.ring.val.length) (seq : U64) :
    Inv { c with sessions := c.sessions.set (UScalar.cast .Usize slot) (some S), ring := ring,
                 next_seqno := seq } := by
  have hcs := hI.cast_slot hk
  have hslt := (hI.slot_of k slot hk).1
  have hlt : slot.val < c.sessions.val.length := by have := hI.unused_le; rw [hI.sess_len]; omega
  obtain ⟨a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19⟩ := hI
  refine ⟨a1, a2, a3, a4, a5, a6, a7, a8, a9, by simpa using a10, by rw [hring]; exact a11, a12, a13,
    ?_, ?_, a16, a17, a18, a19⟩
  · intro k' s' hk'
    obtain ⟨hlt', S', hS', hc'⟩ := a14 k' s' hk'
    refine ⟨hlt', ?_⟩
    simp only [alloc.vec.Vec.set_val_eq, hcs]
    by_cases e : slot.val = s'.val
    · rw [← e, List.getElem?_set_self hlt]
      refine ⟨S, rfl, ?_⟩
      have : k' = k := by
        have := a14 k slot hk
        obtain ⟨-, S0, hS0, hc0⟩ := this
        rw [e, hS'] at hS0; cases hS0; rw [← hc', hc0]
      rw [hS, this]
    · rw [List.getElem?_set_ne e]; exact ⟨S', hS', hc'⟩
  · intro j S' hj
    simp only [alloc.vec.Vec.set_val_eq, hcs] at hj
    by_cases e : slot.val = j
    · subst e; rw [List.getElem?_set_self hlt] at hj; cases hj
      exact ⟨slot, rfl, by rw [hS]; exact hk⟩
    · rw [List.getElem?_set_ne e] at hj; exact a15 j S' hj

/-- `α` after a decision: the session's next seqno, one outcome-ring entry,
    and the stream seqno change. -/
theorem α_decided {c : Core} (ks : table.IdMap) (vs kos : alloc.vec.Vec U64)
    (hI1 : Inv { c with keys := ks, versions := vs, key_of_slot := kos })
    {k : U64} {slot : U32} (hk : slotOf c k = some slot) (S' : Session) (i1 : Usize)
    {idx : Nat} (hidx : idx < c.cfg.outcome_ring.val)
    (hi1 : i1.val = slot.val * c.cfg.outcome_ring.val + idx) (rs : RingSlot) (ns : U64) :
    α { c with next_seqno := ns, sessions := c.sessions.set (UScalar.cast .Usize slot) (some S'),
               ring := c.ring.set i1 (some rs), keys := ks, versions := vs, key_of_slot := kos } =
      { α { c with keys := ks, versions := vs, key_of_slot := kos } with
        seq := ns.val,
        sess := spec.upd (sessA c) k (some (αsess S')),
        mem := spec.upd (memA c) k (spec.upd (memA c k) idx (some (rs.client_seqno.val, αdec rs.decided))) } := by
  have hk1 : slotOf { c with keys := ks, versions := vs, key_of_slot := kos } k = some slot := hk
  have hs := sessA_set hI1 hk1 S'
  have hm := memA_set hI1 hk1 hidx i1 hi1 rs
  simp only [α]
  congr 1

/-! ## A message of a live session -/

def InSessionPost (c : Core) (r : Request) (S : Session) (x : Effects × Core) : Prop :=
  let sp := spec.inSession (αcfg c.cfg) (α c) (αreq r) (αsess S)
  αeff x.1 = sp.1 ∧ α x.2 = sp.2.1 ∧ Inv x.2 ∧ x.2.cfg = c.cfg

theorem in_session_spec (c : Core) (slot : U32) (r : Request) (S : Session) (hI : Inv c)
    (hk : slotOf c (UScalar.cast .U64 r.client_id) = some slot)
    (hS : c.sessions.val[slot.val]? = some (some S)) :
    Core.in_session c slot r ⦃ x => InSessionPost c r S x ⦄ := by
  have hcs := hI.cast_slot hk
  have hslt := hI.slot_lt hk
  have hsl : (UScalar.cast .Usize slot).val < c.sessions.val.length := by rw [hcs, hI.sess_len]; exact hslt
  have hSv : c.sessions.val[(UScalar.cast .Usize slot).val] = some S := by
    rw [List.getElem?_eq_getElem (by rw [hcs] at hsl; exact hsl)] at hS; simp [hcs] at hS ⊢; exact hS
  have hsessA : sessA c (UScalar.cast .U64 r.client_id) = some (αsess S) := by
    unfold sessA; rw [hk]; simp [hS]
  unfold Core.in_session
  simp only [lift, bind_ok, vidx _ _ hsl, hSv, core.option.Option.expect, ofOption]
  unfold InSessionPost
  have hreply := αeff_reply r
  have hDA := decide_append_spec c r hI
  unfold DecideAppendPost at hDA
  generalize hR : αreq r = R at hreply hDA ⊢
  have hRc : R.client = UScalar.cast .U64 r.client_id := by rw [← hR]; rfl
  have hRi : R.inc = r.incarnation.val := by rw [← hR]; rfl
  have hRs : R.seq = r.client_seqno.val := by rw [← hR]; rfl
  unfold spec.inSession
  simp only [hRs, hRc, hRi, αsess, UScalar.lt_equiv, gt_iff_lt]
  by_cases h1 : r.client_seqno.val < S.next.val
  · simp only [h1, ite_true]
    step with ring_index_spec c slot r.client_seqno hI hslt as ⟨ i1, hi1, hi1l ⟩
    simp only [vidxu _ _ hi1l, bind_ok]
    have hmem : (α c).mem (UScalar.cast .U64 r.client_id) (r.client_seqno.val % c.cfg.outcome_ring.val) =
        match c.ring.val[i1.val] with
        | some rs => some (rs.client_seqno.val, αdec rs.decided)
        | none => none := by
      simp only [α, memA, hk]
      rw [if_pos (Nat.mod_lt _ (by have := hI.ring_pos; omega)), ← hi1, List.getElem?_eq_getElem hi1l]
      cases c.ring.val[i1.val] <;> rfl
    simp only [αcfg, hmem]
    cases hc : c.ring.val[i1.val] with
    | none => simp [WP.spec_ok, αeff, spec.noEff, hI]
    | some rs =>
      simp only
      by_cases h2 : rs.client_seqno = r.client_seqno
      · have h2' : rs.client_seqno.val = r.client_seqno.val := by rw [h2]
        simp only [h2, ite_true, h2', αout_outcome, bind_ok, Core.reply, WP.spec_ok]
        refine ⟨?_, by simp, by simp [hI], by simp⟩
        rw [hreply]; congr 1; cases rs.decided <;> rfl
      · have h2' : ¬ rs.client_seqno.val = r.client_seqno.val := fun e => h2 ((UScalar.eq_equiv _ _).mpr e)
        simp [h2, h2', WP.spec_ok, αeff, spec.noEff, hI]
  · simp only [h1, ite_false]
    have hmax : (core.num.U64.MAX).val = spec.MAX := by simp [core.num.U64.MAX, U64.rMax, spec.MAX]
    have hrm : U64.rMax = spec.MAX := by simp [U64.rMax, spec.MAX]
    by_cases h2 : S.next.val < r.client_seqno.val
    · simp only [h2, ite_true, Core.reply, bind_ok, WP.spec_ok]
      exact ⟨by rw [hreply]; rfl, by simp, by simp [hI], by simp⟩
    · simp only [h2, ite_false]
      by_cases h3 : r.client_seqno.val = spec.MAX
      · have h3' : r.client_seqno = core.num.U64.MAX := (UScalar.eq_equiv _ _).mpr (by rw [hmax]; exact h3)
        simp only [h3, h3', ite_true, Core.reply, bind_ok, WP.spec_ok, lift]
        refine ⟨?_, by simp [hrm], by simp [hI], by simp⟩
        rw [hreply]; simp [αout, sat1_eq, hrm]
      · have h3' : ¬ r.client_seqno = core.num.U64.MAX := fun e => h3 (by rw [e, hmax])
        simp only [h3, h3', ite_false]
        have hseq : (α c).seq = c.next_seqno.val := rfl
        by_cases h4 : c.next_seqno.val = spec.MAX
        · have h4' : c.next_seqno = core.num.U64.MAX := (UScalar.eq_equiv _ _).mpr (by rw [hmax]; exact h4)
          simp only [hseq, h4, h4', ite_true, Core.busy, Core.reply, bind_ok, WP.spec_ok]
          refine ⟨?_, by simp [hrm], by simp [hI], by simp⟩
          rw [hreply]; simp [αout, hrm]
        · have h4' : ¬ c.next_seqno = core.num.U64.MAX := fun e => h4 (by rw [e, hmax])
          simp only [hseq, h4, h4', ite_false]
          step with hDA as ⟨ dd, pub, c1, hdd, hpub, hα1, hI1, ks, vs, kos, hc1 ⟩
          subst hc1
          have hsp : spec.decideAppend (αcfg c.cfg) (α c) R =
              (dd.map αdec, pub.map (αpub c.next_seqno), α { c with keys := ks, versions := vs, key_of_slot := kos }) := by
            rw [hdd, hpub, hα1]
          rw [hsp]
          cases dd with
          | none =>
            simp only [Option.map_none, Core.busy, Core.reply, bind_ok, WP.spec_ok]
            refine ⟨?_, by simp, by simpa using hI1, by simp⟩
            rw [hreply]; simp [αout]
          | some d =>
            simp only [Option.map_some]
            have hk1 : slotOf { c with keys := ks, versions := vs, key_of_slot := kos }
                (UScalar.cast .U64 r.client_id) = some slot := hk
            have hseqn : r.client_seqno.val = S.next.val := by omega
            have hnext : S.next.val + 1 ≤ U64.max := by
              have hb := u64_lt r.client_seqno
              have hM := u64max
              unfold spec.MAX at h3; omega
            step with ring_index_spec _ slot r.client_seqno hI1 hslt as ⟨ i1, hi1, hi1l ⟩
            step*
            subst index_mut_back_post index_mut_back1_post
            have hnsq : c.next_seqno.val + 1 ≤ U64.max := by
              have hb := u64_lt c.next_seqno
              have hM := u64max
              unfold spec.MAX at h4; omega
            -- The new session and outcome-ring entry, and what α makes of them.
            have hS' : UScalar.cast .U64 ({ S with next := i2, last_active_ns := c.now_ns } : Session).client_id =
                UScalar.cast .U64 r.client_id := by
              obtain ⟨-, S0, hS0, hc0⟩ := hI.slot_of _ slot hk
              rw [hS] at hS0; cases hS0; exact hc0
            have hsess := sessA_set hI1 hk1 { S with next := i2, last_active_ns := c.now_ns }
            have hmemA := memA_set hI1 hk1 (idx := r.client_seqno.val % c.cfg.outcome_ring.val)
              (Nat.mod_lt _ (by have := hI.ring_pos; omega)) i1 hi1
              { client_seqno := r.client_seqno, decided := d }
            have hring : (c.ring.set i1 (some { client_seqno := r.client_seqno, decided := d })).val.length =
                c.ring.val.length := by simp
            cases pub with
            | none =>
              simp only [Option.map_none]
              step*
              simp only [αout_outcome, bind_ok, Core.reply, WP.spec_ok]
              refine ⟨?_, ?_, ?_, by simp⟩
              · rw [hreply]; cases d <;> rfl
              · rw [α_decided ks vs kos hI1 hk _ i1 (Nat.mod_lt _ (by have := hI.ring_pos; omega)) hi1]
                simp only [α, αsess, αcfg, i2_post]
                rfl
              · exact hI1.with_sess_ring hk1 _ hS' _ hring _
            | some m =>
              simp only [Option.map_some]
              step*
              simp only [αout_outcome, bind_ok, Core.reply, WP.spec_ok]
              refine ⟨?_, ?_, ?_, by simp⟩
              · subst hR
                cases d <;> simp [αeff, αpub, spec.reply, αout, αdec, spec.Dec.out, αreq, cast_u32_u64]
              · rw [α_decided ks vs kos hI1 hk _ i1 (Nat.mod_lt _ (by have := hI.ring_pos; omega)) hi1]
                simp only [α, αsess, αcfg, i2_post, x_post]
                rfl
              · exact hI1.with_sess_ring hk1 _ hS' _ hring _

/-! ## Starting a session -/

theorem start_loop_spec (v : alloc.vec.Vec (Option RingSlot)) (n base i : Usize)
    (hb : base.val + n.val ≤ v.val.length) (hi : i.val ≤ n.val)
    (orig : List (Option RingSlot)) (hlen : v.val.length = orig.length)
    (hv : ∀ j, v.val[j]? = if base.val ≤ j ∧ j < base.val + i.val then some none else orig[j]?) :
    Core.start_session_loop v n base i ⦃ v' => v'.val.length = orig.length ∧
      ∀ j, v'.val[j]? = if base.val ≤ j ∧ j < base.val + n.val then some none else orig[j]? ⦄ := by
  unfold Core.start_session_loop
  apply loop.spec_decr_nat (fun (p : alloc.vec.Vec (Option RingSlot) × Usize) => n.val - p.2.val)
    (fun p => p.2.val ≤ n.val ∧ p.1.val.length = orig.length ∧
      ∀ j, p.1.val[j]? = if base.val ≤ j ∧ j < base.val + p.2.val then some none else orig[j]?)
  · rintro ⟨w, k⟩ ⟨hk, hwl, hw⟩
    simp only at hk hwl hw
    unfold Core.start_session_loop.body
    simp only
    split
    · rename_i hlt
      simp only [UScalar.lt_equiv] at hlt
      have hnmax : base.val + n.val ≤ Usize.max := by
        have := alloc.vec.Vec.len_ineq v; omega
      step*
      subst_vars
      refine ⟨by omega, by simp [hwl], fun j => ?_, by omega⟩
      simp only [alloc.vec.Vec.set_val_eq]
      by_cases e : i1.val = j
      · rw [← e, List.getElem?_set_self (by rw [hwl, ← hlen]; omega), i1_post, i2_post]
        rw [if_pos (by omega)]
      · rw [List.getElem?_set_ne e, hw j, i2_post]
        have : ¬ j = base.val + k.val := by rw [i1_post] at e; omega
        by_cases hj : base.val ≤ j ∧ j < base.val + k.val
        · rw [if_pos hj, if_pos (by omega)]
        · rw [if_neg hj, if_neg (by omega)]
    · rename_i hge
      simp only [UScalar.lt_equiv] at hge
      simp only [WP.spec_ok]
      have : k.val = n.val := by omega
      exact ⟨hwl, fun j => by rw [hw j, this]⟩
  · exact ⟨hi, hlen, hv⟩

/-- What `start_session` leaves: the invariant, and the spec's `startSession`. -/
def StartPost (c : Core) (slot : U32) (r : Request) (c' : Core) : Prop :=
  Inv c' ∧ α c' = spec.startSession (α c) (UScalar.cast .U64 r.client_id) r.incarnation.val ∧
  c'.sessions.val[slot.val]? = some (some (Session.mk r.client_id r.incarnation 1#u64 c.now_ns)) ∧
  c'.cfg = c.cfg ∧ c'.clients = c.clients ∧ c'.next_seqno = c.next_seqno ∧ c'.now_ns = c.now_ns

theorem vec_set_set {α : Type} (v : alloc.vec.Vec α) (i : Usize) (a b : α) :
    (v.set i a).set i b = v.set i b := by
  apply alloc.vec.Vec.ext; simp [alloc.vec.Vec.set_val_eq]

/-- Starting a session in `slot`, which the client table maps `k` to. The
    precondition: the invariant would hold if the slot held some session
    `D` of `k` (it holds `k`'s old session, or nothing yet). -/
theorem start_session_spec (c : Core) (slot : U32) (r : Request) (D : Session)
    (hD : UScalar.cast .U64 D.client_id = UScalar.cast .U64 r.client_id)
    (hI : Inv { c with sessions := c.sessions.set (UScalar.cast .Usize slot) (some D) })
    (hk : slotOf c (UScalar.cast .U64 r.client_id) = some slot) :
    Core.start_session c slot r ⦃ c' => StartPost c slot r c' ⦄ := by
  have hkD : slotOf { c with sessions := c.sessions.set (UScalar.cast .Usize slot) (some D) }
      (UScalar.cast .U64 r.client_id) = some slot := hk
  have hcs : (UScalar.cast .Usize slot).val = slot.val := hI.cast_slot hkD
  have hslt : slot.val < c.cfg.max_clients.val := hI.slot_lt hkD
  have hsl : (UScalar.cast .Usize slot).val < c.sessions.val.length := by
    have := hI.sess_len; simp at this; rw [hcs, this]; exact hslt
  have hR : 1 ≤ c.cfg.outcome_ring.val := hI.ring_pos
  have hcap : c.cfg.max_clients.val * c.cfg.outcome_ring.val ≤ Usize.max := hI.ring_cap
  have hrl : c.ring.val.length = c.cfg.max_clients.val * c.cfg.outcome_ring.val := by
    have := hI.ring_len; simpa using this
  have hbase : slot.val * c.cfg.outcome_ring.val + c.cfg.outcome_ring.val ≤ c.ring.val.length := by
    have : (slot.val + 1) * c.cfg.outcome_ring.val ≤ c.cfg.max_clients.val * c.cfg.outcome_ring.val :=
      Nat.mul_le_mul_right _ (by omega)
    rw [Nat.add_mul, Nat.one_mul] at this; omega
  unfold Core.start_session
  step*
  subst index_mut_back_post i_post i1_post
  have hb : base.val = slot.val * c.cfg.outcome_ring.val := by rw [base_post, hcs]
  apply WP.spec_bind (start_loop_spec c.ring c.cfg.outcome_ring base 0#usize (by rw [hb]; omega)
    (by simp) c.ring.val rfl (fun j => by simp))
  rintro v1 ⟨hv1l, hv1⟩
  simp only [WP.spec_ok]
  -- Slots of other clients are not `slot`.
  have hother : ∀ k' s', k' ≠ UScalar.cast .U64 r.client_id → slotOf c k' = some s' → s'.val ≠ slot.val := by
    intro k' s' hne hk' e
    exact hne (hI.slot_inj (k := k') hk' (by rw [hkD, u32_eq e]))
  refine ⟨?_, ?_, ?_, rfl, rfl, rfl, rfl⟩
  · have := hI.with_sess_ring hkD (Session.mk r.client_id r.incarnation 1#u64 c.now_ns) rfl v1
      (by rw [hv1l]) c.next_seqno
    simpa [vec_set_set] using this
  · have hk2 : lookup c.clients.buckets.val (UScalar.cast .U64 r.client_id) = some slot := hk
    have hsl' : slot.val < c.sessions.val.length := by rw [← hcs]; exact hsl
    simp only [α, spec.startSession]
    congr 1
    · funext k'
      unfold sessA slotOf spec.upd
      simp only
      by_cases e : k' = UScalar.cast .U64 r.client_id
      · subst e; rw [hk2]; simp [alloc.vec.Vec.set_val_eq, hcs, List.getElem?_set_self hsl', αsess]
      · simp only [e, ite_false]
        cases hk' : lookup c.clients.buckets.val k' with
        | none => rfl
        | some s' =>
          have hne := hother k' s' e hk'
          simp [alloc.vec.Vec.set_val_eq, hcs, List.getElem?_set_ne (Ne.symm hne)]
    · funext k' idx
      unfold memA spec.upd slotOf
      simp only
      by_cases e : k' = UScalar.cast .U64 r.client_id
      · subst e; rw [hk2]; simp only [ite_true]
        split
        · rw [hv1, if_pos (by rw [hb]; constructor <;> omega)]
        · rfl
      · simp only [e, ite_false]
        cases hk' : lookup c.clients.buckets.val k' with
        | none => rfl
        | some s' =>
          simp only
          split
          · rename_i hidx
            have hne := hother k' s' e hk'
            rw [hv1, if_neg]
            rw [hb]; rintro ⟨h1, h2⟩
            exact block_ne hne hidx (j := s'.val * c.cfg.outcome_ring.val + idx - slot.val * c.cfg.outcome_ring.val)
              (by omega) (by omega)
          · rfl
  · have hsl' : slot.val < c.sessions.val.length := by rw [← hcs]; exact hsl
    simp [alloc.vec.Vec.set_val_eq, hcs, List.getElem?_set_self hsl']

/-! ## Allocating a session slot -/

/-- The core's idleness test, on numbers. -/
def IdleC (c : Config) (now : U64) (S : Session) : Prop := c.evict_idle_ns.val ≤ now.val - S.last_active_ns.val

theorem alloc_loop_spec (cfg : Config) (now : U64) (v : alloc.vec.Vec (Option Session))
    (hv : v.val.length ≤ U32.max) :
    Core.allocate_session_loop cfg now v none 0#usize ⦃ res =>
      res.1 = cfg ∧
      (res.2 = none → ∀ (j : Nat) S, v.val[j]? = some (some S) → ¬ IdleC cfg now S) ∧
      (∀ slot S, res.2 = some (slot, S) → v.val[slot.val]? = some (some S) ∧ IdleC cfg now S) ⦄ := by
  unfold Core.allocate_session_loop
  apply loop.spec_decr_nat
    (fun (p : Config × Option (U32 × Session) × Usize) => v.val.length - p.2.2.val)
    (fun p => p.1 = cfg ∧ p.2.2.val ≤ v.val.length ∧
      (p.2.1 = none → ∀ (j : Nat) S, j < p.2.2.val → v.val[j]? = some (some S) → ¬ IdleC cfg now S) ∧
      (∀ slot S, p.2.1 = some (slot, S) → v.val[slot.val]? = some (some S) ∧ IdleC cfg now S))
  · rintro ⟨c, victim, i⟩ ⟨rfl, hi, hnone, hsome⟩
    simp only at hi hnone hsome
    unfold Core.allocate_session_loop.body
    simp only [alloc.vec.Vec.len_val]
    split
    · rename_i hlt
      simp only [UScalar.lt_equiv, alloc.vec.Vec.len_val] at hlt
      have hil : i.val < v.val.length := hlt
      simp only [vidx _ _ hil, bind_ok]
      have hcast : (UScalar.cast .U32 i).val = i.val := by
        rw [UScalar.cast_val_eq]; exact Nat.mod_eq_of_lt (by
          have : U32.max = 2 ^ 32 - 1 := by simp [U32.max_def, U32.numBits]
          simp; omega)
      have hiv : v.val[i.val]? = some v.val[i.val] := List.getElem?_eq_getElem hil
      cases hc : v.val[i.val] with
      | none =>
        rw [hc] at hiv
        step*
        refine ⟨by omega, fun h j S hj hS => ?_, hsome, by omega⟩
        rcases Nat.lt_or_ge j i.val with h' | h'
        · exact hnone h j S h' hS
        · have : j = i.val := by omega
          subst this; rw [hiv] at hS; cases hS
      | some sess =>
        rw [hc] at hiv
        simp only [lift, bind_ok]
        have hidle : (core.num.U64.saturating_sub now sess.last_active_ns ≥ c.evict_idle_ns) ↔
            IdleC c now sess := by
          simp only [ge_iff_le, UScalar.le_equiv, sat_sub_val, IdleC]
        have hlast : ∀ (j : Nat) S, j < i.val + 1 → v.val[j]? = some (some S) → ¬ IdleC c now S →
            j < i.val ∨ ¬ IdleC c now S := fun j S _ _ h => Or.inr h
        have hcur : ∀ (j : Nat) S, j = i.val → v.val[j]? = some (some S) → S = sess := by
          rintro j S rfl hS; rw [hiv] at hS; cases hS; rfl
        have hi1 : i.val + 1 ≤ Usize.max := by have := alloc.vec.Vec.len_ineq v; omega
        rcases victim with _ | ⟨vslot, vS⟩
        · simp only [bind_ok]
          step*
          by_cases hid : IdleC c now sess
          · simp only [hidle.mpr hid, ite_true, bind_ok]
            step*
            refine ⟨by omega, by simp, ?_, by omega⟩
            rintro slot S h; simp only [Option.some.injEq, Prod.mk.injEq] at h
            obtain ⟨rfl, rfl⟩ := h; exact ⟨by rw [hcast]; exact hiv, hid⟩
          · have : ¬ (core.num.U64.saturating_sub now sess.last_active_ns ≥ c.evict_idle_ns) :=
              fun h => hid (hidle.mp h)
            simp only [this, ite_false, bind_ok]
            step*
            refine ⟨by omega, ?_, by simp, by omega⟩
            intro _ j S hj hS
            rcases Nat.lt_or_ge j i.val with h' | h'
            · exact hnone rfl j S h' hS
            · rw [hcur j S (by omega) hS]; exact hid
        · simp only [bind_ok]
          step*
          have hv := hsome vslot vS rfl
          by_cases hid : IdleC c now sess
          · simp only [hidle.mpr hid, ite_true]
            split
            · simp only [bind_ok]
              step*
              refine ⟨by omega, by simp, ?_, by omega⟩
              rintro slot S h; simp only [Option.some.injEq, Prod.mk.injEq] at h
              obtain ⟨rfl, rfl⟩ := h; exact ⟨by rw [hcast]; exact hiv, hid⟩
            · simp only [bind_ok]
              step*
              refine ⟨by omega, by simp, ?_, by omega⟩
              rintro slot S h; simp only [Option.some.injEq, Prod.mk.injEq] at h
              obtain ⟨rfl, rfl⟩ := h; exact hv
          · have : ¬ (core.num.U64.saturating_sub now sess.last_active_ns ≥ c.evict_idle_ns) :=
              fun h => hid (hidle.mp h)
            simp only [this, ite_false, bind_ok]
            step*
            refine ⟨by omega, by simp, ?_, by omega⟩
            rintro slot S h; simp only [Option.some.injEq, Prod.mk.injEq] at h
            obtain ⟨rfl, rfl⟩ := h; exact hv
    · rename_i hge
      simp only [UScalar.lt_equiv, alloc.vec.Vec.len_val, not_lt] at hge
      simp only [WP.spec_ok]
      refine ⟨trivial, fun h j S hS => hnone h j S ?_ hS, hsome⟩
      have := getElem?_some_lt' hS
      have : v.val.length ≤ i.val := by simpa using hge
      omega
  · exact ⟨rfl, by simp, fun _ j _ hj => by simp at hj, fun _ _ h => by cases h⟩

/-- What `allocate_session` does: a never-used slot, or the slot of an idle
    session it evicts (raising the floor), or nothing. -/
def AllocPost (c : Core) (o : Option U32) (c1 : Core) : Prop :=
  match o with
  | none => c1 = c ∧ c.unused.val = c.cfg.max_clients.val ∧
      ∀ (j : Nat) S, c.sessions.val[j]? = some (some S) → ¬ IdleC c.cfg c.now_ns S
  | some slot =>
    (c.unused.val < c.cfg.max_clients.val ∧ slot.val = c.unused.val ∧
      ∃ u : Usize, u.val = c.unused.val + 1 ∧ c1 = { c with unused := u }) ∨
    (c.unused.val = c.cfg.max_clients.val ∧ ∃ S im f,
      c.sessions.val[slot.val]? = some (some S) ∧ IdleC c.cfg c.now_ns S ∧
      idmap.RemovePost c.clients (UScalar.cast .U64 S.client_id) im ∧
      f.val = max c.incarnation_floor.val S.incarnation.val ∧
      c1 = { c with incarnation_floor := f, clients := im,
                    sessions := c.sessions.set (UScalar.cast .Usize slot) none })

theorem allocate_session_spec (c : Core) (hI : Inv c) :
    Core.allocate_session c ⦃ r => AllocPost c r.1 r.2 ⦄ := by
  unfold Core.allocate_session
  have hsl := hI.sess_len
  have hul := hI.unused_le
  have hcu := hI.clients_u32
  simp only
  split
  · rename_i hlt
    simp only [UScalar.lt_equiv, alloc.vec.Vec.len_val] at hlt
    have hlt' : c.unused.val < c.sessions.val.length := hlt
    have : c.unused.val + 1 ≤ Usize.max := by have := alloc.vec.Vec.len_ineq c.sessions; omega
    step*
    simp only [AllocPost]
    left
    refine ⟨by omega, ?_, i1, i1_post, rfl⟩
    rw [i3_post, UScalar.cast_val_eq, i2_post, i1_post]
    exact Nat.mod_eq_of_lt (by have : U32.max = 2 ^ 32 - 1 := by simp [U32.max_def, U32.numBits]
                               simp; omega)
  · rename_i hge
    simp only [UScalar.lt_equiv, alloc.vec.Vec.len_val, not_lt] at hge
    have hge' : c.sessions.val.length ≤ c.unused.val := hge
    have hum : c.unused.val = c.cfg.max_clients.val := by omega
    step with alloc_loop_spec c.cfg c.now_ns c.sessions (by omega) as ⟨ cfg', victim, hcfg, hnone, hsome ⟩
    subst hcfg
    cases victim with
    | none =>
      simp only [WP.spec_ok, AllocPost]
      exact ⟨by first | trivial | rfl, hum, hnone rfl⟩
    | some p =>
      obtain ⟨slot, S⟩ := p
      obtain ⟨hS, hid⟩ := hsome slot S rfl
      step*
      subst i1_post
      step with remove_spec c.clients _ hI.clients_wf as ⟨ im, him ⟩
      have hcs : (UScalar.cast .Usize slot).val = slot.val := by
        rw [UScalar.cast_val_eq]; exact Nat.mod_eq_of_lt (u32_lt_usize _ (by
          have := getElem?_some_lt' hS; omega))
      have hsl' : (UScalar.cast .Usize slot).val < c.sessions.val.length := by
        rw [hcs]; exact getElem?_some_lt' hS
      have hfin : ∀ f : U64, f.val = max c.incarnation_floor.val S.incarnation.val →
          (do
            let i3 ← lift (UScalar.cast UScalarTy.Usize slot)
            let (_, index_mut_back) ← c.sessions.index_mut_usize i3
            ok (some slot, { c with incarnation_floor := f, clients := im, sessions := index_mut_back none }) :
              Result (Option U32 × Core)) ⦃ r => AllocPost c r.1 r.2 ⦄ := by
        intro f hf
        step*
        subst_vars
        simp only [AllocPost]
        exact Or.inr ⟨hum, S, im, f, hS, hid, him, hf, rfl⟩
      split
      · rename_i hgt; simp only [gt_iff_lt, UScalar.lt_equiv] at hgt
        simp only [bind_ok]; exact hfin _ (by omega)
      · rename_i hgt; simp only [gt_iff_lt, UScalar.lt_equiv, not_lt] at hgt
        simp only [bind_ok]; exact hfin _ (by omega)

/-! ## Adding a client -/

/-- Slots at or beyond `unused` are empty. -/
theorem Inv.beyond_none {c : Core} (hI : Inv c) {j : Nat} (hj : c.unused.val ≤ j) {S : Session}
    (hS : c.sessions.val[j]? = some (some S)) : False := by
  obtain ⟨s, hs, hk⟩ := hI.client_of j S hS
  have := (hI.slot_of _ s hk).1; omega

theorem Inv.slot_of_session {c : Core} (hI : Inv c) {slot : U32} {S : Session}
    (hS : c.sessions.val[slot.val]? = some (some S)) : slotOf c (UScalar.cast .U64 S.client_id) = some slot := by
  obtain ⟨s, hs, hk⟩ := hI.client_of slot.val S hS
  rw [hk, u32_eq hs]

/-- A new client in a never-used slot. -/
theorem Inv.add_client {c : Core} (hI : Inv c) {k : U64} (hk : slotOf c k = none)
    (hfresh : c.unused.val < c.cfg.max_clients.val) (u : Usize) (hu : u.val = c.unused.val + 1)
    (slot : U32) (hslot : slot.val = c.unused.val) (b : Bool) (im : table.IdMap)
    (hins : idmap.InsertPost c.clients k slot (b, im)) (D : Session) (hD : UScalar.cast .U64 D.client_id = k) :
    Inv { c with unused := u, clients := im, sessions := c.sessions.set (UScalar.cast .Usize slot) (some D) } ∧
    ∀ k', lookup im.buckets.val k' = if k' = k then some slot else lookup c.clients.buckets.val k' := by
  obtain ⟨hwf, hmax, hrest⟩ := hins
  have hlen := hI.clients_len
  have hcm := hI.clients_max
  have hk' : lookup c.clients.buckets.val k = none := hk
  rw [ite_eq_right_of_eq_false _ _ (eq_false (by rw [hcm]; simp only at *; omega))] at hrest
  simp only at hwf hmax hrest
  obtain ⟨-, hlk, hl⟩ := hrest
  rw [hk'] at hl; simp only [ite_true] at hl
  have hcs : (UScalar.cast .Usize slot).val = slot.val := by
    rw [UScalar.cast_val_eq]; exact Nat.mod_eq_of_lt (u32_lt_usize _ (by have := hI.clients_u32; omega))
  have hsl : slot.val < c.sessions.val.length := by rw [hI.sess_len]; omega
  refine ⟨?_, hlk⟩
  obtain ⟨a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19⟩ := hI
  refine ⟨a1, a2, a3, a4, a5, hwf, by rw [hmax, a7], a8, a9, by simpa using a10, a11,
    by show u.val ≤ c.cfg.max_clients.val; omega, by show im.len.val = u.val; omega,
    ?_, ?_, a16, a17, a18, a19⟩
  · intro k' s' hs'
    have hs'' : lookup im.buckets.val k' = some s' := hs'
    rw [hlk] at hs''
    simp only [alloc.vec.Vec.set_val_eq, hcs]
    split at hs''
    · rename_i e; cases hs''
      refine ⟨by show slot.val < u.val; omega, D, List.getElem?_set_self hsl, by rw [hD, e]⟩
    · obtain ⟨hlt, S', hS', hc'⟩ := a14 k' s' hs''
      have hne : slot.val ≠ s'.val := by omega
      exact ⟨by show s'.val < u.val; omega, S', by rw [List.getElem?_set_ne hne]; exact hS', hc'⟩
  · intro j S' hj
    simp only [alloc.vec.Vec.set_val_eq, hcs] at hj
    by_cases e : slot.val = j
    · subst e; rw [List.getElem?_set_self hsl] at hj; cases hj
      refine ⟨slot, rfl, ?_⟩
      show lookup im.buckets.val _ = _
      rw [hlk, hD]; simp
    · rw [List.getElem?_set_ne e] at hj
      obtain ⟨s', hs', hk2⟩ := a15 j S' hj
      refine ⟨s', hs', ?_⟩
      show lookup im.buckets.val _ = _
      rw [hlk]
      have : UScalar.cast .U64 S'.client_id ≠ k := by
        intro h
        have : slotOf c k = some s' := by rw [← h]; exact hk2
        rw [hk] at this; cases this
      simp only [this, ite_false]; exact hk2

/-- A new client in the slot of an evicted one. -/
theorem Inv.replace_client {c : Core} (hI : Inv c) {k : U64} (hk : slotOf c k = none)
    (hfull : c.unused.val = c.cfg.max_clients.val) {slot : U32} {S : Session}
    (hS : c.sessions.val[slot.val]? = some (some S)) (im1 : table.IdMap)
    (hrem : idmap.RemovePost c.clients (UScalar.cast .U64 S.client_id) im1)
    (b : Bool) (im2 : table.IdMap) (hins : idmap.InsertPost im1 k slot (b, im2)) (f : U64)
    (D : Session) (hD : UScalar.cast .U64 D.client_id = k) :
    Inv { c with incarnation_floor := f, clients := im2,
                 sessions := c.sessions.set (UScalar.cast .Usize slot) (some D) } ∧
    ∀ k', lookup im2.buckets.val k' =
      if k' = k then some slot
      else if k' = UScalar.cast .U64 S.client_id then none else lookup c.clients.buckets.val k' := by
  have hw := hI.slot_of_session hS
  have hw' : lookup c.clients.buckets.val (UScalar.cast .U64 S.client_id) = some slot := hw
  have hk' : lookup c.clients.buckets.val k = none := hk
  have hkw : k ≠ UScalar.cast .U64 S.client_id := by intro e; rw [e] at hk'; rw [hw'] at hk'; cases hk'
  obtain ⟨hwf1, hmax1, hlk1, hl1⟩ := hrem
  rw [hw'] at hl1; simp only [reduceCtorEq, ite_false] at hl1
  have hlen := hI.clients_len
  have hcm := hI.clients_max
  have hlk1k : lookup im1.buckets.val k = none := by rw [hlk1]; simp [hkw, hk']
  have hlpos : 1 ≤ c.clients.len.val := by
    have := (hI.slot_of _ slot hw).1; omega
  obtain ⟨hwf, hmax, hrest⟩ := hins
  rw [ite_eq_right_of_eq_false _ _ (eq_false (by rw [hmax1, hcm]; simp only at *; omega))] at hrest
  simp only at hwf hmax hrest
  obtain ⟨-, hlk, hl⟩ := hrest
  rw [hlk1k] at hl; simp only [ite_true] at hl
  have hlk2 : ∀ k', lookup im2.buckets.val k' =
      if k' = k then some slot
      else if k' = UScalar.cast .U64 S.client_id then none else lookup c.clients.buckets.val k' := by
    intro k'; rw [hlk, hlk1]
  have hslt := (hI.slot_of _ slot hw).1
  have hcs : (UScalar.cast .Usize slot).val = slot.val := by
    rw [UScalar.cast_val_eq]; exact Nat.mod_eq_of_lt (u32_lt_usize _ (by have := hI.clients_u32; omega))
  have hsl : slot.val < c.sessions.val.length := getElem?_some_lt' hS
  refine ⟨?_, hlk2⟩
  have hinj := @Inv.slot_inj c hI
  obtain ⟨a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19⟩ := hI
  refine ⟨a1, a2, a3, a4, a5, hwf, by rw [hmax, hmax1, a7], a8, a9, by simpa using a10, a11,
    a12, by show im2.len.val = c.unused.val; omega, ?_, ?_, a16, a17, a18, a19⟩
  · intro k' s' hs'
    have hs'' : lookup im2.buckets.val k' = some s' := hs'
    rw [hlk2] at hs''
    simp only [alloc.vec.Vec.set_val_eq, hcs]
    split at hs''
    · rename_i e; cases hs''
      exact ⟨hslt, D, List.getElem?_set_self hsl, by rw [hD, e]⟩
    · split at hs''
      · cases hs''
      · rename_i e1 e2
        obtain ⟨hlt, S', hS', hc'⟩ := a14 k' s' hs''
        have hne : slot.val ≠ s'.val := by
          intro h
          exact e2 (hinj (k := k') (k' := UScalar.cast .U64 S.client_id) hs'' (by rw [hw, u32_eq h]))
        exact ⟨hlt, S', by rw [List.getElem?_set_ne hne]; exact hS', hc'⟩
  · intro j S' hj
    simp only [alloc.vec.Vec.set_val_eq, hcs] at hj
    by_cases e : slot.val = j
    · subst e; rw [List.getElem?_set_self hsl] at hj; cases hj
      refine ⟨slot, rfl, ?_⟩
      show lookup im2.buckets.val _ = _
      rw [hlk2, hD]; simp
    · rw [List.getElem?_set_ne e] at hj
      obtain ⟨s', hs', hk2⟩ := a15 j S' hj
      refine ⟨s', hs', ?_⟩
      show lookup im2.buckets.val _ = _
      rw [hlk2]
      have h1 : UScalar.cast .U64 S'.client_id ≠ k := by
        intro h
        have : slotOf c k = some s' := by rw [← h]; exact hk2
        rw [hk] at this; cases this
      have h2 : UScalar.cast .U64 S'.client_id ≠ UScalar.cast .U64 S.client_id := by
        intro h
        have : slotOf c (UScalar.cast .U64 S.client_id) = some s' := by rw [← h]; exact hk2
        rw [hw] at this; cases this; exact e hs'
      simp only [h1, h2, ite_false]; exact hk2

/-! ## The decision on one request -/

/-- The spec state after evicting `w` (session `V`). -/
def evictA (s : spec.St) (w : U64) (V : spec.Sess) : spec.St :=
  { s with sess := spec.upd s.sess w none, mem := spec.upd s.mem w (fun _ => none), floor := max s.floor V.inc }

theorem exists_valid (cfg : spec.Cfg) (s : spec.St) : ∃ v, spec.ValidVictim cfg s v := by
  by_cases h : ∃ c S, s.sess c = some S ∧ spec.Idle cfg S s.now
  · obtain ⟨c, S, h1, h2⟩ := h; exact ⟨some c, S, h1, h2⟩
  · exact ⟨none, fun c S h1 h2 => h ⟨c, S, h1, h2⟩⟩

def DecidePost (c : Core) (r : Request) (x : Effects × Core) : Prop :=
  Inv x.2 ∧ x.2.cfg = c.cfg ∧
  ∃ v g, spec.ValidVictim (αcfg c.cfg) (α c) v ∧
    spec.decide (αcfg c.cfg) (α c) (αreq r) v = (αeff x.1, α x.2, g)

theorem vec_set_self {α : Type} (v : alloc.vec.Vec α) (i : Usize) (a : α)
    (h : v.val[i.val]? = some a) : v.set i a = v := by
  apply alloc.vec.Vec.ext
  have hl := getElem?_some_lt' h
  rw [List.getElem?_eq_getElem hl] at h
  simp only [Option.some.injEq] at h
  simp [alloc.vec.Vec.set_val_eq, ← h]

/-- The spec's `startSession` only reads the rest of the state. -/
theorem startSession_congr {s t : spec.St} {k : U64} (inc : Nat)
    (h1 : s.applied = t.applied) (h2 : s.now = t.now) (h3 : s.floor = t.floor) (h4 : s.seq = t.seq)
    (h5 : s.nclients = t.nclients) (h6 : s.ver = t.ver) (h7 : s.known = t.known) (h8 : s.nkeys = t.nkeys)
    (hs : ∀ k', k' ≠ k → s.sess k' = t.sess k') (hm : ∀ k', k' ≠ k → s.mem k' = t.mem k') :
    spec.startSession s k inc = spec.startSession t k inc := by
  cases s; cases t
  simp only at h1 h2 h3 h4 h5 h6 h7 h8 hs hm
  subst h1 h2 h3 h4 h5 h6 h7 h8
  simp only [spec.startSession, spec.St.mk.injEq, true_and, and_true]
  constructor
  · funext k'; unfold spec.upd; by_cases e : k' = k
    · simp [e]
    · simp only [e, ite_false]; exact hs k' e
  · funext k'; unfold spec.upd; by_cases e : k' = k
    · simp [e]
    · simp only [e, ite_false]; exact hm k' e

theorem prod3_eq {α β γ : Type} {x : α × β × γ} {a : α} {b : β} (h1 : x.1 = a) (h2 : x.2.1 = b) :
    x = (a, b, x.2.2) := by
  obtain ⟨x1, x2, x3⟩ := x; simp only at h1 h2; rw [h1, h2]

/-- Closing a `DecidePost` whose spec decision needs no victim. -/
theorem decide_post_of {c : Core} {r : Request} {x : Effects × Core} (hI' : Inv x.2) (hcfg : x.2.cfg = c.cfg)
    (h : ∀ v, (spec.decide (αcfg c.cfg) (α c) (αreq r) v).1 = αeff x.1 ∧
      (spec.decide (αcfg c.cfg) (α c) (αreq r) v).2.1 = α x.2) : DecidePost c r x := by
  obtain ⟨v, hv⟩ := exists_valid (αcfg c.cfg) (α c)
  exact ⟨hI', hcfg, v, _, hv, prod3_eq (h v).1 (h v).2⟩

theorem decide_spec (c : Core) (r : Request) (hI : Inv c) :
    Core.decide c r ⦃ x => DecidePost c r x ⦄ := by
  unfold Core.decide
  simp only [lift, bind_ok]
  apply WP.spec_bind (get_spec c.clients (UScalar.cast .U64 r.client_id) hI.clients_wf)
  rintro found hfound
  have hreply := αeff_reply r
  have hmax : (core.num.U64.MAX).val = spec.MAX := by simp [core.num.U64.MAX, U64.rMax, spec.MAX]
  cases hf : found with
  | some slot =>
    have hk : slotOf c (UScalar.cast .U64 r.client_id) = some slot := by rw [← hf, hfound]; rfl
    obtain ⟨hslt, S, hS, hcl⟩ := hI.slot_of _ slot hk
    have hsess : (α c).sess (αreq r).client = some (αsess S) := by
      show sessA c (UScalar.cast .U64 r.client_id) = _; unfold sessA; rw [hk]; simp [hS]
    have hcs := hI.cast_slot hk
    have hsl : (UScalar.cast .Usize slot).val < c.sessions.val.length := by
      rw [hcs]; exact getElem?_some_lt' hS
    have hSv : c.sessions.val[(UScalar.cast .Usize slot).val] = some S := by
      rw [List.getElem?_eq_getElem (by rw [hcs] at hsl; exact hsl)] at hS; simp [hcs] at hS ⊢; exact hS
    simp only [lift, bind_ok, vidx _ _ hsl, hSv, core.option.Option.expect, ofOption]
    have hdec : ∀ v, spec.decide (αcfg c.cfg) (α c) (αreq r) v =
        if r.incarnation.val < S.incarnation.val then
          (spec.reply (αreq r) r.client_seqno.val (.expired (spec.sat1 S.incarnation.val)), α c, none)
        else if r.incarnation.val = S.incarnation.val then
          spec.inSession (αcfg c.cfg) (α c) (αreq r) (αsess S)
        else if r.client_seqno.val = 1 then
          spec.inSession (αcfg c.cfg) (spec.startSession (α c) (αreq r).client r.incarnation.val) (αreq r)
            ⟨r.incarnation.val, 1, (α c).now⟩
        else (spec.reply (αreq r) 1 .nak, α c, none) := by
      intro v; unfold spec.decide; rw [hsess]; rfl
    by_cases h1 : r.incarnation.val < S.incarnation.val
    · simp only [UScalar.lt_equiv, h1, ite_true, Core.reply, bind_ok, WP.spec_ok]
      refine decide_post_of hI rfl fun v => ⟨?_, ?_⟩ <;> rw [hdec, if_pos h1]
      rw [hreply]; simp [αout, sat1_eq]
    · have h1' : ¬ r.incarnation < S.incarnation := by simpa [UScalar.lt_equiv] using h1
      simp only [h1', ite_false]
      by_cases h2 : r.incarnation.val = S.incarnation.val
      · have h2' : r.incarnation = S.incarnation := (UScalar.eq_equiv _ _).mpr h2
        simp only [h2', ite_true]
        apply WP.spec_mono (in_session_spec c slot r S hI hk hS)
        rintro ⟨e, c'⟩ ⟨he, hc', hI', hcfg⟩
        refine decide_post_of hI' hcfg fun v => ?_
        rw [hdec, if_neg h1, if_pos h2]; exact ⟨he.symm, hc'.symm⟩
      · have h2' : ¬ r.incarnation = S.incarnation := fun e => h2 (by rw [e])
        simp only [h2', ite_false]
        by_cases h3 : r.client_seqno.val = 1
        · have h3' : r.client_seqno = 1#u64 := (UScalar.eq_equiv _ _).mpr (by simp [h3])
          simp only [h3', ite_true]
          have hset : c.sessions.set (UScalar.cast .Usize slot) (some S) = c.sessions :=
            vec_set_self _ _ _ (by rw [hcs]; exact hS)
          have hIs : Inv { c with sessions := c.sessions.set (UScalar.cast .Usize slot) (some S) } := by
            rw [hset]; exact hI
          apply WP.spec_bind (start_session_spec c slot r S hcl hIs hk)
          rintro c1 ⟨hI1, hα1, hS1, hcfg1, hcl1, hns1, hnow1⟩
          have hk1 : slotOf c1 (UScalar.cast .U64 r.client_id) = some slot := by
            show lookup c1.clients.buckets.val _ = _; rw [hcl1]; exact hk
          apply WP.spec_mono (in_session_spec c1 slot r _ hI1 hk1 hS1)
          rintro ⟨e, c'⟩ ⟨he, hc', hI', hcfg⟩
          refine decide_post_of hI' (hcfg.trans hcfg1) fun v => ?_
          rw [hdec, if_neg h1, if_neg h2, if_pos h3]
          rw [hcfg1, hα1] at he hc'
          have hs1 : αsess (Session.mk r.client_id r.incarnation 1#u64 c.now_ns) =
              ⟨r.incarnation.val, 1, (α c).now⟩ := by simp [αsess, α]
          rw [hs1] at he hc'
          exact ⟨he.symm, hc'.symm⟩
        · have h3' : ¬ r.client_seqno = 1#u64 := fun e => h3 (by rw [e]; rfl)
          simp only [h3', ite_false, Core.reply, bind_ok, WP.spec_ok]
          refine decide_post_of hI rfl fun v => ⟨?_, ?_⟩ <;> rw [hdec, if_neg h1, if_neg h2, if_neg h3]
          rw [hreply]; simp [αout]
  | none =>
    have hk : slotOf c (UScalar.cast .U64 r.client_id) = none := by rw [← hf, hfound]; rfl
    have hsess : (α c).sess (αreq r).client = none := by
      show sessA c (UScalar.cast .U64 r.client_id) = _; unfold sessA; rw [hk]
    have hdec : ∀ v, spec.decide (αcfg c.cfg) (α c) (αreq r) v =
        if r.incarnation.val ≤ c.incarnation_floor.val then
          (spec.reply (αreq r) r.client_seqno.val (.expired (spec.sat1 c.incarnation_floor.val)), α c, none)
        else if r.client_seqno.val ≠ 1 then (spec.reply (αreq r) 1 .nak, α c, none)
        else if c.clients.len.val < c.cfg.max_clients.val then
          spec.inSession (αcfg c.cfg)
            (spec.startSession { α c with nclients := (α c).nclients + 1 } (αreq r).client r.incarnation.val)
            (αreq r) ⟨r.incarnation.val, 1, (α c).now⟩
        else
          match v with
          | none => (spec.reply (αreq r) r.client_seqno.val .busy, α c, none)
          | some w =>
            match (α c).sess w with
            | none => (spec.reply (αreq r) r.client_seqno.val .busy, α c, none)
            | some V =>
              spec.inSession (αcfg c.cfg)
                (spec.startSession (evictA (α c) w V) (αreq r).client r.incarnation.val)
                (αreq r) ⟨r.incarnation.val, 1, (α c).now⟩ := by
      intro v; unfold spec.decide; rw [hsess]; rfl
    by_cases b1 : r.incarnation.val ≤ c.incarnation_floor.val
    · simp only [UScalar.le_equiv, b1, ite_true, Core.reply, bind_ok, WP.spec_ok, lift]
      refine decide_post_of hI rfl fun v => ⟨?_, ?_⟩ <;> rw [hdec, if_pos b1]
      rw [hreply]; simp [αout, sat1_eq]
    · have b1' : ¬ r.incarnation ≤ c.incarnation_floor := by simpa [UScalar.le_equiv] using b1
      simp only [b1', ite_false]
      by_cases b2 : r.client_seqno.val ≠ 1
      · have b2' : (r.client_seqno != 1#u64) = true := by
          simp only [bne_iff_ne, ne_eq]; intro e; exact b2 (by rw [e]; rfl)
        simp only [b2', ite_true, Core.reply, bind_ok, WP.spec_ok]
        refine decide_post_of hI rfl fun v => ⟨?_, ?_⟩ <;> rw [hdec, if_neg b1, if_pos b2]
        rw [hreply]; simp [αout]
      · have b2' : (r.client_seqno != 1#u64) = false := by
          simp only [bne_eq_false_iff_eq]; exact (UScalar.eq_equiv _ _).mpr (by simpa using b2)
        simp only [b2', Bool.false_eq_true, ite_false]
        step with allocate_session_spec c hI as ⟨ o, c1, hpost ⟩
        cases o with
        | none =>
          obtain ⟨rfl, hum, hnoidle⟩ := hpost
          simp only [Core.busy, Core.reply, bind_ok, WP.spec_ok]
          refine ⟨hI, rfl, none, none, ?_, ?_⟩
          · intro k' S' hS' hidle
            unfold α sessA at hS'
            simp only at hS'
            split at hS'
            · rename_i slot' _
              split at hS'
              · rename_i S'' hS''
                cases hS'
                exact hnoidle _ S'' hS'' (by simp [IdleC, spec.Idle, αcfg, αsess] at hidle ⊢; exact hidle)
              · cases hS'
            · cases hS'
          · rw [hdec, if_neg b1, if_neg b2, if_neg (by rw [hI.clients_len, hum]; omega)]
            rw [hreply]; simp [αout]
        | some slot =>
          simp only [lift, bind_ok]
          have hs1 : αsess (Session.mk r.client_id r.incarnation 1#u64 c.now_ns) =
              ⟨r.incarnation.val, 1, (α c).now⟩ := by simp [αsess, α]
          have hk0 : lookup c.clients.buckets.val (UScalar.cast .U64 r.client_id) = none := hk
          rcases hpost with ⟨hlt, hslot, u, hu, rfl⟩ | ⟨hfull, S, im1, f, hS, hid, hrem, hf, rfl⟩
          · -- A never-used slot.
            step with insert_spec _ (UScalar.cast .U64 r.client_id) slot hI.clients_wf as ⟨ b, im, hins ⟩
            obtain ⟨hI2, hlk⟩ := hI.add_client hk hlt u hu slot hslot b im hins
              (Session.mk r.client_id r.incarnation 1#u64 c.now_ns) rfl
            have hk2 : slotOf { c with unused := u, clients := im } (UScalar.cast .U64 r.client_id) = some slot := by
              show lookup im.buckets.val _ = _; rw [hlk]; simp
            apply WP.spec_bind (start_session_spec _ slot r (Session.mk r.client_id r.incarnation 1#u64 c.now_ns)
              rfl hI2 hk2)
            rintro c3 ⟨hI3, hα3, hS3, hcfg3, hcl3, -, -⟩
            have hk3 : slotOf c3 (UScalar.cast .U64 r.client_id) = some slot := by
              show lookup c3.clients.buckets.val _ = _; rw [hcl3]; exact hk2
            apply WP.spec_mono (in_session_spec c3 slot r _ hI3 hk3 hS3)
            rintro ⟨e, c'⟩ ⟨he, hc', hI', hcfg⟩
            refine decide_post_of hI' (hcfg.trans hcfg3) fun v => ?_
            rw [hdec, if_neg b1, if_neg b2, if_pos (by rw [hI.clients_len]; exact hlt)]
            have hcong : spec.startSession (α { c with unused := u, clients := im }) (UScalar.cast .U64 r.client_id)
                r.incarnation.val =
                spec.startSession { α c with nclients := (α c).nclients + 1 } (UScalar.cast .U64 r.client_id)
                  r.incarnation.val := by
              apply startSession_congr
              all_goals try rfl
              case hs =>
                intro k' hk'; show sessA _ k' = sessA c k'
                unfold sessA slotOf; simp only; rw [hlk k', if_neg hk']
              case hm =>
                intro k' hk'; show memA _ k' = memA c k'
                unfold memA slotOf; simp only; rw [hlk k', if_neg hk']
              case h5 =>
                show im.len.val = c.clients.len.val + 1
                have := hI2.clients_len; simp only at this; rw [this, hu, hI.clients_len]
            rw [hcfg3, hα3, hcong, hs1] at he hc'
            exact ⟨he.symm, hc'.symm⟩
          · -- The slot of an evicted session.
            step with insert_spec im1 (UScalar.cast .U64 r.client_id) slot hrem.1 as ⟨ b, im2, hins ⟩
            obtain ⟨hI2, hlk⟩ := hI.replace_client hk hfull hS im1 hrem b im2 hins f
              (Session.mk r.client_id r.incarnation 1#u64 c.now_ns) rfl
            have hk2 : slotOf { c with incarnation_floor := f, clients := im2, sessions := c.sessions.set (UScalar.cast .Usize slot) none } (UScalar.cast .U64 r.client_id) = some slot := by
              show lookup im2.buckets.val _ = _; rw [hlk]; simp
            have hI2' : Inv { c with incarnation_floor := f, clients := im2, sessions := (c.sessions.set (UScalar.cast .Usize slot) none).set (UScalar.cast .Usize slot) (some (Session.mk r.client_id r.incarnation 1#u64 c.now_ns)) } := by
              rw [vec_set_set]; exact hI2
            apply WP.spec_bind (start_session_spec _ slot r (Session.mk r.client_id r.incarnation 1#u64 c.now_ns)
              rfl hI2' hk2)
            rintro c3 ⟨hI3, hα3, hS3, hcfg3, hcl3, -, -⟩
            have hk3 : slotOf c3 (UScalar.cast .U64 r.client_id) = some slot := by
              show lookup c3.clients.buckets.val _ = _; rw [hcl3]; exact hk2
            apply WP.spec_mono (in_session_spec c3 slot r _ hI3 hk3 hS3)
            rintro ⟨e, c'⟩ ⟨he, hc', hI', hcfg⟩
            have hw := hI.slot_of_session hS
            have hsw : (α c).sess (UScalar.cast .U64 S.client_id) = some (αsess S) := by
              show sessA c _ = _; unfold sessA; rw [hw]; simp [hS]
            have hvalid : spec.ValidVictim (αcfg c.cfg) (α c) (some (UScalar.cast .U64 S.client_id)) :=
              ⟨αsess S, hsw, by simpa [spec.Idle, IdleC, αcfg, αsess, α] using hid⟩
            refine ⟨hI', hcfg.trans hcfg3, some (UScalar.cast .U64 S.client_id), _, hvalid, prod3_eq ?_ ?_⟩ <;>
              rw [hdec, if_neg b1, if_neg b2, if_neg (by rw [hI.clients_len, hfull]; omega)] <;>
              simp only [hsw]
            all_goals
              have hcong : spec.startSession (α { c with incarnation_floor := f, clients := im2, sessions := c.sessions.set (UScalar.cast .Usize slot) none }) (UScalar.cast .U64 r.client_id) r.incarnation.val = spec.startSession (evictA (α c) (UScalar.cast .U64 S.client_id) (αsess S)) (UScalar.cast .U64 r.client_id) r.incarnation.val := by
                have hw' : lookup c.clients.buckets.val (UScalar.cast .U64 S.client_id) = some slot := hw
                apply startSession_congr
                all_goals try rfl
                case h3 => show f.val = max c.incarnation_floor.val S.incarnation.val; exact hf
                case h5 =>
                  show im2.len.val = c.clients.len.val
                  have := hI2.clients_len; simp only at this; rw [this, hI.clients_len]
                case hs =>
                  intro k' hk'
                  show sessA _ k' = spec.upd (sessA c) _ none k'
                  unfold sessA slotOf spec.upd; simp only; rw [hlk k', if_neg hk']
                  by_cases e : k' = UScalar.cast .U64 S.client_id
                  · simp [e]
                  · simp only [e, ite_false]
                    cases hk'' : lookup c.clients.buckets.val k' with
                    | none => rfl
                    | some s' =>
                      have hne : slot.val ≠ s'.val := fun h => e (hI.slot_inj (k := k') hk'' (by rw [hw, u32_eq h]))
                      have hcs : (UScalar.cast .Usize slot).val = slot.val := hI.cast_slot hw
                      simp [alloc.vec.Vec.set_val_eq, hcs, List.getElem?_set_ne hne]
                case hm =>
                  intro k' hk'
                  show memA _ k' = spec.upd (memA c) _ (fun _ => none) k'
                  funext idx
                  unfold memA slotOf spec.upd; simp only; rw [hlk k', if_neg hk']
                  by_cases e : k' = UScalar.cast .U64 S.client_id <;> simp [e]
              rw [hcfg3, hα3, hcong, hs1] at he hc'
            · exact he.symm
            · exact hc'.symm

/-! ## The refinement theorem -/

def αsub (s : krusty_wire.ingress.Submit) : spec.Req :=
  αreq ⟨s.client_id, s.incarnation, s.client_seqno, s.append⟩

def αres : core.result.Result Effects ApplyError → Option spec.Eff
  | .Ok e => some (αeff e)
  | .Err _ => none

theorem Inv.with_time {c : Core} (hI : Inv c) (a n : U64) : Inv { c with applied := a, now_ns := n } := by
  obtain ⟨a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19⟩ := hI
  exact ⟨a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19⟩

/-- **Refinement.** Under the invariant, `Core::apply` never panics, keeps the
    invariant, and does what one step of the abstract sequencer does: the
    same effects (or the same out-of-order error) and the same new state. -/
theorem apply_refines (c : Core) (li ts : U64) (sub : krusty_wire.ingress.Submit) (hI : Inv c) :
    Core.apply c li ts sub ⦃ x => Inv x.2 ∧ x.2.cfg = c.cfg ∧
      ∃ g, spec.Step (αcfg c.cfg) (α c) li.val ts.val (αsub sub) (αres x.1) (α x.2) g ⦄ := by
  have hmax : (core.num.U64.MAX).val = spec.MAX := by simp [core.num.U64.MAX, U64.rMax, spec.MAX]
  obtain ⟨v0, hv0⟩ := exists_valid (αcfg c.cfg) { α c with applied := li.val, now := max (α c).now ts.val }
  unfold Core.apply
  by_cases h1 : c.applied.val = spec.MAX
  · have h1' : c.applied = core.num.U64.MAX := (UScalar.eq_equiv _ _).mpr (by rw [hmax]; exact h1)
    simp only [h1', ite_true, lift, bind_ok, WP.spec_ok]
    refine ⟨hI, by first | trivial | rfl, none, v0, hv0, ?_⟩
    simp [spec.apply, α, h1, αres]
  · have h1' : ¬ c.applied = core.num.U64.MAX := fun e => h1 (by rw [e, hmax])
    simp only [h1', ite_false]
    have hlt : c.applied.val + 1 ≤ U64.max := by
      have := u64_lt c.applied; rw [u64max]; unfold spec.MAX at h1; omega
    have hreq : αreq ⟨sub.client_id, sub.incarnation, sub.client_seqno, sub.append⟩ = αsub sub := rfl
    have key : ∀ n : U64, li.val = c.applied.val + 1 → n.val = max c.now_ns.val ts.val →
        (do let r ← Request.of sub
            let (e, self1) ← Core.decide { c with applied := li, now_ns := n } r
            ok (core.result.Result.Ok e, self1)) ⦃ x => Inv x.2 ∧ x.2.cfg = c.cfg ∧
          ∃ g, spec.Step (αcfg c.cfg) (α c) li.val ts.val (αsub sub) (αres x.1) (α x.2) g ⦄ := by
      intro n h2 hn
      simp only [Request.of, bind_ok]
      step with decide_spec _ _ (hI.with_time li n) as ⟨ e, c', hpost ⟩
      obtain ⟨hI', hcfg, v, g, hv, hd⟩ := hpost
      have hα0 : α { c with applied := li, now_ns := n } = { α c with applied := li.val, now := max (α c).now ts.val } := by
        simp only [α]; rw [hn]; rfl
      rw [hα0] at hv hd
      rw [hreq] at hd
      refine ⟨hI', hcfg, g, v, hv, ?_⟩
      have hnot : ¬ ((α c).applied = spec.MAX ∨ li.val ≠ (α c).applied + 1) := by
        simp only [α]; omega
      unfold spec.apply
      rw [if_neg hnot]
      simp only [αres]
      rw [hd]
    step*
    · rename_i hne
      have h2 : li.val ≠ c.applied.val + 1 := by
        rw [bne_iff_ne] at hne; intro e; apply hne; exact (UScalar.eq_equiv _ _).mpr (by rw [i_post]; exact e)
      refine ⟨hI, none, v0, hv0, ?_⟩
      simp [spec.apply, α, h2, αres]
    · rename_i hne hgt
      have h2 : li.val = c.applied.val + 1 := by
        simp only [bne_iff_ne, ne_eq, Decidable.not_not] at hne; rw [hne, i_post]
      simp only [gt_iff_lt, UScalar.lt_equiv] at hgt
      exact key ts h2 (by omega)
    · rename_i hne hgt
      have h2 : li.val = c.applied.val + 1 := by
        simp only [bne_iff_ne, ne_eq, Decidable.not_not] at hne; rw [hne, i_post]
      simp only [gt_iff_lt, UScalar.lt_equiv, not_lt] at hgt
      exact key c.now_ns h2 (by omega)

/-- `Core::new` establishes the invariant (for configurations whose tables
    fit the address space twice over; any `u32`-sized table on a 64-bit host). -/
theorem new_inv (cfg : Config) (h1 : 1 ≤ cfg.max_clients.val) (h2 : cfg.max_clients.val ≤ U32.max)
    (h3 : 1 ≤ cfg.outcome_ring.val) (h4 : cfg.max_keys.val ≤ U32.max)
    (h5 : cfg.max_clients.val * cfg.outcome_ring.val ≤ Usize.max)
    (h6 : 4 * cfg.max_clients.val ≤ Usize.max) (h7 : 4 * cfg.max_keys.val ≤ Usize.max) :
    Core.new cfg ⦃ c => Inv c ∧ c.cfg = cfg ∧ c.applied.val = 0 ∧ c.next_seqno.val = 1 ⦄ := by
  unfold Core.new
  have hu32 : (UScalar.cast .Usize core.num.U32.MAX).val = U32.max := by
    rw [UScalar.cast_val_eq]
    have : (core.num.U32.MAX).val = U32.max := by simp [core.num.U32.MAX, U32.rMax, U32.max_def, U32.numBits]
    rw [this]; exact Nat.mod_eq_of_lt (u32_lt_usize _ (le_refl _))
  have hcl1 : (core.option.Option.Insts.CoreCloneClone Session.Insts.CoreCloneClone).clone none = ok none := rfl
  have hcl2 : (core.option.Option.Insts.CoreCloneClone RingSlot.Insts.CoreCloneClone).clone none = ok none := rfl
  have hone : (1#usize).val = 1 := rfl
  simp only [massert, lift, bind_ok, ge_iff_le, UScalar.le_equiv, hu32, hone, h1, h2, h3, h4, ite_true]
  step with new_spec cfg.max_clients h6 as ⟨ im, hw, hmax, hlen, hnone ⟩
  step*
  step with new_spec cfg.max_keys h7 as ⟨ im1, hw1, hmax1, hlen1, hnone1 ⟩
  refine ⟨⟨h1, h2, h3, h5, h4, hw, hmax, hw1, hmax1, by simp [v_post], by simp [v1_post, i2_post],
    by simp, by simp [hlen], ?_, ?_, by simp [hlen1, alloc.vec.Vec.with_capacity], by simp [hlen1, alloc.vec.Vec.with_capacity],
    ?_, ?_⟩, rfl, rfl⟩
  · intro k slot hk; simp only [slotOf, hnone] at hk; cases hk
  · intro j S hS; simp only [v_post] at hS; rw [List.getElem?_replicate] at hS; split at hS <;> simp at hS
  · intro k i hk; simp only [hnone1] at hk; cases hk
  · intro k i hk; simp only [hnone1] at hk; cases hk

/-- **G4 for the code.** Run from a state the history invariant describes,
    `Core::apply` keeps it described, and decides only messages it never
    decided before: no (client, incarnation, seq) is sequenced twice. -/
theorem apply_at_most_once (c : Core) (li ts : U64) (sub : krusty_wire.ingress.Submit) (hI : Inv c)
    (h : spec.Hist) (hH : spec.HInv (αcfg c.cfg) (α c) h) :
    Core.apply c li ts sub ⦃ x => Inv x.2 ∧ ∃ g, (∀ d, g = some d → h (αsub sub).client (αsub sub).inc (αsub sub).seq = none) ∧
      spec.HInv (αcfg x.2.cfg) (α x.2) (h.after (αsub sub) g) ⦄ := by
  apply WP.spec_mono (apply_refines c li ts sub hI)
  rintro x ⟨hI', hcfg, g, hstep⟩
  obtain ⟨hf, hH'⟩ := spec.step_hinv hH hstep
  exact ⟨hI', g, hf, by rw [hcfg]; exact hH'⟩

#print axioms apply_refines
#print axioms new_inv
#print axioms apply_at_most_once

end krusty_core.refine
