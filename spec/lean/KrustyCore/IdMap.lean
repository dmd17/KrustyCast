/-
  `krusty_core::table::IdMap` (linear probing, backward-shift deletion)
  behaves as a finite map from keys to slots.
-/
import KrustyCore.Funs

open Aeneas Aeneas.Std Result
open krusty_core

namespace krusty_core.idmap

/-! ## Cyclic positions -/

/-- Cyclic distance from `a` forward to `b` in a table of `n` buckets. -/
def dist (n a b : Nat) : Nat := (b + n - a) % n

theorem mod_lt2 {a n : Nat} (h : a < 2 * n) : a % n = if a < n then a else a - n := by
  split
  · exact Nat.mod_eq_of_lt ‹_›
  · rw [Nat.mod_eq_sub_mod (by omega), Nat.mod_eq_of_lt (by omega)]

theorem dist_lt {n a b : Nat} (hn : 0 < n) : dist n a b < n := Nat.mod_lt _ hn

theorem add_dist {n a b : Nat} (ha : a < n) (hb : b < n) : (a + dist n a b) % n = b := by
  unfold dist
  rw [mod_lt2 (a := b + n - a) (by omega)]
  split <;> rw [mod_lt2 (by omega)] <;> split <;> omega

theorem dist_add {n a d : Nat} (ha : a < n) (hd : d < n) : dist n a ((a + d) % n) = d := by
  unfold dist
  rw [mod_lt2 (a := a + d) (by omega)]
  split <;> rw [mod_lt2 (by omega)] <;> split <;> omega

theorem dist_self {n a : Nat} : dist n a a = 0 := by simp [dist]

/-- One step forward. -/
theorem dist_succ {n a b : Nat} (ha : a < n) (hb : b < n) (h : dist n a b + 1 < n) :
    dist n a ((b + 1) % n) = dist n a b + 1 := by
  unfold dist at h ⊢
  rw [mod_lt2 (a := b + n - a) (by omega)] at h ⊢
  rw [mod_lt2 (a := b + 1) (by omega)]
  split <;> split at h <;> rw [mod_lt2 (by omega)] <;> split <;> omega

theorem dist_eq_zero {n a b : Nat} (ha : a < n) (hb : b < n) (h : dist n a b = 0) : a = b := by
  have := add_dist ha hb; rw [h, Nat.add_zero, Nat.mod_eq_of_lt ha] at this; exact this

/-- Going round via `b`: the distances add up, possibly with one extra lap. -/
theorem dist_tri {n a b c : Nat} (ha : a < n) (hb : b < n) (hc : c < n) :
    dist n a b + dist n b c = dist n a c ∨ dist n a b + dist n b c = dist n a c + n := by
  unfold dist
  rw [mod_lt2 (a := b + n - a) (by omega), mod_lt2 (a := c + n - b) (by omega),
    mod_lt2 (a := c + n - a) (by omega)]
  split <;> split <;> split <;> omega

/-- `b` lies on the way from `a` to `c`. -/
theorem dist_split {n a b c : Nat} (ha : a < n) (hb : b < n) (hc : c < n)
    (h : dist n a b < dist n a c) : dist n a b + dist n b c = dist n a c := by
  have := dist_lt (a := b) (b := c) (n := n) (by omega)
  rcases dist_tri ha hb hc with h' | h' <;> omega

/-- The machine's `(x - y) & mask`, for positions of a power-of-two table. -/
theorem wrap_dist {n p a b : Nat} (hn : n = 2 ^ p) (hp : p ≤ 64) (ha : a < n) (hb : b < n) :
    ((a + 2 ^ 64 - b) % 2 ^ 64) % n = dist n b a := by
  have hd : n ∣ 2 ^ 64 := hn ▸ Nat.pow_dvd_pow 2 hp
  obtain ⟨q, hq⟩ := hd
  have hq1 : 1 ≤ q := by rcases q with _ | q <;> simp_all
  rw [Nat.mod_mod_of_dvd _ ⟨q, hq⟩, dist, hq]
  have : a + n * q - b = (a + n - b) + n * (q - 1) := by
    have : n * q = n + n * (q - 1) := by
      obtain ⟨r, rfl⟩ : ∃ r, q = r + 1 := ⟨q - 1, by omega⟩
      simp [Nat.mul_add, Nat.add_comm]
    omega
  rw [this, Nat.add_mul_mod_self_left]

/-! ## Machine-level pieces -/

/-- The hash as a number. -/
def hashN (k : U64) : Nat := (k.val * 11400714819323198485 % 2 ^ 64) / 2 ^ 32

/-- Home bucket of `k` in a table of `n` buckets. -/
def homeN (n : Nat) (k : U64) : Nat := hashN k % n

@[step] theorem hash_spec (k : U64) : table.hash k ⦃ h => h.val = hashN k ⦄ := by
  unfold table.hash hashN
  step*
  rw [h_post, i_post, core.num.U64.wrapping_mul_val_eq]
  simp [Nat.shiftRight_eq_div_pow, U64.size_def, U64.numBits]

theorem land_mask {x n p : Nat} (hn : n = 2 ^ p) : x &&& (n - 1) = x % n := by
  subst hn; exact Nat.and_two_pow_sub_one_eq_mod x p

theorem usize_lt (x : Nat) (h : x ≤ Usize.max) : x < 2 ^ System.Platform.numBits := by
  have : Usize.max = 2 ^ System.Platform.numBits - 1 := by simp [Usize.max_def, Usize.numBits]
  have : 0 < 2 ^ System.Platform.numBits := Nat.two_pow_pos _
  omega

theorem usize_le_u64 : Usize.max ≤ U64.max := by
  have := System.Platform.numBits_eq
  simp only [Usize.max_def, U64.max_def, Usize.numBits, U64.numBits]
  rcases this with h | h <;> simp [UScalarTy.numBits, h]

theorem vidx {α : Type} (v : alloc.vec.Vec α) (i : Usize) (h : i.val < v.val.length) :
    alloc.vec.Vec.index (core.slice.index.SliceIndexUsizeSlice α) v i = ok v.val[i.val] := by
  rw [alloc.vec.Vec.index_slice_index]
  simp [alloc.vec.Vec.index_usize, List.getElem?_eq_getElem h]

theorem vidx_mut {α : Type} (v : alloc.vec.Vec α) (i : Usize) (h : i.val < v.val.length) :
    alloc.vec.Vec.index_mut (core.slice.index.SliceIndexUsizeSlice α) v i =
      ok (v.val[i.val], alloc.vec.Vec.set v i) := by
  rw [alloc.vec.Vec.index_mut_slice_index]
  simp [alloc.vec.Vec.index_mut_usize, alloc.vec.Vec.index_usize, List.getElem?_eq_getElem h]

theorem spec_let_pair {α β γ : Type} (a : α) (b : β) (f : β → Result γ) (P : γ → Prop)
    (h : f b ⦃ P ⦄) : (match (a, b) with | (_, x) => f x) ⦃ P ⦄ := h

/-! ## The map abstraction -/

abbrev BL := List (Option table.Bucket)

/-- The slot `l` holds for `k`, if any. -/
def lookup (l : BL) (k : U64) : Option U32 :=
  l.findSome? fun o => match o with
    | some b => if b.key = k then some b.slot else none
    | none => none

def Distinct (l : BL) : Prop :=
  ∀ (i j : Nat) (bi bj : table.Bucket), l[i]? = some (some bi) → l[j]? = some (some bj) → bi.key = bj.key → i = j

/-- Every entry is reachable from its home bucket without passing an empty one. -/
def Runs (n : Nat) (l : BL) : Prop :=
  ∀ (j : Nat) (b : table.Bucket), l[j]? = some (some b) →
    ∀ d < dist n (homeN n b.key) j, ∃ c, l[(homeN n b.key + d) % n]? = some (some c)

theorem lookup_some_mem {l : BL} {k : U64} {s : U32} (h : lookup l k = some s) :
    ∃ (j : Nat) (b : table.Bucket), l[j]? = some (some b) ∧ b.key = k ∧ b.slot = s := by
  induction l with
  | nil => simp [lookup] at h
  | cons o t ih =>
    simp only [lookup, List.findSome?_cons] at h
    cases o with
    | none => obtain ⟨j, b, h1, h2⟩ := ih h; exact ⟨j + 1, b, by rw [List.getElem?_cons_succ]; exact h1, h2⟩
    | some b =>
      by_cases hk : b.key = k
      · simp [hk] at h; exact ⟨0, b, by simp, hk, h⟩
      · simp [hk] at h; obtain ⟨j, b, h1, h2⟩ := ih h; exact ⟨j + 1, b, by rw [List.getElem?_cons_succ]; exact h1, h2⟩

theorem lookup_isSome {l : BL} {j : Nat} {b : table.Bucket} (h : l[j]? = some (some b)) :
    (lookup l b.key).isSome := by
  induction l generalizing j with
  | nil => simp at h
  | cons o t ih =>
    simp only [lookup, List.findSome?_cons]
    cases j with
    | zero => simp at h; subst h; simp
    | succ j =>
      simp at h
      cases o with
      | none => exact ih h
      | some c =>
        by_cases hk : c.key = b.key
        · simp [hk]
        · simp only [hk, ite_false]; exact ih h

theorem lookup_of_mem {l : BL} (hd : Distinct l) {j : Nat} {b : table.Bucket}
    (h : l[j]? = some (some b)) : lookup l b.key = some b.slot := by
  obtain ⟨s, hs⟩ := Option.isSome_iff_exists.mp (lookup_isSome h)
  obtain ⟨j', b', h1, h2, h3⟩ := lookup_some_mem hs
  have := hd _ _ _ _ h1 h h2
  subst this
  rw [h] at h1; cases h1
  rw [hs, h3]

theorem lookup_none {l : BL} {k : U64}
    (h : ∀ (j : Nat) (b : table.Bucket), l[j]? = some (some b) → b.key ≠ k) :
    lookup l k = none := by
  cases hs : lookup l k with
  | none => rfl
  | some s => obtain ⟨j, b, h1, h2, -⟩ := lookup_some_mem hs; exact absurd h2 (h j b h1)

/-- Two tables with the same entries hold the same map. -/
theorem lookup_ext {l l' : BL} (hd : Distinct l) (hd' : Distinct l')
    (h : ∀ b : table.Bucket, (∃ j : Nat, l[j]? = some (some b)) ↔ (∃ j : Nat, l'[j]? = some (some b)))
    (k : U64) :
    lookup l k = lookup l' k := by
  cases hs : lookup l k with
  | none =>
    cases hs' : lookup l' k with
    | none => rfl
    | some s =>
      obtain ⟨j, b, h1, h2, -⟩ := lookup_some_mem hs'
      obtain ⟨j', h1'⟩ := (h b).mpr ⟨j, h1⟩
      have := lookup_of_mem hd h1'; rw [h2, hs] at this; cases this
  | some s =>
    obtain ⟨j, b, h1, h2, h3⟩ := lookup_some_mem hs
    obtain ⟨j', h1'⟩ := (h b).mp ⟨j, h1⟩
    have := lookup_of_mem hd' h1'; rw [h2, h3] at this; exact this.symm

structure Wf (m : table.IdMap) : Prop where
  pow : ∃ p, 0 < p ∧ m.buckets.val.length = 2 ^ p
  mask : m.mask.val = m.buckets.val.length - 1
  cap : 2 * m.max_len.val ≤ m.buckets.val.length
  len_le : m.len.val ≤ m.max_len.val
  len_eq : m.len.val = m.buckets.val.countP Option.isSome
  distinct : Distinct m.buckets.val
  runs : Runs m.buckets.val.length m.buckets.val

/-- The map an `IdMap` represents. -/
def IdMap.toMap (m : table.IdMap) : U64 → Option U32 := lookup m.buckets.val

/-! ## Machine-level specs -/

/-- Power-of-two size and the matching mask: all `home` and `next` need. -/
structure Shape (m : table.IdMap) : Prop where
  pow : ∃ p, 0 < p ∧ m.buckets.val.length = 2 ^ p
  mask : m.mask.val = m.buckets.val.length - 1

theorem Shape.pos {m : table.IdMap} (hs : Shape m) : 0 < m.buckets.val.length := by
  obtain ⟨p, -, hp⟩ := hs.pow; rw [hp]; exact Nat.two_pow_pos p

theorem Shape.and_mask {m : table.IdMap} (hs : Shape m) (x : Nat) :
    x &&& m.mask.val = x % m.buckets.val.length := by
  obtain ⟨p, -, hp⟩ := hs.pow; rw [hs.mask]; exact land_mask hp

theorem Wf.shape {m : table.IdMap} (hw : Wf m) : Shape m := ⟨hw.pow, hw.mask⟩

theorem Wf.pos {m : table.IdMap} (hw : Wf m) : 0 < m.buckets.val.length := hw.shape.pos

theorem home_spec' (m : table.IdMap) (k : U64) (hw : Shape m) :
    table.IdMap.home m k ⦃ i => i.val = homeN m.buckets.val.length k ∧ i.val < m.buckets.val.length ⦄ := by
  unfold table.IdMap.home
  step*
  have hn : m.buckets.val.length ≤ Usize.max := alloc.vec.Vec.len_ineq m.buckets
  have hpos := hw.pos
  have hlt : homeN m.buckets.val.length k < m.buckets.val.length := Nat.mod_lt _ hpos
  have e : (UScalar.cast UScalarTy.Usize i1).val = homeN m.buckets.val.length k := by
    rw [UScalar.cast_val_eq, i1_post, UScalar.val_and, hw.and_mask, i_post]
    exact Nat.mod_eq_of_lt (usize_lt _ (by show homeN _ k ≤ _; omega))
  exact ⟨e, e ▸ hlt⟩

theorem home_spec (m : table.IdMap) (k : U64) (hw : Wf m) :
    table.IdMap.home m k ⦃ i => i.val = homeN m.buckets.val.length k ∧ i.val < m.buckets.val.length ⦄ :=
  home_spec' m k hw.shape

theorem next_spec' (m : table.IdMap) (i : Usize) (hw : Shape m) (hi : i.val < m.buckets.val.length) :
    table.IdMap.next m i ⦃ j => j.val = (i.val + 1) % m.buckets.val.length ⦄ := by
  unfold table.IdMap.next
  have hn : m.buckets.val.length ≤ Usize.max := alloc.vec.Vec.len_ineq m.buckets
  have h64 := usize_le_u64
  have hu : Usize.max ≤ 2 ^ 64 - 1 := by simpa [U64.max_def, U64.numBits] using h64
  step*
  have hpos := hw.pos
  have hlt : (i.val + 1) % m.buckets.val.length < m.buckets.val.length := Nat.mod_lt _ hpos
  have h1 : i1.val = i.val := by
    rw [i1_post, UScalar.cast_val_eq]; exact Nat.mod_eq_of_lt (by simp; omega)
  rw [UScalar.cast_val_eq, i3_post, UScalar.val_and, hw.and_mask, i2_post, h1]
  exact Nat.mod_eq_of_lt (usize_lt _ (by omega))

theorem next_spec (m : table.IdMap) (i : Usize) (hw : Wf m) (hi : i.val < m.buckets.val.length) :
    table.IdMap.next m i ⦃ j => j.val = (i.val + 1) % m.buckets.val.length ⦄ :=
  next_spec' m i hw.shape hi

/-! ## Probing -/

/-- Positions `h, h + 1, …` before `i` all hold keys other than `k`. -/
def Path (l : BL) (n h i : Nat) (k : U64) : Prop :=
  ∀ d < dist n h i, ∃ c : table.Bucket, l[(h + d) % n]? = some (some c) ∧ c.key ≠ k

theorem Path.occ {l : BL} {n h i : Nat} {k : U64} (hp : Path l n h i k) :
    ∀ d < dist n h i, ∃ c : table.Bucket, l[(h + d) % n]? = some (some c) := by
  intro d hd; obtain ⟨c, hc, -⟩ := hp d hd; exact ⟨c, hc⟩

/-- `i` holds `k`, or is empty. -/
def Found (l : BL) (k : U64) (i : Nat) : Prop :=
  (∃ b : table.Bucket, l[i]? = some (some b) ∧ b.key = k) ∨ l[i]? = some none

/-- A table at most half full is never full. -/
theorem Wf.not_full {m : table.IdMap} (hw : Wf m) :
    ¬ ∀ p < m.buckets.val.length, ∃ c : table.Bucket, m.buckets.val[p]? = some (some c) := by
  intro h
  have hall : m.buckets.val.countP Option.isSome = m.buckets.val.length := by
    rw [List.countP_eq_length]
    intro a ha
    obtain ⟨p, hp, hpa⟩ := List.mem_iff_getElem.mp ha
    subst hpa
    obtain ⟨c, hc⟩ := h p hp
    rw [List.getElem?_eq_getElem hp] at hc
    simp only [Option.some.injEq] at hc
    simp [hc]
  have := hw.len_eq; have := hw.len_le; have := hw.cap; have := hw.pos
  obtain ⟨p, hp, hpn⟩ := hw.pow
  have : 2 ≤ m.buckets.val.length := by
    rw [hpn]; exact Nat.le_trans (by decide) (Nat.pow_le_pow_right (by decide) hp)
  omega

/-- If the path from `h` covers all but one position and that one is occupied,
    the table is full. -/
theorem Wf.path_short {m : table.IdMap} (hw : Wf m) {h i : Nat} {k : U64}
    (hh : h < m.buckets.val.length) (hi : i < m.buckets.val.length)
    (hp : Path m.buckets.val m.buckets.val.length h i k)
    {b : table.Bucket} (hb : m.buckets.val[i]? = some (some b)) :
    dist m.buckets.val.length h i + 1 < m.buckets.val.length := by
  have hd := dist_lt (a := h) (b := i) hw.pos
  by_contra hc
  apply hw.not_full
  intro p hpn
  have hdp := dist_lt (a := h) (b := p) hw.pos
  rw [← add_dist hh hpn]
  by_cases hlt : dist m.buckets.val.length h p < dist m.buckets.val.length h i
  · obtain ⟨c, hc, -⟩ := hp _ hlt; exact ⟨c, hc⟩
  · have : dist m.buckets.val.length h p = dist m.buckets.val.length h i := by omega
    rw [this, add_dist hh hi]; exact ⟨b, hb⟩

theorem probe_spec (m : table.IdMap) (k : U64) (hw : Wf m) :
    table.IdMap.probe m k ⦃ i => i.val < m.buckets.val.length ∧
      Path m.buckets.val m.buckets.val.length (homeN m.buckets.val.length k) i.val k ∧
      Found m.buckets.val k i.val ⦄ := by
  unfold table.IdMap.probe
  apply WP.spec_bind (home_spec m k hw)
  rintro i0 ⟨hi0, hi0n⟩
  unfold table.IdMap.probe_loop
  apply loop.spec_decr_nat
    (fun (p : Usize × Bool) => if p.2 then 2 * m.buckets.val.length - dist m.buckets.val.length (homeN m.buckets.val.length k) p.1.val else 0)
    (fun (p : Usize × Bool) => p.1.val < m.buckets.val.length ∧
      Path m.buckets.val m.buckets.val.length (homeN m.buckets.val.length k) p.1.val k ∧
      (p.2 = false → Found m.buckets.val k p.1.val))
  · rintro ⟨i, s⟩ ⟨hi, hp, hf⟩
    have hhn : homeN m.buckets.val.length k < m.buckets.val.length := Nat.mod_lt _ hw.pos
    have hdn := dist_lt (a := homeN m.buckets.val.length k) (b := i.val) hw.pos
    unfold table.IdMap.probe_loop.body
    cases s with
    | false => simp only [Bool.false_eq_true, ite_false, WP.spec_ok]; exact ⟨hi, hp, hf rfl⟩
    | true =>
      simp only [ite_true]
      step*
      case h1 =>
        rename_i hb hk
        have : m.buckets.val[i.val]? = some (some b1) := by
          rw [List.getElem?_eq_getElem hi, ← b_post, hb]
        refine ⟨hi, hp, fun _ => Or.inl ⟨b1, this, hk⟩, ?_⟩
        simp; omega
      case h2 =>
        rename_i hb hk
        have hb' : m.buckets.val[i.val]? = some (some b1) := by
          rw [List.getElem?_eq_getElem hi, ← b_post, hb]
        apply WP.spec_bind (next_spec m i hw hi)
        intro i1 hi1
        simp only [WP.spec_ok]
        have hs := hw.path_short hhn hi hp hb'
        have hd1 : dist m.buckets.val.length (homeN m.buckets.val.length k) i1.val = dist m.buckets.val.length (homeN m.buckets.val.length k) i.val + 1 := by rw [hi1]; exact dist_succ hhn hi hs
        refine ⟨by rw [hi1]; exact Nat.mod_lt _ hw.pos, ?_, by simp, by simp; omega⟩
        intro d hd
        rw [hd1] at hd
        by_cases hdl : d < dist m.buckets.val.length (homeN m.buckets.val.length k) i.val
        · exact hp d hdl
        · have : d = dist m.buckets.val.length (homeN m.buckets.val.length k) i.val := by omega
          subst this
          rw [add_dist hhn hi]; exact ⟨b1, hb', hk⟩
      all_goals
        rename_i hb
        have : m.buckets.val[i.val]? = some none := by
          rw [List.getElem?_eq_getElem hi, ← b_post, hb]
        exact ⟨hi, hp, fun _ => Or.inr this, by simp; omega⟩
  · refine ⟨by omega, ?_, by simp⟩
    intro d hd; rw [hi0, dist_self] at hd; omega

/-- Probing that stops at an empty bucket proves the key absent. -/
theorem Wf.absent {m : table.IdMap} (hw : Wf m) {k : U64} {i : Nat}
    (hi : i < m.buckets.val.length)
    (hp : Path m.buckets.val m.buckets.val.length (homeN m.buckets.val.length k) i k)
    (hnone : m.buckets.val[i]? = some none) :
    ∀ (j : Nat) (b : table.Bucket), m.buckets.val[j]? = some (some b) → b.key ≠ k := by
  intro j b hj hk
  have hjn : j < m.buckets.val.length := by
    rcases Nat.lt_or_ge j m.buckets.val.length with h | h
    · exact h
    · rw [List.getElem?_eq_none h] at hj; cases hj
  have hhn : homeN m.buckets.val.length k < m.buckets.val.length := Nat.mod_lt _ hw.pos
  have hr := hw.runs j b hj
  rw [hk] at hr
  rcases Nat.lt_trichotomy (dist m.buckets.val.length (homeN m.buckets.val.length k) j)
      (dist m.buckets.val.length (homeN m.buckets.val.length k) i) with hlt | heq | hgt
  · obtain ⟨c, hc, hck⟩ := hp _ hlt
    rw [add_dist hhn hjn, hj] at hc
    cases hc; exact hck hk
  · have : j = i := by rw [← add_dist hhn hjn, heq, add_dist hhn hi]
    subst this; rw [hj] at hnone; cases hnone
  · obtain ⟨c, hc⟩ := hr _ hgt
    rw [add_dist hhn hi, hnone] at hc; cases hc

theorem get_spec (m : table.IdMap) (k : U64) (hw : Wf m) :
    table.IdMap.get m k ⦃ r => r = lookup m.buckets.val k ⦄ := by
  unfold table.IdMap.get
  apply WP.spec_bind (probe_spec m k hw)
  rintro i ⟨hi, hp, hf⟩
  have hl : ∀ o, m.buckets.val[i.val]? = some o → m.buckets.val[i.val] = o := by
    intro o h; rw [List.getElem?_eq_getElem hi] at h; simpa using h
  rcases hf with ⟨b, hb, hk⟩ | hb
  · have e := hl _ hb
    have L := lookup_of_mem hw.distinct hb
    rw [hk] at L
    step*
  · have e := hl _ hb
    have L := lookup_none (hw.absent hi hp hb)
    step*

/-! ## Updating one bucket -/

theorem countP_set {α : Type} (p : α → Bool) (l : List α) (i : Nat) (a : α) (hi : i < l.length) :
    (l.set i a).countP p + (if p l[i] then 1 else 0) = l.countP p + (if p a then 1 else 0) := by
  induction l generalizing i with
  | nil => simp at hi
  | cons x t ih =>
    cases i with
    | zero => simp only [List.set_cons_zero, List.countP_cons, List.getElem_cons_zero]; omega
    | succ i =>
      simp only [List.set_cons_succ, List.countP_cons, List.getElem_cons_succ]
      have := ih i (by simpa using hi); omega

theorem getElem?_some_lt {l : BL} {j : Nat} {o : Option table.Bucket} (h : l[j]? = some o) :
    j < l.length := by
  rcases Nat.lt_or_ge j l.length with h' | h'
  · exact h'
  · rw [List.getElem?_eq_none h'] at h; cases h

theorem set_occ {l : BL} {i : Nat} {b : table.Bucket} {p : Nat} {c : table.Bucket}
    (h : l[p]? = some (some c)) : ∃ c', (l.set i (some b))[p]? = some (some c') := by
  by_cases hp : i = p
  · subst hp; exact ⟨b, by rw [List.getElem?_set_self (getElem?_some_lt h)]⟩
  · exact ⟨c, by rw [List.getElem?_set_ne hp]; exact h⟩

theorem set_distinct {l : BL} (hd : Distinct l) {i : Nat} {b : table.Bucket} (hi : i < l.length)
    (hb : ∀ (j : Nat) (c : table.Bucket), l[j]? = some (some c) → c.key = b.key → j = i) :
    Distinct (l.set i (some b)) := by
  intro j1 j2 c1 c2 h1 h2 hk
  by_cases e1 : i = j1 <;> by_cases e2 : i = j2
  · rw [← e1, ← e2]
  · subst e1; rw [List.getElem?_set_self hi] at h1; cases h1
    rw [List.getElem?_set_ne e2] at h2; exact absurd (hb _ _ h2 hk.symm).symm e2
  · subst e2; rw [List.getElem?_set_self hi] at h2; cases h2
    rw [List.getElem?_set_ne e1] at h1; exact absurd (hb _ _ h1 hk).symm e1
  · rw [List.getElem?_set_ne e1] at h1; rw [List.getElem?_set_ne e2] at h2; exact hd _ _ _ _ h1 h2 hk

theorem set_runs {n : Nat} {l : BL} (hr : Runs n l) {i : Nat} {b : table.Bucket} (hi : i < l.length)
    (hpath : ∀ d < dist n (homeN n b.key) i,
      ∃ c : table.Bucket, l[(homeN n b.key + d) % n]? = some (some c)) :
    Runs n (l.set i (some b)) := by
  intro j c hj d hd
  by_cases e : i = j
  · subst e; rw [List.getElem?_set_self hi] at hj; cases hj
    obtain ⟨c', hc'⟩ := hpath d hd; exact set_occ hc'
  · rw [List.getElem?_set_ne e] at hj
    obtain ⟨c', hc'⟩ := hr j c hj d hd; exact set_occ hc'

/-- Setting bucket `i` (empty, or holding the same key) to `b`. -/
theorem lookup_set {l : BL} (hd : Distinct l) {i : Nat} {b : table.Bucket} (hi : i < l.length)
    (hold : l[i]? = some none ∨ ∃ c : table.Bucket, l[i]? = some (some c) ∧ c.key = b.key)
    (hd' : Distinct (l.set i (some b))) (k : U64) :
    lookup (l.set i (some b)) k = if k = b.key then some b.slot else lookup l k := by
  split
  · rename_i hk; subst hk
    exact lookup_of_mem hd' (List.getElem?_set_self hi)
  · rename_i hk
    have hne : ∀ (j : Nat) (c : table.Bucket), l[j]? = some (some c) → c.key = k → i ≠ j := by
      rintro j c hj rfl rfl
      rcases hold with h | ⟨c', h, hc'⟩
      · rw [h] at hj; cases hj
      · rw [h] at hj; cases hj; exact hk hc'
    cases hs : lookup l k with
    | some s =>
      obtain ⟨j, c, hj, hck, rfl⟩ := lookup_some_mem hs
      have := hne j c hj hck
      have h' : (l.set i (some b))[j]? = some (some c) := by rw [List.getElem?_set_ne this]; exact hj
      rw [← hck]; exact lookup_of_mem hd' h'
    | none =>
      apply lookup_none
      intro j c hj hck
      by_cases e : i = j
      · subst e; rw [List.getElem?_set_self hi] at hj; cases hj; exact hk hck.symm
      · rw [List.getElem?_set_ne e] at hj
        have := lookup_of_mem hd hj; rw [hck, hs] at this; cases this

/-- Setting bucket `i` (empty, or holding the same key) keeps the table well formed. -/
theorem Wf.set {m : table.IdMap} (hw : Wf m) {i : Usize} (hi : i.val < m.buckets.val.length)
    (b : table.Bucket)
    (hold : m.buckets.val[i.val]? = some none ∨
      ∃ c : table.Bucket, m.buckets.val[i.val]? = some (some c) ∧ c.key = b.key)
    (habs : m.buckets.val[i.val]? = some none →
      ∀ (j : Nat) (c : table.Bucket), m.buckets.val[j]? = some (some c) → c.key ≠ b.key)
    (hpath : ∀ d < dist m.buckets.val.length (homeN m.buckets.val.length b.key) i.val,
      ∃ c : table.Bucket,
        m.buckets.val[(homeN m.buckets.val.length b.key + d) % m.buckets.val.length]? = some (some c))
    (len : Usize) (hlen : len.val = (m.buckets.val.set i.val (some b)).countP Option.isSome)
    (hle : len.val ≤ m.max_len.val) :
    Wf { buckets := m.buckets.set i (some b), mask := m.mask, len := len, max_len := m.max_len } := by
  have hlen' : (m.buckets.set i (some b)).val.length = m.buckets.val.length := by simp
  refine ⟨?_, ?_, ?_, hle, ?_, ?_, ?_⟩
  · rw [hlen']; exact hw.pow
  · rw [hlen']; exact hw.mask
  · rw [hlen']; exact hw.cap
  · simpa using hlen
  · simp only [alloc.vec.Vec.set_val_eq]
    apply set_distinct hw.distinct hi
    intro j c hj hk
    rcases hold with h | ⟨c', h, hc'⟩
    · exact absurd hk (habs h j c hj)
    · exact hw.distinct _ _ _ _ hj h (hk.trans hc'.symm)
  · simp only [alloc.vec.Vec.set_val_eq, List.length_set]
    exact set_runs hw.runs hi hpath

/-- What `insert` does to the map. -/
def InsertPost (m : table.IdMap) (k : U64) (s : U32) (r : Bool × table.IdMap) : Prop :=
  Wf r.2 ∧ r.2.max_len = m.max_len ∧
  if lookup m.buckets.val k = none ∧ m.max_len.val ≤ m.len.val then r = (false, m)
  else r.1 = true ∧
    (∀ k', lookup r.2.buckets.val k' = if k' = k then some s else lookup m.buckets.val k') ∧
    r.2.len.val = if lookup m.buckets.val k = none then m.len.val + 1 else m.len.val

theorem insert_spec (m : table.IdMap) (k : U64) (s : U32) (hw : Wf m) :
    table.IdMap.insert m k s ⦃ r => InsertPost m k s r ⦄ := by
  unfold table.IdMap.insert
  apply WP.spec_bind (probe_spec m k hw)
  rintro i ⟨hi, hp, hf⟩
  simp only [vidx _ _ hi, table.IdMap.is_full, bind_ok]
  have hget : ∀ o, m.buckets.val[i.val]? = some o → m.buckets.val[i.val] = o := by
    intro o h; rw [List.getElem?_eq_getElem hi] at h; simpa using h
  have hcnt := countP_set Option.isSome m.buckets.val i.val (some { key := k, slot := s }) hi
  have hd' := fun (hold : m.buckets.val[i.val]? = some none ∨
      ∃ c : table.Bucket, m.buckets.val[i.val]? = some (some c) ∧ c.key = k) =>
    lookup_set (b := { key := k, slot := s }) hw.distinct hi hold
  rcases hf with ⟨c, hc, hck⟩ | hnone
  · -- `k` is present: replace its slot.
    obtain rfl := hck
    have e := hget _ hc
    have hl : lookup m.buckets.val c.key = some c.slot := lookup_of_mem hw.distinct hc
    have hw' := hw.set hi { key := c.key, slot := s } (Or.inr ⟨c, hc, rfl⟩)
      (fun h => by rw [hc] at h; cases h) (fun d hd => hw.runs i.val c hc d hd)
      m.len (by rw [e] at hcnt; simp at hcnt; rw [hw.len_eq]; omega) hw.len_le
    have hlk := hd' (Or.inr ⟨c, hc, rfl⟩) (by simpa using hw'.distinct)
    simp only [e, core.option.Option.is_some, Option.isSome_some, ite_true, bind_ok]
    step*
    subst_vars
    refine ⟨hw', rfl, ?_⟩
    rw [ite_eq_right_of_eq_false _ _ (eq_false (by simp [hl]))]
    refine ⟨rfl, fun k' => ?_, by simp [hl]⟩
    simp only [alloc.vec.Vec.set_val_eq]
    exact hlk k'
  · -- `k` is absent.
    have e := hget _ hnone
    have hl : lookup m.buckets.val k = none := lookup_none (hw.absent hi hp hnone)
    simp only [e, core.option.Option.is_some, Option.isSome_none, Bool.false_eq_true, ite_false]
    split
    · rename_i hfull
      simp only [WP.spec_ok]
      refine ⟨hw, rfl, ?_⟩
      rw [ite_eq_left_of_eq_true _ _ (eq_true ⟨hl, by simpa using hfull⟩)]
    · rename_i hfull
      have hlt : m.len.val < m.max_len.val := by simp at hfull; omega
      have hn : m.buckets.val.length ≤ Usize.max := alloc.vec.Vec.len_ineq m.buckets
      have := hw.cap
      step*
      subst_vars
      have hw' := hw.set hi { key := k, slot := s } (Or.inl hnone) (fun _ => hw.absent hi hp hnone)
        hp.occ i1 (by rw [e] at hcnt; simp at hcnt; rw [i1_post, hw.len_eq]; omega) (by omega)
      refine ⟨hw', rfl, ?_⟩
      rw [ite_eq_right_of_eq_false _ _ (eq_false (by intro h; omega))]
      refine ⟨rfl, fun k' => ?_, by simp [hl, i1_post]⟩
      simp only [alloc.vec.Vec.set_val_eq]
      exact hd' (Or.inl hnone) (by simpa using hw'.distinct) k'

/-! ## Removal (backward-shift deletion), on lists -/

theorem dist_one {n a : Nat} (hn : 2 ≤ n) (ha : a < n) : dist n a ((a + 1) % n) = 1 :=
  dist_add ha (by omega)

theorem dist_ne {n a b c : Nat} (ha : a < n) (hb : b < n) (hc : c < n) (h : b ≠ c) :
    dist n a b ≠ dist n a c := by
  intro e; apply h; rw [← add_dist ha hb, e, add_dist ha hc]

/-- The loop invariant of `remove`. `l0` is the table just after the key's
    bucket `h0` was emptied; `p0` is another empty bucket of `l0` that bounds
    the scan. -/
structure RInv (n h0 p0 : Nat) (l0 v : BL) (hole i : Nat) : Prop where
  len : v.length = n
  hole_lt : hole < n
  i_lt : i < n
  hole_none : v[hole]? = some none
  same : ∀ b : table.Bucket, (∃ j : Nat, v[j]? = some (some b)) ↔ (∃ j : Nat, l0[j]? = some (some b))
  distinct : Distinct v
  count : v.countP Option.isSome = l0.countP Option.isSome
  gap_pos : 0 < dist n hole i
  gap : ∀ d, 0 < d → d < dist n hole i →
    ∃ c : table.Bucket, v[(hole + d) % n]? = some (some c) ∧
      dist n (homeN n c.key) ((hole + d) % n) < d
  runs : ∀ (j : Nat) (c : table.Bucket), v[j]? = some (some c) →
    ∀ d < dist n (homeN n c.key) j,
      (∃ c' : table.Bucket, v[(homeN n c.key + d) % n]? = some (some c')) ∨
      ((homeN n c.key + d) % n = hole ∧ dist n hole i ≤ dist n hole j)
  ahead : ∀ q < n, dist n h0 i ≤ dist n h0 q → v[q]? = l0[q]?
  behind : dist n h0 hole < dist n h0 i
  h0_lt : h0 < n
  p0_lt : p0 < n
  p0_ne : p0 ≠ h0
  p0_none : l0[p0]? = some none
  bound : dist n h0 i ≤ dist n h0 p0

theorem RInv.two_le {n h0 p0 : Nat} {l0 v : BL} {hole i : Nat} (r : RInv n h0 p0 l0 v hole i) :
    2 ≤ n := by
  have := r.p0_lt; have := r.h0_lt; have := r.p0_ne; omega

/-- The scan can move on: `i` is occupied, so it is short of `p0`. -/
theorem RInv.short {n h0 p0 : Nat} {l0 v : BL} {hole i : Nat} (r : RInv n h0 p0 l0 v hole i)
    {b : table.Bucket} (hb : v[i]? = some (some b)) : dist n h0 i < dist n h0 p0 := by
  rcases Nat.lt_or_eq_of_le r.bound with h | h
  · exact h
  · exfalso
    have : i = p0 := by rw [← add_dist r.h0_lt r.i_lt, h, add_dist r.h0_lt r.p0_lt]
    subst this
    rw [r.ahead _ r.i_lt (le_refl _), r.p0_none] at hb; cases hb

theorem RInv.next_dist {n h0 p0 : Nat} {l0 v : BL} {hole i : Nat} (r : RInv n h0 p0 l0 v hole i)
    {b : table.Bucket} (hb : v[i]? = some (some b)) :
    dist n h0 ((i + 1) % n) = dist n h0 i + 1 := by
  have := r.short hb; have := dist_lt (n := n) (a := h0) (b := p0) (by have := r.p0_lt; omega)
  exact dist_succ r.h0_lt r.i_lt (by omega)

/-- Not moving: `i`'s entry stays, the gap grows by one. -/
theorem RInv.stay {n h0 p0 : Nat} {l0 v : BL} {hole i : Nat} (r : RInv n h0 p0 l0 v hole i)
    {b : table.Bucket} (hb : v[i]? = some (some b))
    (hstay : dist n (homeN n b.key) i < dist n hole i) :
    RInv n h0 p0 l0 v hole ((i + 1) % n) := by
  have hn := r.two_le
  have hh0 := r.h0_lt
  have hnext := r.next_dist hb
  have hshort := r.short hb
  have hi1 : (i + 1) % n < n := Nat.mod_lt _ (by omega)
  -- The gap can grow without wrapping onto the hole.
  have hsplit_i := dist_split hh0 r.hole_lt r.i_lt r.behind
  have hsplit_p := dist_split hh0 r.hole_lt r.p0_lt (by have := r.bound; omega)
  have hgap : dist n hole i + 1 < n := by
    have := dist_lt (n := n) (a := hole) (b := p0) (by omega); omega
  have hgap1 : dist n hole ((i + 1) % n) = dist n hole i + 1 := dist_succ r.hole_lt r.i_lt hgap
  have hhb : homeN n b.key < n := Nat.mod_lt _ (by omega)
  refine ⟨r.len, r.hole_lt, hi1, r.hole_none, r.same, r.distinct, r.count, by omega, ?_, ?_, ?_,
    by omega, r.h0_lt, r.p0_lt, r.p0_ne, r.p0_none, by omega⟩
  · intro d hd0 hd
    rw [hgap1] at hd
    rcases Nat.lt_or_eq_of_le (Nat.le_of_lt_succ hd) with h | h
    · exact r.gap d hd0 h
    · subst h; rw [add_dist r.hole_lt r.i_lt]; exact ⟨b, hb, hstay⟩
  · intro j c hj d hd
    rcases r.runs j c hj d hd with h | ⟨hq, hle⟩
    · exact Or.inl h
    · refine Or.inr ⟨hq, ?_⟩
      rw [hgap1]
      have hjn : j < n := by have := getElem?_some_lt hj; rw [r.len] at this; exact this
      have hne : j ≠ i := by
        rintro rfl
        rw [hb] at hj; cases hj
        have e1 : dist n (homeN n b.key) hole = d := by rw [← hq]; exact dist_add hhb (by omega)
        have := dist_split hhb r.hole_lt r.i_lt (by omega)
        omega
      have := dist_ne r.hole_lt hjn r.i_lt hne
      omega
  · intro q hq hd
    exact r.ahead q hq (by omega)

/-- Moving: `i`'s entry fills the hole, and `i` becomes the hole. -/
theorem RInv.move {n h0 p0 : Nat} {l0 v : BL} {hole i : Nat} (r : RInv n h0 p0 l0 v hole i)
    {b : table.Bucket} (hb : v[i]? = some (some b))
    (hmove : dist n hole i ≤ dist n (homeN n b.key) i) :
    RInv n h0 p0 l0 ((v.set hole (some b)).set i none) i ((i + 1) % n) := by
  have hn := r.two_le
  have hh0 := r.h0_lt
  have hnext := r.next_dist hb
  have hshort := r.short hb
  have hhi : hole ≠ i := by intro e; have := r.gap_pos; rw [e, dist_self] at this; omega
  have hlen : hole < v.length := by rw [r.len]; exact r.hole_lt
  have hilen : i < v.length := by rw [r.len]; exact r.i_lt
  have hi1 : (i + 1) % n < n := Nat.mod_lt _ (by omega)
  have hhb : homeN n b.key < n := Nat.mod_lt _ (by omega)
  -- Reading the new table.
  have get_i : ((v.set hole (some b)).set i none)[i]? = some none := by
    rw [List.getElem?_set_self (by simpa using hilen)]
  have get_hole : ((v.set hole (some b)).set i none)[hole]? = some (some b) := by
    rw [List.getElem?_set_ne hhi.symm, List.getElem?_set_self hlen]
  have get_other : ∀ q, q ≠ i → q ≠ hole → ((v.set hole (some b)).set i none)[q]? = v[q]? := by
    intro q h1 h2
    rw [List.getElem?_set_ne (Ne.symm h1), List.getElem?_set_ne (Ne.symm h2)]
  have occ' : ∀ q, q ≠ i → ((∃ c : table.Bucket, v[q]? = some (some c)) ∨ q = hole) →
      ∃ c : table.Bucket, ((v.set hole (some b)).set i none)[q]? = some (some c) := by
    rintro q hqi (⟨c, hc⟩ | rfl)
    · by_cases hqh : q = hole
      · subst hqh; exact ⟨b, get_hole⟩
      · exact ⟨c, by rw [get_other q hqi hqh]; exact hc⟩
    · exact ⟨b, get_hole⟩
  -- Where an entry of the new table was in the old one.
  have back : ∀ (j : Nat) (c : table.Bucket), ((v.set hole (some b)).set i none)[j]? = some (some c) →
      j ≠ i ∧ (j = hole ∧ c = b ∨ j ≠ hole ∧ v[j]? = some (some c)) := by
    intro j c hj
    have hji : j ≠ i := by rintro rfl; rw [get_i] at hj; cases hj
    refine ⟨hji, ?_⟩
    by_cases hjh : j = hole
    · subst hjh; rw [get_hole] at hj; cases hj; exact Or.inl ⟨rfl, rfl⟩
    · exact Or.inr ⟨hjh, by rw [← get_other j hji hjh]; exact hj⟩
  have hdi : dist n i ((i + 1) % n) = 1 := dist_one hn r.i_lt
  refine ⟨by simp [r.len], r.i_lt, hi1, get_i, ?_, ?_, ?_, by omega, ?_, ?_, ?_, by omega,
    r.h0_lt, r.p0_lt, r.p0_ne, r.p0_none, by omega⟩
  · -- same entries
    intro c
    rw [← r.same c]
    constructor
    · rintro ⟨j, hj⟩
      rcases (back j c hj).2 with ⟨-, rfl⟩ | ⟨-, h⟩
      · exact ⟨i, hb⟩
      · exact ⟨j, h⟩
    · rintro ⟨j, hj⟩
      by_cases hji : j = i
      · subst hji; rw [hb] at hj; cases hj; exact ⟨hole, get_hole⟩
      · have hjh : j ≠ hole := by rintro rfl; rw [r.hole_none] at hj; cases hj
        exact ⟨j, by rw [get_other j hji hjh]; exact hj⟩
  · -- distinct
    intro j1 j2 c1 c2 h1 h2 hk
    have b1 := back j1 c1 h1; have b2 := back j2 c2 h2
    let σ := fun j => if j = hole then i else j
    have hs1 : v[σ j1]? = some (some c1) := by
      rcases b1.2 with ⟨rfl, rfl⟩ | ⟨hne, h⟩
      · simp [σ, hb]
      · simp [σ, hne, h]
    have hs2 : v[σ j2]? = some (some c2) := by
      rcases b2.2 with ⟨rfl, rfl⟩ | ⟨hne, h⟩
      · simp [σ, hb]
      · simp [σ, hne, h]
    have := r.distinct _ _ _ _ hs1 hs2 hk
    simp only [σ] at this
    have := b1.1; have := b2.1
    split at * <;> split at * <;> omega
  · -- count
    have c1 := countP_set Option.isSome v hole (some b) hlen
    have c2 := countP_set Option.isSome (v.set hole (some b)) i none (by simpa using hilen)
    have g1 : v[hole] = none := by
      have := r.hole_none; rw [List.getElem?_eq_getElem hlen] at this; simpa using this
    have g2 : (v.set hole (some b))[i]'(by simpa using hilen) = some b := by
      have := hb; rw [List.getElem?_eq_getElem hilen] at this
      simp only [List.getElem_set, Ne.symm hhi.symm, ite_false]
      simpa using this
    rw [g1] at c1; rw [g2] at c2
    simp at c1 c2
    rw [← r.count]; omega
  · -- gap: empty
    intro d hd0 hd; rw [hdi] at hd; omega
  · -- runs
    intro j c hj d hd
    obtain ⟨hji, hj'⟩ := back j c hj
    have hjn : j < n := by
      have := getElem?_some_lt hj; simpa [r.len] using this
    have hc : homeN n c.key < n := Nat.mod_lt _ (by omega)
    have hq : (homeN n c.key + d) % n < n := Nat.mod_lt _ (by omega)
    have hdq : dist n (homeN n c.key) ((homeN n c.key + d) % n) = d :=
      dist_add hc (by have := dist_lt (n := n) (a := homeN n c.key) (b := j) (by omega); omega)
    rcases hj' with ⟨hjh, hcb⟩ | ⟨hjh, hjv⟩
    · -- the moved entry: its run now ends at the old hole
      have hjh := hjh.symm; have hcb := hcb.symm
      subst hjh hcb
      have hlt : dist n (homeN n b.key) hole < dist n (homeN n b.key) i := by
        rcases dist_tri hhb r.hole_lt r.i_lt with h | h
        · have := r.gap_pos; omega
        · have := dist_lt (n := n) (a := homeN n b.key) (b := hole) (by omega)
          have := dist_lt (n := n) (a := homeN n b.key) (b := i) (by omega); omega
      left
      rcases r.runs i b hb d (by omega) with h | ⟨h, -⟩
      · apply occ' _ _ (Or.inl h)
        intro e; rw [e] at hdq; omega
      · exfalso; rw [h] at hdq; omega
    · rcases r.runs j c hjv d hd with h | ⟨h, -⟩
      · by_cases hqi : (homeN n c.key + d) % n = i
        · right
          refine ⟨hqi, ?_⟩
          rw [hdi]
          have := dist_ne r.i_lt r.i_lt hjn (Ne.symm hji)
          rw [dist_self] at this; omega
        · exact Or.inl (occ' _ hqi (Or.inl h))
      · left; apply occ' _ _ (Or.inr h)
        rw [h]; exact hhi
  · -- ahead of the scan nothing changed
    intro q hq hd
    have hqi : q ≠ i := by rintro rfl; omega
    have hqh : q ≠ hole := by rintro rfl; have := r.behind; omega
    rw [get_other q hqi hqh]; exact r.ahead q hq (by omega)

/-- At the end of the scan (an empty bucket at `i`) every run is whole again. -/
theorem RInv.exit {n h0 p0 : Nat} {l0 v : BL} {hole i : Nat} (r : RInv n h0 p0 l0 v hole i)
    (hi : v[i]? = some none) : Runs n v := by
  have hn := r.two_le
  intro j c hj d hd
  have hjn : j < n := by have := getElem?_some_lt hj; simpa [r.len] using this
  have hc : homeN n c.key < n := Nat.mod_lt _ (by omega)
  rcases r.runs j c hj d hd with h | ⟨hq, hle⟩
  · exact h
  · exfalso
    have hdj := dist_lt (n := n) (a := homeN n c.key) (b := j) (by omega)
    have e1 : dist n (homeN n c.key) hole = d := by rw [← hq]; exact dist_add hc (by omega)
    have hsp := dist_split hc r.hole_lt hjn (by omega)
    rcases Nat.lt_or_eq_of_le hle with hlt | heq
    · -- `i` lies on `c`'s run, yet it is empty and not the hole.
      have hpos := r.gap_pos
      have key : (homeN n c.key + (d + dist n hole i)) % n = i := by
        rw [← Nat.add_assoc, Nat.add_mod, hq,
          Nat.mod_eq_of_lt (dist_lt (n := n) (a := hole) (b := i) (by omega)),
          add_dist r.hole_lt r.i_lt]
      rcases r.runs j c hj (d + dist n hole i) (by omega) with ⟨c', hc'⟩ | ⟨hq', -⟩
      · rw [key, hi] at hc'; cases hc'
      · rw [key] at hq'
        rw [hq', dist_self] at hpos; omega
    · have : j = i := by rw [← add_dist r.hole_lt hjn, ← heq, add_dist r.hole_lt r.i_lt]
      subst this; rw [hj] at hi; cases hi

theorem set_none_distinct {l : BL} (hd : Distinct l) (h0 : Nat) : Distinct (l.set h0 none) := by
  intro j1 j2 c1 c2 h1 h2 hk
  have g : ∀ (j : Nat) (c : table.Bucket),
      (l.set h0 none)[j]? = some (some c) → l[j]? = some (some c) := by
    intro j c h
    by_cases e : h0 = j
    · subst e
      rcases Nat.lt_or_ge h0 l.length with hl | hl
      · rw [List.getElem?_set_self hl] at h; cases h
      · rw [List.getElem?_eq_none (by simpa using hl)] at h; cases h
    · rwa [List.getElem?_set_ne e] at h
  exact hd _ _ _ _ (g _ _ h1) (g _ _ h2) hk

/-- Emptying the bucket that holds `c`. -/
theorem lookup_erase {l : BL} (hd : Distinct l) {h0 : Nat} {c : table.Bucket}
    (hc : l[h0]? = some (some c)) (k : U64) :
    lookup (l.set h0 none) k = if k = c.key then none else lookup l k := by
  have hl := getElem?_some_lt hc
  have hd' := set_none_distinct hd h0
  split
  · rename_i hk; subst hk
    apply lookup_none
    intro j b hj hbk
    by_cases e : h0 = j
    · subst e; rw [List.getElem?_set_self hl] at hj; cases hj
    · rw [List.getElem?_set_ne e] at hj; exact e (hd _ _ _ _ hc hj hbk.symm)
  · rename_i hk
    cases hs : lookup l k with
    | some s =>
      obtain ⟨j, b, hj, hbk, rfl⟩ := lookup_some_mem hs
      have e : h0 ≠ j := by rintro rfl; rw [hc] at hj; cases hj; exact hk hbk.symm
      rw [← hbk]; exact lookup_of_mem hd' (by rw [List.getElem?_set_ne e]; exact hj)
    | none =>
      apply lookup_none
      intro j b hj hbk
      by_cases e : h0 = j
      · subst e; rw [List.getElem?_set_self hl] at hj; cases hj
      · rw [List.getElem?_set_ne e] at hj
        have := lookup_of_mem hd hj; rw [hbk, hs] at this; cases this

/-- Another empty bucket exists when at most `n - 2` are occupied. -/
theorem exists_other_empty {l : BL} {h0 : Nat} (x : table.Bucket) (hh0 : h0 < l.length)
    (hcount : l.countP Option.isSome + 2 ≤ l.length) :
    ∃ p0, p0 < l.length ∧ p0 ≠ h0 ∧ l[p0]? = some none := by
  by_contra hne
  have hall : ∀ p < l.length, p ≠ h0 → ∃ c : table.Bucket, l[p]? = some (some c) := by
    intro p hp hph
    rw [List.getElem?_eq_getElem hp]
    cases e : l[p] with
    | none => exact absurd ⟨p, hp, hph, by rw [List.getElem?_eq_getElem hp, e]⟩ hne
    | some c => exact ⟨c, rfl⟩
  have hfull : (l.set h0 (some x)).countP Option.isSome = l.length := by
    rw [← List.length_set (i := h0) (a := some x), List.countP_eq_length]
    intro a ha
    obtain ⟨p, hp, hpa⟩ := List.mem_iff_getElem.mp ha
    subst hpa
    simp only [List.length_set] at hp
    by_cases e : h0 = p
    · subst e; simp
    · obtain ⟨c, hc⟩ := hall p hp (Ne.symm e)
      rw [List.getElem_set_ne e]
      rw [List.getElem?_eq_getElem hp] at hc; simp at hc; simp [hc]
  have := countP_set Option.isSome l h0 (some x) hh0
  rw [hfull] at this
  split at this <;> simp at this <;> omega

theorem RInv.init {n : Nat} {l : BL} (hn : l.length = n) (hd : Distinct l) (hr : Runs n l)
    {h0 : Nat} {c : table.Bucket} (hc : l[h0]? = some (some c))
    (hcount : l.countP Option.isSome + 1 ≤ n) :
    ∃ p0, RInv n h0 p0 (l.set h0 none) (l.set h0 none) h0 ((h0 + 1) % n) := by
  have hh0 : h0 < n := hn ▸ getElem?_some_lt hc
  have cnt := countP_set Option.isSome l h0 none (by omega)
  have g0 : l[h0]'(by omega) = some c := by
    rw [List.getElem?_eq_getElem (by omega)] at hc; simpa using hc
  rw [g0] at cnt; simp at cnt
  obtain ⟨p0, hp0, hp0ne, hp0n⟩ := exists_other_empty (l := l.set h0 none) (h0 := h0) c
    (by simp; omega) (by simp; omega)
  simp only [List.length_set] at hp0
  have hn2 : 2 ≤ n := by omega
  have hdi : dist n h0 ((h0 + 1) % n) = 1 := dist_one hn2 hh0
  have get_h0 : (l.set h0 none)[h0]? = some none := by rw [List.getElem?_set_self (by omega)]
  refine ⟨p0, ⟨by simp [hn], hh0, Nat.mod_lt _ (by omega), get_h0, fun _ => Iff.rfl,
    set_none_distinct hd h0, rfl, by omega, ?_, ?_, fun _ _ _ => rfl, by rw [dist_self]; omega,
    hh0, by omega, hp0ne, hp0n, ?_⟩⟩
  · intro d hd0 hd; omega
  · intro j b hj d hd'
    have hjh : h0 ≠ j := by rintro rfl; rw [get_h0] at hj; cases hj
    rw [List.getElem?_set_ne hjh] at hj
    obtain ⟨c', hc'⟩ := hr j b hj d hd'
    by_cases hq : (homeN n b.key + d) % n = h0
    · right; refine ⟨hq, ?_⟩
      rw [hdi]
      have hjn : j < n := hn ▸ getElem?_some_lt hj
      have := dist_ne hh0 hh0 hjn hjh; rw [dist_self] at this; omega
    · left; exact ⟨c', by rw [List.getElem?_set_ne (Ne.symm hq)]; exact hc'⟩
  · rw [hdi]
    have := dist_ne hh0 hh0 (by omega) (Ne.symm hp0ne); rw [dist_self] at this; omega

/-! ## Removal, on the machine -/

/-- The machine's `(x as u64).wrapping_sub(y as u64) & mask`. -/
theorem mdist {n p : Nat} (hn : n = 2 ^ p) (hnmax : n ≤ Usize.max) (mask : U64)
    (hmask : mask.val = n - 1) (x y : Usize) (hx : x.val < n) (hy : y.val < n) :
    (core.num.U64.wrapping_sub (UScalar.cast .U64 x) (UScalar.cast .U64 y) &&& mask).val =
      dist n y.val x.val := by
  have hu : Usize.max ≤ 2 ^ 64 - 1 := by
    have := usize_le_u64; simpa [U64.max_def, U64.numBits] using this
  have hp : p ≤ 64 := by
    by_contra h
    have : 2 ^ 65 ≤ 2 ^ p := Nat.pow_le_pow_right (by decide) (by omega)
    omega
  have cx : (UScalar.cast .U64 x).val = x.val := by
    rw [UScalar.cast_val_eq]; exact Nat.mod_eq_of_lt (by simp; omega)
  have cy : (UScalar.cast .U64 y).val = y.val := by
    rw [UScalar.cast_val_eq]; exact Nat.mod_eq_of_lt (by simp; omega)
  rw [UScalar.val_and, hmask, land_mask hn, core.num.U64.wrapping_sub_val_eq, cx, cy]
  have : UScalar.size .U64 = 2 ^ 64 := by simp [UScalar.size_def, UScalarTy.numBits]
  rw [this, ← wrap_dist hn hp hx hy]
  congr 2; omega

/-- What the removal loop leaves: `l0`'s entries, with every run whole. -/
def RPost (n : Nat) (l0 v : BL) : Prop :=
  v.length = n ∧ Runs n v ∧ Distinct v ∧
  (∀ b : table.Bucket, (∃ j : Nat, v[j]? = some (some b)) ↔ (∃ j : Nat, l0[j]? = some (some b))) ∧
  v.countP Option.isSome = l0.countP Option.isSome

theorem remove_loop_spec {n p h0 p0 : Nat} {l0 : BL} (hn : n = 2 ^ p) (hp : 0 < p)
    (mask : U64) (hmask : mask.val = n - 1) (len maxl : Usize)
    (v : alloc.vec.Vec (Option table.Bucket)) (hole i : Usize)
    (r : RInv n h0 p0 l0 v.val hole.val i.val) :
    table.IdMap.remove_loop v mask len maxl hole i true ⦃ v' => RPost n l0 v'.val ⦄ := by
  unfold table.IdMap.remove_loop
  apply loop.spec_decr_nat
    (fun (st : alloc.vec.Vec (Option table.Bucket) × Usize × Usize × Bool) =>
      if st.2.2.2 then n + 1 + dist n h0 p0 - dist n h0 st.2.2.1.val else 0)
    (fun st => RInv n h0 p0 l0 st.1.val st.2.1.val st.2.2.1.val ∧
      (st.2.2.2 = false → st.1.val[st.2.2.1.val]? = some none))
  · rintro ⟨v, hole, i, sh⟩ ⟨r, hsh⟩
    have hvn : v.val.length = n := r.len
    have hnmax : n ≤ Usize.max := hvn ▸ alloc.vec.Vec.len_ineq v
    unfold table.IdMap.remove_loop.body
    cases sh with
    | false =>
      simp only [Bool.false_eq_true, ite_false, WP.spec_ok]
      have hi := hsh rfl
      exact ⟨r.len, r.exit hi, r.distinct, r.same, r.count⟩
    | true =>
      simp only [ite_true]
      have hil : i.val < v.val.length := by rw [hvn]; exact r.i_lt
      simp only [vidx _ _ hil, bind_ok]
      cases hb : v.val[i.val] with
      | none =>
        simp only [WP.spec_ok]
        simp only at r
        have := dist_lt (n := n) (a := h0) (b := i.val) (by have := r.two_le; omega)
        refine ⟨⟨r, fun _ => by rw [List.getElem?_eq_getElem hil, hb]⟩, by simp; omega⟩
      | some b =>
        simp only
        have hshape : Shape { buckets := v, mask := mask, len := len, max_len := maxl } :=
          ⟨⟨p, hp, hvn ▸ hn⟩, by simp [hmask, hvn]⟩
        apply WP.spec_bind (home_spec' _ b.key hshape)
        rintro home ⟨hhome, hhl⟩
        simp only at hhome hhl r
        rw [hvn] at hhome hhl
        have hbv : v.val[i.val]? = some (some b) := by rw [List.getElem?_eq_getElem hil, hb]
        have hholel : hole.val < v.val.length := by rw [hvn]; exact r.hole_lt
        have dh := mdist hn hnmax mask hmask i home r.i_lt hhl
        have dho := mdist hn hnmax mask hmask i hole r.i_lt r.hole_lt
        rw [hhome] at dh
        simp only [lift, bind_ok]
        have hnext := r.next_dist hbv
        have hshort := r.short hbv
        by_cases hmv : dist n hole.val i.val ≤ dist n (homeN n b.key) i.val
        · have hc : (core.num.U64.wrapping_sub (UScalar.cast .U64 i) (UScalar.cast .U64 home) &&& mask ≥
              core.num.U64.wrapping_sub (UScalar.cast .U64 i) (UScalar.cast .U64 hole) &&& mask) := by
            simp only [ge_iff_le, UScalar.le_equiv, dh, dho]; exact hmv
          simp only [hc, ite_true]
          have r' := r.move hbv hmv
          step*
          rcases x with ⟨x1, x2⟩
          simp only at x_post1
          subst x_post1
          step*
          rcases x with ⟨y1, y2⟩
          simp only at x_post1
          subst x_post1
          step*
          apply WP.spec_bind (next_spec' _ i ⟨⟨p, hp, by simp [hvn, hn]⟩, by simp [hmask, hvn]⟩
            (by simpa using hil))
          intro i10 hi10
          simp only [WP.spec_ok]
          simp only [alloc.vec.Vec.set_val_eq, List.length_set] at hi10
          rw [hvn] at hi10
          refine ⟨?_, by simp, ?_⟩
          · rw [hi10]; simpa only [alloc.vec.Vec.set_val_eq] using r'
          · simp only [ite_true]; rw [hi10, hnext]; omega
        · have hc : ¬ (core.num.U64.wrapping_sub (UScalar.cast .U64 i) (UScalar.cast .U64 home) &&& mask ≥
              core.num.U64.wrapping_sub (UScalar.cast .U64 i) (UScalar.cast .U64 hole) &&& mask) := by
            simp only [ge_iff_le, UScalar.le_equiv, dh, dho]; omega
          simp only [hc, ite_false, bind_ok]
          have r' := r.stay hbv (by omega)
          apply WP.spec_bind (next_spec' _ i hshape (by simpa using hil))
          intro i10 hi10
          simp only [WP.spec_ok]
          simp only at hi10
          rw [hvn] at hi10
          refine ⟨⟨?_, by simp⟩, ?_⟩
          · rw [hi10]; exact r'
          · simp only [ite_true]; rw [hi10, hnext]; omega
  · exact ⟨r, by simp⟩

/-- What `remove` does to the map. -/
def RemovePost (m : table.IdMap) (k : U64) (m' : table.IdMap) : Prop :=
  Wf m' ∧ m'.max_len = m.max_len ∧
  (∀ k', lookup m'.buckets.val k' = if k' = k then none else lookup m.buckets.val k') ∧
  m'.len.val = if lookup m.buckets.val k = none then m.len.val else m.len.val - 1

theorem remove_spec (m : table.IdMap) (k : U64) (hw : Wf m) :
    table.IdMap.remove m k ⦃ m' => RemovePost m k m' ⦄ := by
  unfold table.IdMap.remove
  apply WP.spec_bind (probe_spec m k hw)
  rintro hole ⟨hi, hp, hf⟩
  simp only [vidx _ _ hi, bind_ok]
  have hget : ∀ o, m.buckets.val[hole.val]? = some o → m.buckets.val[hole.val] = o := by
    intro o h; rw [List.getElem?_eq_getElem hi] at h; simpa using h
  rcases hf with ⟨c, hc, hck⟩ | hnone
  · obtain rfl := hck
    have e := hget _ hc
    have hl : lookup m.buckets.val c.key = some c.slot := lookup_of_mem hw.distinct hc
    obtain ⟨p, hp0, hpn⟩ := hw.pow
    have hlen1 : 1 ≤ m.len.val := by
      have := countP_set Option.isSome m.buckets.val hole.val none hi
      rw [e] at this; simp at this; rw [hw.len_eq]; omega
    have hcap := hw.cap; have hle := hw.len_le
    obtain ⟨p0, r⟩ := RInv.init rfl hw.distinct hw.runs hc
      (by rw [← hw.len_eq]; have : 2 ≤ m.buckets.val.length := by
            rw [hpn]; exact Nat.le_trans (by decide) (Nat.pow_le_pow_right (by decide) hp0)
          omega)
    simp only [e, core.option.Option.is_none, Option.isNone_some, Bool.false_eq_true, ite_false]
    step*
    subst index_mut_back_post
    have hcnt := countP_set Option.isSome m.buckets.val hole.val none hi
    rw [e] at hcnt; simp at hcnt
    apply WP.spec_bind (next_spec' _ hole ⟨⟨p, hp0, by simpa using hpn⟩, by simpa using hw.mask⟩
      (by simpa using hi))
    intro i1 hi1
    simp only [alloc.vec.Vec.set_val_eq, List.length_set] at hi1
    apply WP.spec_bind (remove_loop_spec hpn hp0 m.mask hw.mask i m.max_len _ hole i1
      (by rw [alloc.vec.Vec.set_val_eq, hi1]; exact r))
    rintro v1 ⟨hv1n, hv1r, hv1d, hv1s, hv1c⟩
    simp only [WP.spec_ok]
    refine ⟨⟨⟨p, hp0, by rw [hv1n, hpn]⟩, by rw [hv1n]; exact hw.mask, by rw [hv1n]; exact hcap,
      by show i.val ≤ m.max_len.val; rw [i_post]; omega,
      by show i.val = _; rw [i_post, hv1c, hw.len_eq]; omega, hv1d, by rw [hv1n]; exact hv1r⟩,
      rfl, fun k' => ?_, by simp [hl, i_post]⟩
    rw [lookup_ext hv1d (set_none_distinct hw.distinct _) hv1s k']
    exact lookup_erase hw.distinct hc k'
  · have e := hget _ hnone
    have hl : lookup m.buckets.val k = none := lookup_none (hw.absent hi hp hnone)
    simp only [e, core.option.Option.is_none, Option.isNone_none, ite_true, WP.spec_ok]
    refine ⟨hw, rfl, fun k' => ?_, by simp [hl]⟩
    split
    · rename_i h; rw [h, hl]
    · rfl

/-! ## Construction -/

theorem new_loop_spec (max_len : Usize) (hmax : 4 * max_len.val ≤ Usize.max) (n : Usize)
    (hn : ∃ p, 0 < p ∧ n.val = 2 ^ p) (hle : n.val ≤ 2 ∨ n.val < 4 * max_len.val) :
    table.IdMap.new_loop max_len n ⦃ r => (∃ p, 0 < p ∧ r.val = 2 ^ p) ∧
      2 * max_len.val ≤ r.val ∧ (r.val ≤ 2 ∨ r.val < 4 * max_len.val) ⦄ := by
  unfold table.IdMap.new_loop
  apply loop.spec_decr_nat (fun (x : Usize) => 4 * max_len.val + 4 - x.val)
    (fun x => (∃ p, 0 < p ∧ x.val = 2 ^ p) ∧ (x.val ≤ 2 ∨ x.val < 4 * max_len.val))
  · rintro x ⟨⟨p, hp, hx⟩, hxle⟩
    unfold table.IdMap.new_loop.body
    have hx2 : 2 ≤ x.val := by
      rw [hx]; exact Nat.le_trans (by decide) (Nat.pow_le_pow_right (by decide) hp)
    step*
    · rename_i hlt
      simp only [UScalar.lt_equiv] at hlt
      refine ⟨⟨p + 1, by omega, by rw [n1_post, hx, Nat.pow_succ]⟩, by omega, by omega⟩
    · rename_i hlt
      simp only [UScalar.lt_equiv] at hlt
      exact ⟨⟨p, hp, hx⟩, by omega, hxle⟩
  · exact ⟨hn, hle⟩

theorem new_spec (max_len : Usize) (hmax : 4 * max_len.val ≤ Usize.max) :
    table.IdMap.new max_len ⦃ m => Wf m ∧ m.max_len = max_len ∧ m.len.val = 0 ∧
      ∀ k, lookup m.buckets.val k = none ⦄ := by
  unfold table.IdMap.new
  apply WP.spec_bind (new_loop_spec max_len hmax 2#usize ⟨1, by decide, by simp⟩ (Or.inl (by simp)))
  rintro n ⟨⟨p, hp, hn⟩, hcap, -⟩
  have hn2 : 2 ≤ n.val := by
    rw [hn]; exact Nat.le_trans (by decide) (Nat.pow_le_pow_right (by decide) hp)
  have hcl : (core.option.Option.Insts.CoreCloneClone table.Bucket.Insts.CoreCloneClone).clone
      none = ok none := by rfl
  step*
  have hnmax : n.val ≤ Usize.max := by rw [← v_post1]; exact alloc.vec.Vec.len_ineq v
  have hu : Usize.max ≤ 2 ^ 64 - 1 := by
    have := usize_le_u64; simpa [U64.max_def, U64.numBits] using this
  have hmask : i1.val = n.val - 1 := by
    rw [i1_post, UScalar.cast_val_eq, i_post]; exact Nat.mod_eq_of_lt (by simp; omega)
  have hnone : ∀ (j : Nat) (b : table.Bucket), v.val[j]? = some (some b) → False := by
    intro j b h; rw [v_post, List.getElem?_replicate] at h; split at h <;> simp at h
  refine ⟨⟨⟨p, hp, by simp [v_post, hn]⟩, by simp [hmask, v_post], by simp [v_post]; omega,
    by simp, by simp [v_post], fun j _ b _ h => (hnone j b h).elim,
    fun j b h => (hnone j b h).elim⟩, by simp, fun k => lookup_none fun j b h => (hnone j b h).elim⟩

#print axioms get_spec
#print axioms insert_spec
#print axioms remove_spec
#print axioms new_spec

end krusty_core.idmap