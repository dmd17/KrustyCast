/-
  First theorems about the stage-B stream tracker (`spikes/aeneas-stream/subset`),
  stated against the Lean code Aeneas generated from the Rust source.

  Main result, `deliver_and_drain_spec`: when the tracker delivers the next
  expected message, the output grows by a gap-free, in-order run of seqnos that
  starts at the old `next`, the new `next` is one past the end of that run, and
  the function cannot fail (no overflow, no out-of-bounds), given headroom below
  `u64::MAX` and below the maximum `Vec` length.
-/
import StreamSubset.Funs

open Aeneas Aeneas.Std Result ControlFlow
open stream_subset

namespace stream_subset.proofs

/-- Number of occupied slots in the reorder window. -/
def occ (w : Array (Option Slot) 64#usize) : Nat := w.val.countP (fun o => o.isSome)

theorem occ_le (w : Array (Option Slot) 64#usize) : occ w ≤ 64 := by
  have h := w.property
  have := List.countP_le_length (p := fun o : Option Slot => o.isSome) (l := w.val)
  simp only [occ]; simp_all

theorem countP_set_none {α : Type} (l : List (Option α)) (i : Nat) (x : α)
    (h : l[i]? = some (some x)) :
    (l.set i none).countP (fun o => o.isSome) + 1 = l.countP (fun o => o.isSome) := by
  induction l generalizing i with
  | nil => simp at h
  | cons a l ih =>
    cases i with
    | zero => simp_all
    | succ i =>
      have := ih i (by simpa using h)
      simp [List.countP_cons]; omega

@[step]
theorem slot_index_spec (s : U64) :
    slot_index s ⦃ i => i.val = s.val % 64 ∧ i.val < 64 ⦄ := by
  unfold slot_index
  have hw : (UScalar.cast UScalarTy.U64 WINDOW).val = 64 := by
    unfold WINDOW; simp [UScalar.cast_val_eq, UScalarTy.numBits]
  step*
  have hlt : i1.val < 64 := by rw [i1_post, i_post, hw]; exact Nat.mod_lt _ (by decide)
  have hc : (UScalar.cast UScalarTy.Usize i1).val = i1.val := by
    apply UScalar.cast_val_mod_pow_of_inBounds_eq
    rcases System.Platform.numBits_eq with h | h <;>
      simp [UScalarTy.numBits, h] <;> omega
  rw [hc, i1_post, i_post, hw]
  exact ⟨rfl, Nat.mod_lt _ (by decide)⟩

/-- `take` either leaves the tracker unchanged, or frees exactly one window slot.
    It touches nothing but the window. -/
@[step]
theorem take_spec (t : Tracker) (s : U64) :
    t.take s ⦃ r => ∃ w, r.2 = { t with window := w } ∧
      (match r.1 with
       | none => w = t.window
       | some _ => occ w + 1 = occ t.window) ⦄ := by
  unfold Tracker.take
  step*
  all_goals refine ⟨_, rfl, ?_⟩
  subst a_post
  simp only [occ, Std.Array.set_val_eq]
  exact countP_set_none _ _ slot (by simp_all)

/-- Seqnos `n, n+1, ..., n+k-1` -/
def run (n k : Nat) : List Nat := List.range' n k

/-- What `deliver_and_drain t h out` guarantees: only `window` and `next`
    change, `next` advances, and the output gains `(t.next, h)` followed by
    the seqnos up to the new `next`, gap-free and in order. -/
def DrainPost (t : Tracker) (h : U64) (out : alloc.vec.Vec Delivered)
    (r : Tracker × alloc.vec.Vec Delivered) : Prop :=
  ∃ w n, r.1 = { t with window := w, next := n } ∧ t.next.val < n.val ∧
    ∃ ds : List Delivered,
      r.2.val = out.val ++ ({ seqno := t.next, handle := h } : Delivered) :: ds ∧
      (({ seqno := t.next, handle := h } : Delivered) :: ds).map (fun d => d.seqno.val)
        = run t.next.val (n.val - t.next.val)

/-- Loop invariant of `deliver_and_drain`: `DrainPost` so far, plus the
    bookkeeping that rules out overflow (each further delivery frees a slot). -/
def DrainInv (t : Tracker) (h : U64) (out : alloc.vec.Vec Delivered)
    (x : Tracker × alloc.vec.Vec Delivered × Bool) : Prop :=
  ∃ w n, x.1 = { t with window := w, next := n } ∧ t.next.val < n.val ∧
    n.val + occ w ≤ t.next.val + 1 + occ t.window ∧
    x.2.1.val.length + t.next.val = out.val.length + n.val ∧
    ∃ ds : List Delivered,
      x.2.1.val = out.val ++ ({ seqno := t.next, handle := h } : Delivered) :: ds ∧
      (({ seqno := t.next, handle := h } : Delivered) :: ds).map (fun d => d.seqno.val)
        = run t.next.val (n.val - t.next.val)

theorem deliver_and_drain_spec (t : Tracker) (h : U64) (out : alloc.vec.Vec Delivered)
    (hn : t.next.val + 1 + occ t.window ≤ U64.max)
    (hl : out.val.length + 1 + occ t.window < Usize.max) :
    t.deliver_and_drain h out ⦃ r => DrainPost t h out r ⦄ := by
  unfold Tracker.deliver_and_drain
  step as ⟨ out1, hout1 ⟩
  step as ⟨ n1, hn1 ⟩
  unfold Tracker.deliver_and_drain_loop
  apply loop.spec_decr_nat (fun x => 2 * occ x.1.window + (if x.2.2 then 1 else 0))
    (DrainInv t h out)
  · rintro ⟨s, o, m⟩ ⟨w, n, hs, hlt, hocc, hlen, ds, hds, hrun⟩
    simp only at hs hlen hds
    subst hs
    unfold Tracker.deliver_and_drain_loop.body
    cases m
    · simp only [Bool.false_eq_true, ↓reduceIte, WP.spec_ok]
      exact ⟨w, n, rfl, hlt, ds, hds, hrun⟩
    · simp only [↓reduceIte]
      step as ⟨ oh, s1, w1, hs1, hw1 ⟩
      subst hs1
      cases oh with
      | none =>
        simp only at hw1
        simp only [WP.spec_ok]
        exact ⟨⟨w, n, by simp [hw1], hlt, hocc, hlen, ds, hds, hrun⟩, by simp [hw1]⟩
      | some hh =>
        step as ⟨ o2, ho2 ⟩
        step as ⟨ n2, hn2 ⟩
        refine ⟨⟨w1, n2, rfl, by simp_all; omega, by simp_all; omega, by simp_all; omega,
          ds ++ [{ seqno := n, handle := hh }], by simp_all, ?_⟩, by simp; omega⟩
        have hk : n2.val - t.next.val = (n.val - t.next.val) + 1 := by omega
        rw [hk, run, List.range'_concat]
        simp only [run] at hrun
        rw [← hrun]
        simp; omega
  · refine ⟨t.window, n1, rfl, by scalar_tac, by scalar_tac, by simp [hout1]; scalar_tac, [], by simp [hout1], ?_⟩
    simp [run, hn1]

end stream_subset.proofs

#print axioms stream_subset.proofs.deliver_and_drain_spec
