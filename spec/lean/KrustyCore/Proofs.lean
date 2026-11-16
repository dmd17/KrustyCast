/-
  First theorems about `krusty-core` (crates/krusty-core), stated against
  the Lean code Aeneas generates from the Rust source (`regen.sh`).

  Small, exact specifications of pieces of the decision logic. The
  refinement proof of `Core::apply` (ARCHITECTURE §11.2) is in
  `Spec.lean` and `Refine.lean`.
-/
import KrustyCore.Funs

open Aeneas Aeneas.Std Result
open krusty_core

namespace krusty_core.proofs

/-- Applying an entry out of log order changes nothing and reports an
    error; it never fails (panics) either. -/
theorem apply_out_of_order (t : Core) (li ts : U64) (s : krusty_wire.ingress.Submit)
    (h : li.val ≠ t.applied.val + 1) :
    t.apply li ts s ⦃ r => r.2 = t ∧ ∃ e, r.1 = core.result.Result.Err e ⦄ := by
  unfold Core.apply
  split
  · step*
    all_goals first | (simp_all; done) | exact ⟨rfl, _, rfl⟩
  · rename_i hne
    have hmax : (core.num.U64.MAX).val = 18446744073709551615 := by
      simp [core.num.U64.MAX, U64.rMax]
    rw [UScalar.eq_equiv, hmax] at hne
    step*
    all_goals first | (simp_all; done) | scalar_tac

/-- An unconditional append is accepted at the next stream seqno, is
    published with the leader's clock, and changes no state. -/
theorem decide_append_unconditional (t : Core) (s : Request)
    (h : s.append = krusty_wire.ingress.Append.Unconditional) :
    t.decide_append s ⦃ r =>
      r.2 = t ∧
      r.1.1 = some (Decided.Accepted t.next_seqno) ∧
      r.1.2 = some { timestamp_ns := t.now_ns, client_id := s.client_id, conditional := none } ⦄ := by
  unfold Core.decide_append
  rw [h]
  simp

/-- A conditional append whose expected version is stale is rejected with
    the key's current version, and changes no state. -/
theorem decide_append_stale (t : Core) (s : Request) (key ev cur : U64)
    (h : s.append = krusty_wire.ingress.Append.Conditional key ev)
    (hv : t.version key = ok cur) (hne : cur.val ≠ ev.val) :
    t.decide_append s ⦃ r =>
      r.2 = t ∧ r.1 = (some (Decided.Rejected key cur), none) ⦄ := by
  unfold Core.decide_append
  rw [h]
  simp [hv, hne]

#print axioms apply_out_of_order
#print axioms decide_append_unconditional
#print axioms decide_append_stale

end krusty_core.proofs
