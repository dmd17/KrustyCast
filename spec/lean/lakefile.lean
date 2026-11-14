import Lake
open Lake DSL

-- Reuses the Aeneas checkout's Mathlib instead of downloading another copy.
-- AENEAS_LEAN points at aeneas/backends/lean (default: ~/.local/share/krusty-tools).
def aeneasDir : String := run_io do
  match (← IO.getEnv "AENEAS_LEAN") with
  | some d => return d
  | none => return (← IO.getEnv "HOME").getD "" ++ "/.local/share/krusty-tools/aeneas/backends/lean"

package «krusty_proofs» where
  packagesDir := aeneasDir ++ "/.lake/packages"

require aeneas from aeneasDir

@[default_target] lean_lib KrustyCore {}
