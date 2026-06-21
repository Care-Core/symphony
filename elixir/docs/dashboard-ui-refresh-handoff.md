# Symphony dashboard UI refresh handoff

## Scope
This work was exploratory + implementation in the Symphony repo only. We reviewed:
- `~/Developer/tools/symphony/elixir` (the actual dashboard code)
- `~/Developer/carecore/care-core-platform` (the host project / expected usage context)
- Hermes Dashboard / Hermes Atlas as the design reference for a better observability console

Main user pain points:
- almost no visibility into what the agent is doing / whether it is stuck / where tokens are going
- no dashboard controls yet (stop a job, move status backward, etc.)

The explicit decision was: observability first, controls later.

## Current repo / branch state
Symphony dev repo:
- repo/worktree root: `~/Developer/tools/symphony`
- active code path: `~/Developer/tools/symphony/elixir`
- branch: `feat/dashboard-observability-inspector`
- current work is uncommitted

CareCore repo:
- main checkout: `~/Developer/carecore/care-core-platform`
- branch: `main`
- local status currently shows `WORKFLOW.md` modified
- there are existing Symphony-created issue worktrees under `~/Developer/carecore/worktrees/symphony/`
- no CareCore code changes were part of this dashboard refresh

## What we accomplished
1. Reviewed Symphony dashboard code, CareCore context, and Hermes Dashboard / Hermes Atlas styling cues.
2. Built out the observability direction in Symphony:
   - command-center style dashboard
   - dedicated run inspector / observability routes
   - recent outcomes surface
   - presenter/controller/router plumbing for run data
   - live Codex observability proof path and tests
3. Refreshed the dashboard UI to feel much closer to Hermes Atlas:
   - warm dark palette
   - ivory text + amber accents
   - editorial headline/meta strip
   - thinner borders / less generic SaaS card styling
4. Simplified interaction hierarchy:
   - issue title click = quick in-place view
   - single explicit CTA = `Inspect`
   - removed the confusing `Preview` vs `Open Inspector` split
5. Added / updated docs and tests around the dashboard work.

## Key files touched
Core UI / observability:
- `lib/symphony_elixir_web/live/dashboard_live.ex`
- `priv/static/dashboard.css`
- `lib/symphony_elixir_web/presenter.ex`
- `lib/symphony_elixir_web/controllers/observability_api_controller.ex`
- `lib/symphony_elixir_web/router.ex`
- `lib/symphony_elixir/orchestrator.ex`
- `lib/symphony_elixir/run_archive.ex`

Supporting / docs / tests:
- `lib/symphony_elixir_web/components/layouts.ex`
- `test/symphony_elixir/extensions_test.exs`
- `test/symphony_elixir/core_test.exs`
- `test/symphony_elixir/live_codex_observability_test.exs`
- `docs/dashboard-command-center-spec.md`
- `docs/dashboard-implementation-plan.md`

## Last known good validation
- targeted UI test passed:
  - `mise exec -- mix format && mise exec -- mix test test/symphony_elixir/extensions_test.exs --no-color`
- earlier full-suite baseline was green before the latest Atlas polish:
  - `240 tests, 0 failures, 3 skipped`

## Where we left off
The Atlas-style pass is materially better, but not fully finished.

What still wants work:
- clearer “what is this agent doing right now?” visibility on the homepage
- easier drill-down into raw messages / events / token burn so Austin can tell stuck vs healthy
- final layout polish on the command center so it feels less like KPI cards and more like a real operations console
- controls are still intentionally deferred for now

Important product decision:
- do not add stop / retry / move-state controls until the observability story feels solid

## Resume checklist
1. Re-open the current branch in Symphony:
   - `cd ~/Developer/tools/symphony/elixir`
   - `git branch --show-current`
2. Inspect current diff:
   - `git status --short`
   - `git diff --stat`
3. Rebuild:
   - `mise exec -- mix build`
4. Relaunch demo board:
   - `mise exec -- mix run --no-halt tmp/launch_demo_dashboard.exs`
5. Relaunch real board:
   - `LINEAR_API_KEY="$LINEAR_API_KEY" mise exec -- ./bin/symphony --i-understand-that-this-will-be-running-without-the-usual-guardrails ./tmp/LINEAR_OBSERVABILITY_DEMO_WORKFLOW.md`
6. Then review:
   - demo board: `http://100.104.200.6:4242/?v=13`
   - real board: `http://100.104.200.6:4343/?v=13`

Note: at handoff time, ports `4242` and `4343` were closed, so the dashboards need to be relaunched.

## Useful live-review artifact
A disposable Linear issue was created for real-board review:
- `CC-1077`
- title: `Symphony dashboard style review 5378`
- URL: `https://linear.app/care-core/issue/CC-1077/symphony-dashboard-style-review-5378`

## Suggested next moves
1. Do one more blunt visual pass on both the demo board and the real inspector.
2. Make the current-agent-activity / raw-message observability much more obvious.
3. Re-run full test suite after the next UI pass.
4. Only after that, design dashboard controls (stop job, move status, retry/backlog transitions, etc.).

## Important nuance for the next agent
The current todo list is a bit stale: some “inspect / implement / verify Atlas style” work was already done. The real next step is polish + stronger observability detail, not starting the redesign from scratch.
