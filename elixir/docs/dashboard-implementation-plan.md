# Symphony Dashboard Buildout Plan

Goal: ship the first real version of the command center + run inspector by building the backend event-retention pipeline first, then exposing it through new API payloads and a LiveView UI shell.

Scope for this tranche:
1. Retain bounded per-run Codex events in the orchestrator.
2. Expose run inspector data and paginated event feeds via API.
3. Upgrade the command center overview with richer health/activity data.
4. Add a first run-inspector UI route and drawer/shell, driven by the new data.
5. Keep the scope tight: stop/retry controls may be stubbed to existing refresh behavior if full mutation wiring grows too large.

Execution order:

## Task 1: Inspect the existing backend seams
- Files:
  - `lib/symphony_elixir/orchestrator.ex`
  - `lib/symphony_elixir/agent_runner.ex`
  - `lib/symphony_elixir_web/presenter.ex`
  - `lib/symphony_elixir_web/router.ex`
  - `lib/symphony_elixir_web/controllers/observability_api_controller.ex`
  - tests under `test/symphony_elixir/*`
- Output:
  - exact place where codex updates enter orchestrator
  - exact snapshot shape to extend
  - exact tests to copy for new API behavior

## Task 2: Write failing backend tests
- Add tests for:
  - retained per-run event logs in orchestrator snapshot/presenter issue payload
  - paginated events endpoint for a run
  - richer command-center state payload (health/activity/run metadata)
- Run tests and confirm failure before implementation.

## Task 3: Implement bounded event retention
- Extend orchestrator running-entry state with:
  - `recent_codex_events`
  - `thread_id`
  - `current_turn_id`
  - richer last-activity metadata
- Retain a bounded ring buffer per running issue.
- Store raw event + derived category/method/summary fields.

## Task 4: Extend presenter and API
- Add inspector-oriented payloads:
  - `GET /api/v1/state` richer overview data
  - `GET /api/v1/runs/:issue_identifier`
  - `GET /api/v1/runs/:issue_identifier/events`
- Keep old endpoints working where practical.

## Task 5: Build first command-center UI shell
- Upgrade `/` LiveView to show:
  - alert strip
  - richer active-runs table
  - retry queue
  - recent activity improvements
- Keep layout efficient and dark-native.

## Task 6: Build first run-inspector shell
- Add route + LiveView for `/runs/:issue_identifier`
- Include:
  - header
  - live transcript timeline
  - filter/search basics if cheap
  - right sidebar metadata/tokens/IDs

## Task 7: Verify and review
- Targeted tests first.
- Then broader relevant test files.
- Then independent review before commit.

Definition of done for this tranche:
- Operators can see multiple active jobs in the overview.
- Operators can click into one run and inspect real retained Codex events.
- The inspector shows structured timeline data, not only JSON links.
- Tests cover the new backend contract.
