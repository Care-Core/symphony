# Symphony Phase 1 Run Archive Plan

> For Hermes: planning only. Do not implement in this pass.

Goal
- Make completed run inspection survive Symphony runner restarts for all new runs going forward.

Current context
- Today `/runs/:issue_identifier` only works if the run is still in the live orchestrator snapshot (`running`, `retrying`, `recent_outcomes`).
- `recent_outcomes` lives only in GenServer memory and resets on restart.
- `recent_codex_events` are also tail-clipped to 200 events, so long runs can look suspiciously short even before restart.
- Rich raw data already exists in Codex JSONL files, but that is not a good primary data store.

Decision
- Phase 1 should add a Symphony-owned archive for new runs only.
- Do not backfill old runs from Codex session files yet.
- Do not add a new dashboard surface yet; just make existing inspector/event endpoints durable.

Architecture
- Add a small archive module that writes per-run files under `--logs-root`.
- On each Codex update, append a normalized raw event record to `runs/<session_id>/events.jsonl`.
- On run completion/termination, write `runs/<session_id>/summary.json` with durable metadata and a normalized operator transcript.
- Maintain a tiny issue index so `/runs/CC-1234` can resolve to the latest archived session for that issue when no live snapshot entry exists.

Proposed on-disk layout
- `<logs_root>/runs/<session_id>/events.jsonl`
- `<logs_root>/runs/<session_id>/summary.json`
- `<logs_root>/issues/<issue_identifier>.json`

`summary.json` fields
- issue_id
- issue_identifier
- issue_url
- title
- session_id
- thread_id
- current_turn_id
- worker_host
- workspace_path
- started_at
- finished_at
- outcome
- runtime_seconds
- turn_count
- token totals
- last_event / last_message
- operator_transcript
- archive_version

Files likely to change
- Create: `lib/symphony_elixir/run_archive.ex`
- Modify: `lib/symphony_elixir/orchestrator.ex`
- Modify: `lib/symphony_elixir_web/presenter.ex`
- Modify: `lib/symphony_elixir/cli.ex`
- Possibly modify: `lib/symphony_elixir/config/schema.ex` only if we decide archive path needs explicit config rather than inheriting `logs_root`
- Test: `test/symphony_elixir/extensions_test.exs`
- Test: `test/symphony_elixir/core_test.exs`

Implementation plan

1. Add archive root wiring
- In `CLI`, set a dedicated application env for the archive root when `--logs-root` is provided.
- Keep it separate from the rotating app log path; don’t infer archive paths from `log/symphony.log` at read time if you can avoid it.

2. Add `RunArchive` module
- Responsibility: append event JSONL, write summary JSON, read latest summary by issue, read events by issue/session.
- Keep API tiny and synchronous.
- Use plain files, no DB.

3. Archive events as they stream
- In `orchestrator.ex`, after a running entry absorbs a Codex update, append the normalized event payload to archive if `session_id` is known.
- The archived event format should be close to today’s presenter-facing `event_payload`, not raw internal structs.
- Do not wait until run end; stream to disk as events arrive.

4. Write final summary on completion
- In the same completion paths that call `record_recent_outcome/3`, also write `summary.json`.
- Build summary from the full archived event stream for that session, not the 200-event in-memory tail.
- Also update `issues/<issue_identifier>.json` to point to the latest `session_id`.

5. Add presenter fallback
- Keep current lookup order for live data first.
- If `fetch_issue_entries/3` cannot find the issue in snapshot memory, try archive lookup by issue identifier.
- Make both `run_payload/3` and `events_payload/4` work from archive-backed summaries/events.
- `state_payload/2` can stay mostly live for Phase 1; no need to rebuild the whole board from archive yet.

6. Leave routes alone for now
- Keep `/runs/:issue_identifier` and `/api/v1/runs/:issue_identifier/events`.
- Resolve them to the latest archived session for that issue when the live snapshot misses.
- Defer stable run-id/session-id routes to Phase 2/3.

Test plan

1. Archive writer unit test
- Append several events to a fake session.
- Assert `events.jsonl` exists and all events are recoverable.

2. Summary writer unit test
- Feed a representative event stream.
- Assert `summary.json` includes issue metadata, token totals, and a multi-block operator transcript.

3. Presenter fallback test
- Simulate snapshot miss for `MT-DONE`.
- Seed archive files for that issue.
- Assert `/api/v1/runs/MT-DONE` returns 200 with transcript and metadata.

4. Events fallback test
- Same setup, but hit `/api/v1/runs/MT-DONE/events`.
- Assert full archived event list is returned, not `issue_not_found`.

5. Restart-survival integration test
- Seed or write an archived completed run.
- Recreate orchestrator state with empty `recent_outcomes`.
- Assert `/runs/<issue>` still renders inspector content.

Verification commands
- `cd ~/Developer/tools/symphony/elixir && mise exec -- mix test test/symphony_elixir/extensions_test.exs --no-color`
- `cd ~/Developer/tools/symphony/elixir && mise exec -- mix test test/symphony_elixir/core_test.exs --no-color`
- `cd ~/Developer/tools/symphony/elixir && mise exec -- mix test --no-color`
- `cd ~/Developer/tools/symphony/elixir && mise exec -- mix build`

Manual acceptance checks
- Start runner with `--logs-root`.
- Run a fresh disposable ticket.
- Confirm archive files appear under logs root.
- Restart Symphony.
- Confirm `/runs/<issue_identifier>` still shows transcript, health, and events.
- Confirm `/api/v1/runs/<issue_identifier>/events` still works after restart.

Non-goals for Phase 1
- No backfill of old runs from `~/.codex/sessions`
- No archived-runs browser/index page
- No session-id routes yet
- No dashboard controls work

Risks / tradeoffs
- If the process crashes hard before final summary write, you may have durable `events.jsonl` but no `summary.json`. That is acceptable in Phase 1 if presenter can degrade or if completion writes are reliable enough.
- Repeated runs for the same issue will collapse to “latest run wins” because the issue index points to the newest session. Fine for Phase 1.
- Remote workers are safe as long as the orchestrator archives the updates it already receives centrally.

My take
- This is the right first cut: small, durable, boring, and enough to stop the inspector from lying after restarts.