# Symphony Dashboard Command Center & Run Inspector Spec

> For Hermes: this is the exact UI/UX spec for turning Symphony's dashboard from a read-only status postcard into a multi-job operator cockpit with transcript-grade run inspection.

## Goal

Give operators a central command center that works when multiple tickets are active at once, while also letting them click into any single job and inspect the real Codex event stream, transcript, token burn, and controls without touching the underlying repo manually.

## Product split

Symphony should explicitly become two surfaces:

1. Command Center
   - Fast scan of all active and queued work.
   - Optimized for triage, intervention, and prioritization.
2. Run Inspector
   - Deep inspection of a single run.
   - Optimized for answering: "what the hell is this agent actually doing?"

Trying to make one page do both will produce a bloated, mediocre UI.

---

## Design principles

### 1. Health beats state
`In Progress` is not useful if the run is dead. Show `Stalled`, `Waiting`, `Retrying`, `Failed`, and `Healthy` louder than tracker state.

### 2. Transcript is first-class
The primary new capability is real Codex event inspection. The transcript is not a debug modal, not a raw JSON page, and not a hidden advanced tab.

### 3. Summary first, raw on demand
Default rendering should be structured, human-readable event cards. Raw protocol payloads should always be one click away.

### 4. Multi-job first
The command center must remain usable with 1, 3, 8, or 15 simultaneous jobs. That means compact rows, drawer-based preview, and fast filtering/sorting.

### 5. Controls live beside evidence
Stop / retry / move-state actions should appear next to the affected run, not in a separate admin screen.

### 6. Orchestration-first
For CareCore and similar integrations, Symphony remains an orchestration console, not a domain admin console. It should link out to product/admin tools rather than duplicate them.

### 7. Dark-native and dense
Adopt a Linear-like dark operations shell:
- dark background
- restrained accent color
- strong hierarchy
- compact density
- monospace only for technical values

---

## Information architecture

## Primary routes

### `GET /`
Command Center.

### `GET /?inspect=<ISSUE_IDENTIFIER>`
Command Center with the preview drawer open for a selected run.

### `GET /runs/<ISSUE_IDENTIFIER>`
Full-screen Run Inspector.

This gives operators:
- a stable overview route
- a shareable deep link to a specific run
- a lightweight preview mode without navigating away from the overview

---

## Command Center spec

## Purpose
Answer these questions in under 5 seconds:
- What is running now?
- Which runs need intervention?
- Which tickets are eligible but not started?
- Which runs are burning tokens without meaningful progress?
- Which run should I click into next?

## Page layout

### A. Top alert strip
Persistent alert section at the top of the page.

Alert categories:
- stalled runs
- approval/input blocked runs
- retry pileups
- rate-limit pressure
- multiple claimable Agent Ready issues
- merge/deploy failures
- workspace/bootstrap failures

Rules:
- Alerts are ordered by severity.
- Each alert links directly to the affected run or queue section.
- Alerts should be phrased bluntly, e.g.:
  - `CC-1018 stalled for 4m 12s`
  - `2 issues are claimable; pilot expects exactly 1`
  - `Retry queue growing: 5 issues waiting`

### B. Summary metrics row
Compact, scannable metrics. No vanity chart spam.

Recommended cards:
- Running
- Needs Attention
- Retrying
- Eligible Queue
- Total burn/min
- Rate-limit health

Optional later:
- Completed today
- Failure rate last 24h

### C. Pinned watchlist strip
A horizontally scrollable strip of pinned runs.

Each pill shows:
- issue identifier
- health color
- short phase label
- live badge / stalled badge / retry timer
- current burn rate

Purpose:
Give operators a mini war room for 2–5 especially important runs.

### D. Main section: Active Runs table
This is the core command-center view.

Columns:
- Issue
- Tracker State
- Health
- Phase
- Last Activity
- Tokens
- Burn Rate
- Runtime
- Worker
- Quick Actions

Behavior:
- Single click: open preview drawer
- Double click or Enter: open full Run Inspector
- Rows are color-coded by health, not by tracker state

Health colors:
- Green: progressing normally
- Yellow: waiting / mild concern / idle but not stale
- Red: stalled / failed / approval blocked too long
- Gray: completed / inactive

### E. Secondary section: Eligible Queue
Shows tickets that Symphony could or should pick up.

Columns:
- Issue
- Claimable?
- Why / why not
- Labels
- State
- Priority (optional later)

This section is crucial for CareCore-style workflows where the command center must prevent wrong-ticket pickup.

### F. Secondary section: Retry / Backoff queue
Columns:
- Issue
- Attempt
- Backoff until
- Error reason
- Suggested action

Important rule:
Healthy continuation should not be visually mixed with genuine failure backoff. If both use the same underlying machinery, the UI must still label them differently.

### G. Secondary section: Recent outcomes
Shows recent completions, failures, and blocks.

Columns:
- Issue
- Outcome
- Runtime
- Tokens
- Last meaningful event
- Completed / failed at

Purpose:
Helps operators understand what just finished without opening transcripts.

---

## Command Center sorting rules

Default sorting for Active Runs:
1. Failed / blocked by operator action needed
2. Stalled
3. Approval or input blocked
4. Retrying soon / backoff expiring soon
5. High burn with low recent progress
6. Healthy progressing runs
7. Idle but healthy runs

Secondary sorting inside a bucket:
1. Longer time since last activity
2. Higher burn rate
3. Longer total runtime
4. Issue identifier ascending

### Derived health model
Each run should compute a derived health label.

Recommended set:
- `failed`
- `blocked`
- `stalled`
- `waiting_approval`
- `waiting_input`
- `retrying`
- `warning`
- `healthy`
- `completed`

Suggested thresholds:
- `stalled`: last activity exceeds configurable percentage of `codex.stall_timeout_ms`
  - e.g. warning at 50%
  - stale at 100%
- `warning`: no meaningful progress for N seconds but still receiving low-value protocol churn
- `waiting_approval`: approval/input event present and unresolved
- `retrying`: explicit backoff in effect

---

## Preview drawer spec

## Purpose
Provide a fast, lightweight run preview without leaving the command center.

## Open behavior
- Opens from row click
- Closes via escape, close button, or clicking outside
- URL synced via `?inspect=<ISSUE_IDENTIFIER>`

## Drawer contents

### Header
- issue identifier + title
- health badge
- tracker state badge
- last activity time
- quick controls:
  - Stop run
  - Retry now
  - Refresh now
  - Open Inspector
  - Open in Linear

### Body sections

#### 1. Latest transcript tail
Show the most recent 10–20 meaningful events.
Default filter should hide low-value noise unless no higher-value events exist.

#### 2. Status snapshot
- current phase
- runtime
- tokens in/out/total
- burn rate
- backoff / approval / input state if present

#### 3. Runtime identity
- session ID
- thread ID
- current turn ID
- worker host
- workspace path

#### 4. Recent file / diff summary
- changed file count
- last changed file
- link to full diff in inspector

The drawer is for triage, not exhaustive reading.

---

## Run Inspector spec

## Purpose
Be the single best place to inspect one live agent run.

The operator should be able to answer:
- What is it doing right now?
- What did it just say?
- What changed on disk?
- Is it making progress or stuck?
- How much is it burning?
- Should I intervene?

## Layout
Three-pane desktop layout.

### Header row
Contents:
- issue identifier + issue title
- tracker state badge
- health badge
- last activity age
- controls:
  - Stop run
  - Retry now
  - Refresh now
  - Move to Todo
  - Move to Blocked
  - Open in Linear
  - Copy session/thread IDs

### Left rail
Purpose: navigation and filtering.

Sections:
- Turn list
- Event type filters
- Search input
- Quick jumps:
  - latest event
  - first warning/error
  - first token update
  - first file change
  - current turn start

Turn list entries show:
- turn number or turn ID suffix
- started time
- ended/completed state
- tokens for that turn if available

### Main pane: transcript timeline
This is the hero surface.

Default behavior:
- live auto-follow on
- pause auto-scroll toggle
- newest events append at bottom
- grouped by turn

Toolbar controls:
- Follow live
- Pause auto-scroll
- Search transcript
- Filter menu
- Raw mode toggle
- Errors only toggle
- Jump to latest
- Jump to first warning/error

### Right sidebar
This is the operator brain.

Cards:

#### 1. Run health
- health label
- current phase
- last activity age
- stalled threshold countdown
- waiting-on state

#### 2. Resource burn
- input/output/total tokens
- tokens/sec or tokens/min
- recent sparkline
- context-window utilization if available

#### 3. Runtime identity
- session ID
- thread ID
- turn ID
- worker host
- app-server pid
- workspace path

#### 4. Artifacts
- changed files
- latest diff summary
- PR / deploy / workpad links later if available

#### 5. Actions
- stop
- retry now
- clear backoff
- move state
- open workspace
- copy IDs

---

## Transcript rendering rules

## Core principle
Do not show raw JSON by default.
Show structured event cards, with raw JSON available on expand.

## Event grouping
Events are grouped by `turn_id`.

Each turn is rendered as:
- Turn started marker
- event timeline inside the turn
- Turn completed / failed / cancelled marker

## Event rendering types

### 1. Agent message deltas
Source examples:
- `item/agentMessage/delta`

Render as:
- accumulated markdown message bubbles
- live typing effect while deltas stream in
- one logical bubble per item/message ID

Why:
This is the most human-readable representation of what the agent is saying.

### 2. Reasoning events
Source examples:
- `item/reasoning/*`
- reasoning-related wrapper events

Render as:
- subdued collapsible "Reasoning" blocks
- summary text first, raw event details on expand

Important rule:
Show only actual emitted reasoning content. Do not invent hidden chain-of-thought.

### 3. File changes
Source examples:
- `item/fileChange/outputDelta`
- `turn/diff/updated`

Render as:
- compact file-change cards
- filename / path emphasized
- expandable inline diff preview
- syntax-colored diff on expand

### 4. Command output
Source examples:
- `item/commandExecution/outputDelta`

Render as:
- terminal-style output blocks
- collapsed by default when large
- searchable
- line-wrapped but copyable

### 5. Token usage updates
Source examples:
- `thread/tokenUsage/updated`

Render as:
- subtle inline system events only when useful
- primary representation should be sidebar metrics + mini chart

Do not flood the main timeline with repetitive token rows.

### 6. Lifecycle events
Source examples:
- `turn/started`
- `turn/completed`
- `thread/status/changed`
- approvals
- retries

Render as:
- compact system markers
- timestamped
- visually distinct from agent content

### 7. Unknown / uncategorized events
Render as:
- generic system event cards
- one-line summary + expand for raw JSON

---

## Transcript filters

Required filters:
- Agent messages
- Reasoning
- File changes
- Diff
- Command output
- Token usage
- Approvals / input required
- Lifecycle
- Raw / uncategorized

Additional toggles:
- Errors only
- Meaningful events only
- Hide protocol noise

Meaningful events only should exclude low-value churn such as repetitive streaming or status updates when more readable grouped content already exists.

---

## Multi-job data model

## Overview
To support both the command center and inspector, Symphony needs two levels of retained state:

1. Per-run summary state
2. Per-run bounded event log

### A. Per-run summary state
Per issue / active run, retain:
- `issue_id`
- `issue_identifier`
- `title`
- `tracker_state`
- `health`
- `health_reason`
- `worker_host`
- `workspace_path`
- `session_id`
- `thread_id`
- `current_turn_id`
- `turn_count`
- `started_at`
- `last_activity_at`
- `last_meaningful_event`
- `last_meaningful_summary`
- `phase`
- `phase_detail`
- `input_tokens`
- `output_tokens`
- `total_tokens`
- `burn_rate_tokens_per_sec`
- `retry_attempt`
- `retry_due_at`
- `waiting_on`
- `codex_app_server_pid`

### B. Per-run bounded event log
Keep a ring buffer per active issue.

Recommended initial size:
- 500 to 2,000 events per run

The buffer should be bounded by both:
- max event count
- max approximate byte budget

The dashboard does not need infinite history for active sessions in v1.

### Event schema
Each retained event should look like:

```json
{
  "event_id": "uuid-or-monotonic-id",
  "issue_identifier": "CC-1234",
  "session_id": "thread-turn",
  "thread_id": "thread-id",
  "turn_id": "turn-id",
  "timestamp": "2026-04-19T01:17:37.430701Z",
  "event": "notification",
  "method": "item/agentMessage/delta",
  "category": "agent_message",
  "item_id": "msg_...",
  "summary": "Agent message streaming",
  "raw": "{...}",
  "payload": {"method": "item/agentMessage/delta", "params": {...}},
  "metadata": {
    "worker_host": null,
    "codex_app_server_pid": "2429047"
  }
}
```

Derived fields like `category`, `summary`, and `item_id` should be computed once when the event is ingested.

### Grouping state for streaming items
To render deltas as coherent messages, keep per-run aggregation state keyed by `item_id`:
- current accumulated agent message text
- current accumulated reasoning summary text
- current accumulated command output text
- current accumulated file-change text if needed

The event log remains raw-ish and append-only; the UI consumes both raw events and aggregated item state.

---

## API spec

## Existing endpoints to extend
Current endpoints:
- `GET /api/v1/state`
- `GET /api/v1/:issue_identifier`
- `POST /api/v1/refresh`

These are insufficient for a command center + inspector.

## Proposed endpoints

### 1. `GET /api/v1/state`
Purpose: command center payload.

Should return:
- top-level counts
- alerts
- running run summaries
- eligible queue summaries
- retry queue summaries
- recent outcome summaries
- rate limit summary

Example shape:

```json
{
  "generated_at": "...",
  "counts": {
    "running": 4,
    "needs_attention": 2,
    "retrying": 1,
    "eligible": 3
  },
  "alerts": [
    {
      "severity": "critical",
      "issue_identifier": "CC-1018",
      "code": "stalled",
      "message": "CC-1018 stalled for 4m 12s"
    }
  ],
  "running": [...],
  "eligible": [...],
  "retrying": [...],
  "recent_outcomes": [...],
  "rate_limits": {...}
}
```

### 2. `GET /api/v1/runs/:issue_identifier`
Purpose: run inspector shell data.

Should return:
- run summary
- sidebar metrics
- latest artifacts summary
- available turns
- last N events preview

### 3. `GET /api/v1/runs/:issue_identifier/events`
Purpose: paginated transcript data.

Query params:
- `limit`
- `before`
- `after`
- `event_types`
- `categories`
- `errors_only`

Response:
- event list
- next/prev cursors
- grouped turn metadata if convenient

### 4. `GET /api/v1/runs/:issue_identifier/summary`
Optional thin endpoint for drawer-only quick fetch.

### 5. Control endpoints

#### `POST /api/v1/runs/:issue_identifier/stop`
#### `POST /api/v1/runs/:issue_identifier/retry`
#### `POST /api/v1/runs/:issue_identifier/move`
#### `POST /api/v1/runs/:issue_identifier/clear_backoff`

These should be explicit and narrow. Avoid one giant generic mutate endpoint.

---

## Action model

## Overview actions
Per-row quick actions:
- Stop
- Retry now
- Refresh
- Open inspector

## Inspector actions
Primary actions:
- Stop run
- Retry now
- Refresh now
- Move to Todo
- Move to Blocked
- Open in Linear
- Copy IDs

Later actions:
- Clear backoff
- Move to Merging
- Open workspace

All actions should return structured feedback so the UI can show optimistic toasts and row state updates.

---

## Multiple jobs: exact UX model

### Primary operator flow
1. Open Command Center
2. Scan alerts and active run health
3. Click a row to open preview drawer
4. If needed, open full Run Inspector
5. Switch back to Command Center without losing context

### Why this scales
This model works for many simultaneous runs because:
- the overview remains compact
- the drawer supports rapid triage
- the full inspector supports deep focus
- transcript rendering stays scoped to one run at a time

### What not to do
Do not create one giant page that streams all transcripts at once by default.
That becomes unreadable immediately once multiple jobs are active.

---

## Recommended v1 scope

### Must-have
- command center route
- active runs table
- eligible queue
- retry queue
- alerts
- preview drawer
- full run inspector route
- per-run bounded event log retention
- structured transcript rendering
- search/filter inside a run
- health + token + metadata sidebar

### Nice-to-have after v1
- pinned watchlist strip
- compare two runs side-by-side
- global search across active runs
- unread markers since last viewed
- historical analytics beyond active runs
- plugin/extensibility model

---

## Implementation order

### Phase 1: event retention
- retain bounded per-run raw events
- derive categories / summaries on ingest
- retain item-level aggregated message text

### Phase 2: API
- extend command center payload
- add per-run inspector payload
- add paginated events endpoint
- add explicit control endpoints

### Phase 3: command center UI
- alerts
- metrics
- active runs table
- eligible queue
- retry queue

### Phase 4: preview drawer
- transcript tail
- quick status snapshot
- quick actions

### Phase 5: full run inspector
- left rail
- transcript timeline
- right sidebar
- filters / search / raw mode

### Phase 6: interaction polish
- live follow
- pause auto-scroll
- jump shortcuts
- pinned watchlist

---

## Definition of done

The new dashboard is successful when an operator can:
- see all active jobs in one command center
- instantly identify which run needs attention most
- click any run and inspect the real Codex transcript/log stream
- distinguish healthy progress from idle churn, approval blocking, and genuine stall
- stop or retry a run without leaving the dashboard
- do all of the above without opening raw JSON unless they choose to
