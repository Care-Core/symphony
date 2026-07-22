# Symphony Elixir

This directory contains the current Elixir/OTP implementation of Symphony, based on
[`SPEC.md`](../SPEC.md) at the repository root.

> [!WARNING]
> Symphony Elixir is prototype software intended for evaluation only and is presented as-is.
> We recommend implementing your own hardened version based on `SPEC.md`.

## Screenshot

![Symphony Elixir screenshot](../.github/media/elixir-screenshot.png)

## How it works

1. Polls Linear for candidate work
2. Creates a workspace per issue
3. Launches Codex in [App Server mode](https://developers.openai.com/codex/app-server/) inside the
   workspace
4. Sends a workflow prompt to Codex
5. Keeps Codex working on the issue until the work is done

During app-server sessions, Symphony also serves a client-side `linear_graphql` tool so that repo
skills can make raw Linear GraphQL calls.

If a claimed issue moves to a terminal state (`Done`, `Closed`, `Cancelled`, or `Duplicate`),
Symphony stops the active agent for that issue and cleans up matching workspaces.

## How to use it

1. Make sure your codebase is set up to work well with agents: see
   [Harness engineering](https://openai.com/index/harness-engineering/).
2. Get a new personal token in Linear via Settings → Security & access → Personal API keys, and
   set it as the `LINEAR_API_KEY` environment variable.
3. Copy this directory's `WORKFLOW.md` to your repo.
4. Optionally copy the `commit`, `push`, `pull`, `land`, and `linear` skills to your repo.
   - The `linear` skill expects Symphony's `linear_graphql` app-server tool for raw Linear GraphQL
     operations such as comment editing or upload flows.
5. Customize the copied `WORKFLOW.md` file for your project.
   - To get your project's slug, right-click the project and copy its URL. The slug is part of the
     URL.
   - When creating a workflow based on this repo, note that it depends on non-standard Linear
     issue statuses: "Rework", "Human Review", and "Merging". You can customize them in
     Team Settings → Workflow in Linear.
6. Follow the instructions below to install the required runtime dependencies and start the service.

## Prerequisites

We recommend using [mise](https://mise.jdx.dev/) to manage Elixir/Erlang versions.
Local process-tree cleanup also requires a process-group launcher: Perl on macOS or `setsid` on
Linux. The hardened runner profile validates this before issue polling or claims.

```bash
mise install
mise exec -- elixir --version
```

## Run

```bash
git clone https://github.com/openai/symphony
cd symphony/elixir
mise trust
mise install
mise exec -- mix setup
mise exec -- mix build
mise exec -- ./bin/symphony ./WORKFLOW.md
```

## Configuration

Pass a custom workflow file path to `./bin/symphony` when starting the service:

```bash
./bin/symphony /path/to/custom/WORKFLOW.md
```

If no path is passed, Symphony defaults to `./WORKFLOW.md`.

Optional flags:

- `--logs-root` tells Symphony to write logs under a different directory (default: `./log`)
- `--port` also starts the Phoenix observability service (default: disabled)

The `WORKFLOW.md` file uses YAML front matter for configuration, plus a Markdown body used as the
Codex session prompt.

Minimal example:

```md
---
tracker:
  kind: linear
  # Optional: narrow polling to one Linear project slugId.
  # Omit for workspace-wide polling.
  project_slug: "..."
workspace:
  root: ~/code/workspaces
hooks:
  after_create: |
    git clone git@github.com:your-org/your-repo.git .
agent:
  max_concurrent_agents: 10
  max_turns: 20
codex:
  command: codex app-server
  # Optional per-run input-token budget. Omit to disable.
  input_token_limit: 200000
  input_token_limits_by_label:
    expensive: 120000
    urgent: 80000
  input_token_warning_ratio: 0.70
---

You are working on a Linear issue {{ issue.identifier }}.

Title: {{ issue.title }} Body: {{ issue.description }}
```

Notes:

- If a value is missing, defaults are used.
- Safer Codex defaults are used when policy fields are omitted:
  - `codex.approval_policy` defaults to `{"reject":{"sandbox_approval":true,"rules":true,"mcp_elicitations":true}}`
  - `codex.thread_sandbox` defaults to `workspace-write`
  - `codex.turn_sandbox_policy` defaults to a `workspaceWrite` policy rooted at the current issue workspace
- Supported `codex.approval_policy` values depend on the targeted Codex app-server version. In the current local Codex schema, string values include `untrusted`, `on-failure`, `on-request`, and `never`, and object-form `reject` is also supported.
- Supported `codex.thread_sandbox` values: `read-only`, `workspace-write`, `danger-full-access`.
- When `codex.turn_sandbox_policy` is set explicitly, Symphony passes the map through to Codex
  unchanged. Compatibility then depends on the targeted Codex app-server version rather than local
  Symphony validation.
- `agent.max_turns` caps how many back-to-back Codex turns Symphony will run in a single agent
  invocation when a turn completes normally but the issue is still in an active state. Default: `20`.
- Input-token budgets are opt-in. `codex.input_token_limit` sets the default positive per-run limit;
  `codex.input_token_limits_by_label` supplies lower positive limits for matching issue labels. Label
  matching is case-insensitive, and the smallest configured limit wins when several labels match.
  `codex.input_token_warning_ratio` defaults to `0.70` and must be greater than `0` and less than
  `1`.
- On the first warning-threshold crossing, Symphony asks an active app-server turn to checkpoint via
  `turn/steer`. The state/API reports `requested`, `delivered`, or `unsupported`; older app-server
  versions that reject steering are reported as unsupported rather than as a successful warning.
- At the exact input-token limit or above, Symphony interrupts the turn, terminates the app-server
  process tree locally or on its SSH worker, preserves the workspace, and puts the issue on a
  durable internal hold. Holds are atomically stored with owner-only permissions in
  `<workspace.root>/.symphony-holds.json` and restored before polling after a restart. A held issue
  is not retried or polled into another run until a fetched tracker state verifiably changes or the
  authenticated local resume control is used; missing tracker results keep the hold. Corrupt,
  insecure, or unreadable hold state fails startup closed. Manual and token-budget stops do not
  mutate Linear.
- If the Markdown body is blank, Symphony uses a default prompt template that includes the issue
  identifier, title, and body.
- Use `hooks.after_create` to bootstrap a fresh workspace. For a Git-backed repo, you can run
  `git clone ... .` there, along with any other setup commands you need.
- If a hook needs `mise exec` inside a freshly cloned workspace, trust the repo config and fetch
  the project dependencies in `hooks.after_create` before invoking `mise` later from other hooks.
- `tracker.api_key` reads from `LINEAR_API_KEY` when unset or when value is `$LINEAR_API_KEY`.
- For path values, `~` is expanded to the home directory.
- For env-backed path values, use `$VAR`. `workspace.root` resolves `$VAR` before path handling,
  while `codex.command` stays a shell command string and any `$VAR` expansion there happens in the
  launched shell.

```yaml
tracker:
  api_key: $LINEAR_API_KEY
workspace:
  root: $SYMPHONY_WORKSPACE_ROOT
hooks:
  after_create: |
    git clone --depth 1 "$SOURCE_REPO_URL" .
codex:
  command: "$CODEX_BIN app-server --model gpt-5.3-codex"
```

- If `WORKFLOW.md` is missing or has invalid YAML at startup, Symphony does not boot.
- If a later reload fails, Symphony keeps running with the last known good workflow and logs the
  reload error until the file is fixed.
- `server.port` or CLI `--port` enables the optional Phoenix LiveView dashboard and JSON API at
  `/`, `/api/v1/state`, `/api/v1/<issue_identifier>`, and `/api/v1/refresh`. The existing loopback
  service also exposes `POST /api/v1/<issue_identifier>/stop` and `/resume`; these control endpoints
  reject non-loopback callers and require the `X-Symphony-Control-Token` header to match the
  `SYMPHONY_CONTROL_TOKEN` environment secret.

### Hardened local runner capability profile

Unattended macOS runners can opt into a fail-closed startup preflight. The preflight completes
before workspace cleanup, tracker polling, or issue claims, and verifies that nested Codex review
and a loopback Camofox browser backend actually work from the configured source repository.

```yaml
codex:
  command: >-
    "${SYMPHONY_CODEX_BIN:-codex}" --config shell_environment_policy.inherit=none
    --config "shell_environment_policy.set.SYMPHONY_CODEX_BIN=\"${SYMPHONY_CODEX_BIN:-codex}\""
    --config "shell_environment_policy.set.SYMPHONY_REAL_CODEX_BIN=\"${SYMPHONY_REAL_CODEX_BIN}\""
    --config "shell_environment_policy.set.SYMPHONY_REVIEW_CODEX_HOME=\"${SYMPHONY_REVIEW_CODEX_HOME}\""
    --config "shell_environment_policy.set.SYMPHONY_BROWSER_BACKEND_URL=\"${SYMPHONY_BROWSER_BACKEND_URL}\""
    app-server
runner:
  capability_preflight: true
  source_repo: $CARECORE_SOURCE_REPO
  reviewer_codex_home: $SYMPHONY_REVIEW_CODEX_HOME
  primary_codex_home: $CODEX_HOME
  required_skills:
    - code-simplifier
    - security-best-practices
  sandbox_codex_bin: /Applications/ChatGPT.app/Contents/Resources/codex
  browser_backend: camofox
  browser_backend_url: $SYMPHONY_BROWSER_BACKEND_URL
  review_timeout_ms: 20000
  browser_timeout_ms: 15000
  process_cleanup_timeout_ms: 2000
```

Runner contract:

- The profile currently supports local workers only. Startup fails when `worker.ssh_hosts` is also
  configured.
- `source_repo` must be absolute. For CareCore, set it to `$CARECORE_SOURCE_REPO`; the nested review
  canary runs with that repository as its working directory.
- `reviewer_codex_home` must be absolute and its canonical parent must already be owned by the
  current user with mode `700`. Symphony creates or validates the reviewer home and its `tmp`
  directory at mode `700`, and rejects reviewer/primary homes that overlap after macOS-aware path
  normalization.
- The primary `auth.json` must be an owner-only regular file at mode `600` or `400`. Reviewer auth
  symlinks are rejected. Authentication is copied through a mode-`600` temporary file and atomic
  rename; credential contents are never logged.
- Any pre-existing custom `SYMPHONY_CODEX_BIN` is treated as the real binary for compatibility,
  saved as `SYMPHONY_REAL_CODEX_BIN`, and replaced with Symphony's isolation wrapper. App-server
  uses the configured primary Codex home; nested `review`/`exec` commands use the private reviewer
  home. With shell inheritance disabled, all four `shell_environment_policy.set` entries shown
  above are required so nested shells keep using the wrapper and explicit browser backend.
- `browser_backend: camofox` is explicit and accepts loopback HTTP(S) URLs only. The preflight
  requires `GET /health`, creates a tab with `POST /tabs`, requires a PNG from
  `GET /tabs/:id/screenshot`, and always deletes the tab and canary session.
- Local app-server commands run in their own process group. Normal shutdown, startup failure, turn
  timeout, and orchestrator stall restart terminate the recorded process tree with a bounded
  shared TERM-to-KILL cleanup window controlled by `process_cleanup_timeout_ms` (maximum 4000ms).
  The process group uses Perl on macOS or `setsid` on Linux. Remote cleanup uses the same deadline,
  kills its SSH subprocess on expiry, and surfaces timeouts or non-zero exits; Symphony will not
  report a successful hold when the remote run could still be active.

Set `SYMPHONY_REAL_CODEX_BIN` directly for new installations. The legacy normalization exists so an
older `SYMPHONY_CODEX_BIN=/custom/path/codex` configuration cannot bypass the wrapper.

## Web dashboard

The observability UI now runs on a minimal Phoenix stack:

- LiveView for the dashboard at `/`
- JSON API for operational debugging under `/api/v1/*`
- Bandit as the HTTP server
- Phoenix dependency static assets for the LiveView client bootstrap

Operational control endpoints:

- `POST /api/v1/<issue_identifier>/stop` waits until a known running issue has left `running`, then
  returns its hold details. It also converts a known queued retry into a hold. The workspace is
  preserved. Remote cleanup timeout/failure returns `503 cleanup_failed`; the durable hold remains
  active with `cleanup_pending: true` and cannot be released by tracker changes.
- `POST /api/v1/<issue_identifier>/resume` retries any pending cleanup from stored process proof,
  clears the hold only after cleanup is confirmed, and queues an immediate poll.
- Set a non-empty `SYMPHONY_CONTROL_TOKEN` environment secret before using either endpoint and send
  it in `X-Symphony-Control-Token`. Missing configuration returns `503`; a missing or invalid token
  returns `401`. Unknown issue identifiers return `404`. Both endpoints are loopback-only and never
  mutate the tracker.

## Project Layout

- `lib/`: application code and Mix tasks
- `test/`: ExUnit coverage for runtime behavior
- `WORKFLOW.md`: in-repo workflow contract used by local runs
- `../.codex/`: repository-local Codex skills and setup helpers

## Testing

```bash
make all
```

Run the real external end-to-end test only when you want Symphony to create disposable Linear
resources and launch a real `codex app-server` session:

```bash
cd elixir
export LINEAR_API_KEY=...
make e2e
```

Optional environment variables:

- `SYMPHONY_LIVE_LINEAR_TEAM_KEY` defaults to `SYME2E`
- `SYMPHONY_LIVE_SSH_WORKER_HOSTS` uses those SSH hosts when set, as a comma-separated list

`make e2e` runs two live scenarios:
- one with a local worker
- one with SSH workers

If `SYMPHONY_LIVE_SSH_WORKER_HOSTS` is unset, the SSH scenario uses `docker compose` to start two
disposable SSH workers on `localhost:<port>`. The live test generates a temporary SSH keypair,
mounts the host `~/.codex/auth.json` into each worker, verifies that Symphony can talk to them
over real SSH, then runs the same orchestration flow against those worker addresses. This keeps
the transport representative without depending on long-lived external machines.

Set `SYMPHONY_LIVE_SSH_WORKER_HOSTS` if you want `make e2e` to target real SSH hosts instead.

The live test creates a temporary Linear project and issue, writes a temporary `WORKFLOW.md`, runs
a real agent turn, verifies the workspace side effect, requires Codex to comment on and close the
Linear issue, then marks the project completed so the run remains visible in Linear.

## FAQ

### Why Elixir?

Elixir is built on Erlang/BEAM/OTP, which is great for supervising long-running processes. It has an
active ecosystem of tools and libraries. It also supports hot code reloading without stopping
actively running subagents, which is very useful during development.

### What's the easiest way to set this up for my own codebase?

Launch `codex` in your repo, give it the URL to the Symphony repo, and ask it to set things up for
you.

## License

This project is licensed under the [Apache License 2.0](../LICENSE).
