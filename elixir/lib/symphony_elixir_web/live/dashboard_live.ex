defmodule SymphonyElixirWeb.DashboardLive do
  @moduledoc """
  Live command center and run inspector for Symphony observability.
  """

  use Phoenix.LiveView, layout: {SymphonyElixirWeb.Layouts, :app}

  alias SymphonyElixir.Config
  alias SymphonyElixirWeb.{Endpoint, ObservabilityPubSub, Presenter}
  @runtime_tick_ms 1_000

  @impl true
  def mount(params, _session, socket) do
    socket =
      socket
      |> assign(:payload, load_payload())
      |> assign(:now, DateTime.utc_now())
      |> assign(:selected_issue_identifier, nil)
      |> assign(:selected_run, nil)
      |> assign(:selected_runtime, nil)
      |> assign_selected_run(params)

    if connected?(socket) do
      :ok = ObservabilityPubSub.subscribe()
      schedule_runtime_tick()
    end

    {:ok, socket}
  end

  @impl true
  def handle_params(params, _uri, socket) do
    {:noreply, assign_selected_run(socket, params)}
  end

  @impl true
  def handle_info(:runtime_tick, socket) do
    schedule_runtime_tick()
    {:noreply, assign(socket, :now, DateTime.utc_now())}
  end

  @impl true
  def handle_info(:observability_updated, socket) do
    {:noreply,
     socket
     |> assign(:payload, load_payload())
     |> assign(:now, DateTime.utc_now())
     |> reload_selected_run()}
  end

  @impl true
  def render(assigns) do
    ~H"""
    <section class="dashboard-shell dark-shell">
      <header class="hero-card hero-card-dark">
        <div class="hero-meta-strip mono">
          <span>symphony · observability</span>
          <span>running <%= @payload[:error] && 0 || @payload.counts.running %></span>
          <span>retrying <%= @payload[:error] && 0 || @payload.counts.retrying %></span>
          <span>recent <%= @payload[:error] && 0 || length(@payload.recent_outcomes || []) %></span>
          <span>generated <%= @payload.generated_at || "n/a" %></span>
        </div>
        <div class="hero-grid">
          <div>
            <p class="eyebrow">Symphony</p>
            <h1 class="hero-title">
              <%= if @live_action == :run, do: "Run Inspector", else: "Operations Dashboard" %>
            </h1>
            <p class="hero-copy">
              <%= if @live_action == :run do %>
                Structured Codex event timeline, health, token burn, and runtime metadata for a single run.
              <% else %>
                Command center for active jobs, retry pressure, token burn, and drill-down into live Codex activity. Click an issue title to open it in Linear, or Inspect for the full run page.
              <% end %>
            </p>
          </div>

          <div class="status-stack">
            <span class="status-badge status-badge-live">
              <span class="status-badge-dot"></span>
              Live
            </span>
            <span class="status-badge status-badge-offline">
              <span class="status-badge-dot"></span>
              Offline
            </span>
          </div>
        </div>
      </header>

      <%= if @payload[:error] do %>
        <section class="error-card">
          <h2 class="error-title">Snapshot unavailable</h2>
          <p class="error-copy">
            <strong><%= @payload.error.code %>:</strong> <%= @payload.error.message %>
          </p>
        </section>
      <% else %>
        <%= if @live_action == :run do %>
          <%= if @selected_run do %>
            <section class="inspector-layout">
              <div class="inspector-main-stack">
                <section class="section-card section-card-dark inspector-header-card">
                  <div class="section-header inspector-header-grid">
                    <div>
                      <p class="eyebrow">Run Inspector</p>
                      <h2 class="section-title inspector-title">
                        <span class="issue-id"><%= @selected_run.issue_identifier %></span>
                        <span class="muted">·</span>
                        <span><%= @selected_run.title || "Untitled run" %></span>
                      </h2>
                      <p class="section-copy">
                        Health, transcript, and runtime details for the currently selected run.
                      </p>
                    </div>

                    <div class="inspector-actions">
                      <button :if={resume_command(@selected_run, @selected_runtime)} id="copy-runtime-resume-command" type="button" class="subtle-button subtle-button-primary" phx-hook="ClipboardCopy" data-copy-text={resume_command(@selected_run, @selected_runtime)} data-copy-label="Copy Resume Cmd">
                        Copy Resume Cmd
                      </button>
                      <%= if @selected_run.issue_url do %>
                        <a class="subtle-button subtle-button-ghost" href={@selected_run.issue_url}>Open in Linear</a>
                      <% end %>
                      <a class="subtle-button" href="/">Back to dashboard</a>
                      <a class="subtle-button" href={"/api/v1/runs/#{@selected_run.issue_identifier}/events"}>Events JSON</a>
                    </div>
                  </div>
                </section>

                <section class="section-card section-card-dark">
                  <div class="section-header">
                    <div>
                      <h2 class="section-title">Latest Assistant Update</h2>
                      <p class="section-copy">Most recent clean assistant message, surfaced first so you do not need to hunt through the transcript.</p>
                    </div>
                  </div>

                  <%= if latest_assistant_block(@selected_run.operator_transcript || []) do %>
                    <% latest_block = latest_assistant_block(@selected_run.operator_transcript || []) %>
                    <article class="timeline-event-card transcript-block transcript-block-assistant transcript-highlight-card">
                      <div class="timeline-event-meta transcript-block-meta">
                        <span class={transcript_badge_class(latest_block.kind)}><%= latest_block.title %></span>
                        <span class="muted"><%= latest_block.timestamp || "n/a" %></span>
                        <button :if={latest_block.body} id={copy_button_dom_id("latest-copy", details_state_key(latest_block))} type="button" class="copy-button" phx-hook="ClipboardCopy" data-copy-text={latest_block.body} data-copy-label="Copy">
                          Copy
                        </button>
                      </div>
                      <div class="timeline-event-body">
                        <pre class={transcript_body_class(latest_block.kind)}><%= latest_block.body %></pre>
                        <%= if latest_block.raw do %>
                          <details id={details_dom_id("latest-raw-details", details_state_key(latest_block))} class="raw-details" phx-hook="PersistDetailsState" data-details-key={"latest-raw:#{details_state_key(latest_block)}"}>
                            <summary>Raw payload</summary>
                            <pre class="code-panel"><%= latest_block.raw %></pre>
                          </details>
                        <% end %>
                      </div>
                    </article>
                  <% else %>
                    <p class="empty-state">No assistant summary retained yet.</p>
                  <% end %>
                </section>

                <section class="section-card section-card-dark">
                  <div class="section-header">
                    <div>
                      <h2 class="section-title">Operator Transcript</h2>
                      <p class="section-copy">Codex-style run narrative synthesized from retained events. Newest blocks are shown first. Raw payloads stay available per block.</p>
                    </div>
                  </div>

                  <%= if @selected_run.operator_transcript == [] do %>
                    <p class="empty-state">No human-readable transcript blocks retained yet. Use Events JSON for the raw event firehose.</p>
                  <% else %>
                    <div class="timeline-list operator-transcript-list">
                      <%= for block <- Enum.reverse(@selected_run.operator_transcript) do %>
                        <article class={transcript_block_class(block.kind)}>
                          <div class="timeline-event-meta transcript-block-meta">
                            <span class={transcript_badge_class(block.kind)}><%= block.title %></span>
                            <%= if block[:meta] do %>
                              <span class="mono muted"><%= block.meta %></span>
                            <% end %>
                            <span class="muted"><%= block.timestamp || "n/a" %></span>
                            <button :if={block.body} id={copy_button_dom_id("transcript-copy", details_state_key(block))} type="button" class="copy-button" phx-hook="ClipboardCopy" data-copy-text={block.body} data-copy-label="Copy">
                              Copy
                            </button>
                          </div>
                          <div class="timeline-event-body">
                            <pre class={transcript_body_class(block.kind)}><%= block.body %></pre>
                            <%= if block.raw do %>
                              <details id={details_dom_id("raw-details", details_state_key(block))} class="raw-details" phx-hook="PersistDetailsState" data-details-key={"raw:#{details_state_key(block)}"}>
                                <summary>Raw payload</summary>
                                <pre class="code-panel"><%= block.raw %></pre>
                              </details>
                            <% end %>
                          </div>
                        </article>
                      <% end %>
                    </div>
                  <% end %>
                </section>

                <section class="section-card section-card-dark">
                  <div class="section-header">
                    <div>
                      <h2 class="section-title">Recent Event Feed</h2>
                      <p class="section-copy">Structured retained event evidence for this run. Newest events are shown first. Use the raw payload toggle when the transcript summary is too lossy.</p>
                    </div>
                  </div>

                  <%= if @selected_run.recent_events == [] do %>
                    <p class="empty-state">No recent events retained for this run yet.</p>
                  <% else %>
                    <div class="timeline-list event-feed-list">
                      <%= for event <- Enum.reverse(@selected_run.recent_events) do %>
                        <article class="timeline-event-card event-feed-card">
                          <div class="timeline-event-meta">
                            <span class={event_badge_class(event_value(event, :category))}><%= event_value(event, :category) || event_value(event, :event) || "event" %></span>
                            <span class="mono muted"><%= event_value(event, :method) || event_value(event, :event) || "n/a" %></span>
                            <span class="muted"><%= event_value(event, :at) || "n/a" %></span>
                            <button :if={event_copy_text(event)} id={copy_button_dom_id("event-copy", event_details_state_key(event))} type="button" class="copy-button" phx-hook="ClipboardCopy" data-copy-text={event_copy_text(event)} data-copy-label="Copy">
                              Copy
                            </button>
                          </div>
                          <p class="timeline-event-summary"><%= event_value(event, :summary) || event_value(event, :message) || "No event summary retained" %></p>
                          <%= if event_value(event, :turn_id) || event_value(event, :item_id) do %>
                            <p class="timeline-event-detail mono muted">
                              <%= if event_value(event, :turn_id) do %><span>turn <%= event_value(event, :turn_id) %></span><% end %>
                              <%= if event_value(event, :turn_id) && event_value(event, :item_id) do %><span> · </span><% end %>
                              <%= if event_value(event, :item_id) do %><span>item <%= event_value(event, :item_id) %></span><% end %>
                            </p>
                          <% end %>
                          <%= if event_value(event, :raw) do %>
                            <details id={details_dom_id("event-raw-details", event_details_state_key(event))} class="raw-details" phx-hook="PersistDetailsState" data-details-key={"event-raw:#{event_details_state_key(event)}"}>
                              <summary>Raw payload</summary>
                              <pre class="code-panel"><%= event_value(event, :raw) %></pre>
                            </details>
                          <% end %>
                        </article>
                      <% end %>
                    </div>
                  <% end %>
                </section>
              </div>

              <aside class="inspector-sidebar">
                <section class="section-card section-card-dark">
                  <div class="section-header compact-header">
                    <div>
                      <h2 class="section-title">Health</h2>
                    </div>
                  </div>

                  <div class="stat-list">
                    <% {selected_health, selected_health_reason} = live_health(@selected_runtime, @now) %>
                    <div class="stat-row">
                      <span class="muted">State</span>
                      <span class={state_badge_class(@selected_runtime.state)}><%= @selected_runtime.state %></span>
                    </div>
                    <div class="stat-row">
                      <span class="muted">Health</span>
                      <span class={health_badge_class(selected_health)}><%= selected_health %></span>
                    </div>
                    <div class="stat-row">
                      <span class="muted">Reason</span>
                      <span><%= selected_health_reason %></span>
                    </div>
                    <div class="stat-row">
                      <span class="muted">Runtime</span>
                      <span class="numeric"><%= format_runtime_seconds(runtime_seconds_for_display(@selected_runtime, @now)) %></span>
                    </div>
                    <div class="stat-row">
                      <span class="muted">Last activity</span>
                      <span class="numeric"><%= format_last_activity(live_last_activity_seconds(@selected_runtime, @now)) %></span>
                    </div>
                  </div>
                </section>

                <section class="section-card section-card-dark">
                  <div class="section-header compact-header">
                    <div>
                      <h2 class="section-title">Tokens</h2>
                    </div>
                  </div>

                  <div class="stat-list">
                    <div class="stat-row"><span class="muted">Input</span><span class="numeric"><%= format_int(@selected_runtime.tokens.input_tokens) %></span></div>
                    <div class="stat-row"><span class="muted">Output</span><span class="numeric"><%= format_int(@selected_runtime.tokens.output_tokens) %></span></div>
                    <div class="stat-row"><span class="muted">Total</span><span class="numeric"><%= format_int(@selected_runtime.tokens.total_tokens) %></span></div>
                    <div class="stat-row"><span class="muted">Burn</span><span class="numeric"><%= format_burn_rate(@selected_runtime.burn_rate_tokens_per_min) %></span></div>
                  </div>
                </section>

                <section class="section-card section-card-dark">
                  <div class="section-header compact-header">
                    <div>
                      <h2 class="section-title">Runtime Identity</h2>
                    </div>
                  </div>

                  <div class="stat-list mono">
                    <.copyable_stat_row id="copy-runtime-session" label="Session" value={@selected_runtime.session_id || "n/a"} />
                    <.copyable_stat_row id="copy-runtime-thread" label="Thread" value={@selected_runtime.thread_id || "n/a"} />
                    <.copyable_stat_row id="copy-runtime-turn" label="Turn" value={@selected_runtime.current_turn_id || "n/a"} />
                    <.copyable_stat_row id="copy-runtime-worker" label="Worker" value={@selected_runtime.worker_host || "local"} />
                    <.copyable_stat_row id="copy-runtime-workspace" label="Workspace" value={@selected_run.workspace.path || "n/a"} />
                    <.copyable_stat_row id="copy-runtime-trace-command" label="Trace logs" value={debug_trace_command(@selected_run, @selected_runtime)} copy_label="Copy Cmd" />
                  </div>
                </section>
              </aside>
            </section>
          <% else %>
            <section class="error-card">
              <h2 class="error-title">Run unavailable</h2>
              <p class="error-copy">The requested run is not currently available in the active runtime snapshot.</p>
            </section>
          <% end %>
        <% else %>
          <section class="metric-grid">
            <article class="metric-card metric-card-dark">
              <p class="metric-label">Running</p>
              <p class="metric-value numeric"><%= @payload.counts.running %></p>
              <p class="metric-detail">Active issue sessions in the current runtime.</p>
            </article>

            <article class="metric-card metric-card-dark">
              <p class="metric-label">Retrying</p>
              <p class="metric-value numeric"><%= @payload.counts.retrying %></p>
              <p class="metric-detail">Issues waiting for the next retry window.</p>
            </article>

            <article class="metric-card metric-card-dark">
              <p class="metric-label">Total tokens</p>
              <p class="metric-value numeric"><%= format_int(@payload.codex_totals.total_tokens) %></p>
              <p class="metric-detail numeric">
                In <%= format_int(@payload.codex_totals.input_tokens) %> / Out <%= format_int(@payload.codex_totals.output_tokens) %>
              </p>
            </article>

            <article class="metric-card metric-card-dark">
              <p class="metric-label">Runtime</p>
              <p class="metric-value numeric"><%= format_runtime_seconds(total_runtime_seconds(@payload, @now)) %></p>
              <p class="metric-detail">Total Codex runtime across completed and active sessions.</p>
            </article>
          </section>

          <%= if @payload.alerts != [] do %>
            <section class="section-card section-card-dark alerts-card">
              <div class="section-header">
                <div>
                  <h2 class="section-title">Needs Attention</h2>
                  <p class="section-copy">Runs requiring operator attention.</p>
                </div>
              </div>

              <div class="alerts-list">
                <%= for alert <- @payload.alerts do %>
                  <div class={"alert-pill alert-pill-" <> alert.severity}>
                    <span class="issue-id"><%= alert.issue_identifier %></span>
                    <span><%= alert.message %></span>
                  </div>
                <% end %>
              </div>
            </section>
          <% end %>

          <section class="section-card section-card-dark">
            <div class="section-header">
              <div>
                <h2 class="section-title">Active Runs</h2>
                <p class="section-copy">Multi-job command center. Click an issue title to open it in Linear, or Inspect for the full run page.</p>
              </div>
            </div>

            <%= if @payload.running == [] do %>
              <p class="empty-state">No active sessions.</p>
            <% else %>
              <div class="table-wrap">
                <table class="data-table data-table-running command-center-table">
                  <thead>
                    <tr>
                      <th>Issue</th>
                      <th>State</th>
                      <th>Health</th>
                      <th>Phase</th>
                      <th>Last activity</th>
                      <th>Tokens</th>
                      <th>Burn</th>
                      <th>Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr :for={entry <- @payload.running} class={command_center_row_class(entry, @selected_issue_identifier)}>
                      <% {entry_health, entry_health_reason} = live_health(entry, @now) %>
                      <td>
                        <div class="issue-stack">
                          <a class="issue-link-primary" href={entry.issue_url || "/?inspect=#{entry.issue_identifier}"}>
                            <span class="issue-id"><%= entry.issue_identifier %></span>
                            <span class="issue-title"><%= entry.title || "Untitled run" %></span>
                          </a>
                          <span class="muted mono"><%= entry.thread_id || entry.session_id || "n/a" %></span>
                        </div>
                      </td>
                      <td><span class={state_badge_class(entry.state)}><%= entry.state %></span></td>
                      <td>
                        <div class="detail-stack">
                          <span class={health_badge_class(entry_health)}><%= entry_health %></span>
                          <span class="muted event-meta"><%= entry_health_reason %></span>
                        </div>
                      </td>
                      <td>
                        <div class="detail-stack">
                          <span class="event-text"><%= progress_headline(entry) %></span>
                          <span class="muted event-meta"><%= progress_detail(entry) %></span>
                        </div>
                      </td>
                      <td class="numeric"><%= format_last_activity(live_last_activity_seconds(entry, @now)) %></td>
                      <td>
                        <div class="token-stack numeric">
                          <span>Total: <%= format_int(entry.tokens.total_tokens) %></span>
                          <span class="muted">In <%= format_int(entry.tokens.input_tokens) %> / Out <%= format_int(entry.tokens.output_tokens) %></span>
                        </div>
                      </td>
                      <td class="numeric"><%= format_burn_rate(entry.burn_rate_tokens_per_min) %></td>
                      <td>
                        <div class="action-stack">
                          <a class="subtle-button subtle-button-primary" href={"/runs/#{entry.issue_identifier}"}>Inspect</a>
                        </div>
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            <% end %>
          </section>

          <section class="section-card section-card-dark">
            <div class="section-header">
              <div>
                <h2 class="section-title">Retry Queue</h2>
                <p class="section-copy">Issues waiting for the next retry window.</p>
              </div>
            </div>

            <%= if @payload.retrying == [] do %>
              <p class="empty-state">No issues are currently backing off.</p>
            <% else %>
              <div class="table-wrap">
                <table class="data-table command-center-table">
                  <thead>
                    <tr>
                      <th>Issue</th>
                      <th>Attempt</th>
                      <th>Due at</th>
                      <th>Error</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr :for={entry <- @payload.retrying}>
                      <td><span class="issue-id"><%= entry.issue_identifier %></span></td>
                      <td><%= entry.attempt %></td>
                      <td class="mono"><%= entry.due_at || "n/a" %></td>
                      <td><%= entry.error || "n/a" %></td>
                    </tr>
                  </tbody>
                </table>
              </div>
            <% end %>
          </section>

          <section class="section-card section-card-dark">
            <div class="section-header">
              <div>
                <h2 class="section-title">Recent Outcomes</h2>
                <p class="section-copy">Recently finished runs so the board still has a pulse when nothing is actively running.</p>
              </div>
            </div>

            <%= if @payload.recent_outcomes == [] do %>
              <p class="empty-state">No recent outcomes yet.</p>
            <% else %>
              <div class="table-wrap">
                <table class="data-table command-center-table">
                  <thead>
                    <tr>
                      <th>Issue</th>
                      <th>Outcome</th>
                      <th>Runtime</th>
                      <th>Tokens</th>
                      <th>Finished</th>
                      <th>Last event</th>
                      <th>Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr :for={entry <- @payload.recent_outcomes}>
                      <td>
                        <div class="issue-stack">
                          <a class="issue-link-primary" href={entry.issue_url || "/?inspect=#{entry.issue_identifier}"}>
                            <span class="issue-id"><%= entry.issue_identifier %></span>
                            <span class="issue-title"><%= entry.title || "Untitled run" %></span>
                          </a>
                        </div>
                      </td>
                      <td><span class={health_badge_class(recent_outcome_health(entry.status || entry.outcome))}><%= entry.state || entry.status || entry.outcome %></span></td>
                      <td class="numeric"><%= format_runtime_seconds(entry.runtime_seconds || 0) %></td>
                      <td class="numeric"><%= format_int(get_in(entry, [:tokens, :total_tokens]) || get_in(entry, ["tokens", "total_tokens"]) || 0) %></td>
                      <td class="mono"><%= entry.finished_at || "n/a" %></td>
                      <td>
                        <%= if entry.display_message_expandable do %>
                          <details id={details_dom_id("recent-outcome-preview", entry.issue_identifier)} class="recent-outcome-preview" phx-hook="PersistDetailsState" data-details-key={"recent-outcome:#{entry.issue_identifier}"}>
                            <summary>
                              <span><%= entry.display_message_preview %></span>
                              <span class="recent-outcome-preview-toggle">Show more</span>
                            </summary>
                            <div class="recent-outcome-preview-full"><%= entry.display_message %></div>
                          </details>
                        <% else %>
                          <span><%= entry.display_message_preview || entry.display_message || entry.last_message || entry.last_event || "n/a" %></span>
                        <% end %>
                      </td>
                      <td>
                        <div class="action-stack">
                          <a class="subtle-button subtle-button-primary" href={"/runs/#{entry.issue_identifier}"}>Inspect</a>
                        </div>
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            <% end %>
          </section>

          <%= if @selected_run do %>
            <section class="section-card section-card-dark selected-run-panel">
              <div class="section-header">
                <div>
                  <p class="eyebrow">Quick view</p>
                  <h2 class="section-title">Selected Run — <%= @selected_run.issue_identifier %></h2>
                  <p class="section-copy">A fast in-place peek at the selected run. Use Inspect for the full dedicated replay booth.</p>
                </div>
                <div class="action-stack">
                  <a class="subtle-button subtle-button-primary" href={"/runs/#{@selected_run.issue_identifier}"}>Inspect</a>
                </div>
              </div>

              <div class="timeline-list compact-timeline">
                <%= for block <- Enum.take(@selected_run.operator_transcript || [], 8) do %>
                  <article class="timeline-event-card compact-card transcript-compact-card">
                    <div class="timeline-event-meta transcript-block-meta">
                      <span class={transcript_badge_class(block.kind)}><%= block.title %></span>
                    </div>
                    <pre class={compact_transcript_body_class(block.kind)}><%= block.body %></pre>
                  </article>
                <% end %>
              </div>

              <%= if @selected_runtime do %>
                <div class="quick-view-meta">
                  <% {selected_health, selected_health_reason} = live_health(@selected_runtime, @now) %>
                  <div class="quick-view-stat">
                    <span class="metric-label">Health</span>
                    <span class={health_badge_class(selected_health)}><%= selected_health %></span>
                    <span class="muted event-meta"><%= selected_health_reason %></span>
                  </div>
                  <div class="quick-view-stat">
                    <span class="metric-label">Current phase</span>
                    <span><%= progress_headline(@selected_runtime) %></span>
                    <span class="muted event-meta"><%= progress_detail(@selected_runtime) %></span>
                  </div>
                  <div class="quick-view-stat">
                    <span class="metric-label">Tokens</span>
                    <span class="numeric"><%= format_int(@selected_runtime.tokens.total_tokens) %></span>
                    <span class="muted event-meta"><%= format_burn_rate(@selected_runtime.burn_rate_tokens_per_min) %></span>
                  </div>
                </div>
              <% end %>

              <div class="event-inline-list quick-view-events">
                <%= for event <- Enum.take(@selected_run.recent_events || [], -4) do %>
                  <article class="event-inline-card">
                    <div class="timeline-event-meta">
                      <span class={event_badge_class(event_value(event, :category))}><%= event_value(event, :category) || event_value(event, :event) || "event" %></span>
                      <span class="mono muted"><%= event_value(event, :method) || event_value(event, :event) || "n/a" %></span>
                      <span class="muted"><%= event_value(event, :at) || "n/a" %></span>
                    </div>
                    <p class="timeline-event-summary"><%= event_value(event, :summary) || event_value(event, :message) || "No event summary retained" %></p>
                    <%= if event_value(event, :raw) do %>
                      <details id={details_dom_id("quick-view-raw-details", event_details_state_key(event))} class="raw-details" phx-hook="PersistDetailsState" data-details-key={"quick-view-raw:#{event_details_state_key(event)}"}>
                        <summary>Raw payload</summary>
                        <pre class="code-panel"><%= event_value(event, :raw) %></pre>
                      </details>
                    <% end %>
                  </article>
                <% end %>
              </div>
            </section>
          <% end %>
        <% end %>
      <% end %>
    </section>
    """
  end

  defp assign_selected_run(socket, params) do
    issue_identifier = selected_issue_identifier(params, socket.assigns.live_action)
    selected_run = load_selected_run(issue_identifier)

    socket
    |> assign(:selected_issue_identifier, issue_identifier)
    |> assign(:selected_run, selected_run)
    |> assign(:selected_runtime, selected_run_runtime(selected_run))
  end

  defp reload_selected_run(%{assigns: %{selected_issue_identifier: issue_identifier}} = socket) do
    selected_run = load_selected_run(issue_identifier)

    socket
    |> assign(:selected_run, selected_run)
    |> assign(:selected_runtime, selected_run_runtime(selected_run))
  end

  defp load_selected_run(nil), do: nil

  defp load_selected_run(issue_identifier) when is_binary(issue_identifier) do
    case Presenter.run_payload(issue_identifier, orchestrator(), snapshot_timeout_ms()) do
      {:ok, payload} -> payload
      {:error, _reason} -> nil
    end
  end

  defp selected_run_runtime(nil), do: nil

  defp selected_run_runtime(%{running: %{} = running}), do: running
  defp selected_run_runtime(%{recent_outcome: %{} = recent_outcome}), do: recent_outcome

  defp selected_run_runtime(%{retry: %{} = retry}) do
    due_at = Map.get(retry, :due_at)

    %{
      state: "Retrying",
      health: "warning",
      health_reason: if(is_binary(due_at), do: "Retry attempt #{retry.attempt} queued until #{due_at}", else: retry.error || "Retry queued"),
      started_at: nil,
      last_activity_seconds: nil,
      burn_rate_tokens_per_min: 0.0,
      session_id: nil,
      thread_id: nil,
      current_turn_id: nil,
      worker_host: retry.worker_host,
      tokens: %{input_tokens: 0, output_tokens: 0, total_tokens: 0}
    }
  end

  defp selected_run_runtime(_run), do: nil

  defp selected_issue_identifier(params, :run) when is_map(params), do: params["issue_identifier"]
  defp selected_issue_identifier(params, _live_action) when is_map(params), do: params["inspect"]
  defp selected_issue_identifier(_params, _live_action), do: nil

  defp load_payload do
    Presenter.state_payload(orchestrator(), snapshot_timeout_ms())
  end

  defp orchestrator do
    Endpoint.config(:orchestrator) || SymphonyElixir.Orchestrator
  end

  defp progress_headline(entry) do
    entry.progress_phase || entry.last_message || to_string(entry.last_event || "n/a")
  end

  defp progress_detail(entry) do
    entry.progress_detail || entry.last_message || "No detailed progress yet"
  end

  defp snapshot_timeout_ms do
    Endpoint.config(:snapshot_timeout_ms) || 15_000
  end

  defp completed_runtime_seconds(payload) do
    payload.codex_totals.seconds_running || 0
  end

  defp total_runtime_seconds(payload, now) do
    completed_runtime_seconds(payload) +
      Enum.reduce(payload.running, 0, fn entry, total ->
        total + runtime_seconds_from_started_at(entry.started_at, now)
      end)
  end

  defp format_runtime_seconds(seconds) when is_number(seconds) do
    whole_seconds = max(trunc(seconds), 0)
    mins = div(whole_seconds, 60)
    secs = rem(whole_seconds, 60)
    "#{mins}m #{secs}s"
  end

  defp runtime_seconds_for_display(entry, %DateTime{} = now) when is_map(entry) do
    case entry[:runtime_seconds] do
      seconds when is_number(seconds) -> seconds
      _ -> runtime_seconds_from_started_at(entry.started_at, now)
    end
  end

  defp runtime_seconds_for_display(_entry, _now), do: 0

  defp runtime_seconds_from_started_at(%DateTime{} = started_at, %DateTime{} = now) do
    DateTime.diff(now, started_at, :second)
  end

  defp runtime_seconds_from_started_at(started_at, %DateTime{} = now) when is_binary(started_at) do
    case DateTime.from_iso8601(started_at) do
      {:ok, parsed, _offset} -> runtime_seconds_from_started_at(parsed, now)
      _ -> 0
    end
  end

  defp runtime_seconds_from_started_at(_started_at, _now), do: 0

  defp format_int(value) when is_integer(value) do
    value
    |> Integer.to_string()
    |> String.reverse()
    |> String.replace(~r/.{3}(?=.)/, "\\0,")
    |> String.reverse()
  end

  defp format_int(_value), do: "n/a"

  defp format_last_activity(seconds) when is_integer(seconds), do: "#{seconds}s ago"
  defp format_last_activity(_seconds), do: "n/a"

  defp format_burn_rate(value) when is_number(value), do: "#{Float.round(value, 2)} tok/min"
  defp format_burn_rate(_value), do: "n/a"

  defp event_value(event, key) when is_map(event) and is_atom(key) do
    Map.get(event, key) || Map.get(event, Atom.to_string(key))
  end

  defp event_value(_event, _key), do: nil

  defp command_center_row_class(entry, selected_issue_identifier) when is_map(entry) do
    if entry.issue_identifier == selected_issue_identifier do
      "command-center-row-selected"
    else
      nil
    end
  end

  defp command_center_row_class(_entry, _selected_issue_identifier), do: nil

  defp live_health(%{health: health, health_reason: reason}, _now)
       when is_binary(health) and is_binary(reason),
       do: {health, reason}

  defp live_health(entry, %DateTime{} = now) when is_map(entry) do
    idle_seconds = live_last_activity_seconds(entry, now)
    timeout_seconds = max(div(Config.settings!().codex.stall_timeout_ms, 1_000), 1)
    last_event = entry[:last_event] |> to_string()

    cond do
      last_event == "approval_required" and idle_seconds >= timeout_seconds ->
        {"stalled", "Waiting on approval for #{idle_seconds}s"}

      last_event == "turn_input_required" and idle_seconds >= timeout_seconds ->
        {"stalled", "Waiting on input for #{idle_seconds}s"}

      last_event == "approval_required" ->
        {"waiting_approval", "Waiting on approval"}

      last_event == "turn_input_required" ->
        {"waiting_input", "Waiting on non-interactive input"}

      idle_seconds >= timeout_seconds ->
        {"stalled", "No Codex activity for #{idle_seconds}s"}

      idle_seconds >= max(div(timeout_seconds, 2), 1) ->
        {"warning", "No Codex activity for #{idle_seconds}s"}

      true ->
        {"healthy", "Receiving Codex activity"}
    end
  end

  defp live_health(_entry, _now), do: {"healthy", "Receiving Codex activity"}

  defp live_last_activity_seconds(entry, %DateTime{} = now) when is_map(entry) do
    entry
    |> last_activity_reference()
    |> runtime_seconds_from_started_at(now)
  end

  defp live_last_activity_seconds(_entry, _now), do: 0

  defp last_activity_reference(entry) when is_map(entry) do
    entry[:last_event_at] || entry[:finished_at] || entry[:started_at]
  end

  defp last_activity_reference(_entry), do: nil

  defp state_badge_class(state) do
    base = "state-badge"
    normalized = state |> to_string() |> String.downcase()

    cond do
      String.contains?(normalized, ["progress", "running", "active"]) -> "#{base} state-badge-active"
      String.contains?(normalized, ["blocked", "error", "failed"]) -> "#{base} state-badge-danger"
      String.contains?(normalized, ["todo", "queued", "pending", "retry"]) -> "#{base} state-badge-warning"
      true -> base
    end
  end

  defp health_badge_class(health) do
    base = "state-badge"

    case to_string(health) do
      "healthy" -> "#{base} state-badge-active"
      value when value in ["warning", "waiting_approval", "waiting_input"] -> "#{base} state-badge-warning"
      value when value in ["stalled", "failed"] -> "#{base} state-badge-danger"
      _ -> base
    end
  end

  defp recent_outcome_health(outcome) do
    case to_string(outcome) do
      "completed" -> "healthy"
      "cancelled" -> "warning"
      _ -> "failed"
    end
  end

  defp latest_assistant_block(blocks) when is_list(blocks) do
    blocks
    |> Enum.reverse()
    |> Enum.find(&(Map.get(&1, :kind) == "assistant"))
  end

  defp latest_assistant_block(_blocks), do: nil

  defp event_badge_class(category) do
    base = "event-badge"

    case to_string(category) do
      "approval" -> "#{base} event-badge-approval"
      "token_usage" -> "#{base} event-badge-token"
      "agent_message" -> "#{base} event-badge-agent"
      "reasoning" -> "#{base} event-badge-reasoning"
      "file_change" -> "#{base} event-badge-file"
      "command" -> "#{base} event-badge-command"
      _ -> base
    end
  end

  defp transcript_block_class(kind), do: "timeline-event-card transcript-block transcript-block-#{kind}"

  defp transcript_badge_class(kind) do
    base = "state-badge transcript-badge"

    case to_string(kind) do
      "assistant" -> "#{base} transcript-badge-assistant"
      "thinking" -> "#{base} transcript-badge-thinking"
      "tool_call" -> "#{base} transcript-badge-tool"
      "tool_response" -> "#{base} transcript-badge-tool"
      "command" -> "#{base} transcript-badge-command"
      "command_output" -> "#{base} transcript-badge-command"
      "file_change" -> "#{base} transcript-badge-file"
      _ -> base
    end
  end

  defp transcript_body_class(kind) when kind in ["command", "command_output", "tool_call", "tool_response", "file_change"],
    do: "transcript-body transcript-body-code"

  defp transcript_body_class(_kind), do: "transcript-body"

  defp compact_transcript_body_class(kind) when kind in ["command", "command_output", "tool_call", "tool_response", "file_change"],
    do: "transcript-body transcript-body-code compact-transcript-body"

  defp compact_transcript_body_class(_kind), do: "transcript-body compact-transcript-body"

  defp details_state_key(block) when is_map(block) do
    [Map.get(block, :kind), Map.get(block, :title), Map.get(block, :timestamp)]
    |> Enum.map(&to_string(&1 || ""))
    |> Enum.join(":")
  end

  defp details_state_key(_block), do: "details"

  defp event_details_state_key(event) when is_map(event) do
    [
      event_value(event, :category),
      event_value(event, :method),
      event_value(event, :at),
      event_value(event, :event_id)
    ]
    |> Enum.map(&to_string(&1 || ""))
    |> Enum.join(":")
  end

  defp event_details_state_key(_event), do: "event"

  defp details_dom_id(prefix, key) when is_binary(prefix) do
    suffix =
      key
      |> to_string()
      |> String.replace(~r/[^a-zA-Z0-9_-]+/, "-")
      |> String.trim("-")

    if suffix == "" do
      prefix
    else
      "#{prefix}-#{suffix}"
    end
  end

  defp schedule_runtime_tick do
    Process.send_after(self(), :runtime_tick, @runtime_tick_ms)
  end

  defp event_copy_text(event) when is_map(event) do
    event_value(event, :raw) || event_value(event, :message) || event_value(event, :summary)
  end

  defp event_copy_text(_event), do: nil

  defp debug_trace_command(selected_run, runtime) do
    session_id = runtime && Map.get(runtime, :session_id)
    issue_identifier = selected_run && Map.get(selected_run, :issue_identifier)

    cond do
      is_binary(session_id) and session_id != "" ->
        "rg -n \"session_id=#{session_id}\" log/symphony.log* ~/Developer/tools/logs/symphony/**/* 2>/dev/null"

      is_binary(issue_identifier) and issue_identifier != "" ->
        "rg -n \"issue_identifier=#{issue_identifier}\" log/symphony.log* ~/Developer/tools/logs/symphony/**/* 2>/dev/null"

      true ->
        "rg -n \"session_id=<thread>-<turn>\" log/symphony.log* ~/Developer/tools/logs/symphony/**/* 2>/dev/null"
    end
  end

  defp resume_command(selected_run, runtime) do
    session_id = runtime && Map.get(runtime, :session_id)
    resume_cwd = resume_cwd(selected_run)

    cond do
      !present_string?(resume_cwd) or !present_string?(session_id) ->
        nil

      true ->
        "cd #{shell_quote(resume_cwd)} && codex --yolo resume #{shell_quote(session_id)}"
    end
  end

  defp resume_cwd(selected_run) do
    workspace_path = selected_run && get_in(selected_run, [:workspace, :path])
    source_repo = System.get_env("CARECORE_SOURCE_REPO")

    cond do
      present_string?(workspace_path) and File.dir?(workspace_path) ->
        workspace_path

      present_string?(source_repo) and File.dir?(source_repo) ->
        source_repo

      present_string?(workspace_path) ->
        workspace_path

      true ->
        nil
    end
  end

  defp present_string?(value) when is_binary(value), do: String.trim(value) != "" and value != "n/a"
  defp present_string?(_value), do: false

  defp shell_quote(value) when is_binary(value) do
    escaped = String.replace(value, "'", "'\"'\"'")
    "'#{escaped}'"
  end

  defp shell_quote(value), do: shell_quote(to_string(value))

  attr :id, :string, required: true
  attr :label, :string, required: true
  attr :value, :string, required: true
  attr :copy_label, :string, default: "Copy"

  defp copyable_stat_row(assigns) do
    ~H"""
    <div class="stat-row">
      <span class="muted"><%= @label %></span>
      <div class="copyable-stat-value">
        <span><%= @value %></span>
        <button :if={@value not in [nil, "", "n/a"]} id={@id} type="button" class="copy-button" phx-hook="ClipboardCopy" data-copy-text={@value} data-copy-label={@copy_label}>
          <%= @copy_label %>
        </button>
      </div>
    </div>
    """
  end

  defp copy_button_dom_id(prefix, key), do: details_dom_id(prefix, key)
end
