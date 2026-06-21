defmodule SymphonyElixirWeb.Layouts do
  @moduledoc """
  Shared layouts for the observability dashboard.
  """

  use Phoenix.Component

  @dashboard_css_source Path.expand("../../../priv/static/dashboard.css", __DIR__)
  @external_resource @dashboard_css_source
  @dashboard_css_version @dashboard_css_source |> File.read!() |> then(&:crypto.hash(:sha256, &1)) |> Base.encode16(case: :lower) |> binary_part(0, 12)

  @spec root(map()) :: Phoenix.LiveView.Rendered.t()
  def root(assigns) do
    assigns =
      assigns
      |> assign(:csrf_token, Plug.CSRFProtection.get_csrf_token())
      |> assign(:dashboard_css_href, "/dashboard.css?v=#{@dashboard_css_version}")

    ~H"""
    <!DOCTYPE html>
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <meta name="csrf-token" content={@csrf_token} />
        <title>Symphony Observability</title>
        <link rel="preconnect" href="https://fonts.googleapis.com" />
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
        <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=Space+Grotesk:wght@500;700&family=IBM+Plex+Mono:wght@400;500;600&display=swap" rel="stylesheet" />
        <script defer src="/vendor/phoenix_html/phoenix_html.js"></script>
        <script defer src="/vendor/phoenix/phoenix.js"></script>
        <script defer src="/vendor/phoenix_live_view/phoenix_live_view.js"></script>
        <script>
          window.addEventListener("DOMContentLoaded", function () {
            var csrfToken = document
              .querySelector("meta[name='csrf-token']")
              ?.getAttribute("content");

            if (!window.Phoenix || !window.LiveView) return;

            var detailsStorageKey = function (key) {
              return "symphony:details:" + key;
            };

            var detailsHooks = {
              PersistDetailsState: {
                mounted: function () {
                  var el = this.el;
                  var key = el.dataset.detailsKey;
                  if (!key) return;

                  var restore = function () {
                    try {
                      var saved = window.sessionStorage.getItem(detailsStorageKey(key));
                      if (saved === "open") el.open = true;
                      if (saved === "closed") el.open = false;
                    } catch (_error) {}
                  };

                  var persist = function () {
                    try {
                      window.sessionStorage.setItem(detailsStorageKey(key), el.open ? "open" : "closed");
                    } catch (_error) {}
                  };

                  this.__restoreDetailsState = restore;
                  this.__persistDetailsState = persist;
                  el.addEventListener("toggle", persist);
                  restore();
                },
                updated: function () {
                  if (this.__restoreDetailsState) this.__restoreDetailsState();
                },
                destroyed: function () {
                  if (this.__persistDetailsState) {
                    this.el.removeEventListener("toggle", this.__persistDetailsState);
                  }
                }
              },
              ClipboardCopy: {
                mounted: function () {
                  this.handleClick = async (event) => {
                    event.preventDefault();

                    var text = this.el.dataset.copyText || "";
                    if (!text) return;

                    var originalLabel = this.el.dataset.copyLabel || this.el.textContent;

                    try {
                      if (navigator.clipboard && navigator.clipboard.writeText) {
                        await navigator.clipboard.writeText(text);
                      } else {
                        var input = document.createElement("textarea");
                        input.value = text;
                        input.setAttribute("readonly", "");
                        input.style.position = "absolute";
                        input.style.left = "-9999px";
                        document.body.appendChild(input);
                        input.select();
                        document.execCommand("copy");
                        document.body.removeChild(input);
                      }

                      this.el.textContent = "Copied";
                      window.clearTimeout(this.copyResetTimer);
                      this.copyResetTimer = window.setTimeout(() => {
                        this.el.textContent = originalLabel;
                      }, 1200);
                    } catch (_error) {
                      this.el.textContent = "Failed";
                      window.clearTimeout(this.copyResetTimer);
                      this.copyResetTimer = window.setTimeout(() => {
                        this.el.textContent = originalLabel;
                      }, 1200);
                    }
                  };

                  this.el.addEventListener("click", this.handleClick);
                },
                destroyed: function () {
                  if (this.handleClick) this.el.removeEventListener("click", this.handleClick);
                  if (this.copyResetTimer) window.clearTimeout(this.copyResetTimer);
                }
              }
            };

            var liveSocket = new window.LiveView.LiveSocket("/live", window.Phoenix.Socket, {
              params: {_csrf_token: csrfToken},
              hooks: detailsHooks
            });

            liveSocket.connect();
            window.liveSocket = liveSocket;
          });
        </script>
        <link rel="stylesheet" href={@dashboard_css_href} />
      </head>
      <body>
        {@inner_content}
      </body>
    </html>
    """
  end

  @spec app(map()) :: Phoenix.LiveView.Rendered.t()
  def app(assigns) do
    ~H"""
    <main class="app-shell">
      {@inner_content}
    </main>
    """
  end
end
