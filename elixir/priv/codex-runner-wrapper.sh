#!/usr/bin/env bash
set -euo pipefail

real_codex="${SYMPHONY_REAL_CODEX_BIN:-}"
review_codex_home="${SYMPHONY_REVIEW_CODEX_HOME:-}"

if [[ -z "$real_codex" || ! -x "$real_codex" ]]; then
  echo "SYMPHONY_REAL_CODEX_BIN must name an executable Codex binary" >&2
  exit 1
fi

codex_subcommand() {
  while (( $# > 0 )); do
    case "$1" in
      -c|--config|--enable|--disable|--remote|--remote-auth-token-env|-i|--image|-m|--model|--local-provider|-p|--profile|-s|--sandbox|-C|--cd|--add-dir|-a|--ask-for-approval)
        if (( $# < 2 )); then
          return 0
        fi
        shift 2
        ;;
      --config=*|--enable=*|--disable=*|--remote=*|--remote-auth-token-env=*|--image=*|--model=*|--local-provider=*|--profile=*|--sandbox=*|--cd=*|--add-dir=*|--ask-for-approval=*)
        shift
        ;;
      --strict-config|--oss|--dangerously-bypass-approvals-and-sandbox|--dangerously-bypass-hook-trust|--search|--no-alt-screen|-h|--help|-V|--version)
        shift
        ;;
      --|-*)
        return 0
        ;;
      *)
        printf '%s\n' "$1"
        return 0
        ;;
    esac
  done
}

if [[ "$(codex_subcommand "$@")" == "app-server" ]]; then
  exec "$real_codex" "$@"
fi

if [[ -z "$review_codex_home" || ! -d "$review_codex_home" || -L "$review_codex_home" ]]; then
  echo "Symphony reviewer CODEX_HOME is not initialized" >&2
  exit 1
fi

if [[ ! -f "$review_codex_home/auth.json" || -L "$review_codex_home/auth.json" ]]; then
  echo "Symphony reviewer authentication is not initialized" >&2
  exit 1
fi

export CODEX_HOME="$review_codex_home"
export HOME="$review_codex_home"
export TMPDIR="$review_codex_home/tmp"

exec "$real_codex" \
  --model "${SYMPHONY_REVIEW_CODEX_MODEL:-gpt-5.6-sol}" \
  --config "model_reasoning_effort=${SYMPHONY_REVIEW_CODEX_REASONING_EFFORT:-xhigh}" \
  "$@"
