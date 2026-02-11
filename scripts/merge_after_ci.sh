#!/usr/bin/env bash
set -euo pipefail

if ! command -v gh >/dev/null 2>&1; then
  echo "gh CLI is required." >&2
  exit 1
fi

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <pr-number> [--merge|--squash|--rebase]" >&2
  exit 1
fi

PR_NUMBER="$1"
MERGE_MODE="${2:---merge}"

case "$MERGE_MODE" in
  --merge|--squash|--rebase) ;;
  *)
    echo "Invalid merge mode: $MERGE_MODE" >&2
    echo "Allowed: --merge, --squash, --rebase" >&2
    exit 1
    ;;
esac

REPO="$(gh repo view --json nameWithOwner -q .nameWithOwner)"
HEAD_SHA="$(gh pr view "$PR_NUMBER" --json headRefOid -q .headRefOid)"

echo "repo=$REPO"
echo "pr=$PR_NUMBER"
echo "head_sha=$HEAD_SHA"
echo "waiting for contexts: worker-tests, web-build"

MAX_ATTEMPTS=60
SLEEP_SEC=10

for ((i=1; i<=MAX_ATTEMPTS; i++)); do
  worker_state="$(gh api "repos/$REPO/commits/$HEAD_SHA/status" --jq '.statuses[] | select(.context=="worker-tests") | .state' | head -n1 || true)"
  web_state="$(gh api "repos/$REPO/commits/$HEAD_SHA/status" --jq '.statuses[] | select(.context=="web-build") | .state' | head -n1 || true)"

  worker_state="${worker_state:-pending}"
  web_state="${web_state:-pending}"

  echo "attempt=$i worker-tests=$worker_state web-build=$web_state"

  if [[ "$worker_state" == "failure" || "$worker_state" == "error" || "$web_state" == "failure" || "$web_state" == "error" ]]; then
    echo "CI failed. Aborting merge." >&2
    exit 1
  fi

  if [[ "$worker_state" == "success" && "$web_state" == "success" ]]; then
    echo "CI checks passed. Merging PR #$PR_NUMBER..."
    gh pr merge "$PR_NUMBER" "$MERGE_MODE" --delete-branch
    exit 0
  fi

  sleep "$SLEEP_SEC"
done

echo "Timed out waiting for CI checks." >&2
exit 1
