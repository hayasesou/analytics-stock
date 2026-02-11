#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

if [ -z "${NEON_DATABASE_URL:-}" ] && [ -f "$ROOT_DIR/.env" ]; then
  NEON_DATABASE_URL="$(grep -m1 '^NEON_DATABASE_URL=' "$ROOT_DIR/.env" | cut -d'=' -f2- || true)"
  # Strip optional surrounding quotes.
  NEON_DATABASE_URL="${NEON_DATABASE_URL#\"}"
  NEON_DATABASE_URL="${NEON_DATABASE_URL%\"}"
  NEON_DATABASE_URL="${NEON_DATABASE_URL#\'}"
  NEON_DATABASE_URL="${NEON_DATABASE_URL%\'}"
fi

if [ -z "${NEON_DATABASE_URL:-}" ]; then
  echo "NEON_DATABASE_URL is required"
  exit 1
fi

if command -v psql >/dev/null 2>&1; then
  psql "$NEON_DATABASE_URL" -f "$ROOT_DIR/infra/sql/schema.sql"
  echo "Schema applied."
  exit 0
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "psql is not installed and docker is not available"
  exit 1
fi

if ! docker compose version >/dev/null 2>&1; then
  echo "docker compose is required but not available"
  exit 1
fi

echo "psql not found; applying schema via docker compose (db-bootstrap service)."
(
  cd "$ROOT_DIR"
  docker compose --profile setup run --rm \
    -e NEON_DATABASE_URL="$NEON_DATABASE_URL" \
    db-bootstrap
)

echo "Schema applied."
