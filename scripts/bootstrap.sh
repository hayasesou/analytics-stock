#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

if [ -z "${NEON_DATABASE_URL:-}" ]; then
  echo "NEON_DATABASE_URL is required"
  exit 1
fi

psql "$NEON_DATABASE_URL" -f "$ROOT_DIR/infra/sql/schema.sql"

echo "Schema applied."
