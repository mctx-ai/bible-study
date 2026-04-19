#!/usr/bin/env bash
# scripts/setup-infra.sh
#
# Sets up Cloudflare infrastructure for the Bible Study MCP Server.
# Creates a D1 database and a Vectorize index if they don't already exist.
#
# Prerequisites:
#   - wrangler CLI installed and authenticated (npx wrangler login)
#   - jq installed
#   - CLOUDFLARE_ACCOUNT_ID environment variable set
#
# Usage:
#   CLOUDFLARE_ACCOUNT_ID=<your-account-id> bash scripts/setup-infra.sh
#
# After running, copy the output values into the mctx dashboard as secrets:
#   D1_DATABASE_ID     → the UUID assigned to the 'bible' D1 database
#   VECTORIZE_INDEX_NAME → the name of the Vectorize index (bible-embeddings)

set -euo pipefail

# ---------------------------------------------------------------------------
# Load .env file if present (mirrors load-env.ts behavior for bash scripts)
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/../.env"
if [[ -f "$ENV_FILE" ]]; then
  while IFS= read -r line || [[ -n "$line" ]]; do
    # Skip blank lines and comments
    [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue
    # Only export KEY=VALUE lines; shell-exported vars take precedence
    if [[ "$line" =~ ^([A-Za-z_][A-Za-z0-9_]*)=(.*)$ ]]; then
      key="${BASH_REMATCH[1]}"
      val="${BASH_REMATCH[2]}"
      if [[ -z "${!key+x}" ]]; then
        export "$key=$val"
      fi
    fi
  done < "$ENV_FILE"
fi

# ---------------------------------------------------------------------------
# Validate required environment variables
# ---------------------------------------------------------------------------
if [[ -z "${CLOUDFLARE_ACCOUNT_ID:-}" ]]; then
  echo "ERROR: CLOUDFLARE_ACCOUNT_ID is not set." >&2
  echo "Export it before running this script:" >&2
  echo "  export CLOUDFLARE_ACCOUNT_ID=<your-cloudflare-account-id>" >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# Helper: check that required tools are available
# ---------------------------------------------------------------------------
if ! command -v jq &>/dev/null; then
  echo "ERROR: 'jq' is required but not found in PATH." >&2
  exit 1
fi

# Use npx wrangler if wrangler is not in PATH (common when installed as devDependency)
if command -v wrangler &>/dev/null; then
  WRANGLER="wrangler"
else
  WRANGLER="npx wrangler"
fi

echo "==> Cloudflare infrastructure setup for Bible Study MCP Server"
echo ""

# ---------------------------------------------------------------------------
# D1 Database — 'bible'
# ---------------------------------------------------------------------------
D1_DB_NAME="bible"

echo "--- D1 Database: $D1_DB_NAME ---"

# List existing D1 databases and check for a match by name.
existing_d1=$($WRANGLER d1 list --json 2>/dev/null | jq -r --arg name "$D1_DB_NAME" '.[] | select(.name == $name) | .uuid')

if [[ -n "$existing_d1" ]]; then
  echo "D1 database '$D1_DB_NAME' already exists (uuid: $existing_d1). Skipping creation."
  d1_database_id="$existing_d1"
else
  echo "Creating D1 database '$D1_DB_NAME'..."
  create_output=$($WRANGLER d1 create "$D1_DB_NAME" 2>&1)
  d1_database_id=$(echo "$create_output" | grep -oE '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')
  if [[ -z "$d1_database_id" ]]; then
    echo "ERROR: Failed to parse UUID from d1 create output:" >&2
    echo "$create_output" >&2
    exit 1
  fi
  echo "D1 database '$D1_DB_NAME' created (uuid: $d1_database_id)."
fi

echo ""

# ---------------------------------------------------------------------------
# Vectorize Index — 'bible-embeddings'
# ---------------------------------------------------------------------------
VECTORIZE_INDEX_NAME="bible-embeddings"
VECTORIZE_DIMENSIONS=768
VECTORIZE_METRIC="cosine"

echo "--- Vectorize Index: $VECTORIZE_INDEX_NAME ---"

# List existing Vectorize indexes and check for a match by name.
existing_vectorize=$($WRANGLER vectorize list --json 2>/dev/null | jq -r --arg name "$VECTORIZE_INDEX_NAME" '.[] | select(.name == $name) | .name')

if [[ -n "$existing_vectorize" ]]; then
  echo "Vectorize index '$VECTORIZE_INDEX_NAME' already exists. Skipping creation."
else
  echo "Creating Vectorize index '$VECTORIZE_INDEX_NAME' (dimensions=$VECTORIZE_DIMENSIONS, metric=$VECTORIZE_METRIC)..."
  $WRANGLER vectorize create "$VECTORIZE_INDEX_NAME" \
    --dimensions="$VECTORIZE_DIMENSIONS" \
    --metric="$VECTORIZE_METRIC"
  echo "Vectorize index '$VECTORIZE_INDEX_NAME' created."
fi

echo ""

# ---------------------------------------------------------------------------
# Vectorize Index — 'bible-topics'
# ---------------------------------------------------------------------------
VECTORIZE_TOPICS_INDEX_NAME="bible-topics"
VECTORIZE_TOPICS_DIMENSIONS=768
VECTORIZE_TOPICS_METRIC="cosine"

echo "--- Vectorize Index: $VECTORIZE_TOPICS_INDEX_NAME ---"

# List existing Vectorize indexes and check for a match by name.
existing_topics_vectorize=$($WRANGLER vectorize list --json 2>/dev/null | jq -r --arg name "$VECTORIZE_TOPICS_INDEX_NAME" '.[] | select(.name == $name) | .name')

if [[ -n "$existing_topics_vectorize" ]]; then
  echo "Vectorize index '$VECTORIZE_TOPICS_INDEX_NAME' already exists. Skipping creation."
else
  echo "Creating Vectorize index '$VECTORIZE_TOPICS_INDEX_NAME' (dimensions=$VECTORIZE_TOPICS_DIMENSIONS, metric=$VECTORIZE_TOPICS_METRIC)..."
  $WRANGLER vectorize create "$VECTORIZE_TOPICS_INDEX_NAME" \
    --dimensions="$VECTORIZE_TOPICS_DIMENSIONS" \
    --metric="$VECTORIZE_TOPICS_METRIC"
  echo "Vectorize index '$VECTORIZE_TOPICS_INDEX_NAME' created."
fi

echo ""

# ---------------------------------------------------------------------------
# Output — add these values as secrets in the mctx dashboard
# ---------------------------------------------------------------------------
echo "======================================================================"
echo "Infrastructure setup complete. Add the following as secrets in the"
echo "mctx dashboard for this server:"
echo ""
echo "  D1_DATABASE_ID        = $d1_database_id"
echo "  VECTORIZE_INDEX_NAME  = $VECTORIZE_INDEX_NAME"
echo "  VECTORIZE_TOPICS_INDEX_NAME = $VECTORIZE_TOPICS_INDEX_NAME"
echo "======================================================================"
