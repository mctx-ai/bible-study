#!/usr/bin/env bash
set -euo pipefail

# ===========================================================================
# etl-all.sh — Run the complete Bible Study MCP Server ETL pipeline in order
# ===========================================================================

# ---------------------------------------------------------------------------
# Env var validation
# Note: data:acquire (step 2) does NOT need Cloudflare credentials, but all
# other steps do.
# ---------------------------------------------------------------------------
missing_vars=()

if [[ -z "${CLOUDFLARE_API_TOKEN:-}" ]]; then
  missing_vars+=("CLOUDFLARE_API_TOKEN")
fi
if [[ -z "${CLOUDFLARE_ACCOUNT_ID:-}" ]]; then
  missing_vars+=("CLOUDFLARE_ACCOUNT_ID")
fi
if [[ -z "${D1_DATABASE_ID:-}" ]]; then
  missing_vars+=("D1_DATABASE_ID")
fi
if [[ -z "${VECTORIZE_INDEX_NAME:-}" ]]; then
  missing_vars+=("VECTORIZE_INDEX_NAME")
fi

if [[ ${#missing_vars[@]} -gt 0 ]]; then
  echo "ERROR: The following required environment variables are not set:"
  for var in "${missing_vars[@]}"; do
    echo "  - ${var}"
  done
  echo ""
  echo "Set them in your shell or via 'op read' before running this script."
  exit 1
fi

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
pipeline_start=$(date +%s)

run_step() {
  local step_num="$1"
  local total="$2"
  local label="$3"
  local cmd="$4"

  echo ""
  echo "==> Step ${step_num}/${total}: ${label}..."
  local step_start
  step_start=$(date +%s)

  eval "${cmd}"

  local step_end
  step_end=$(date +%s)
  local elapsed=$(( step_end - step_start ))
  echo "    Done in ${elapsed}s"
}

# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
TOTAL_STEPS=11

run_step  1 "${TOTAL_STEPS}" "Recreating database schema"         "npm run db:schema"
run_step  2 "${TOTAL_STEPS}" "Downloading raw data files"         "npm run data:acquire"
run_step  3 "${TOTAL_STEPS}" "Loading Bible text"                 "npm run etl:bible"
run_step  4 "${TOTAL_STEPS}" "Loading Strong's concordance"       "npm run etl:strongs"
run_step  5 "${TOTAL_STEPS}" "Loading morphology"                 "npm run etl:morphology"
run_step  6 "${TOTAL_STEPS}" "Loading cross-references"           "npm run etl:crossrefs"
run_step  7 "${TOTAL_STEPS}" "Loading Nave's Topical Bible"       "npm run etl:naves"
run_step  8 "${TOTAL_STEPS}" "Computing salience weights"         "npm run etl:salience"
run_step  9 "${TOTAL_STEPS}" "Populating FTS5 index"              "npm run search:fts5"
run_step 10 "${TOTAL_STEPS}" "Generating verse embeddings"        "npm run search:embeddings -- --resume"
run_step 11 "${TOTAL_STEPS}" "Embedding topics and book summaries" "npm run search:topic-embeddings"

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
pipeline_end=$(date +%s)
total_elapsed=$(( pipeline_end - pipeline_start ))
total_minutes=$(( total_elapsed / 60 ))
total_seconds=$(( total_elapsed % 60 ))

echo ""
echo "=================================================================="
echo "  ETL pipeline complete!"
echo "  Total time: ${total_minutes}m ${total_seconds}s"
echo "=================================================================="
