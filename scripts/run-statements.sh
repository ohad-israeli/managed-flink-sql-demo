#!/usr/bin/env bash
# Submits the full demo lifecycle as named CMF Statements, in order:
# create tables -> seed data -> run the enrichment job. Reproduces the whole
# thing from a fresh Kafka catalog with empty topics.
#
# Prerequisites:
#   - A CMF Kafka Catalog + Database already registered (see WALKTHROUGH.md /
#     README for how) whose names match the `sql/*.sql` files (default:
#     `fresh-catalog`.`fresh-db` -- rename in the .sql files if yours differ).
#   - That Database's `ddlEnvironments` must include the environment you pass
#     below, or every CREATE TABLE statement will fail with
#     "This environment does not have CREATE TABLE permissions" (hit this
#     exact error rebuilding this demo -- see WALKTHROUGH.md's findings).
#   - The Kafka topics `clicks`, `customers`, `customer_click_activity` must
#     NOT already exist with data in them, or the seed steps will duplicate
#     rows into an already-populated append log and the join output won't
#     match what's documented. Delete them first if you're re-running this
#     against a previously-used catalog.
#
# Usage:
#   export CONFLUENT_CMF_URL=http://localhost:8080
#   scripts/run-statements.sh <environment> <compute-pool>
#   scripts/run-statements.sh sql-demo sql-demo-pool
set -euo pipefail

ENVIRONMENT="${1:?Usage: $0 <environment> <compute-pool>}"
COMPUTE_POOL="${2:?Usage: $0 <environment> <compute-pool>}"
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SQL_DIR="$REPO_DIR/sql"

for f in "$SQL_DIR"/*.sql; do
  name="$(basename "$f" .sql)"
  name="${name#??_}"          # drop the leading "NN_" ordering prefix
  name="${name//_/-}"          # match the hyphenated statement names used in CMF
  echo "=== submitting '$name' (from $(basename "$f")) ==="
  confluent --environment "$ENVIRONMENT" --compute-pool "$COMPUTE_POOL" \
    flink statement create "$name" --sql "$(cat "$f")"
  echo
done

echo "All statements submitted. Check status with:"
echo "  confluent flink statement list --environment $ENVIRONMENT"
