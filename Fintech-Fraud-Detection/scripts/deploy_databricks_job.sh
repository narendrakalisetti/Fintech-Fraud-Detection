#!/usr/bin/env bash
# scripts/deploy_databricks_job.sh
# Deploys the fraud detection streaming job to Databricks via Jobs API.
set -euo pipefail

ENV="${1:-prod}"
JOB_CONFIG="databricks/job_config.json"
DATABRICKS_HOST="${DATABRICKS_HOST:-}"
DATABRICKS_TOKEN="${DATABRICKS_TOKEN:-}"

if [[ -z "$DATABRICKS_HOST" || -z "$DATABRICKS_TOKEN" ]]; then
  echo "ERROR: Set DATABRICKS_HOST and DATABRICKS_TOKEN environment variables."
  exit 1
fi

echo "Deploying fraud detection job to Databricks ($ENV)..."

# Check if job already exists
EXISTING_JOB_ID=$(curl -sf \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  "$DATABRICKS_HOST/api/2.1/jobs/list" | \
  python3 -c "
import sys, json
jobs = json.load(sys.stdin).get('jobs', [])
match = [j['job_id'] for j in jobs if j['settings']['name'] == 'fraud-detection-streaming-$ENV']
print(match[0] if match else '')
")

if [[ -n "$EXISTING_JOB_ID" ]]; then
  echo "Updating existing job ID: $EXISTING_JOB_ID"
  curl -sf -X POST \
    -H "Authorization: Bearer $DATABRICKS_TOKEN" \
    -H "Content-Type: application/json" \
    -d "$(cat $JOB_CONFIG)" \
    "$DATABRICKS_HOST/api/2.1/jobs/reset?job_id=$EXISTING_JOB_ID"
  echo "Job updated: $EXISTING_JOB_ID"
else
  echo "Creating new Databricks job..."
  JOB_ID=$(curl -sf -X POST \
    -H "Authorization: Bearer $DATABRICKS_TOKEN" \
    -H "Content-Type: application/json" \
    -d "$(cat $JOB_CONFIG)" \
    "$DATABRICKS_HOST/api/2.1/jobs/create" | python3 -c "import sys,json; print(json.load(sys.stdin)['job_id'])")
  echo "Job created: $JOB_ID"
fi

echo "Deployment complete."
