#!/bin/bash
# =============================================================================
# Deploy ClearPay Fraud Detection Job to Databricks
# =============================================================================
set -e

ENV=${1:-prod}
echo "Deploying to environment: $ENV"

# Upload source files to DBFS
databricks fs cp -r src/ dbfs:/clearplay/src/ --overwrite
databricks fs cp -r notebooks/ dbfs:/clearplay/notebooks/ --overwrite

# Create or update the Databricks job
JOB_NAME="ClearPay-FraudDetection-StreamingJob"
EXISTING_JOB=$(databricks jobs list --output json | \
  python3 -c "import sys,json; jobs=json.load(sys.stdin); \
  print(next((j['job_id'] for j in jobs.get('jobs',[]) if j['settings']['name']=='$JOB_NAME'), ''))")

if [ -z "$EXISTING_JOB" ]; then
  echo "Creating new job..."
  databricks jobs create --json @databricks/job_config.json
else
  echo "Updating existing job ID: $EXISTING_JOB"
  databricks jobs reset --job-id $EXISTING_JOB --json @databricks/job_config.json
fi

echo "Deployment complete."
