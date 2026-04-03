#!/usr/bin/env bash

# exit on error, unset variables, and pipe failures
set -euo pipefail

usage() {
    echo "Usage: $0 [--env dev|prod] [--force]"
    echo
    echo "  --env   Target environment (default: dev)"
    echo "  --force Re-ingest already loaded years"
    exit 1
}

# defaults
ENV="dev"
FORCE=""

# parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --env)   ENV="$2"; shift 2 ;;
        --force) FORCE="--force"; shift ;;
        *)       usage ;;
    esac
done

# validate env
if [[ "$ENV" != "dev" && "$ENV" != "prod" ]]; then
    echo "Error: --env must be 'dev' or 'prod'"
    exit 1
fi

echo "Environment: $ENV"

echo
echo "══ Ingestion ══"
python ingestion/ingest.py --env "$ENV" $FORCE

echo
echo "══ dbt ══"
dbt run --profiles-dir . --target "$ENV"
