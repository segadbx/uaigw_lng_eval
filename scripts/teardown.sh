#!/usr/bin/env bash
# Teardown: job-created objects first, then bundle-managed resources.
# Usage: scripts/teardown.sh [target] [profile]
set -euo pipefail
TARGET="${1:-dev}"
PROFILE="${2:-${DATABRICKS_CONFIG_PROFILE:?pass a workspace profile}}"
cd "$(dirname "$0")/.."
databricks bundle run teardown -t "${TARGET}" --profile "${PROFILE}"
echo
echo "Complete the MANUAL CLEANUP printed above (detach service policies first)."
read -r -p "Run 'databricks bundle destroy -t ${TARGET}' now? [y/N] " ans
[[ "${ans}" == "y" ]] && databricks bundle destroy -t "${TARGET}" --profile "${PROFILE}"
