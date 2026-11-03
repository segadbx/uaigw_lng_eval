#!/usr/bin/env bash
# Store persona SP OAuth credentials in the bundle's secret scope (values are prompted, never echoed).
# Usage: scripts/put_persona_secrets.sh [scope] [profile]
set -euo pipefail
SCOPE="${1:?secret scope (bundle variable secret_scope)}"
PROFILE="${2:-${DATABRICKS_CONFIG_PROFILE:?pass a workspace profile}}"
for persona in trader unentitled_trader developer operator security auditor app; do
  read -r -p "${persona} SP application (client) ID [blank = skip]: " cid
  [[ -z "${cid}" ]] && { echo "  skipped ${persona} -> its tests report NOT_TESTED"; continue; }
  databricks secrets put-secret "${SCOPE}" "${persona}_client_id" --string-value "${cid}" --profile "${PROFILE}"
  read -r -s -p "${persona} SP OAuth client secret: " secret; echo
  databricks secrets put-secret "${SCOPE}" "${persona}_client_secret" --string-value "${secret}" --profile "${PROFILE}"
  unset secret
done
