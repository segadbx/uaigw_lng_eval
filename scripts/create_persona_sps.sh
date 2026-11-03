#!/usr/bin/env bash
# Create (idempotently) one workspace service principal per persona, named <sp_name_prefix><persona>,
# and give each the entitlements Unity Gateway / SQL calls need (workspace-access, databricks-sql-access).
# Usage: scripts/create_persona_sps.sh <workspace-profile> [target]
set -euo pipefail
WS_PROFILE="${1:-${DATABRICKS_CONFIG_PROFILE:-}}"; TARGET="${2:-dev}"
source "$(dirname "$0")/_bundle_vars.sh"
for persona in "${PERSONAS[@]}"; do
  name="${SP_PREFIX}${persona}"
  row="$(ws_sp "${persona}")"
  if [[ -z "${row}" ]]; then
    databricks service-principals create --display-name "${name}" -p "${WS_PROFILE}" -o json >/dev/null
    row="$(ws_sp "${persona}")"; echo "created ${name}"
  fi
  id="${row%%$'\t'*}"
  databricks service-principals patch "${id}" -p "${WS_PROFILE}" --json '{"schemas":["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
    "Operations":[{"op":"add","path":"entitlements","value":[{"value":"workspace-access"},{"value":"databricks-sql-access"}]}]}' >/dev/null
  echo "${name}: application_id=${row#*$'\t'} (entitlements ok)"
done
