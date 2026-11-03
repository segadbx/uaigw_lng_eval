#!/usr/bin/env bash
# Run by an ACCOUNT ADMIN. Creates the persona account groups named in the bundle variables, adds the
# matching persona SP (<sp_name_prefix><persona>) to each, and assigns each group to the workspace
# (needed for secret-scope ACLs and app/dashboard permissions). Idempotent. No IDs are hard-coded:
# group names come from the bundle, SPs are found by display name, the workspace ID from the profile.
# Usage: scripts/create_persona_groups.sh <account-profile> <workspace-profile> [target]
set -euo pipefail
ACC="${1:?account-level CLI profile}"; WS_PROFILE="${2:-${DATABRICKS_CONFIG_PROFILE:-}}"; TARGET="${3:-dev}"
source "$(dirname "$0")/_bundle_vars.sh"
WS_ID="$(databricks metastores current -p "${WS_PROFILE}" -o json | jq -r '.workspace_id')"
for persona in "${GROUP_PERSONAS[@]}"; do
  group="$(group_of "${persona}")"
  app_id="$(ws_sp "${persona}" | cut -f2)"
  [[ -z "${app_id}" ]] && { echo "missing SP ${SP_PREFIX}${persona}: run scripts/create_persona_sps.sh first"; exit 1; }
  gid="$(databricks account groups list --filter "displayName eq \"${group}\"" -p "${ACC}" -o json | jq -r '.[0].id // empty')"
  if [[ -z "${gid}" ]]; then
    gid="$(databricks account groups create --display-name "${group}" -p "${ACC}" -o json | jq -r '.id')"; echo "created ${group}"
  fi
  sp_id="$(databricks account service-principals list --filter "applicationId eq \"${app_id}\"" -p "${ACC}" -o json | jq -r '.[0].id')"
  databricks account groups patch "${gid}" -p "${ACC}" --json "{\"schemas\":[\"urn:ietf:params:scim:api:messages:2.0:PatchOp\"],
    \"Operations\":[{\"op\":\"add\",\"path\":\"members\",\"value\":[{\"value\":\"${sp_id}\"}]}]}"
  databricks account workspace-assignment update "${WS_ID}" "${gid}" --json '{"permissions":["USER"]}' -p "${ACC}" >/dev/null
  echo "${group} <- ${SP_PREFIX}${persona}; assigned to workspace ${WS_ID}"
done
