#!/usr/bin/env bash
# Create a 30-day OAuth secret for each persona SP (<sp_name_prefix><persona>) and store
# <persona>_client_id / <persona>_client_secret in the bundle's secret scope. Values are never printed.
# Skips personas that already have a stored secret. Persona keys use underscores (unentitled_trader).
# Usage: scripts/create_persona_oauth_secrets.sh <workspace-profile> [target]
set -euo pipefail
WS_PROFILE="${1:-${DATABRICKS_CONFIG_PROFILE:-}}"; TARGET="${2:-dev}"
source "$(dirname "$0")/_bundle_vars.sh"
existing="$(databricks secrets list-secrets "${SECRET_SCOPE}" -p "${WS_PROFILE}" -o json | jq -r '.[]?.key')"
for persona in "${PERSONAS[@]}"; do
  key="${persona//-/_}"
  if grep -qx "${key}_client_secret" <<< "${existing}"; then echo "${key}: already stored, skipped"; continue; fi
  row="$(ws_sp "${persona}")"; [[ -z "${row}" ]] && { echo "${key}: SP ${SP_PREFIX}${persona} not found"; continue; }
  spid="${row%%$'\t'*}"; appid="${row#*$'\t'}"
  secret="$(databricks service-principal-secrets-proxy create "${spid}" --lifetime 2592000s -p "${WS_PROFILE}" -o json | jq -r '.secret')"
  [[ -z "${secret}" || "${secret}" == "null" ]] && { echo "${key}: FAILED to create secret"; continue; }
  databricks secrets put-secret "${SECRET_SCOPE}" "${key}_client_id" --string-value "${appid}" -p "${WS_PROFILE}"
  databricks secrets put-secret "${SECRET_SCOPE}" "${key}_client_secret" --string-value "${secret}" -p "${WS_PROFILE}"
  unset secret; echo "${key}: stored"
done
echo "Keys in ${SECRET_SCOPE}:"; databricks secrets list-secrets "${SECRET_SCOPE}" -p "${WS_PROFILE}" -o json | jq -r '.[].key' | sort | tr '\n' ' '; echo
