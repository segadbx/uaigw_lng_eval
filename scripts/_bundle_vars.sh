#!/usr/bin/env bash
# Shared helper: resolve bundle variables (group names, secret scope, SP prefix) from the bundle itself,
# so the scripts carry no hard-coded names or IDs. Source it: `source scripts/_bundle_vars.sh <target>`.
set -euo pipefail
BUNDLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
: "${WS_PROFILE:?set WS_PROFILE (workspace CLI profile) or pass it as an argument}"
TARGET="${TARGET:-dev}"
_VARS_JSON="$(cd "${BUNDLE_DIR}" && databricks bundle validate -t "${TARGET}" -p "${WS_PROFILE}" -o json | jq '.variables')"
var() { jq -r --arg k "$1" '.[$k].value // .[$k].default // empty' <<< "${_VARS_JSON}"; }
SECRET_SCOPE="$(var secret_scope)"
SP_PREFIX="$(var sp_name_prefix)"
# persona -> group name from the bundle variables (bash 3.2 compatible: no associative arrays).
# The app SP has no group.
group_of() {
  case "$1" in
    trader) var trader_group ;; unentitled-trader) var unentitled_trader_group ;;
    developer) var developer_group ;; operator) var operator_group ;;
    security) var security_group ;; auditor) var auditor_group ;;
    *) echo "" ;;
  esac
}
GROUP_PERSONAS=(trader unentitled-trader developer operator security auditor)
PERSONAS=(trader unentitled-trader developer operator security auditor app)
# workspace SP lookup by display name -> "<workspace_id_of_sp>\t<application_id>"
ws_sp() { databricks service-principals list --filter "displayName eq \"${SP_PREFIX}$1\"" -p "${WS_PROFILE}" -o json | jq -r '.[0] | select(.) | "\(.id)\t\(.applicationId)"'; }
