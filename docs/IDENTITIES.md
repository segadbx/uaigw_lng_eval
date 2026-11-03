# Persona identities on FE-SANDBOX-AWS

Recreated on 2026-11-02 (lngdemo naming) with `databricks service-principals create` (workspace `dbc-206b78ef-29e7`).
The six account groups **don't exist yet**. An **account admin** must create them and add the SPs
(`scripts/create_persona_sps.sh <workspace-profile>`, then `scripts/create_persona_groups.sh <account-profile> <workspace-profile>`) before `databricks bundle deploy`, because the bundle grants
privileges to these groups and the deploy fails on unknown principals.

| Persona | Account group (bundle variable default) | Service principal | Application ID |
|---|---|---|---|
| trader | `lngdemo-trader-pilot` | `lngdemo-sp-trader` | `a117908a-4a26-4083-9201-2263c2a3736d` |
| unentitled_trader | `lngdemo-trader-unentitled` | `lngdemo-sp-unentitled-trader` | `11c9197c-64d0-420c-a385-a70cd3cbc713` |
| developer | `lngdemo-developers` | `lngdemo-sp-developer` | `b54ef58a-d4e8-4476-a753-9e144def33a8` |
| operator | `lngdemo-operators` | `lngdemo-sp-operator` | `6624836c-d3f0-4773-97c0-c0d8a48614ec` |
| security | `lngdemo-security` | `lngdemo-sp-security` | `ec9d3941-9f92-4b76-bc20-0ae8bc46b09f` |
| auditor | `lngdemo-auditors` | `lngdemo-sp-auditor` | `9c18e29b-f757-42c6-827f-cb23d81fe4d8` |
| app | — (granted directly) | `lngdemo-sp-app` | `53e13554-29c7-40d1-80df-a8f3350826ae` |

Also add the human presenters to `lngdemo-trader-pilot` / `lngdemo-security` as needed for the app demo (M8).

Teardown: delete these SPs (`databricks service-principals delete <id>`) and the account groups after the demo.
