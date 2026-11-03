-- Fallback for P2 if 01_deny_contract_terms is still rejected ("cannot be transpiled to CEL").
-- Same shape as the documented block_codenames example: request phase only, CONTAINS on
-- letters-only literals (no '%', '$', '/'), no OR on event:type. If this attaches and 01 doesn't,
-- the difference (output-phase OR / special characters) is the unsupported construct; record it
-- in docs/VERIFICATION.md.
CREATE OR REPLACE FUNCTION `{{catalog}}`.`{{services_schema}}`.deny_contract_terms_minimal(
  event VARIANT)
RETURNS VARIANT
LANGUAGE SQL
RETURN
  CASE
    WHEN event:type::string = 'request'
      AND (
            CONTAINS(LOWER(event:context.message::string), 'counterparty a')
         OR CONTAINS(LOWER(event:context.message::string), 'counterparty b')
         OR CONTAINS(LOWER(event:context.message::string), 'counterparty c')
         OR CONTAINS(LOWER(event:context.message::string), 'counterparty d')
         OR CONTAINS(LOWER(event:context.message::string), 'counterparty e')
         OR CONTAINS(LOWER(event:context.message::string), 'counterparty f')
         OR CONTAINS(LOWER(event:context.message::string), 'counterparty g')
         OR CONTAINS(LOWER(event:context.message::string), 'counterparty h')
         OR CONTAINS(LOWER(event:context.message::string), 'mmbtu')
         OR CONTAINS(LOWER(event:context.message::string), 'henry hub')
      )
    THEN to_variant_object(named_struct('result', 'DENY', 'reason', 'Blocked by deny_contract_terms_minimal: counterparty or contract price terms.'))
    ELSE to_variant_object(named_struct('result', 'ALLOW', 'reason', ''))
  END;
