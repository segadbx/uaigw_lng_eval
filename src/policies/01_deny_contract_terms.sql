-- Custom SQL service policy: deny messages that carry contract-price terms or counterparty aliases.
-- Attach to trading_copilot in the UI (docs/MANUAL_STEPS.md, step P2).
--
-- Written to mirror the documented examples (policy-examples: block_codenames,
-- block_internal_links_in_response) construct for construct, because a first version using
-- COALESCE(...), LIKE '%...%', IN (...) on event:type and a COMMENT clause was rejected at attach
-- time in the demo workspace (2026-10-28) with "SQL cannot be transpiled to CEL".
--
-- Constraints (policy function reference, checked 2026-10-28):
--   * one `event VARIANT` parameter, RETURNS VARIANT, LANGUAGE SQL; body is transpiled to CEL;
--   * no regex: price detection is a fixed CONTAINS list (deliberately brittle, M5);
--   * accessing a missing field raises an error and errors fail closed (DENY). If a response has
--     no extractable message (e.g. tool calls only), this policy will deny it; that is the
--     documented fail-closed behaviour, recorded rather than worked around;
--   * custom SQL policies run on input AND output; both branches use the same terms so the model
--     cannot regurgitate them in a response.
CREATE OR REPLACE FUNCTION `{{catalog}}`.`{{services_schema}}`.deny_contract_terms(
  event VARIANT)
RETURNS VARIANT
LANGUAGE SQL
RETURN
  CASE
    WHEN (event:type::string = 'request' OR event:type::string = 'response')
      AND (
            CONTAINS(LOWER(event:context.message::string), 'counterparty a')
         OR CONTAINS(LOWER(event:context.message::string), 'counterparty b')
         OR CONTAINS(LOWER(event:context.message::string), 'counterparty c')
         OR CONTAINS(LOWER(event:context.message::string), 'counterparty d')
         OR CONTAINS(LOWER(event:context.message::string), 'counterparty e')
         OR CONTAINS(LOWER(event:context.message::string), 'counterparty f')
         OR CONTAINS(LOWER(event:context.message::string), 'counterparty g')
         OR CONTAINS(LOWER(event:context.message::string), 'counterparty h')
         OR CONTAINS(LOWER(event:context.message::string), 'usd/mmbtu')
         OR CONTAINS(LOWER(event:context.message::string), '$/mmbtu')
         OR CONTAINS(LOWER(event:context.message::string), 'per mmbtu')
         OR CONTAINS(LOWER(event:context.message::string), ' x hh')
         OR CONTAINS(LOWER(event:context.message::string), 'x henry hub')
         OR CONTAINS(LOWER(event:context.message::string), '% brent')
         OR CONTAINS(LOWER(event:context.message::string), '% hh')
      )
    THEN to_variant_object(named_struct('result', 'DENY', 'reason', 'Blocked by deny_contract_terms: the message references counterparty contract terms or a contract price. Use the desk summary tool for aggregates.'))
    ELSE to_variant_object(named_struct('result', 'ALLOW', 'reason', ''))
  END;
