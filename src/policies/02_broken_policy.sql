-- Deliberately broken policy for the fail-closed demo (M7). Attach ONLY to trading_copilot_dev
-- (docs/MANUAL_STEPS.md, step P5), run job lngdemo-gov-05-fail-closed, then DETACH it.
-- It reads a field that never exists in the event; per the reference, accessing a missing field
-- raises an error, and evaluation errors result in DENY (fail closed).
CREATE OR REPLACE FUNCTION `{{catalog}}`.`{{services_schema}}`.broken_policy_fail_closed(event VARIANT)
RETURNS VARIANT
LANGUAGE SQL
RETURN
  CASE
    WHEN event:context.this_field_does_not_exist.value::string = 'never'
    THEN to_variant_object(named_struct('result', 'ALLOW', 'reason', 'unreachable'))
    ELSE to_variant_object(named_struct('result', 'ALLOW', 'reason', 'should not be reached if the field access errors'))
  END;
