-- Optional attribution policy (M3/M8): require the `lngdemo_desk` request tag on input.
-- Request tags arrive in event:context.request_tags from the Databricks-Ai-Gateway-Request-Tags
-- header (JSON). Tags are caller-supplied and do NOT establish identity or permission.
-- Attach in Log mode first (docs/MANUAL_STEPS.md, step P6).
CREATE OR REPLACE FUNCTION `{{catalog}}`.`{{services_schema}}`.require_desk_tag(event VARIANT)
RETURNS VARIANT
LANGUAGE SQL
RETURN
  CASE
    WHEN event:type::string = 'response'
    THEN to_variant_object(named_struct('result', 'ALLOW', 'reason', ''))
    WHEN event:type::string = 'request'
      AND event:context.request_tags.lngdemo_desk::string IN ('atlantic', 'pacific', 'portfolio')
    THEN to_variant_object(named_struct('result', 'ALLOW', 'reason', ''))
    ELSE to_variant_object(named_struct('result', 'DENY', 'reason', 'Request tag lngdemo_desk (atlantic, pacific, portfolio) is required.'))
  END;
