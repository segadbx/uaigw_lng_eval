-- Attaching a custom SQL policy needs EXECUTE on the function (create-service-policy docs).
GRANT EXECUTE ON FUNCTION `{{catalog}}`.`{{services_schema}}`.deny_contract_terms TO `{{security_group}}`;
GRANT EXECUTE ON FUNCTION `{{catalog}}`.`{{services_schema}}`.require_desk_tag TO `{{security_group}}`;
GRANT EXECUTE ON FUNCTION `{{catalog}}`.`{{services_schema}}`.deny_contract_terms_minimal TO `{{security_group}}`;
GRANT EXECUTE ON FUNCTION `{{catalog}}`.`{{services_schema}}`.broken_policy_fail_closed TO `{{security_group}}`;
GRANT EXECUTE ON FUNCTION `{{catalog}}`.`{{services_schema}}`.broken_policy_fail_closed TO `{{developer_group}}`;
