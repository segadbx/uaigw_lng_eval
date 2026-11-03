-- 06 · Object grants inside bundle schemas (idempotent).
GRANT SELECT ON TABLE `{{catalog}}`.`{{trading_schema}}`.contracts TO `{{trader_group}}`;
GRANT SELECT ON TABLE `{{catalog}}`.`{{trading_schema}}`.contracts TO `{{unentitled_trader_group}}`;
GRANT EXECUTE ON FUNCTION `{{catalog}}`.`{{services_schema}}`.desk_price_summary TO `{{trader_group}}`;
GRANT EXECUTE ON FUNCTION `{{catalog}}`.`{{services_schema}}`.desk_price_summary TO `{{unentitled_trader_group}}`;
GRANT EXECUTE ON FUNCTION `{{catalog}}`.`{{services_schema}}`.desk_price_summary TO `{{app_sp_application_id}}`;
-- Gold aggregate (no prices, small cells suppressed) for traders and operators.
GRANT SELECT ON TABLE `{{catalog}}`.`{{trading_schema}}`.desk_exposure TO `{{trader_group}}`;
GRANT SELECT ON TABLE `{{catalog}}`.`{{trading_schema}}`.desk_exposure TO `{{unentitled_trader_group}}`;
GRANT SELECT ON TABLE `{{catalog}}`.`{{trading_schema}}`.desk_exposure TO `{{operator_group}}`;
