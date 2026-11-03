-- 02 · Row-filter and column-mask UDFs for the contract book (idempotent).
-- Desk entitlement: a caller sees a contract row only if one of their account groups is
-- mapped to the row's desk in desk_entitlements, or they are in the security group.
CREATE OR REPLACE FUNCTION `{{catalog}}`.`{{trading_schema}}`.desk_filter(row_desk STRING)
RETURNS BOOLEAN
COMMENT 'LNG demo: desk entitlement row filter'
RETURN is_account_group_member('{{security_group}}')
    OR EXISTS (
      SELECT 1 FROM `{{catalog}}`.`{{trading_schema}}`.desk_entitlements e
      WHERE e.desk = row_desk AND is_account_group_member(e.group_name)
    );

-- Raw price terms are visible only to the trader pilot group and security.
CREATE OR REPLACE FUNCTION `{{catalog}}`.`{{trading_schema}}`.mask_price_text(v STRING)
RETURNS STRING
COMMENT 'LNG demo: mask contract price formula'
RETURN CASE
  WHEN is_account_group_member('{{trader_group}}') OR is_account_group_member('{{security_group}}') THEN v
  ELSE '*** masked: contract price ***'
END;

CREATE OR REPLACE FUNCTION `{{catalog}}`.`{{trading_schema}}`.mask_price_num(v DOUBLE)
RETURNS DOUBLE
COMMENT 'LNG demo: mask numeric contract price terms'
RETURN CASE
  WHEN is_account_group_member('{{trader_group}}') OR is_account_group_member('{{security_group}}') THEN v
  ELSE NULL
END;
