{% macro create_decimal_to_ts() %}

CREATE OR REPLACE SECURE FUNCTION DM.DECIMAL_TO_TS("DEC_TS" VARCHAR(16777216))
  RETURNS TIMESTAMP_NTZ(9)
  LANGUAGE SQL
  AS '{{ decimal_to_ts() }}';
  
{% endmacro %}


/*
CREATE OR REPLACE SECURE FUNCTION DM_CO_CARE_COORD_DEV.DM.DECIMAL_TO_TS("DEC_TS" VARCHAR(16777216))
RETURNS TIMESTAMP_NTZ(9)
LANGUAGE SQL
AS '
   convert_timezone(''UTC'',''America/Denver'', to_timestamp_ntz(dec_ts::decimal(38,5) * 86400))
';
*/