{% macro create_phone_display() %}

CREATE OR REPLACE FUNCTION DM.PHONE_DISPLAY("PHONE" VARCHAR(16777216))
  RETURNS VARCHAR(16777216)
  LANGUAGE SQL
  AS '{{ phone_display() }}';

{% endmacro %}


/*
CREATE OR REPLACE FUNCTION DM_CO_CARE_COORD_DEV.DM.PHONE_DISPLAY("PHONE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
AS '
  CASE 
    WHEN phone = '''' OR phone IS NULL THEN NULL
    ELSE 
      ''('' || SUBSTR(REGEXP_REPLACE(phone, ''[^a-zA-Z0-9]'', ''''), 1, 3) || '') '' ||
      SUBSTR(REGEXP_REPLACE(phone, ''[^a-zA-Z0-9]'', ''''), 4, 3) || ''-'' ||
      SUBSTR(REGEXP_REPLACE(phone, ''[^a-zA-Z0-9]'', ''''), 7, 4)
  END
';
*/