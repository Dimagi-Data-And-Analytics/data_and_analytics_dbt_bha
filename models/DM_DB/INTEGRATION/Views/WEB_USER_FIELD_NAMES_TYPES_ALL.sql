with final as (
SELECT 
  FIELD_NAME
  ,REGEXP_REPLACE(
  REGEXP_REPLACE(
    IFNULL(
      REGEXP_SUBSTR(FIELD_NAME, '(\\.|\\[\')([^\']*)(\'\\])?', 1, 1, 'c', 2), 
      FIELD_NAME),
    '-|\\.|\\s','_'),
  '[^a-zA-Z0-9 _ \\$]','') FIELD_ALIAS
  ,FIELD_COUNT
  ,CASE WHEN DATA_TYPE_PERC >= 99 THEN VALUE_DATA_TYPE
        ELSE 'Varchar' END                              as Field_Data_Type
FROM (
    SELECT
      FIELD_NAME
      ,FIELD_COUNT
      ,VALUE_DATA_TYPE
      ,case when VALUE_DATA_TYPE = 'Varchar' then 1 
            when VALUE_DATA_TYPE = 'Number' then 2
            when VALUE_DATA_TYPE = 'Date' then 3
            when VALUE_DATA_TYPE = 'Datetime' then 4
            when VALUE_DATA_TYPE = 'Boolean' then 5
        else 6 end TYPE_SORT
      ,DATA_TYPE_PERC
      ,RANK() OVER (PARTITION BY FIELD_NAME ORDER BY DATA_TYPE_PERC, TYPE_SORT DESC) as VALUE_DATA_TYPE_RANK
    FROM WEB_USER_FIELD_VALUE_DATA_TYPES_ALL
    )
WHERE
    VALUE_DATA_TYPE_RANK = 1
)

select 
	FIELD_NAME,
	FIELD_ALIAS,
	FIELD_COUNT,
	FIELD_DATA_TYPE
from final