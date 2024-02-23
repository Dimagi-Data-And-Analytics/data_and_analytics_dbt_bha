with 
integration_table_data_config_case as (
      select * from  {{ source('integration_table_data', 'CONFIG_CASE_FIELDS') }}
), 
final as (
SELECT 
  CFNT.CASE_TYPE
  ,CFNT.FIELD_NAME
  ,CFNT.FIELD_ALIAS
  ,CFNT.FIELD_DATA_TYPE
  ,NULL::varchar            as Data_Type_Override
  ,NULL::varchar            as Field_Name_Override
  ,False::Boolean           as Include_In_Dim
  ,NULL::varchar			AS Tags
  ,CURRENT_TIMESTAMP        as Insert_Date
  ,CURRENT_USER::varchar    as Insert_By
  ,NULL::Datetime           as Update_Date
  ,NULL::varchar            as Update_By
FROM
    CASE_FIELD_NAMES_TYPES_STG CFNT
    LEFT JOIN integration_table_data_config_case CCF on CFNT.CASE_TYPE = CCF.CASE_TYPE and  CFNT.FIELD_NAME = CCF.FIELD_NAME
WHERE 
    NULLIF(CFNT.CASE_TYPE,'') is not null
    and CCF.FIELD_NAME is NULL
)

select 
	CASE_TYPE,
	FIELD_NAME,
	FIELD_ALIAS,
	FIELD_DATA_TYPE,
	DATA_TYPE_OVERRIDE,
	FIELD_NAME_OVERRIDE,
	INCLUDE_IN_DIM,
	TAGS,
	INSERT_DATE,
	INSERT_BY,
	UPDATE_DATE,
	UPDATE_BY
from final