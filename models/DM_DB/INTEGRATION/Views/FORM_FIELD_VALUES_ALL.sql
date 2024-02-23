with 
integration_table_data_config_form as (
      select * from  {{ source('integration_table_data', 'CONFIG_FORM_FIELDS') }}
), 
final as (
   SELECT 
      JSON:id::string                                   as Form_Id
      ,Domain                                           as Domain
      ,JSON:form."@name"::varchar                       as form_Name
      ,JSON:form."@xmlns"::varchar                      as form_XMLNS
      ,JSON:server_modified_on::datetime                as Server_Modified_On
      ,cfv.FIELD_NAME                                   as FIELD_NAME
      ,cfv.FIELD_ALIAS                                  as FIELD_ALIAS
      ,f.path                                           as Field_Path
      ,f.key::varchar                                   as Key_Name
      ,f.value                                          as Field_Value
      ,try_cast(f.value::string as boolean)             as Boolean_Value
      ,try_cast(f.value::string as timestamp)           as Datetime_Value
      ,try_cast(f.value::string as date)                as Date_Value
      ,try_cast(f.value::string as number)              as Number_Value
      ,f.value::string                                  as String_Value
    FROM 
        SRC_FORMS_RAW fr
        ,lateral flatten(JSON, recursive=>true) f
        JOIN integration_table_data_config_form cfv on f.path like cfv.FIELD_PATTERN1
    WHERE 
        typeof(f.value) <> 'OBJECT'
        and f.path not like 'xform_ids%'
        and cfv.INCLUDE_IN_FORM_VALUES = TRUE
)

select 
	FORM_ID,
	DOMAIN,
	FORM_NAME,
	FORM_XMLNS,
	SERVER_MODIFIED_ON,
	FIELD_NAME,
	FIELD_ALIAS,
	FIELD_PATH,
	KEY_NAME,
	FIELD_VALUE,
	BOOLEAN_VALUE,
	DATETIME_VALUE,
	DATE_VALUE,
	NUMBER_VALUE,
	STRING_VALUE
from final