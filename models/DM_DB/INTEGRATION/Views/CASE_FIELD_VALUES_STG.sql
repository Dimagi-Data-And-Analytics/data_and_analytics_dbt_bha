with final as (
    SELECT 
      JSON:id::string                           as Case_Id
      ,JSON:closed:: boolean                    as Closed
      ,JSON:closed_by::varchar                  as Closed_By
      ,JSON:date_closed::datetime               as Date_Closed
      ,JSON:date_modified::datetime             as Date_Modified
      ,JSON:domain::varchar                     as Domain
      ,JSON:indexed_on::datetime                as Indexed_On
      ,JSON:opened_by::varchar                  as Opened_By
      ,JSON:properties.case_type::string        as Case_Type
      ,f.path                                   as Field_Name
      ,f.value                                  as Field_Value
      ,try_cast(f.value::string as boolean)     as Boolean_Value
      ,try_cast(f.value::string as timestamp)   as Datetime_Value
      ,try_cast(f.value::string as date)        as Date_Value
      ,try_cast(f.value::string as number)      as Number_Value
      ,f.value::string                          as String_Value
    FROM 
        SRC_CASES_RAW_STAGE, 
        lateral flatten(JSON, recursive=>true) f
    WHERE 
        typeof(f.value) <> 'OBJECT'
        and f.path not like 'xform_ids%'
)

select
	CASE_ID,
	CLOSED,
	CLOSED_BY,
	DATE_CLOSED,
	DATE_MODIFIED,
	DOMAIN,
	INDEXED_ON,
	OPENED_BY,
	CASE_TYPE,
	FIELD_NAME,
	FIELD_VALUE,
	BOOLEAN_VALUE,
	DATETIME_VALUE,
	DATE_VALUE,
	NUMBER_VALUE,
	STRING_VALUE
from final