with final as (
    SELECT 
      JSON:id::string                           as ACTION_TIME_Id
      ,f.path                                   as Field_Name
      ,f.value                                  as Field_Value
      ,try_cast(f.value::string as boolean)     as Boolean_Value
      ,try_cast(f.value::string as timestamp)   as Datetime_Value
      ,try_cast(f.value::string as date)        as Date_Value
      ,try_cast(f.value::string as number)      as Number_Value
      ,f.value::string                          as String_Value
    FROM 
        SRC_ACTION_TIMES_RAW, 
        lateral flatten(JSON, recursive=>true) f
    WHERE 
        typeof(f.value) <> 'OBJECT'
        and f.path not like 'xform_ids%'
)

select 
	ACTION_TIME_ID,
	FIELD_NAME,
	FIELD_VALUE,
	BOOLEAN_VALUE,
	DATETIME_VALUE,
	DATE_VALUE,
	NUMBER_VALUE,
	STRING_VALUE
from final