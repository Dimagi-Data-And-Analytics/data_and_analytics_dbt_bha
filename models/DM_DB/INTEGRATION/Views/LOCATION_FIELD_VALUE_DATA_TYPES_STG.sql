
  WITH Field_Types as (
    SELECT
      FIELD_NAME
      ,FIELD_VALUE
      ,CASE WHEN Boolean_Value  IS NOT NULL THEN 'Boolean'
            WHEN Datetime_Value IS NOT NULL and (hour(Datetime_Value)<>0 or minute(Datetime_Value)<>0 or second(Datetime_Value)<>0) THEN 'Datetime'
            WHEN Date_Value     IS NOT NULL THEN 'Date'
            WHEN Number_Value   IS NOT NULL THEN 'Number'
            ELSE 'Varchar' END as Value_Data_Type
    FROM
        Location_Field_Values_Stg
  )
  ,Field_Type_Count as (
    SELECT DISTINCT
      FIELD_NAME
      ,VALUE_DATA_TYPE
      ,count(*) OVER (PARTITION BY FIELD_NAME,VALUE_DATA_TYPE) as Data_Type_Count
      ,count(*) OVER (PARTITION BY FIELD_NAME)                 as Field_Count
      ,(Data_Type_Count/Field_Count*100)::decimal(5,2)                   as Data_Type_Perc
    FROM
        Field_Types
    WHERE
        NULLIF(FIELD_VALUE,'') is not null
  )
  ,Field_Distinct as (
    SELECT DISTINCT
      FIELD_NAME
    FROM
        Field_Types
  ), 
final as (
  SELECT 
      FD.FIELD_NAME, IFNULL(TC.VALUE_DATA_TYPE, 'Varchar') VALUE_DATA_TYPE,
      IFNULL(TC.Data_Type_Count, 0) Data_Type_Count, IFNULL(TC.Field_Count, 0) Field_Count, IFNULL(TC.Data_Type_Perc, 100.00) Data_Type_Perc
      ,CASE WHEN Dupes.FIELD_NAME IS NOT NULL THEN True ELSE False END::Boolean as Is_Dupe
  FROM Field_Distinct FD
      LEFT JOIN Field_Type_Count TC on TC.FIELD_NAME = FD.FIELD_NAME
      LEFT JOIN (
            SELECT
              FIELD_NAME
            FROM
                Field_Type_Count
            GROUP BY
                FIELD_NAME
            HAVING count(distinct VALUE_DATA_TYPE)>1
            ) as Dupes
          on TC.FIELD_NAME = Dupes.FIELD_NAME
  ORDER BY
      FIELD_NAME
      ,Data_Type_Count desc
)

select 
	FIELD_NAME,
	VALUE_DATA_TYPE,
	DATA_TYPE_COUNT,
	FIELD_COUNT,
	DATA_TYPE_PERC,
	IS_DUPE
from final