with integration_table_data_config_case as (
      select * from  {{ source('integration_table_data', 'CONFIG_CASE_FIELDS') }}
), 
final as (
select case_type, 
    'create or replace table <<dm_db>>.DM.CASE_' || replace(upper(case_type),'-','_') 
    || '(\n   DOMAIN\n   ,ID\n   ,LAST_UPDATED\n   ,' ||
    listagg(ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') || '\n) as \n' ||
    'select\n   DOMAIN\n   ,ID\n   ,LAST_UPDATED\n   ,' 
    || listagg(ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') || 
    '\nfrom <<dm_db>>.INTEGRATION.VW_CASE_' || replace(upper(case_type),'-','_') || '_ALL;' sql_text
from integration_table_data_config_case
where include_in_dim
group by case_type
)

select 
	CASE_TYPE,
	SQL_TEXT
from final