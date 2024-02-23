with integration_table_data_config_web_user as (
      select * from  {{ source('integration_table_data', 'CONFIG_WEB_USER_FIELDS') }}
), 
final as (
select  
    'create or replace table <<dm_db>>.DM.WEB_USER'
    || '(\n   DOMAIN\n   ,ID\n   ,LAST_UPDATED\n   ,' ||
    listagg(ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') || '\n) as \n' ||
    'select\n   DOMAIN\n   ,ID\n   ,LAST_UPDATED\n   ,' 
    || listagg(ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') || 
    '\nfrom <<dm_db>>.INTEGRATION.VW_WEB_USER_ALL;' sql_text
from integration_table_data_config_web_user
where include_in_dim
)

select 
	SQL_TEXT
from final