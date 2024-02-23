with integration_table_data_config_action_time as (
      select * from  {{ source('integration_table_data', 'CONFIG_ACTION_TIME_FIELDS') }}
), 
final as (
select 
    'merge into <<dm_db>>.DM.ACTION_TIME T using \n' ||
    '(select \n   DOMAIN\n   ,ID\n   ,LAST_UPDATED\n   ,' || 
    listagg(ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') ||
    ' from <<dm_db>>.INTEGRATION.VW_ACTION_TIME_STG) S \n' ||
    'on T.ID = S.ID when matched then update set \n   T.DOMAIN = S.DOMAIN\n   ,T.ID = S.ID\n   ,T.LAST_UPDATED = S.LAST_UPDATED\n   ,' || 
    listagg('T.' || ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS) || '= S.' || ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') || 
    '\nwhen not matched then insert(\n   DOMAIN\n   ,ID\n   ,LAST_UPDATED\n   ,' 
    || listagg(ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') || ') \n' || 
    'values (\n   S.DOMAIN\n   ,S.ID\n   ,S.LAST_UPDATED\n   ,' 
    || listagg('S.' || ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') || ');' sql_text
from integration_table_data_config_action_time
where include_in_dim
)

select 
	SQL_TEXT
from final