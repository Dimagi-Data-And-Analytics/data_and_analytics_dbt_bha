with 
integration_table_data_config_web_user as (
      select * from  {{ source('integration_table_data', 'CONFIG_WEB_USER_FIELDS') }}
), 
final as (
select vw_type,
    'create or replace view <<dm_db>>.INTEGRATION.VW_WEB_USER_' || vt.vw_type 
    || '(\n   DOMAIN\n   ,ID\n   ,LAST_UPDATED\n   ,' || 
    listagg(ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') || '\n) as \n' ||
    'select\n   DOMAIN\n   ,ID\n   ,SYSTEM_QUERY_TS::datetime LAST_UPDATED\n   ,' || 
    listagg('nullif(JSON:' || field_name || '::string, \'\')::' || ifnull(data_type_override, field_data_type) || ' ' || ifnull(field_name_override, field_alias), '\n   ,') 
    || '\nfrom dl.src_web_users_raw' || max(vt.src_type)
    || (case when vw_type = 'STG' then '' else '' end) || ';' sql_text
from integration_table_data_config_web_user
full join (select $1 vw_type, $2 src_type from (values ('STG', '_STAGE'), ('ALL', ''))) vt
where include_in_dim
group by vw_type
)

select 
	VW_TYPE,
	SQL_TEXT
from final