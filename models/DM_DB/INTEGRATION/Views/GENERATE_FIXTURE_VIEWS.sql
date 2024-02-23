with 
integration_table_data_config_fixture as (
      select * from  {{ source('integration_table_data', 'CONFIG_FIXTURE_FIELDS') }}
), 
final as (
select fixture_type, vw_type,
    'create or replace view <<dm_db>>.INTEGRATION.VW_FIXTURE_' || replace(upper(fixture_type),'-','_') || '_' || vt.vw_type 
    || '(\n   DOMAIN\n   ,ID\n   ,LAST_UPDATED\n   ,' || 
    listagg(ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') || '\n) as \n' ||
    'select\n   DOMAIN\n   ,ID\n   ,SYSTEM_QUERY_TS::datetime LAST_UPDATED\n   ,' || 
    listagg('nullif(JSON:' || field_name || '::string, \'\')::' || ifnull(data_type_override, field_data_type) || ' ' || ifnull(field_name_override, field_alias), '\n   ,') 
    || '\nfrom dl.src_fixtures_raw' || max(vt.src_type) || ' \nwhere JSON:fixture_type::string = \'' || fixture_type || '\'' 
    || (case when vw_type = 'STG' then '' else '' end) || ';' sql_text
from integration_table_data_config_fixture
full join (select $1 vw_type, $2 src_type from (values ('STG', '_STAGE'), ('ALL', ''))) vt
where include_in_dim
group by fixture_type, vw_type
)

select 
	FIXTURE_TYPE,
	VW_TYPE,
	SQL_TEXT
from final