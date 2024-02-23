with 
integration_table_data_config_case as (
      select * from  {{ source('integration_table_data', 'CONFIG_CASE_FIELDS') }}
), 
final as (
select case_type, vw_type,
    'create or replace view <<dm_db>>.INTEGRATION.VW_CASE_' || replace(upper(case_type),'-','_') || '_' || vt.vw_type 
    || '(\n   DOMAIN\n   ,ID\n   ,LAST_UPDATED\n   ,' || 
    (case when vw_type = 'STG' then '' else 'DELETED_IN_COMMCARE\n   ,ARCHIVED_FORM_DATETME\n   ,ARCHIVED_FORM_ID\n   ,' end) ||
    listagg(ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') || '\n) as \n' ||
    'select\n   DOMAIN\n   ,ID\n   ,SYSTEM_QUERY_TS::datetime LAST_UPDATED\n   ,' || 
    (case when vw_type = 'STG' then '' else 'DELETED_FLAG DELETED_IN_COMMCARE\n   ,DELETED_DATE::datetime ARCHIVED_FORM_DATETME\n   ,DELETED_FORMID ARCHIVED_FORM_ID\n   ,' end) 
    || listagg('nullif(JSON:' || field_name || '::string, \'\')::' || ifnull(data_type_override, field_data_type) || ' ' || ifnull(field_name_override, field_alias), '\n   ,') 
    || '\nfrom dl.src_cases_raw' || max(vt.src_type) || ' \nwhere JSON:properties.case_type::string = \'' || case_type || '\'' 
    || (case when vw_type = 'STG' then '' else ' and not deleted_flag' end) || ';' sql_text
from integration_table_data_config_case
full join (select $1 vw_type, $2 src_type from (values ('STG', '_STAGE'), ('ALL', ''))) vt
where include_in_dim
group by case_type, vw_type
)

select 
	CASE_TYPE,
	VW_TYPE,
	SQL_TEXT
from final