with integration_table_data_config_case as (
      select * from  {{ source('integration_table_data', 'CONFIG_CASE_FIELDS') }}
), 
final as (
select 'case' source, case_type source_type, 'insert into <<dm_db>>.util.message_log (TASK_ID, EXECUTION_ID, TYPE, SUBTYPE, MESSAGE) ' ||
    'select <<task_id>> task_id, <<exec_id>> execution_id, \'SQLMessage\' type, \'DM_CASE_COUNT\' subtype, ' ||
    'object_construct(\'domain\', domain, \'case_type\', \'' || case_type || '\', \'count\', cnt::string) message ' ||
    'from (select domain, count(*) cnt from <<dm_db>>.DM.CASE_' || replace(upper(case_type),'-','_') || ' group by 1) c;' sql_text
from integration_table_data_config_case group by case_type
)

select
	SOURCE,
	SOURCE_TYPE,
	SQL_TEXT
from final