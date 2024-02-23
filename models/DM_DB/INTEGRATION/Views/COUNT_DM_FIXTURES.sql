with integration_table_data_config_fixture as (
      select * from  {{ source('integration_table_data', 'CONFIG_FIXTURE_FIELDS') }}
), 
final as (
select 'fixture' source, fixture_type source_type, 'insert into <<dm_db>>.util.message_log (TASK_ID, EXECUTION_ID, TYPE, SUBTYPE, MESSAGE) ' ||
    'select <<task_id>> task_id, <<exec_id>> execution_id, \'SQLMessage\' type, \'DM_FIXTURE_COUNT\' subtype, ' ||
    'object_construct(\'domain\', domain, \'fixture_type\', \'' || fixture_type || '\', \'count\', cnt::string) message ' ||
    'from (select domain, count(*) cnt from <<dm_db>>.DM.FIXTURE_' || replace(upper(fixture_type),'-','_') || ' group by 1) c;' sql_text
from integration_table_data_config_fixture group by fixture_type
)

select 
	SOURCE,
	SOURCE_TYPE,
	SQL_TEXT
from final