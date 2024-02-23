with final as (
select 'action_times' source, null::string source_type, 'insert into <<dm_db>>.util.message_log (TASK_ID, EXECUTION_ID, TYPE, SUBTYPE, MESSAGE) ' ||
    'select <<task_id>> task_id, <<exec_id>> execution_id, \'SQLMessage\' type, \'DM_ACTION_TIME_COUNT\' subtype, ' ||
    'object_construct(\'domain\', domain, \'count\', cnt::string) message ' ||
    'from (select domain, count(*) cnt from <<dm_db>>.DM.ACTION_TIME group by 1) c;' sql_text
)

select
	SOURCE,
	SOURCE_TYPE,
	SQL_TEXT
from final