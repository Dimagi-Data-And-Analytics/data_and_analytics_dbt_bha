with 
util_table_data_execution as (
      select * from  {{ source('util_table_data', 'EXECUTION_LOG') }}
), 

util_table_data_message as (
      select * from  {{ source('util_table_data', 'MESSAGE_LOG') }}
), 

ingestion as 
(
    select s.task_id, ingestion_start, ingestion_end from
    ((select task_id, min(execution_start) ingestion_start from util_table_data_execution group by task_id) s left join 
    (select task_id, max(execution_start) ingestion_end from util_table_data_execution group by task_id) e on s.task_id = e.task_id)
),

DL_CASE_COUNT as (
    select 
        message_id, 
        message_log.task_id, 
        message_log.execution_id,
        ingestion.ingestion_start,
        ingestion.ingestion_end,
        message_log.subtype, 
        message_log.message:case_type::string as CASE_TYPE, 
        ifnull(message:total_count::int,0) as DL_CASE_COUNT
    from util_table_data_message message_log left join ingestion on ingestion.task_id = message_log.task_id
    where message_log.SUBTYPE = 'DL_CASE_COUNT'
    order by task_id, execution_id 
),

DL_ARCHIVE_COUNT as (
    select 
        message_id, 
        message_log.task_id, 
        message_log.execution_id,
        ingestion.ingestion_start,
        ingestion.ingestion_end,
        message_log.subtype, 
        message_log.message:case_type::string as CASE_TYPE, 
        ifnull(message_log.message:delete_count::int,0) as DELETE_CASE_COUNT
    from util_table_data_message message_log left join ingestion on ingestion.task_id = message_log.task_id
    where message_log.SUBTYPE = 'DL_CASE_COUNT'
    order by task_id, execution_id 
),

DM_CASE_COUNT as (

select message_id, 
        message_log.task_id, 
        message_log.execution_id,
        ingestion.ingestion_start,
        ingestion.ingestion_end,
        message_log.subtype, 
        message_log.message:case_type::string as CASE_TYPE, 
        ifnull(message:count::int,0) as DM_CASE_COUNT
from util_table_data_message message_log left join ingestion on ingestion.task_id = message_log.task_id
where SUBTYPE = 'DM_CASE_COUNT'), 

final as ( 
select 
    dl.task_id,
     convert_timezone('UTC', 'America/Denver',i.ingestion_start) as ingestion_start,
     convert_timezone('UTC', 'America/Denver',i.ingestion_end) as ingestion_end,
    dl.case_type,
    ifnull(dl.dl_case_count,0) as dl_case_count,
    ifnull(da.delete_case_count,0) as delete_case_count,
    ifnull(dmc.dm_case_count,0) as dm_case_count

from dl_case_count dl 
    left join dl_archive_count da on dl.task_id = da.task_id and dl.case_type = da.case_type
    left join dm_case_count dmc on dl.task_id = dmc.task_id and dl.case_type = dmc.case_type
    left join ingestion i on i.task_id = dl.task_id
)

select 
	TASK_ID,
	INGESTION_START,
	INGESTION_END,
	CASE_TYPE,
	DL_CASE_COUNT,
	DELETE_CASE_COUNT,
	DM_CASE_COUNT
from final