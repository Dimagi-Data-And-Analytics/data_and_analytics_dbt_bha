  select * from DM_CO_CARE_COORD_DEV.util.task_log order by task_id desc;
  select * from DM_CO_CARE_COORD_DEV.util.execution_log order by task_id desc;
  // 1758 2215
  //desc table DM_CO_CARE_COORD_DEV.util.execution_log;  
  select * from DM_CO_CARE_COORD_DEV.util.sql_logs order by sql_log_id desc;
  select * from DM_CO_CARE_COORD_DEV.util.message_log order by task_id desc;

  select count(*) from DATALAKE_DEV.CO_CARE_COORDINATION_COMMCARE_DEV.cases_raw_stage order by system_query_ts desc;
  select * from DATALAKE_DEV.CO_CARE_COORDINATION_COMMCARE_DEV.cases_raw where json like '%Mar 30, 2024%' order by system_query_ts desc;
    select MAP_COORDINATES from DM_CO_CARE_COORD_DEV.DM.case_clinic where case_id='4590e9b9-eedc-4b10-85e8-1815543aad03';
  select * from DATALAKE_DEV.CO_CARE_COORDINATION_COMMCARE_DEV.action_times_raw order by system_query_ts desc;  
  select * from DATALAKE_DEV.CO_CARE_COORDINATION_COMMCARE_DEV.locations_raw order by system_query_ts desc; 
  // 9 -> 277
  select * from DATALAKE_DEV.CO_CARE_COORDINATION_COMMCARE_DEV.fixtures_raw order by system_query_ts desc; 
  select * from DATALAKE_DEV.CO_CARE_COORDINATION_COMMCARE_DEV.web_users_raw order by system_query_ts desc; 
  select * from DATALAKE_DEV.CO_CARE_COORDINATION_COMMCARE_DEV.forms_raw order by system_query_ts desc; 

  select * from DM_CO_CARE_COORD_DEV.DM.location where location_id='d7f997cb4a414510b2ac58e988c0c6bf' order by last_updated desc;
    
  select * from DATALAKE_DEV.CO_CARE_COORDINATION_COMMCARE_DEV.VW_LAST_TS_BY_DOMAIN_SOURCE where source_domain='co-carecoordination-dev';
  // /2023/10/25/16/
  
select * from DM_CO_CARE_COORD_DEV.util.sql_logs where execution_status <> 'SUCCESS' order by start_time desc;

//select * from "DM_CO_CARE_COORD_DEV"."UTIL"."SQL_LOGS" where run_id in (select run_id from "DM_CO_CARE_COORD_QA"."UTIL"."TALEND_RUNS") and execution_status <> 'SUCCESS' order by start_time desc;

//select * from "DM_CO_CARE_COORD_DEV"."UTIL"."SQL_LOGS" where execution_status <> 'SUCCESS' order by start_time desc;

select v.*, c.field_data_type from DM_CO_CARE_COORD_DEV.INTEGRATION.CASE_FIELD_VALUES_ALL v left join INTEGRATION.CONFIG_CASE_FIELDS c on c.case_type = v.case_type and c.field_name = v.field_name
where v.field_value is not null and v.field_value<>'' and (
    ifnull(c.data_type_override, c.field_data_type) = 'Date' and v.date_value is null or
    ifnull(c.data_type_override, c.field_data_type) = 'Datetime' and v.datetime_value is null or
    ifnull(c.data_type_override, c.field_data_type) = 'Boolean' and v.boolean_value is null or
    ifnull(c.data_type_override, c.field_data_type) = 'Number' and v.number_value is null)
;

// procedure call 
//  set sql_temp = 'select SQL_TEXT from DM_ETL_REPLACE_TEST_BHA_DEV.INTEGRATION.generate_case_table_delete_updates where vw_type = \'STG\';';
//  Call metadata.procedures.sp_ingest_transform('INITIAL_LOAD', 'GENERATE_ACTION_TIME_VIEWS', 'co-carecoordination-dev', 'DATALAKE_DEV', 'ETL_REPLACE_TEST_BHA_DEV', 'DM_ETL_REPLACE_TEST_BHA_DEV', $sql_temp, null);
  
 //   Call metadata.procedures.sp_ingest_transform('TEST_LOAD', 'GENERATE_ACTION_TIME_VIEWS', 'co-carecoordination-dev', 'DATALAKE_DEV', 'ETL_REPLACE_TEST_BHA_DEV', 'DM_ETL_REPLACE_TEST_BHA_DEV', 'select sql_text from DM_ETL_REPLACE_TEST_BHA_DEV.INTEGRATION.generate_action_time_table_incr_load;', null);
  
//  Call metadata.procedures.sp_ingest_transform('TEST_LOAD', 'GENERATE_ACTION_TIME_VIEWS', 'co-carecoordination-dev', 'DATALAKE_DEV', 'ETL_REPLACE_TEST_BHA_DEV', 'DM_ETL_REPLACE_TEST_BHA_DEV', 'select sql_text from DM_ETL_REPLACE_TEST_BHA_DEV.INTEGRATION.count_dm_cases;', null);

//Call metadata.procedures.sp_ingest_transform('DATA_CONFIG_UPDATE', 'manual_call_sp_ingest_transform', 'co-carecoordination-dev', 'DATALAKE_DEV', 'CO_CARE_COORDINATION_COMMCARE_DEV', 'DM_CO_CARE_COORD_DEV', 'UPDATE_CONFIG|RECREATE_VIEWS|RECREATE_TABLES|', null);

//Call metadata.procedures.sp_ingest_transform('S3_DATA_LOAD', 'manual_call_sp_ingest_transform', 'co-carecoordination-dev', 'DATALAKE_DEV', 'CO_CARE_COORDINATION_COMMCARE_DEV', 'DM_CO_CARE_COORD_DEV', 'S3_INGEST|STAGE_TO_RAW|INCR_TABLES|STAGE_DELETE|', '/2023/10/23/03/');

//Call metadata.procedures.sp_ingest_transform('S3_DATA_LOAD', 'manual_call_sp_ingest_transform', 'co-carecoordination-dev', 'DATALAKE_DEV', 'CO_CARE_COORDINATION_COMMCARE_DEV', 'DM_CO_CARE_COORD_DEV', 'S3_INGEST|', '/2023/10/10/11/');


//CALL METADATA.PROCEDURES.sp_s3_ingest_test('DATA_LOAD', 'RAW_STAGE', 'co-carecoordination-dev', 'DATALAKE_DEV', 'ETL_REPLACE_TEST_BHA_DEV', 'DM_ETL_REPLACE_TEST_BHA_DEV', '561', null);

 
------
-- bha testing --

select count(*) from DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.fixtures_raw order by system_query_ts desc;

select count(*) from DM_ETL_REPLACE_TEST_BHA_DEV.DL.fixtures_raw order by system_query_ts desc;

select count(*) from DM_ETL_REPLACE_TEST_BHA_DEV.DL.src_fixtures_raw order by system_query_ts desc;

//select * from DM_ETL_REPLACE_TEST_BHA_DEV.DM.ACTION_TIME order by last_updated desc;

// table diff
select domain,json from DM_CO_CARE_COORD_DEV.DL.SRC_LOCATIONS_RAW 
except
select domain,json from DM_CO_CARE_COORD_DEV_CLONE.DL.SRC_LOCATIONS_RAW;

select * exclude last_updated, created_at from DM_CO_CARE_COORD_DEV.DM.location
except
select * exclude last_updated, created_at from DM_CO_CARE_COORD_DEV_CLONE.DM.location;


select domain,json from DATALAKE_DEV.CO_CARE_COORDINATION_COMMCARE_DEV.fixtures_raw 
except
select domain,json from DM_CO_CARE_COORD_DEV.DL.fixtures_raw;

select * exclude last_updated from DM_CO_CARE_COORD_DEV.DM.FIXTURE_ACCESSIBILITY
except
select * exclude last_updated from DM_CO_CARE_COORD_DEV_CLONE.DM.FIXTURE_ACCESSIBILITY;

select domain, id, value, fixture_type from DM_CO_CARE_COORD_DEV.DM.FIXTURE_BHE_POPULATION_ENDORSEMENTS
except
select domain, id, value, fixture_type from DM_CO_CARE_COORD_DEV_CLONE.DM.FIXTURE_BHE_POPULATION_ENDORSEMENTS;

select domain, id, active_location_id, capacity_registry_location_id, central_registry_location_id, clinic_users_location_id, clinics_location_id, commcare_project, county_sort, inactive_location_id, inactive_registry_location_id, fixture_type from DM_CO_CARE_COORD_DEV.DM.FIXTURE_DOMAINS 
except
select domain, id, active_location_id, capacity_registry_location_id, central_registry_location_id, clinic_users_location_id, clinics_location_id, commcare_project, county_sort, inactive_location_id, inactive_registry_location_id, fixture_type from DM_CO_CARE_COORD_DEV_CLONE.DM.FIXTURE_DOMAINS;

-- for payload view

select * from DM_ETL_REPLACE_TEST_BHA_DEV.DM.VW_CLIENT_DUPLICATES_API_PAYLOAD
except
select * from DM_CO_CARE_COORD_DEV.DM.VW_CLIENT_DUPLICATES_API_PAYLOAD;

// end of table diff

// mismatch case type
select * from DM_CO_CARE_COORD_DEV.INTEGRATION.CONFIG_CASE_FIELDS where field_name like '%longitude%';
select * from DM_CO_CARE_COORD_DEV.INTEGRATION.CONFIG_CASE_FIELDS where field_name like '%properties.escalate_referral%';

update DM_CO_CARE_COORD_DEV.INTEGRATION.CONFIG_CASE_FIELDS set DATA_TYPE_OVERRIDE = 'Varchar' where field_name = 'properties.request_info_email' and case_type='message';


select case_type, lower(ifnull(field_name_override, field_alias)) field_alias2, count(*) 
from INTEGRATION.CONFIG_CASE_FIELDS where include_in_dim group by 1,2 having count(*)>1 or field_alias2 in ('domain','id');
//update DM_CO_CARE_COORD_DEV.INTEGRATION.CONFIG_CASE_FIELDS set field_name_override = 'parent_relationship2' where field_name = 'indices.parent.relationship';


select original_licensure_date from DM_CO_CARE_COORD_DEV.DM.case_clinic where case_id='e7ac4a68acbf4622984e5464a63c49c9';
select original_licensure_date from DM_CO_CARE_COORD_DEV.INTEGRATION.VW_CASE_CLINIC_STG where 
case_id='e7ac4a68acbf4622984e5464a63c49c9';

select * from DM_CO_CARE_COORD_DEV.DM.case_clinic where case_id='e7ac4a68acbf4622984e5464a63c49c9';

desc view DM_CO_CARE_COORD_DEV.INTEGRATION.VW_CASE_CLINIC_STG;
desc table DM_CO_CARE_COORD_DEV.DM.CASE_CLINIC;


// end of mismatch case type


// show views
use database DM_CO_CARE_COORD_DEV;
use schema DM;
show views;



//select * from table(information_schema.query_history_by_session(RESULT_LIMIT => 10000)) where execution_status<>'RUNNING' order by start_time limit 100;

//alter table DM_ETL_REPLACE_TEST_BHA_DEV.util.sql_logs add column
//    QUERY_HASH VARCHAR(16777216),
//    QUERY_HASH_VERSION NUMBER(38,0),
//    QUERY_PARAMETERIZED_HASH VARCHAR(16777216),
//    QUERY_PARAMETERIZED_HASH_VERSION NUMBER(38,0);

select * from DM_CO_CARE_COORD_DEV.UTIL.sql_job_step;

select C.case_id, C.case_name, C.display_name, C.account_name, L.name from DM_CO_CARE_COORD_DEV.DM.CASE_CLINIC as C
join DM_CO_CARE_COORD_DEV.DM.LOCATION as L
on C.owner_id = L.id
where l.name != c.display_name
and l.id != '6fc5b9a0b5eb477f81fcd9cb0942cd12';

select * from DM_CO_CARE_COORD_DEV.DM.CASE_CLINIC where
case_name='Set Apart Treatment Inc'
and closed=false
;

select c.case_id, c.external_id, l.site_code from DM_CO_CARE_COORD_DEV.DM.CASE_CLINIC as c
left outer join DM_CO_CARE_COORD_DEV.DM.LOCATION as l
on c.external_id = l.site_code
where l.site_code is NULL
and c.external_id is not null
and c.closed = false;