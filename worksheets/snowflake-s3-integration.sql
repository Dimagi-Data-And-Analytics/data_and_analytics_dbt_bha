use role accountadmin;
// the following can only be created by accountadmin
create or replace storage integration s3_int_obj  
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE 
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::936263849005:role/snowflake-s3-access-role'
  STORAGE_ALLOWED_LOCATIONS = (
      's3://commcare-snowflake-data-sync/co-carecoordination-dev/', 
      's3://commcare-snowflake-data-sync/co-carecoordination-uat/', 
      's3://commcare-snowflake-data-sync/co-carecoordination/', 
      's3://commcare-snowflake-data-sync/navajo-covid19-staging/', 
      's3://commcare-snowflake-data-sync/hw-covid-response-team/')
   COMMENT = 'storage integration object for aws s3';


alter storage integration s3_int_obj set STORAGE_ALLOWED_LOCATIONS = (
      's3://commcare-snowflake-data-sync/bha-location-redesign-1/', 
      's3://commcare-snowflake-data-sync/staging-co-carecoordination-test/', 
      's3://commcare-snowflake-data-sync/co-carecoordination-test/', 
      's3://commcare-snowflake-data-sync/co-carecoordination-perf/', 
      's3://commcare-snowflake-data-sync/co-carecoordination-dev/', 
      's3://commcare-snowflake-data-sync/co-carecoordination-uat/', 
      's3://commcare-snowflake-data-sync/co-carecoordination/', 
      's3://commcare-snowflake-data-sync/navajo-covid19-staging/', 
      's3://commcare-snowflake-data-sync/chw-covid-response-team/');

DESC integration s3_int_obj;

grant usage on integration s3_int_obj to role sysadmin;

// initial create db in DL and DM --> this should be done in script MAIN-SCRIPT-DEV
use database DATALAKE_DEV;
use role sysadmin;
CREATE schema IF NOT EXISTS BHA_LOCATION_REDESIGN_1;

use role sysadmin;
CREATE database IF NOT EXISTS DM_BHA_LOCATION_REDESIGN_1;
// end initial




use role sysadmin;
//CREATE OR REPLACE schema DM_ETL_REPLACE_TEST_BHA_DEV.file_formats;
//use schema DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV;
CREATE OR REPLACE file format DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.json_file_format
	type=JSON TIME_FORMAT=AUTO;
    

//CREATE OR REPLACE schema DM_ETL_REPLACE_TEST_BHA_DEV.external_stages;
/*
CREATE OR REPLACE STAGE DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.s3_json_stage
    url='s3://commcare-snowflake-data-sync/co-carecoordination-dev/snowflake-copy/case-test/2023/07/27/'
    Storage_integration = s3_int_obj
    file_format = DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.json_file_format;
*/
//set mypattern = '\.*'||to_char(current_date(), 'YYYY-MM-DD')||'\.*';
//set s3_path_case = 's3://commcare-snowflake-data-sync/co-carecoordination-dev/snowflake-copy/case-test/2023/08/17/16/';
set s3_path_root = 's3://commcare-snowflake-data-sync/co-carecoordination-dev/snowflake-copy/';
set s3_path_case = 's3://commcare-snowflake-data-sync/co-carecoordination-dev/snowflake-copy/case-test/'||to_char(dateadd('HH', -1, sysdate()), 'YYYY/MM/DD/HH')||'/';
set s3_path_test = 's3://commcare-snowflake-data-sync/co-carecoordination-dev/snowflake-copy/case-test/'||to_char(dateadd('HH', -1, sysdate()), 'YYYY/MM/DD/HH')||'/';
set ss =  to_char(sysdate(), '/YYYY/MM/DD/HH/');
//set stest = '2023-10-12 17:59:11.332000000Z';
//set ss =  to_char('2023-10-12 17:59:11.332000000Z', '/YYYY/MM/DD/HH/');
show variables;
/*
set s3_path = 's3://commcare-snowflake-data-sync/co-carecoordination-dev/snowflake-copy/';
set s3_path = 's3://commcare-snowflake-data-sync/co-carecoordination-dev/snowflake-copy/case/'||to_char(current_timestamp(), 'YYYY/MM/DD/HH')||'/';
set s3_path = 's3://commcare-snowflake-data-sync/co-carecoordination-dev/snowflake-copy/web-user/'||to_char(current_timestamp(), 'YYYY/MM/DD/HH')||'/';
set s3_path = 's3://commcare-snowflake-data-sync/co-carecoordination-dev/snowflake-copy/action-time/'||to_char(current_timestamp(), 'YYYY/MM/DD/HH')||'/';
*/
//set s3_path = 's3://commcare-snowflake-data-sync/co-carecoordination-dev/snowflake-copy/case-test/'||to_char(sysdate(), 'YYYY/MM/DD/HH')||'/';
set s3_path_form = 's3://commcare-snowflake-data-sync/co-carecoordination-dev/snowflake-copy/form-test/'||to_char(dateadd('HH', -1, sysdate()), 'YYYY/MM/DD/HH')||'/';
set s3_path_fixture = 's3://commcare-snowflake-data-sync/co-carecoordination-dev/snowflake-copy/fixture-test/'||to_char(dateadd('HH', -1, sysdate()), 'YYYY/MM/DD/HH')||'/';
set s3_path_location = 's3://commcare-snowflake-data-sync/co-carecoordination-dev/snowflake-copy/location-test/'||to_char(dateadd('HH', -1, sysdate()), 'YYYY/MM/DD/HH')||'/';
set s3_path_web_user = 's3://commcare-snowflake-data-sync/co-carecoordination-dev/snowflake-copy/web-user-test/'||to_char(dateadd('HH', -1, sysdate()), 'YYYY/MM/DD/HH')||'/';
set s3_path_action_times = 's3://commcare-snowflake-data-sync/co-carecoordination-dev/snowflake-copy/action_times-test/'||to_char(dateadd('HH', -1, sysdate()), 'YYYY/MM/DD/HH')||'/';
show variables;

CREATE OR REPLACE STAGE DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.s3_json_stage
    url=$s3_path_root
    Storage_integration = s3_int_obj
    file_format = DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.json_file_format;


CREATE OR REPLACE STAGE DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.s3_json_stage_daily_case
    url=$s3_path_case
    Storage_integration = s3_int_obj
    file_format = DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.json_file_format;
CREATE OR REPLACE STAGE DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.s3_json_stage_daily_form
    url=$s3_path_form
    Storage_integration = s3_int_obj
    file_format = DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.json_file_format;
CREATE OR REPLACE STAGE DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.s3_json_stage_daily_fixture
    url=$s3_path_fixture
    Storage_integration = s3_int_obj
    file_format = DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.json_file_format;
CREATE OR REPLACE STAGE DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.s3_json_stage_daily_location
    url=$s3_path_location
    Storage_integration = s3_int_obj
    file_format = DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.json_file_format;
CREATE OR REPLACE STAGE DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.s3_json_stage_daily_web_user
    url=$s3_path_web_user
    Storage_integration = s3_int_obj
    file_format = DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.json_file_format;
CREATE OR REPLACE STAGE DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.s3_json_stage_daily_action_times
    url=$s3_path_action_times
    Storage_integration = s3_int_obj
    file_format = DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.json_file_format;
    
select * from table(DESC STAGE s3_json_stage);
select GET_STAGE_LOCATION(@s3_json_stage);
LIST @s3_json_stage pattern = '.*-test/2023/09/.*';

LIST @s3_json_stage pattern = '.*/2023/09/06/16/.*';
s3://commcare-snowflake-data-sync/co-carecoordination-dev/snowflake-copy/web-user-test/2023/09/06/18/
LIST @s3_json_stage;
LIST @s3_json_stage_daily_case;
LIST @s3_json_stage_daily_form;
LIST @s3_json_stage_daily_fixture;
LIST @s3_json_stage_daily_location;
LIST @s3_json_stage_daily_web_user;
LIST @s3_json_stage_daily_action_times;
LIST @DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.s3_json_stage;
select * from @DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.s3_json_stage;
LIST @DATALAKE_DEV.BHA_LOCATION_REDESIGN_1.s3_json_stage_daily_location;

/*
CREATE or REPLACE table DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.json_raw_test 
    (raw_file variant);

COPY INTO DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.json_raw_test
    FROM @s3_json_stage
    file_format= DM_ETL_REPLACE_TEST_BHA_DEV.file_formats.json_file_format
    //files = (xx.json)
    ON_ERROR = CONTINUE;
*/

// case
TRUNCATE DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.CASES_RAW_STAGE;
//TRUNCATE DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.CASES_RAW;

insert into DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.CASES_RAW_STAGE(domain, metadata, metadata_filename, json, id, SYSTEM_QUERY_TS, task_id, execution_id)
select
 f.value:domain::string domain,
 s.$1:meta metadata,
 metadata$filename metadata_filename,
 f.value JSON,
 f.value:case_id::string id,
 replace(split_part(metadata_filename, '_', -1), '.json') SYSTEM_QUERY_TS,
 1 task_id,
 1 execution_id
from @DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.s3_json_stage_daily_case s, lateral flatten(s.$1:objects) f
qualify row_number() over (partition by id order by metadata$file_last_modified desc) = 1
;

select * from datalake_dev.INFORMATION_SCHEMA.load_history where error_count>0;
  
select count(*) from DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.CASES_RAW_STAGE;
select * from DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.CASES_RAW order by task_id desc;
select count(*) from DM_ETL_REPLACE_TEST_BHA_DEV.DM.case_clinic where date(indexed_on)>DATEADD(days, -60, CURRENT_DATE) order by indexed_on;
//select count(*) from DATALAKE_DEV.CO_CARE_COORDINATION_COMMCARE_DEV.CASES_RAW_STAGE;

// end case

// form
TRUNCATE DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.FORMS_RAW_STAGE;

insert into DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.FORMS_RAW_STAGE(domain, metadata, metadata_filename, json, id, SYSTEM_QUERY_TS, task_id, execution_id)
select
 f.value:domain::string domain,
 s.$1:meta metadata,
 metadata$filename metadata_filename,
 f.value JSON,
 f.value:id::string id,
 replace(split_part(metadata_filename, '_', -1), '.json') SYSTEM_QUERY_TS,
 1 task_id,
 1 execution_id
from @DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.s3_json_stage_daily_form s, lateral flatten(s.$1:objects) f
qualify row_number() over (partition by id order by metadata$file_last_modified desc) = 1
;

select * from DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.FORMS_RAW order by task_id desc;;
// end form

// fixture
TRUNCATE DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.FIXTURES_RAW_STAGE;

insert into DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.FIXTURES_RAW_STAGE(metadata, metadata_filename, domain, json, id, SYSTEM_QUERY_TS, task_id, execution_id)
select
 //f.value:domain::string domain,
 s.$1:meta metadata,
 metadata$filename metadata_filename,
 replace(split_part(metadata_filename, '/', 1), '.json') domain,
 f.value JSON,
 f.value:id::string id,
 replace(split_part(metadata_filename, '_', -1), '.json') SYSTEM_QUERY_TS,
 1 task_id,
 1 execution_id
from @DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.s3_json_stage_daily_fixture s, lateral flatten(s.$1:objects) f
qualify row_number() over (partition by id order by metadata$file_last_modified desc) = 1
;

select * from DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.FIXTURES_RAW order by task_id desc;;
select count(*) from DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.FIXTURES_RAW_STAGE;
// end fixture

// location
TRUNCATE DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.LOCATIONS_RAW_STAGE;

insert into DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.LOCATIONS_RAW_STAGE(domain, metadata, metadata_filename, json, id, SYSTEM_QUERY_TS, task_id, execution_id)
select
 f.value:domain::string domain,
 s.$1:meta metadata,
 metadata$filename metadata_filename,
 f.value JSON,
 f.value:id::string id,
 replace(split_part(metadata_filename, '_', -1), '.json') SYSTEM_QUERY_TS,
 1 task_id,
 1 execution_id
from @DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.s3_json_stage_daily_location s, lateral flatten(s.$1:objects) f
qualify row_number() over (partition by id order by metadata$file_last_modified desc) = 1
;

select * from DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.LOCATIONS_RAW order by task_id desc;;
select count(*) from DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.LOCATIONS_RAW_STAGE;
// end location

// web users
TRUNCATE DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.WEB_USERS_RAW_STAGE;

insert into DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.WEB_USERS_RAW_STAGE(metadata, metadata_filename, domain, json, id, SYSTEM_QUERY_TS, task_id, execution_id)
select
 //f.value:domain::string domain,
 s.$1:meta metadata,
 metadata$filename metadata_filename,
 replace(split_part(metadata_filename, '/', 1), '.json') domain,
 f.value JSON,
 f.value:id::string id,
 replace(split_part(metadata_filename, '_', -1), '.json') SYSTEM_QUERY_TS,
 1 task_id,
 1 execution_id
from @DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.s3_json_stage_daily_web_user s, lateral flatten(s.$1:objects) f
qualify row_number() over (partition by id order by metadata$file_last_modified desc) = 1
;

select * from DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.WEB_USERS_RAW order by task_id desc;;
select count(*) from DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.WEB_USERS_RAW_STAGE;
// end web users

// action times
TRUNCATE DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.ACTION_TIMES_RAW_STAGE;

insert into DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.ACTION_TIMES_RAW_STAGE(metadata, metadata_filename, domain, json, id, SYSTEM_QUERY_TS, task_id, execution_id)
select
 //f.value:domain::string domain,
 s.$1:meta metadata,
 metadata$filename metadata_filename,
 replace(split_part(metadata_filename, '/', 1), '.json') domain,
 f.value JSON,
 f.value:user_id::string id,
 replace(split_part(metadata_filename, '_', -1), '.json') SYSTEM_QUERY_TS,
 1 task_id,
 1 execution_id
from @DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.s3_json_stage_daily_action_times s, lateral flatten(s.$1:objects) f
qualify row_number() over (partition by id order by metadata$file_last_modified desc) = 1
;

select * from DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.ACTION_TIMES_RAW order by task_id desc;;
select count(*) from DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.ACTION_TIMES_RAW_STAGE;
// end action times


// notification integration 
use role accountadmin;
create or replace NOTIFICATION INTEGRATION sns_int_obj
  ENABLED = true
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = AWS_SNS
  DIRECTION = OUTBOUND
  AWS_SNS_TOPIC_ARN = 'arn:aws:sns:us-east-1:936263849005:snowflake-task-alert-notifications'
  AWS_SNS_ROLE_ARN = 'arn:aws:iam::936263849005:role/snowflake-sns-access-role';

grant usage on integration sns_int_obj to role sysadmin;
DESC notification integration sns_int_obj;
  

 
// tasks
use role sysadmin;
/*
CREATE OR REPLACE TASK DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.s3_int_task
schedule='USING CRON 03 17 * * * America/New_York'
ERROR_INTEGRATION = sns_int_obj
as 
insert into DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.CASES_RAW_STAGE(metadata, domain, json, id, metadata_filename, metadata_last_modified)
select
 s.$1:meta metadata,
 f.value:domain::string domain,
 f.value JSON,
 f.value:case_id::string id,
 metadata$filename filename,
 metadata$file_last_modified last_modified
from @DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.s3_json_stage s, lateral flatten(s.$1:objects) f
;
*/
use role sysadmin;
CREATE OR REPLACE TASK DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.s3_int_task_daily
schedule='USING CRON 00 23 * * * America/New_York'
ERROR_INTEGRATION = sns_int_obj
as 
Call metadata.procedures.sp_ingest_transform('DATA_CONFIG_UPDATE', 'task_call_sp_ingest_transform', 'co-carecoordination-dev', 'DATALAKE_DEV', 'ETL_REPLACE_TEST_BHA_DEV', 'DM_ETL_REPLACE_TEST_BHA_DEV', 'UPDATE_CONFIG|RECREATE_VIEWS|RECREATE_TABLES|', null)
;
alter task DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.s3_int_task_daily resume;

use role sysadmin;
create or replace task DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.S3_INT_TASK
	schedule='USING CRON 00 * * * * America/New_York'
	error_integration=SNS_INT_OBJ
	as Call metadata.procedures.sp_ingest_transform('S3_DATA_LOAD', 'task_call_sp_ingest_transform', 'co-carecoordination-dev', 'DATALAKE_DEV', 'ETL_REPLACE_TEST_BHA_DEV', 'DM_ETL_REPLACE_TEST_BHA_DEV', 'S3_INGEST|STAGE_TO_RAW|INCR_TABLES|STAGE_DELETE|', null);
    
/*CREATE OR REPLACE TASK DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.s3_int_task
schedule='USING CRON 00 12-16 * * * America/New_York'
ERROR_INTEGRATION = sns_int_obj
as 
Call metadata.procedures.sp_ingest_transform('S3_DATA_LOAD', 'task_call_sp_ingest_transform', 'co-carecoordination-dev', 'DATALAKE_DEV', 'ETL_REPLACE_TEST_BHA_DEV', 'DM_ETL_REPLACE_TEST_BHA_DEV', 'S3_INGEST|STAGE_TO_RAW|INCR_TABLES|STAGE_DELETE|', null);
;
*/
alter task DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.s3_int_task resume;

use role sysadmin;
CREATE OR REPLACE TASK DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.s3_int_task_test
schedule='USING CRON 00 12-16 * * * America/New_York'
ERROR_INTEGRATION = sns_int_obj
as 
Call metadata.procedures.sp_ingest_transform('S3_DATA_LOAD', 'task_call_sp_ingest_transform', 'co-carecoordination-dev', 'DATALAKE_DEV', 'ETL_REPLACE_TEST_BHA_DEV', 'DM_ETL_REPLACE_TEST_BHA_DEV', 'xS3_INGEST|STAGE_TO_RAW|INCR_TABLES|STAGE_DELETE|', null);
;
alter task DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.S3_INT_TASK suspend;

// to run the task immediately
EXECUTE TASK DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.S3_UNLOAD_TASK;

show tasks;

select * from DM_ETL_REPLACE_TEST_BHA_DEV.UTIL.task_log order by task_id desc;
select * from DM_ETL_REPLACE_TEST_BHA_DEV.UTIL.execution_log order by task_id desc;
select * from DM_ETL_REPLACE_TEST_BHA_DEV.UTIL.message_log order by task_id desc;
// end tasks

select * from table(information_schema.task_history()) order by scheduled_time;
select * from table(information_schema.task_history(
    scheduled_time_range_start=>dateadd('hour',-1,current_timestamp()),
    result_limit => 10,
    task_name=>'s3_int_task'));

    

// email notification object
use role accountadmin;
//drop INTEGRATION email_int_obj;
CREATE OR REPLACE NOTIFICATION INTEGRATION email_int_obj
    TYPE=EMAIL
    ENABLED=TRUE
    ALLOWED_RECIPIENTS=('slu@dimagi.com','rsingh@dimagi.com');

DESC integration email_int_obj;

//GRANT USAGE ON INTEGRATION email_int_obj TO ROLE sysadmin;

// email alert
use role accountadmin;
/*
CREATE OR REPLACE ALERT DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.s3_int_alert
WAREHOUSE = COMPUTE_WH
schedule='USING CRON 36 17 * * * America/New_York'
  IF (EXISTS(
    select * from snowflake.account_usage.query_history where database_name='DATALAKE_DEV' and schema_name='ETL_REPLACE_TEST_BHA_DEV' and error_code > 0 and TO_TIMESTAMP(start_time) > DATEADD(days, -1, CURRENT_TIMESTAMP) order by start_time desc
    ))
  THEN
  CALL SYSTEM$SEND_EMAIL(
    'email_int_obj',
    'slu@dimagi.com',
    'Email Alert: ETL_REPLACE_TEST_BHA_DEV error.',
    'there are errors from query_history \ndatabase_name=DATALAKE_DEV \nschema_name=ETL_REPLACE_TEST_BHA_DEV \nerror_code>0 \nstart_time='||DATE(CURRENT_DATE)||' greater than '||DATEADD(days, -1, CURRENT_DATE)
);
*/
//drop alert DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.s3_int_alert;
CREATE OR REPLACE ALERT DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.s3_int_alert
WAREHOUSE = COMPUTE_WH
schedule='USING CRON 00 12-16 * * * America/New_York'
  IF (EXISTS(
    select * from DM_ETL_REPLACE_TEST_BHA_DEV.UTIL.task_log where status='FAILURE' and TO_TIMESTAMP(task_start) > DATEADD(days, -1, CURRENT_TIMESTAMP) order by task_start desc
    ))
  THEN
  CALL SYSTEM$SEND_EMAIL(
    'email_int_obj',
    'slu@dimagi.com',
    'Email Alert: ETL_REPLACE_TEST_BHA_DEV error.',
    'there are errors from task_log \nstart_time='||DATE(CURRENT_DATE)||' greater than '||DATEADD(days, -1, CURRENT_DATE)
);

use role accountadmin;
//drop alert DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.s3_int_alert;
alter alert DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.s3_int_alert resume;
alter alert DATALAKE_DEV.ETL_REPLACE_TEST_BHA_DEV.s3_int_alert suspend;

show alerts; 


// procedure sp_ingest_transform tessting
--proc configs
-- once daily recreate call; recreates views and tables, eventually we can add push to commcare as a step
Call metadata.procedures.sp_ingest_transform('DATA_CONFIG_UPDATE', 'manual_call_sp_ingest_transform', 'co-carecoordination-dev', 'DATALAKE_DEV', 'ETL_REPLACE_TEST_BHA_DEV', 'DM_ETL_REPLACE_TEST_BHA_DEV', 'UPDATE_CONFIG|RECREATE_VIEWS|RECREATE_TABLES|', null);

--hourly call; ingest from s3 and incrementally add to dm
Call metadata.procedures.sp_ingest_transform('S3_DATA_LOAD', 'manual_call_sp_ingest_transform', 'co-carecoordination-dev', 'DATALAKE_DEV', 'ETL_REPLACE_TEST_BHA_DEV', 'DM_ETL_REPLACE_TEST_BHA_DEV', 'S3_INGEST|STAGE_TO_RAW|INCR_TABLES|STAGE_DELETE|', null);
Call metadata.procedures.sp_ingest_transform('S3_DATA_LOAD', 'manual_call_sp_ingest_transform', 'co-carecoordination-dev', 'DATALAKE_DEV', 'ETL_REPLACE_TEST_BHA_DEV', 'DM_ETL_REPLACE_TEST_BHA_DEV', 'S3_INGEST|STAGE_TO_RAW|INCR_TABLES|STAGE_DELETE|', '/2023/09/06/16/');


