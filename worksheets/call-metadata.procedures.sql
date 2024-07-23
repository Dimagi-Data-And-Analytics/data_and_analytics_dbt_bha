use role sysadmin;

////// bha dev //////
// old
Call metadata.procedures_dev.sp_ingest_transform_old('S3_DATA_LOAD', 'manual_call_sp_ingest_transform', 'co-carecoordination-dev', 'DATALAKE_DEV', 'CO_CARE_COORDINATION_COMMCARE_DEV', 'DM_CO_CARE_COORD_DEV', 'S3_INGEST|STAGE_TO_RAW|INCR_TABLES|STAGE_DELETE|', '/2023/10/23/03/');

// new
Call metadata.procedures_dev.sp_ingest_transform('S3_DATA_LOAD', 'manual_call_sp_ingest_transform', 'co-carecoordination-dev', 'DATALAKE_DEV', 'CO_CARE_COORDINATION_COMMCARE_DEV', 'DM_CO_CARE_COORD_DEV', 'S3_INGEST|STAGE_TO_RAW|INCR_TABLES|STAGE_DELETE|', null, 'fixture|web-user|action_times');

Call metadata.procedures_dev.sp_ingest_transform('S3_DATA_LOAD', 'manual_call_sp_ingest_transform', 'co-carecoordination-dev', 'DATALAKE_DEV', 'CO_CARE_COORDINATION_COMMCARE_DEV', 'DM_CO_CARE_COORD_DEV', 'S3_INGEST|STAGE_TO_RAW|INCR_TABLES|STAGE_DELETE|', null, 'case|form|location|fixture|web-user|action_times');

-- test
Call metadata.procedures_dev.sp_ingest_transform('S3_DATA_LOAD', 'manual_call_sp_ingest_transform', 'co-carecoordination-dev', 'DATALAKE_DEV', 'CO_CARE_COORDINATION_COMMCARE_DEV', 'DM_CO_CARE_COORD_DEV', 'S3_INGEST|', null, 'location');


// daily
Call metadata.procedures_dev.sp_ingest_transform('DATA_CONFIG_UPDATE', 'manual_call_sp_ingest_transform', 'co-carecoordination-dev', 'DATALAKE_DEV', 'CO_CARE_COORDINATION_COMMCARE_DEV', 'DM_CO_CARE_COORD_DEV', 'UPDATE_CONFIG|RECREATE_VIEWS|RECREATE_TABLES|', null, '');

// unload 
Call metadata.procedures_dev.sp_data_unload('CLIENT_PAYLOAD_UPDATE', 'manual_call_sp_data_unload', 'co-carecoordination-dev', 'DATALAKE_DEV', 'CO_CARE_COORDINATION_COMMCARE_DEV', 'DM_CO_CARE_COORD_DEV', 'UNLOAD_SF_TO_S3_AWS|', null);
// test that fail on purpose
Call metadata.procedures_dev.sp_data_unload('CLIENT_PAYLOAD_UPDATE', 'test_call_sp_data_unload', 'co-carecoordination-dev', 'DATALAKE_DEV', 'CO_CARE_COORDINATION_COMMCARE_DEV', 'DM_CO_CARE_COORD_DEV', 'UNLOAD_SF_TO_S3_AWSx|', null);

// tasks
ALTER TASK DATALAKE_DEV.CO_CARE_COORDINATION_COMMCARE_DEV.S3_INT_TASK resume;
EXECUTE TASK DATALAKE_DEV.CO_CARE_COORDINATION_COMMCARE_DEV.S3_INT_TASK;
EXECUTE TASK DATALAKE_DEV.CO_CARE_COORDINATION_COMMCARE_DEV.S3_INT_TASK_DAILY;
EXECUTE TASK DATALAKE_DEV.CO_CARE_COORDINATION_COMMCARE_DEV.S3_UNLOAD_TASK;



////// bha qa //////
// old
Call metadata.procedures_qa.sp_ingest_transform_old('S3_DATA_LOAD', 'manual_call_sp_ingest_transform', 'co-carecoordination-uat', 'DATALAKE_QA', 'CO_CARE_COORDINATION_COMMCARE_UAT', 'DM_CO_CARE_COORD_QA', 'S3_INGEST|STAGE_TO_RAW|INCR_TABLES|STAGE_DELETE|', '/2023/10/06/18/');
    
// new
Call metadata.procedures_qa.sp_ingest_transform('S3_DATA_LOAD', 'manual_call_sp_ingest_transform', 'co-carecoordination-uat', 'DATALAKE_QA', 'CO_CARE_COORDINATION_COMMCARE_UAT', 'DM_CO_CARE_COORD_QA', 'S3_INGEST|STAGE_TO_RAW|INCR_TABLES|STAGE_DELETE|', '/2024/02/08/22/', 'case|fixture');

Call metadata.procedures_qa.sp_ingest_transform('S3_DATA_LOAD', 'manual_call_sp_ingest_transform', 'co-carecoordination-uat', 'DATALAKE_QA', 'CO_CARE_COORDINATION_COMMCARE_UAT', 'DM_CO_CARE_COORD_QA', 'S3_INGEST|STAGE_TO_RAW|INCR_TABLES|STAGE_DELETE|', null, 'case|form|location|fixture|web-user|action_times');

// daily
Call metadata.procedures_qa.sp_ingest_transform('DATA_CONFIG_UPDATE', 'manual_call_sp_ingest_transform', 'co-carecoordination-uat', 'DATALAKE_QA', 'CO_CARE_COORDINATION_COMMCARE_UAT', 'DM_CO_CARE_COORD_QA', 'UPDATE_CONFIG|RECREATE_VIEWS|RECREATE_TABLES|', null, '');
    
// unload 
Call metadata.procedures_qa.sp_data_unload('CLIENT_PAYLOAD_UPDATE', 'manual_call_sp_data_unload', 'co-carecoordination-uat', 'DATALAKE_QA', 'CO_CARE_COORDINATION_COMMCARE_UAT', 'DM_CO_CARE_COORD_QA', 'UNLOAD_SF_TO_S3_AWS|', null);

// tasks
ALTER TASK DATALAKE_QA.CO_CARE_COORDINATION_COMMCARE_UAT.S3_INT_TASK resume;
EXECUTE TASK DATALAKE_QA.CO_CARE_COORDINATION_COMMCARE_UAT.S3_INT_TASK;
EXECUTE TASK DATALAKE_QA.CO_CARE_COORDINATION_COMMCARE_UAT.S3_INT_TASK_DAILY;
EXECUTE TASK DATALAKE_QA.CO_CARE_COORDINATION_COMMCARE_UAT.S3_UNLOAD_TASK;



////// bha prod //////
// old
Call metadata.procedures.sp_ingest_transform('S3_DATA_LOAD', 'manual_call_sp_ingest_transform', 'co-carecoordination', 'DATALAKE_PROD', 'CO_CARE_COORDINATION_COMMCARE_PROD', 'DM_CO_CARE_COORD_PROD', 'S3_INGEST|STAGE_TO_RAW|INCR_TABLES|STAGE_DELETE|', '/2023/10/24/16/');

// new
Call metadata.procedures.sp_ingest_transform('S3_DATA_LOAD', 'manual_call_sp_ingest_transform', 'co-carecoordination', 'DATALAKE_PROD', 'CO_CARE_COORDINATION_COMMCARE_PROD', 'DM_CO_CARE_COORD_PROD', 'S3_INGEST|STAGE_TO_RAW|INCR_TABLES|STAGE_DELETE|', '/2024/05/08/22/', 'location');

-- test
Call metadata.procedures.sp_ingest_transform('S3_DATA_LOAD', 'manual_call_sp_ingest_transform', 'co-carecoordination', 'DATALAKE_PROD', 'CO_CARE_COORDINATION_COMMCARE_PROD', 'DM_CO_CARE_COORD_PROD', 'S3_INGEST|STAGE_TO_RAW|INCR_TABLES|STAGE_DELETE|', null, 'form');

Call metadata.procedures.sp_ingest_transform('S3_DATA_LOAD', 'manual_call_sp_ingest_transform', 'co-carecoordination', 'DATALAKE_PROD', 'CO_CARE_COORDINATION_COMMCARE_PROD', 'DM_CO_CARE_COORD_PROD', 'S3_INGEST|STAGE_TO_RAW|INCR_TABLES|STAGE_DELETE|', null, 'case|form|location|fixture|web-user|action_times');

// daily
Call metadata.procedures.sp_ingest_transform('DATA_CONFIG_UPDATE', 'manual_call_sp_ingest_transform', 'co-carecoordination', 'DATALAKE_PROD', 'CO_CARE_COORDINATION_COMMCARE_PROD', 'DM_CO_CARE_COORD_PROD', 'UPDATE_CONFIG|RECREATE_VIEWS|RECREATE_TABLES|', null, '');

// unload 
Call metadata.procedures.sp_data_unload('CLIENT_PAYLOAD_UPDATE', 'manual_call_sp_data_unload', 'co-carecoordination', 'DATALAKE_PROD', 'CO_CARE_COORDINATION_COMMCARE_PROD', 'DM_CO_CARE_COORD_PROD', 'UNLOAD_SF_TO_S3_AWS|', null);

// tasks
ALTER TASK DATALAKE_PROD.CO_CARE_COORDINATION_COMMCARE_PROD.S3_INT_TASK resume;
EXECUTE TASK DATALAKE_PROD.CO_CARE_COORDINATION_COMMCARE_PROD.S3_INT_TASK;
EXECUTE TASK DATALAKE_PROD.CO_CARE_COORDINATION_COMMCARE_PROD.S3_INT_TASK_DAILY;
EXECUTE TASK DATALAKE_PROD.CO_CARE_COORDINATION_COMMCARE_PROD.S3_UNLOAD_TASK;

-----------------------

////// tuba city qa //////
// old
    Call metadata.procedures.sp_ingest_transform('S3_DATA_LOAD', 'manual_call_sp_ingest_transform', 'navajo-covid19-staging', 'DATALAKE_QA', 'TUBA_CITY_CICT_COMMCARE_QA', 'DM_TUBA_CITY_CICT_QA', 'S3_INGEST|STAGE_TO_RAW|INCR_TABLES|STAGE_DELETE|', null);

// new 
Call metadata.procedures.sp_ingest_transform('S3_DATA_LOAD', 'manual_call_sp_ingest_transform', 'navajo-covid19-staging', 'DATALAKE_QA', 'TUBA_CITY_CICT_COMMCARE_QA', 'DM_TUBA_CITY_CICT_QA', 'S3_INGEST|STAGE_TO_RAW|INCR_TABLES|STAGE_DELETE|', null, 'fixture|web-user|action_times');

Call metadata.procedures.sp_ingest_transform('S3_DATA_LOAD', 'manual_call_sp_ingest_transform', 'navajo-covid19-staging', 'DATALAKE_QA', 'TUBA_CITY_CICT_COMMCARE_QA', 'DM_TUBA_CITY_CICT_QA', 'S3_INGEST|STAGE_TO_RAW|INCR_TABLES|STAGE_DELETE|', null, 'case|form|location|fixture|web-user|action_times');

// daily
Call metadata.procedures.sp_ingest_transform('DATA_CONFIG_UPDATE', 'manual_call_sp_ingest_transform', 'navajo-covid19-staging', 'DATALAKE_QA', 'TUBA_CITY_CICT_COMMCARE_QA', 'DM_TUBA_CITY_CICT_QA', 'UPDATE_CONFIG|RECREATE_VIEWS|RECREATE_TABLES|', null);

// task
ALTER TASK DATALAKE_QA.TUBA_CITY_CICT_COMMCARE_QA.S3_INT_TASK resume;
EXECUTE TASK DATALAKE_QA.TUBA_CITY_CICT_COMMCARE_QA.S3_INT_TASK;
EXECUTE TASK DATALAKE_QA.TUBA_CITY_CICT_COMMCARE_QA.S3_INT_TASK_DAILY;




////// tuba city prod //////
// old
Call metadata.procedures.sp_ingest_transform('S3_DATA_LOAD', 'manual_call_sp_ingest_transform', 'chw-covid-response-team', 'DATALAKE_PROD', 'TUBA_CITY_CICT_COMMCARE_PROD', 'DM_TUBA_CITY_CICT_PROD', 'S3_INGEST|STAGE_TO_RAW|INCR_TABLES|STAGE_DELETE|', null);

// new 
Call metadata.procedures.sp_ingest_transform('S3_DATA_LOAD', 'manual_call_sp_ingest_transform', 'chw-covid-response-team', 'DATALAKE_PROD', 'TUBA_CITY_CICT_COMMCARE_PROD', 'DM_TUBA_CITY_CICT_PROD', 'S3_INGEST|STAGE_TO_RAW|INCR_TABLES|STAGE_DELETE|', null, 'fixture|web-user|action_times');

Call metadata.procedures.sp_ingest_transform('S3_DATA_LOAD', 'manual_call_sp_ingest_transform', 'chw-covid-response-team', 'DATALAKE_PROD', 'TUBA_CITY_CICT_COMMCARE_PROD', 'DM_TUBA_CITY_CICT_PROD', 'S3_INGEST|STAGE_TO_RAW|INCR_TABLES|STAGE_DELETE|', null, 'case|form|location|fixture|web-user|action_times');

// daily
Call metadata.procedures.sp_ingest_transform('DATA_CONFIG_UPDATE', 'manual_call_sp_ingest_transform', 'chw-covid-response-team', 'DATALAKE_PROD', 'TUBA_CITY_CICT_COMMCARE_PROD', 'DM_TUBA_CITY_CICT_PROD', 'UPDATE_CONFIG|RECREATE_VIEWS|RECREATE_TABLES|', null);

// task
EXECUTE TASK DATALAKE_PROD.TUBA_CITY_CICT_COMMCARE_PROD.S3_INT_TASK;
EXECUTE TASK DATALAKE_PROD.TUBA_CITY_CICT_COMMCARE_PROD.S3_INT_TASK_DAILY;
