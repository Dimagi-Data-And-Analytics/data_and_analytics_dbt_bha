-- version control only, will not be compiled or deployed

-- dev
CREATE OR REPLACE TASK DATALAKE_DEV.CO_CARE_COORDINATION_COMMCARE_DEV.S3_INT_TASK_DAILY
	schedule='USING CRON 30 23 * * * America/New_York'
	error_integration=SNS_INT_OBJ
AS 
DECLARE 
    task_result string default null;
    task_exception EXCEPTION (-20003, 'Task had an error');
BEGIN
    Call metadata.procedures.sp_ingest_transform('DATA_CONFIG_UPDATE', 'task_call_sp_ingest_transform', 'co-carecoordination-dev', 'DATALAKE_DEV', 'CO_CARE_COORDINATION_COMMCARE_DEV', 'DM_CO_CARE_COORD_DEV', 'UPDATE_CONFIG|RECREATE_VIEWS|RECREATE_TABLES|', null, '') into :task_result;
    IF (task_result ilike '%error%') THEN 
        RAISE task_exception;
    END IF;
END
;

-- qa
CREATE OR REPLACE TASK DATALAKE_QA.CO_CARE_COORDINATION_COMMCARE_UAT.S3_INT_TASK_DAILY
	schedule='USING CRON 30 23 * * * America/New_York'
	error_integration=SNS_INT_OBJ
AS 
DECLARE 
    task_result string default null;
    task_exception EXCEPTION (-20003, 'Task had an error');
BEGIN
    Call metadata.procedures.sp_ingest_transform('DATA_CONFIG_UPDATE', 'task_call_sp_ingest_transform', 'co-carecoordination-uat', 'DATALAKE_QA', 'CO_CARE_COORDINATION_COMMCARE_UAT', 'DM_CO_CARE_COORD_QA', 'UPDATE_CONFIG|RECREATE_VIEWS|RECREATE_TABLES|', null, '') into :task_result;
    IF (task_result ilike '%error%') THEN 
        RAISE task_exception;
    END IF;
END
;

-- prod
CREATE OR REPLACE TASK DATALAKE_PROD.CO_CARE_COORDINATION_COMMCARE_PROD.S3_INT_TASK_DAILY
	schedule='USING CRON 45 05 * * * America/New_York'
	error_integration=SNS_INT_OBJ
AS 
DECLARE 
    task_result string default null;
    task_exception EXCEPTION (-20003, 'Task had an error');
BEGIN
    Call metadata.procedures.sp_ingest_transform('DATA_CONFIG_UPDATE', 'task_call_sp_ingest_transform', 'co-carecoordination', 'DATALAKE_PROD', 'CO_CARE_COORDINATION_COMMCARE_PROD', 'DM_CO_CARE_COORD_PROD', 'UPDATE_CONFIG|RECREATE_VIEWS|RECREATE_TABLES|', null, '') into :task_result;
    IF (task_result ilike '%error%') THEN 
        RAISE task_exception;
    END IF;
END
;