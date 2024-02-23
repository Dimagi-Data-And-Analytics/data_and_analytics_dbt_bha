-- version control only, will not be compiled or deployed

-- dev
CREATE OR REPLACE STAGE s3_json_payload_stage
    url='s3://commcare-snowflake-data-sync/co-carecoordination-dev/payload/'
    Storage_integration = s3_int_obj
    file_format = sf_to_s3_unload_file_format;

-- qa
CREATE OR REPLACE STAGE s3_json_payload_stage
    url='s3://commcare-snowflake-data-sync/co-carecoordination-uat/payload/'
    Storage_integration = s3_int_obj
    file_format = sf_to_s3_unload_file_format;

-- prod
CREATE OR REPLACE STAGE s3_json_payload_stage
    url='s3://commcare-snowflake-data-sync/co-carecoordination/payload/'
    Storage_integration = s3_int_obj
    file_format = sf_to_s3_unload_file_format;