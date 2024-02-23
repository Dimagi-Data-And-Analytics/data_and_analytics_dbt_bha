with 
dm_table_data_service as (
      select * from  {{ source('dm_table_data', 'CASE_SERVICE') }}
), 
dm_table_data_clinic as (
      select * from  {{ source('dm_table_data', 'CASE_CLINIC') }}
), 
dm_table_data_provider as (
      select * from  {{ source('dm_table_data', 'CASE_PROVIDER') }}
), 
dm_table_data_client as (
      select * from  {{ source('dm_table_data', 'CASE_CLIENT') }}
), 
final as (
select 
  s.case_id as case_id_service
  ,p.case_id as case_id_provider
  ,cl.case_id as case_id_clinic
  ,ct.case_id as case_id_client
  ,s.case_name as service_name
  ,p.case_name as provider_name
  ,cl.case_name as clinic_name
  ,cl.display_name as clinic_display_name
  ,ct.case_name as client_name
  ,s.date_opened
  ,s.date_closed
  ,  TIMESTAMPDIFF('days', s.admission_date,nvl(s.discharge_date, current_timestamp())) as Length_of_service_days
  ,s.central_registry
  ,s.closed as service_closed
  ,s.service_status
  ,s.current_status
  ,s.domain
  ,s.matching_admission_discharge_date
  ,s.admission_date
    ,s.admission_time
    ,s.discharge_date
    ,s.discharge_time
    ,s.discharge_reason
    ,s.service_type
from dm_table_data_service s
left join dm_table_data_clinic cl on s.clinic_case_id = cl.case_id
left join dm_table_data_provider p on cl.parent_case_id = p.case_id
left join dm_table_data_client ct on s.parent_case_id = ct.case_id
where s.parent_case_type <> 'pending_admission'

)

select 
	CASE_ID_SERVICE,
	CASE_ID_PROVIDER,
	CASE_ID_CLINIC,
	CASE_ID_CLIENT,
	SERVICE_NAME,
	PROVIDER_NAME,
	CLINIC_NAME,
	CLINIC_DISPLAY_NAME,
	CLIENT_NAME,
	DATE_OPENED,
	DATE_CLOSED,
	LENGTH_OF_SERVICE_DAYS,
	CENTRAL_REGISTRY,
	SERVICE_CLOSED,
	SERVICE_STATUS,
	CURRENT_STATUS,
	DOMAIN,
	MATCHING_ADMISSION_DISCHARGE_DATE,
	ADMISSION_DATE,
	ADMISSION_TIME,
	DISCHARGE_DATE,
	DISCHARGE_TIME,
	DISCHARGE_REASON,
	SERVICE_TYPE
from final