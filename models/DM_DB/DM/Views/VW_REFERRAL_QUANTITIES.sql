with 
dm_table_data_referral as (
      select * from  {{ source('dm_table_data', 'CASE_REFERRAL') }}
), 
dm_table_data_clinic as (
      select * from  {{ source('dm_table_data', 'CASE_CLINIC') }}
), 
dm_table_data_client as (
      select * from  {{ source('dm_table_data', 'CASE_CLIENT') }}
)
select 
    cr.case_id as referral_case_id,
    cr.current_status as referral_current_status,
    cr.rejection_rationale as referral_rejection_reason,
    cc.case_id as client_case_id,
    DM.DECIMAL_TO_TS(cc.open_ts) as client_date_opened,
    DM.DECIMAL_TO_TS(cc.client_placed_ts) as client_place_date_time,
    cc.type_of_care as client_type_of_care,
    cc.profile_checks as client_population,
    cc.age as client_age,
    DM.DECIMAL_TO_TS(cr.open_ts) as "created_date_time",
    dc.case_name as destination_facility_name,
    dc.county as destination_facility_county,
    rc.case_name as referring_facility_name,
    rc.county as referring_facility_county,
    dc.clinic_type as destination_facility_clinic_type,
    rc.clinic_type as referring_facility_clinic_type,
    cr.send_to_destination_clinic as "on_platform_referral",
    cr.request_info_needs as request_info_needs,
    DM.DECIMAL_TO_TS(cr.info_requested_ts) as referral_info_requested_date_time,
    DM.DECIMAL_TO_TS(cr.rejected_ts) as referral_rejected_date_time,
    DM.DECIMAL_TO_TS(cr.client_placed_ts) as referral_client_placed_date_time,
    DM.DECIMAL_TO_TS(cr.resolved_ts) as referral_resolved_date_time,
    DM.DECIMAL_TO_TS(cr.withdrawn_ts) as referral_withdrawn_date_time,
    DM.DECIMAL_TO_TS(cr.closed_ts) as referral_closed_date_time
from 
    dm_table_data_referral cr
left join 
    dm_table_data_clinic dc on cr.destination_clinic_case_id = dc.case_id
left join 
    dm_table_data_clinic rc on cr.referring_clinic_case_id = rc.case_id
join
    dm_table_data_client cc on cr.parent_case_id = cc.case_id
where cr.destination_clinic_case_id is not NULL and cr.referring_clinic_case_id is not NULL and cr.closed = 'false'