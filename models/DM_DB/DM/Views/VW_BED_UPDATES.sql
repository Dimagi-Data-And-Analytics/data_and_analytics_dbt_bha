with 
dm_table_data_clinic as (
      select * from  {{ source('dm_table_data', 'CASE_CLINIC') }}
), 
dm_table_data_commcare_user as (
      select * from  {{ source('dm_table_data', 'CASE_COMMCARE_USER') }}
), 
dm_table_data_provider as (
      select * from  {{ source('dm_table_data', 'CASE_PROVIDER') }}
), 
dm_table_data_unit as (
      select * from  {{ source('dm_table_data', 'CASE_UNIT') }}
), 
inpatient_clinics as (
    select *  from dm_table_data_clinic as c
    where c.closed = 'false' and 
    (mental_health_settings is not NULL and
    (contains(c.mental_health_settings, '72_hour_treatment_and_evaluation') or contains(c.mental_health_settings, 'residential_long_term_treatment')
    Or contains(c.mental_health_settings, 'residential_short_term_treatment')
    Or contains(c.mental_health_settings, 'psychiatric_residential')
    Or contains(c.mental_health_settings, 'residential_child_care_facility')
    Or contains(c.mental_health_settings, 'crisis_stabilization_unit')
    Or contains(c.mental_health_settings, 'acute_treatment_unit')))
    Or
    (residential_services is not NULL and (
    contains(c.residential_services, 'clinically_managed_low_intensity_residential_services')
    or contains(c.residential_services, 'clinically_managed_residential_detoxification')
    or contains(c.residential_services, 'clinically_managed_medium_intensity_residential_services')
    or contains(c.residential_services, 'clinically_managed_high_intensity_residential_services')
    or contains(c.residential_services, 'medically_monitored_intensive_residential_treatment')
    or contains(c.residential_services, 'medically_monitored_inpatient_detoxification')))
    order by 1 asc
),
user_clinics as (
    select users.hq_user_id, clinics.case_id as clinic_case_id 
    from dm_table_data_commcare_user as users,  
    lateral split_to_table( users.commcare_location_ids, ' ')
    join dm_table_data_clinic as clinics 
    on clinics.owner_id = value
),
active_inpatient_clinics as (
    select * from inpatient_clinics
    where case_id in (select clinic_case_id from  user_clinics)
),
clinic_updates (clinic_case_id, clinic_name, last_updated, provider_name, county) as (
    select clinic.case_id as "clinic_case_id", 
    clinic.case_name as "Clinic Name",
    //unit_ts.case_id as "unit_case_id", 
    date(DECIMAL_TO_TS(unit_ts.last_updated_date_time_raw)) as "last_updated" ,
    provider.case_name,
    clinic.county
    from  {{ ref('VW_UNIT_LAST_UPDATED_DATE_TIME_RAW') }} as unit_ts
    right outer join dm_table_data_unit as unit
    on unit_ts.case_id = unit.case_id
    join active_inpatient_clinics as clinic
    on unit.parent_case_id = clinic.case_id
    join dm_table_data_provider as provider
    on clinic.parent_case_id = provider.case_id
    group by 1, 2, 3, 4,5
    order by 2
    //order by 2, 3 asc
)
select * from clinic_updates