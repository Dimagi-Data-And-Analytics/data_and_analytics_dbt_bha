with 
dm_table_data_clinic as (
      select * from  {{ source('dm_table_data', 'CASE_CLINIC') }}
), 
dm_table_data_commcare_user as (
      select * from  {{ source('dm_table_data', 'CASE_COMMCARE_USER') }}
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
    order by 1 asc),

user_clinics as (
    select users.hq_user_id, clinics.case_id as clinic_case_id 
    from dm_table_data_commcare_user as users,  lateral split_to_table( users.commcare_location_ids, ' ')
    join dm_table_data_clinic as clinics 
    on clinics.owner_id = value),

active_inpatient_clinics as (
select * from inpatient_clinics
where case_id in (select clinic_case_id from  user_clinics))

select a.case_name, a.case_id, a.accepts_commcare_referrals, a.clinic_type, a.county, min(u.date_opened::date) as date_onboarded from active_inpatient_clinics as a
join dm_table_data_commcare_user as U
on contains(u.commcare_location_ids, a.owner_id)

group by 1, 2, 3, 4, 5