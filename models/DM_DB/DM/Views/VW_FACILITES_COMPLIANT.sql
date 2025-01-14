with  
dm_table_data_clinic as (
      select * from  {{ source('dm_table_data', 'CASE_CLINIC') }}
), 
dm_table_data_unit as (
      select * from  {{ source('dm_table_data', 'CASE_UNIT') }}
), 
dm_table_data_provider as (
      select * from  {{ source('dm_table_data', 'CASE_PROVIDER') }}
), 
inpatient_clinics as (
    select * from dm_table_data_clinic as c
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
), clinic_updates as (
    select 
        c.case_id as clinic_case_id, 
        c.case_name as clinic_name,
        date(DM.DECIMAL_TO_TS(unit_ts.last_updated_date_time_raw)) as last_updated_from_ts,
        p.case_name as provider_name
    from {{ ref('VW_UNIT_LAST_UPDATED_DATE_TIME_RAW') }}  as unit_ts
    join dm_table_data_unit unit
        on unit_ts.case_id = unit.case_id
    join inpatient_clinics as c
        on unit.parent_case_id = c.case_id
    join dm_table_data_provider as p
        on c.parent_case_id = p.case_id
    group by c.case_id, c.case_name, last_updated_from_ts, p.case_name
    order by c.case_name
), monthly_compliance_by_clinic as (
    select 
        provider_name, 
        clinic_name,
        -- this gives us the first of the month by truncating to the month from a date
        -- because the default is the first day at 00:00
        DATE_TRUNC('MONTH', last_updated_from_ts) as compliance_month, 
        -- the end of the compliance period is...
        -- 1. today + 1 (if current month)
        -- 2. the first of the next month (if a past month)
        case 
            when compliance_month = DATE_TRUNC('MONTH', current_date())
                then
                     -- +1 because diff
                     -- also if today is the first day of the month
                     -- datediff would return 0 and we'd get a divide by zero error
                        current_date() + 1 
            else
                    -- no +1 needed because the interval puts us
                    -- automatically +1 day on the first of the next month
                    compliance_month + INTERVAL '1 MONTH'
        end as compliance_month_end,
        datediff(
            day, 
            compliance_month,
            compliance_month_end
        ) as compliance_days_in_month,
        count(
            distinct(last_updated_from_ts)
        ) as total_days_updated,
        -- getting all the distinct days a facility has submitted an update
        -- and then dividing that by compliance_month_end
        total_days_updated / compliance_days_in_month * 100 as compliance_rate,
    from clinic_updates
    where 
        last_updated_from_ts >= date('2024-05-01') -- PROD data real capture start
    group by provider_name, clinic_name, compliance_month
    order by provider_name, clinic_name, compliance_month desc
),
total_clinics as (
     select count(1) as total_clinics from inpatient_clinics
)
select 
    compliance_month,
    count(clinic_name) as total_clinics_in_compliance,
    -- (
    --     select count(1) from inpatient_clinics
    -- ) as total_clinics,
    total_clinics_in_compliance / (tc.total_clinics)  * 100 as percent_clinics_in_compliance
from monthly_compliance_by_clinic, 
    (select * from total_clinics) as tc
where
    compliance_rate = 100
group by compliance_month, total_clinics
order by compliance_month desc
