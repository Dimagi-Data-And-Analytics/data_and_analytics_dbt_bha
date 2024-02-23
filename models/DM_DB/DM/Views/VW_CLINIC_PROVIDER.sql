with 
dm_table_data_clinic as (
      select * from  {{ source('dm_table_data', 'CASE_CLINIC') }}
), 
dm_table_data_provider as (
      select * from  {{ source('dm_table_data', 'CASE_PROVIDER') }}
), 
final as (
select 
    c.case_id as clinic_case_id
    , c.parent_case_id as provider_case_id
    , p.case_name as provider_name
    , c.case_name as clinic_name
    , c.display_name as clinic_display_name
    , c.date_opened as clinic_date_opened
   , c.date_closed as clinic_date_closed
  , c.date_modified as clinic_date_modified
  , p.date_opened as provider_date_opened
  , p.date_closed as provider_date_closed
  , p.date_modified as provider_date_modified
  , c.ladders_export_date as clinic_export_date
  , c.import_date as clinic_import_date
  , c.active_mh_designation as clinic_active_mh_designation
  , c.active_sud_license as clinic_active_sud_license
  , c.county as clinic_county
  , c.county_display as clinic_county_display
  , c.cs_license_number as clinic_license_number
  --, c.EXTERNAL_CENTRAL_REGISTRY_CLINIC_ID as clinic_external_central_registry_clinic_id
  , c.mh_designation as clinic_mh_designation
  , INITCAP(replace(replace(c.mental_health_services::string, ' ', ', '), '_', ' '), ' ') as clinic_mental_health_services
  , INITCAP(replace(replace(c.population_served::string, ' ', ', '), '_', ' '), ' ') as clinic_population_served
  , INITCAP(replace(replace(c.programs::string, ' ', ', '), '_', ' '), ' ') as clinic_programs
  -- not in prod , c.licensed_bed_capacity as clinic_licensed_bed_capacity
  -- not in prod  , c.capacity_last_updated_date as clinic_capacity_last_updated_date
  -- not in prod  , c.phone as clinic_phone
    , c.phone_number as clinic_phone_number
   , c.phone_details as clinic_phone_details
    , c.address_full as clinic_address_full
    , c.address_street as clinic_address_street
  -- not in prod    , c.address_county as clinic_address_county
    , c.address_city as clinic_address_city
    , c.address_state as clinic_address_state
    , c.address_zip as clinic_address_zip
    , c.longitude as clinic_longitude
    , c.latitude as clinic_latitude
    , c.map_coordinates as clinic_map_coordinates
   -- , c.civil_criminal_status as clinic_civil_criminal_status
  -- not in prod    , INITCAP(replace(replace(c.civil_criminal_status::string, ' ', ', '), '_', ' '), ' ') as clinic_civil_criminal_status
   -- , c.other_accommodations as clinic_other_accommodations
  -- not in prod    , INITCAP(replace(replace(c.other_accommodations::string, ' ', ', '), '_', ' '), ' ') as clinic_other_accommodations
   -- , c.su_residential_services as clinic_su_residential_services
   -- not in prod   , INITCAP(replace(replace(c.su_residential_services::string, ' ', ', '), '_', ' '), ' ') as clinic_su_residential_services
   -- , c.languages_spoken as clinic_languages_spoken
    , INITCAP(replace(replace(c.languages_spoken::string, ' ', ', '), '_', ' '), ' ') as clinic_languages_spoken
   -- , c.program_type as clinic_program_type
   -- not in prod   , INITCAP(replace(replace(c.program_type::string, ' ', ', '), '_', ' '), ' ') as clinic_program_type
  -- not in prod    , c.license_number as clinic_license_number
  -- not in prod    , c.age_range_accepted as clinic_age_range_accepted
   -- , c.comorbid_diagnoses as clinic_comorbid_diagnoses
  -- not in prod    , INITCAP(replace(replace(c.comorbid_diagnoses::string, ' ', ', '), '_', ' '), ' ') as clinic_comorbid_diagnoses
  -- not in prod    , c.gender_specific_beds as clinic_gender_specific_beds
   -- , c.clinic_type 
    , INITCAP(replace(replace(c.clinic_type::string, ' ', ', '), '_', ' '), ' ') as clinic_type
   -- , c.payers_accepted as clinic_payers_accepted
    , INITCAP(replace(replace(c.payers_accepted::string, ' ', ', '), '_', ' '), ' ') as clinic_payers_accepted
   -- , c.intake_requirements as clinic_intake_requirements
  -- not in prod    , INITCAP(replace(replace(c.intake_requirements::string, ' ', ', '), '_', ' '), ' ') as clinic_intake_requirements
   --  , c.bed_types as clinic_bed_types
    -- not in prod  , INITCAP(replace(replace(c.bed_types::string, ' ', ', '), '_', ' '), ' ') as clinic_bed_types
   -- not in prod   , c.program_name as clinic_program_name
  , c.sunday_open as clinic_sunday_open
  , c.sunday_close as clinic_sunday_close
  , c.monday_open as clinic_monday_open
  , c.monday_close as clinic_monday_close
  , c.tuesday_open as clinic_tuesday_open
  , c.tuesday_close as clinic_tuesday_close
  , c.wednesday_open as clinic_wednesday_open
  , c.wednesday_close as clinic_wednesday_close
  , c.thursday_open as clinic_thursday_open
  , c.thursday_close as clinic_thursday_close
  , c.friday_open as clinic_friday_open
  , c.friday_close as clinic_friday_close
  , c.saturday_open as clinic_saturday_open
  , c.saturday_close as clinic_saturday_close
 
    from  dm_table_data_clinic c left join dm_table_data_provider p on c.parent_case_id = p.case_id
)

select 
	CLINIC_CASE_ID,
	PROVIDER_CASE_ID,
	PROVIDER_NAME,
	CLINIC_NAME,
	CLINIC_DISPLAY_NAME,
	CLINIC_DATE_OPENED,
	CLINIC_DATE_CLOSED,
	CLINIC_DATE_MODIFIED,
	PROVIDER_DATE_OPENED,
	PROVIDER_DATE_CLOSED,
	PROVIDER_DATE_MODIFIED,
	CLINIC_EXPORT_DATE,
	CLINIC_IMPORT_DATE,
	CLINIC_ACTIVE_MH_DESIGNATION,
	CLINIC_ACTIVE_SUD_LICENSE,
	CLINIC_COUNTY,
	CLINIC_COUNTY_DISPLAY,
	CLINIC_LICENSE_NUMBER,
	CLINIC_MH_DESIGNATION,
	CLINIC_MENTAL_HEALTH_SERVICES,
	CLINIC_POPULATION_SERVED,
	CLINIC_PROGRAMS,
	CLINIC_PHONE_NUMBER,
	CLINIC_PHONE_DETAILS,
	CLINIC_ADDRESS_FULL,
	CLINIC_ADDRESS_STREET,
	CLINIC_ADDRESS_CITY,
	CLINIC_ADDRESS_STATE,
	CLINIC_ADDRESS_ZIP,
	CLINIC_LONGITUDE,
	CLINIC_LATITUDE,
	CLINIC_MAP_COORDINATES,
	CLINIC_LANGUAGES_SPOKEN,
	CLINIC_TYPE,
	CLINIC_PAYERS_ACCEPTED,
	CLINIC_SUNDAY_OPEN,
	CLINIC_SUNDAY_CLOSE,
	CLINIC_MONDAY_OPEN,
	CLINIC_MONDAY_CLOSE,
	CLINIC_TUESDAY_OPEN,
	CLINIC_TUESDAY_CLOSE,
	CLINIC_WEDNESDAY_OPEN,
	CLINIC_WEDNESDAY_CLOSE,
	CLINIC_THURSDAY_OPEN,
	CLINIC_THURSDAY_CLOSE,
	CLINIC_FRIDAY_OPEN,
	CLINIC_FRIDAY_CLOSE,
	CLINIC_SATURDAY_OPEN,
	CLINIC_SATURDAY_CLOSE
from final