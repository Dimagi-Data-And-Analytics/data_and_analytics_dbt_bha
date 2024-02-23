with
dl_table_data_forms as (
      select * from  {{ source('dl_table_data', 'FORMS_RAW') }}
)

select * from dl_table_data_forms where domain <> 
    {% if target.name=='qa' %}
      'co-carecoordination-uat'
    {% elif target.name=='prod' %}
      'co-carecoordination-prod'
    {% else %}
      'co-carecoordination-dev'
    {% endif %}

