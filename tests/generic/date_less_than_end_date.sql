{% test date_less_than_end_date(model, column_name, end_date) %}

select {{ column_name }}
from {{ model }}
where {{ column_name }} > {{ end_date }}

{% endtest %}