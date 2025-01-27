{% test date_greater_than_given_date(model, column_name, given_date) %}

select {{ column_name }}
from {{ model }}
where date({{ column_name }}) <= date({{ given_date }})

{% endtest %}