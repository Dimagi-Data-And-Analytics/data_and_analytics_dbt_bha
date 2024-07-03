{% test number_accepted_range(model, column_name, min_value=none, max_value=none) %}

select {{ column_name }}
from {{ model }}
where {{ column_name }} is not null and 
    ({{ column_name }} < {{min_value}} and {{ column_name }} > {{max_value}})

{% endtest %}