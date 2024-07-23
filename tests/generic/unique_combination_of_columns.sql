{% test unique_combination_of_columns(model, combination_of_columns) %}

{%- set column_list=[] %}
{% for column in combination_of_columns -%}
    {% set column_list = column_list.append( adapter.quote(column) ) %}
{%- endfor %}

{%- set columns_list_join=column_list | join(', ') %}

select {{ columns_list_join }}
from {{ model }}
group by {{ columns_list_join }}
having count(*) > 1

{% endtest %}