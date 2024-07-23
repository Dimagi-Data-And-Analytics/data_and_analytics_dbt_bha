{% test two_column_value_matching(model, matching_columns) %}

{%- set column_list=[] %}
{% for column in matching_columns -%}
    {% set column_list = column_list.append( adapter.quote(column) ) %}
{%- endfor %}

{%- set columns_list_join=column_list | join(', ') %}

select {{ columns_list_join }}
from {{ model }}
where {{column_list[0]}} = {{column_list[1]}}

{% endtest %}