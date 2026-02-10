{#
    Macro: pivot_eav_attributes
    Descrição: Transforma atributos EAV (linhas) em colunas planas
    
    Parâmetros:
        - source_table: tabela com os custom_attributes desnormalizados pelo DLT
        - key_column: coluna que identifica o produto (ex: _dlt_parent_id)
        - attribute_code_column: coluna com o código do atributo
        - attribute_value_column: coluna com o valor do atributo
        - attributes: lista de atributos a pivotar
#}

{% macro pivot_eav_attributes(
    source_table,
    key_column,
    attribute_code_column,
    attribute_value_column,
    attributes
) %}

SELECT
    {{ key_column }},
    {% for attr in attributes %}
    MAX(CASE 
        WHEN {{ attribute_code_column }} = '{{ attr.code }}' 
        THEN {{ 'TRY_CAST(' ~ attribute_value_column ~ ' AS ' ~ attr.type ~ ')' if attr.type else attribute_value_column }}
    END) AS {{ attr.name }}{{ ',' if not loop.last else '' }}
    {% endfor %}
FROM {{ source_table }}
GROUP BY {{ key_column }}

{% endmacro %}