MODEL (
    name staging.stg_products__custom_attributes,
    kind FULL,
    cron '@daily',
    description 'Atributos customizados dos produtos - tabela EAV desnormalizada pelo DLT'
);

WITH source AS (
    SELECT
        _dlt_parent_id,
        _dlt_id,
        attribute_code,
        value
    FROM magento_data.products__custom_attributes
),

renamed AS (
    SELECT
        _dlt_parent_id AS product_dlt_id,
        _dlt_id AS attribute_dlt_id,
        attribute_code,
        value AS attribute_value
    FROM source
),

-- Remove duplicatas de atributos para o mesmo produto (pega o último)
deduplicated AS (
    SELECT
        product_dlt_id,
        attribute_code,
        attribute_value,
        ROW_NUMBER() OVER (
            PARTITION BY product_dlt_id, attribute_code 
            ORDER BY attribute_dlt_id DESC
        ) AS row_num
    FROM renamed
)

SELECT
    product_dlt_id,
    attribute_code,
    attribute_value
FROM deduplicated
WHERE row_num = 1