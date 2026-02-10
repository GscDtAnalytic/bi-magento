MODEL (
    name staging.stg_products,
    kind FULL,
    cron '@daily',
    grain [product_id],
    description 'Produtos do Magento - dados base normalizados'
);

WITH source AS (
    SELECT
        id,
        sku,
        name,
        attribute_set_id,
        price,
        status,
        visibility,
        type_id,
        weight,
        created_at,
        updated_at,
        _dlt_id,
        _dlt_load_id
    FROM magento_data.products
),

casted AS (
    SELECT
        id AS product_id,
        sku,
        name AS product_name,
        attribute_set_id,
        TRY_CAST(price AS DECIMAL(10, 2)) AS price,
        status,
        visibility,
        type_id AS product_type,
        TRY_CAST(weight AS DECIMAL(10, 4)) AS weight,
        created_at,
        updated_at,
        _dlt_id,
        _dlt_load_id
    FROM source
)

SELECT * FROM casted