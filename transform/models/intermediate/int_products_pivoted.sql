MODEL (
    name intermediate.int_products_pivoted,
    kind FULL,
    cron '@daily',
    grain [product_dlt_id],
    description 'Atributos EAV pivotados para colunas - transformação do modelo EAV para colunas planas'
);

WITH source AS (
    SELECT
        product_dlt_id,
        attribute_code,
        attribute_value
    FROM staging.stg_products__custom_attributes
),

-- Pivot: Atributos visuais e descritivos
visual_attributes AS (
    SELECT
        product_dlt_id,
        MAX(CASE WHEN attribute_code = 'color' THEN attribute_value END) AS color,
        MAX(CASE WHEN attribute_code = 'size' THEN attribute_value END) AS size,
        MAX(CASE WHEN attribute_code = 'material' THEN attribute_value END) AS material,
        MAX(CASE WHEN attribute_code = 'pattern' THEN attribute_value END) AS pattern,
        MAX(CASE WHEN attribute_code = 'climate' THEN attribute_value END) AS climate,
        MAX(CASE WHEN attribute_code = 'gender' THEN attribute_value END) AS gender
    FROM source
    GROUP BY product_dlt_id
),

-- Pivot: Categorização e estilos
style_attributes AS (
    SELECT
        product_dlt_id,
        MAX(CASE WHEN attribute_code = 'style_general' THEN attribute_value END) AS style_general,
        MAX(CASE WHEN attribute_code = 'style_bottom' THEN attribute_value END) AS style_bottom,
        MAX(CASE WHEN attribute_code = 'style_bags' THEN attribute_value END) AS style_bags,
        MAX(CASE WHEN attribute_code = 'activity' THEN attribute_value END) AS activity,
        MAX(CASE WHEN attribute_code = 'category_gear' THEN attribute_value END) AS category_gear,
        MAX(CASE WHEN attribute_code = 'features_bags' THEN attribute_value END) AS features_bags,
        MAX(CASE WHEN attribute_code = 'strap_bags' THEN attribute_value END) AS strap_bags
    FROM source
    GROUP BY product_dlt_id
),

-- Pivot: Flags de marketing
marketing_flags AS (
    SELECT
        product_dlt_id,
        MAX(CASE WHEN attribute_code = 'new' THEN attribute_value END) AS is_new,
        MAX(CASE WHEN attribute_code = 'sale' THEN attribute_value END) AS is_on_sale,
        MAX(CASE WHEN attribute_code = 'erin_recommends' THEN attribute_value END) AS erin_recommends,
        MAX(CASE WHEN attribute_code = 'eco_collection' THEN attribute_value END) AS eco_collection,
        MAX(CASE WHEN attribute_code = 'performance_fabric' THEN attribute_value END) AS performance_fabric
    FROM source
    GROUP BY product_dlt_id
),

-- Pivot: Preços especiais (com casting)
pricing_attributes AS (
    SELECT
        product_dlt_id,
        MAX(CASE WHEN attribute_code = 'msrp' 
            THEN TRY_CAST(attribute_value AS DECIMAL(10,2)) END) AS msrp,
        MAX(CASE WHEN attribute_code = 'special_price' 
            THEN TRY_CAST(attribute_value AS DECIMAL(10,2)) END) AS special_price,
        MAX(CASE WHEN attribute_code = 'special_from_date' 
            THEN attribute_value END) AS special_from_date
    FROM source
    GROUP BY product_dlt_id
),

-- Pivot: SEO e conteúdo
seo_attributes AS (
    SELECT
        product_dlt_id,
        MAX(CASE WHEN attribute_code = 'url_key' THEN attribute_value END) AS url_key,
        MAX(CASE WHEN attribute_code = 'description' THEN attribute_value END) AS description,
        MAX(CASE WHEN attribute_code = 'category_ids' THEN attribute_value END) AS category_ids
    FROM source
    GROUP BY product_dlt_id
),

-- Combina todos os atributos pivotados
combined AS (
    SELECT
        v.product_dlt_id,
        -- Visual
        v.color,
        v.size,
        v.material,
        v.pattern,
        v.climate,
        v.gender,
        -- Estilo
        s.style_general,
        s.style_bottom,
        s.style_bags,
        s.activity,
        s.category_gear,
        s.features_bags,
        s.strap_bags,
        -- Marketing
        m.is_new,
        m.is_on_sale,
        m.erin_recommends,
        m.eco_collection,
        m.performance_fabric,
        -- Preços
        p.msrp,
        p.special_price,
        p.special_from_date,
        -- SEO
        seo.url_key,
        seo.description,
        seo.category_ids
    FROM visual_attributes v
    LEFT JOIN style_attributes s ON v.product_dlt_id = s.product_dlt_id
    LEFT JOIN marketing_flags m ON v.product_dlt_id = m.product_dlt_id
    LEFT JOIN pricing_attributes p ON v.product_dlt_id = p.product_dlt_id
    LEFT JOIN seo_attributes seo ON v.product_dlt_id = seo.product_dlt_id
)

SELECT * FROM combined