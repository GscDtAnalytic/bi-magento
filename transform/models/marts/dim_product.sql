MODEL (
    name marts.dim_product,
    kind FULL,
    cron '@daily',
    grain [product_id],
    description 'Dimensão Produto com atributos EAV pivotados e métricas calculadas. Pronto para dashboards.'
);

-- CTE 1: Fonte dos produtos base

WITH products AS (
    SELECT
        product_id,
        sku,
        product_name,
        product_type,
        attribute_set_id,
        status,
        visibility,
        weight,
        price,
        created_at,
        updated_at,
        _dlt_id
    FROM staging.stg_products
),

-- CTE 2: Atributos EAV pivotados

eav_attributes AS (
    SELECT
        product_dlt_id,
        color,
        size,
        material,
        pattern,
        climate,
        gender,
        style_general,
        style_bottom,
        style_bags,
        activity,
        category_gear,
        features_bags,
        strap_bags,
        is_new,
        is_on_sale,
        erin_recommends,
        eco_collection,
        performance_fabric,
        msrp,
        special_price,
        special_from_date,
        url_key,
        description,
        category_ids
    FROM intermediate.int_products_pivoted
),


-- CTE 3: Join produtos com atributos EAV

products_with_eav AS (
    SELECT
        p.product_id,
        p.sku,
        p.product_name,
        p.product_type,
        p.attribute_set_id,
        p.status,
        p.visibility,
        p.weight,
        p.price,
        p.created_at,
        p.updated_at,
        -- EAV attributes
        eav.color,
        eav.size,
        eav.material,
        eav.pattern,
        eav.climate,
        eav.gender,
        eav.style_general,
        eav.style_bottom,
        eav.style_bags,
        eav.activity,
        eav.category_gear,
        eav.features_bags,
        eav.strap_bags,
        eav.is_new,
        eav.is_on_sale,
        eav.erin_recommends,
        eav.eco_collection,
        eav.performance_fabric,
        eav.msrp,
        eav.special_price,
        eav.special_from_date,
        eav.url_key,
        eav.description,
        eav.category_ids
    FROM products p
    LEFT JOIN eav_attributes eav ON p._dlt_id = eav.product_dlt_id
),

-- CTE 4: Tratamento de nulos e conversão de tipos

cleaned AS (
    SELECT
        product_id,
        sku,
        product_name,
        product_type,
        attribute_set_id,
        status,
        visibility,
        weight,
        price,
        created_at,
        updated_at,
        -- Atributos visuais com defaults
        COALESCE(color, 'Sem Cor') AS color,
        COALESCE(size, 'Sem Tamanho') AS size,
        COALESCE(material, 'Sem Material') AS material,
        pattern,
        climate,
        gender,
        -- Estilos
        style_general,
        style_bottom,
        style_bags,
        activity,
        category_gear,
        features_bags,
        strap_bags,
        -- Flags convertidos para boolean
        CASE WHEN is_new = '1' THEN TRUE ELSE FALSE END AS is_new,
        CASE WHEN is_on_sale = '1' THEN TRUE ELSE FALSE END AS is_on_sale,
        CASE WHEN erin_recommends = '1' THEN TRUE ELSE FALSE END AS erin_recommends,
        CASE WHEN eco_collection = '1' THEN TRUE ELSE FALSE END AS eco_collection,
        CASE WHEN performance_fabric = '1' THEN TRUE ELSE FALSE END AS performance_fabric,
        -- Preços
        msrp,
        special_price,
        special_from_date,
        -- SEO
        url_key,
        description,
        category_ids
    FROM products_with_eav
),

-- CTE 5: Cálculo de métricas
with_metrics AS (
    SELECT
        *,
        CASE 
            WHEN msrp IS NOT NULL AND msrp > 0 
            THEN ROUND(msrp - price, 2)
            ELSE 0 
        END AS discount_from_msrp,
        
        CASE 
            WHEN msrp IS NOT NULL AND msrp > 0 
            THEN ROUND(((msrp - price) / msrp) * 100, 2)
            ELSE 0 
        END AS discount_pct_from_msrp,
        
        CASE 
            WHEN special_price IS NOT NULL AND special_price > 0 
            THEN ROUND(price - special_price, 2)
            ELSE 0 
        END AS promotional_discount,
        
        CASE 
            WHEN special_price IS NOT NULL AND price > 0 
            THEN ROUND(((price - special_price) / price) * 100, 2)
            ELSE 0 
        END AS promotional_discount_pct,
        
        -- Status label
        CASE WHEN status = 1 THEN 'Ativo' ELSE 'Inativo' END AS status_label
    FROM cleaned
),


-- CTE 6: Seleção final com metadados

final AS (
    SELECT
        -- Chaves
        product_id,
        sku,
        
        -- Atributos base
        product_name,
        product_type,
        attribute_set_id,
        status,
        status_label,
        visibility,
        weight,
        
        -- Atributos visuais
        color,
        size,
        material,
        pattern,
        climate,
        gender,
        
        -- Estilos e categorias
        style_general,
        style_bottom,
        style_bags,
        activity,
        category_gear,
        features_bags,
        strap_bags,
        
        -- Flags de marketing
        is_new,
        is_on_sale,
        erin_recommends,
        eco_collection,
        performance_fabric,
        
        -- Preços
        price,
        msrp,
        special_price,
        special_from_date,
        
        -- Métricas calculadas
        discount_from_msrp,
        discount_pct_from_msrp,
        promotional_discount,
        promotional_discount_pct,
        
        -- SEO
        url_key,
        description,
        category_ids,
        
        -- Metadados
        created_at,
        updated_at,
        CURRENT_TIMESTAMP AS dw_loaded_at
    FROM with_metrics
)

SELECT * FROM final