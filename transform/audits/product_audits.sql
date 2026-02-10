-- Audit: Preço deve ser positivo
AUDIT (
    name assert_positive_price,
    dialect duckdb
);
SELECT * FROM @this_model WHERE price < 0;

-- Audit: SKU deve ser único
AUDIT (
    name assert_unique_sku,
    dialect duckdb
);
SELECT sku, COUNT(*) as cnt 
FROM @this_model 
GROUP BY sku 
HAVING COUNT(*) > 1;

-- Audit: Não pode ter produto sem nome
AUDIT (
    name assert_product_has_name,
    dialect duckdb
);
SELECT * FROM @this_model WHERE product_name IS NULL OR product_name = '';

-- Audit: Desconto não pode ser maior que 100%
AUDIT (
    name assert_valid_discount_pct,
    dialect duckdb
);
SELECT * FROM @this_model WHERE discount_pct > 100 OR discount_pct < 0;