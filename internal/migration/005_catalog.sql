CREATE INDEX IF NOT EXISTS products_active_sku_covering_idx
    ON products (sku)
    INCLUDE (
        name,
        product_type,
        price_minor,
        currency,
        image
        )
    WHERE active = TRUE;

CREATE INDEX IF NOT EXISTS inventory_sku_available_idx
    ON inventory (sku, available);