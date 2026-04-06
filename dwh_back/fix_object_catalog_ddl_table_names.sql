-- ============================================================================
-- fix_object_catalog_ddl_table_names.sql
-- BD de configuración (mgd_dwh_config).
--
-- destination_table ya dice customer_vehicle / last_customer_seller pero el
-- create_table_sql seguía creando customers_vehicle / last_customer_sale.
-- ============================================================================

-- Customer vehicle: alinear nombre en DDL e índice único
UPDATE object_catalog
SET
    create_table_sql = replace(
        create_table_sql,
        'CREATE TABLE IF NOT EXISTS customers_vehicle',
        'CREATE TABLE IF NOT EXISTS customer_vehicle'
    ),
    create_constraint_sql = CASE
        WHEN create_constraint_sql IS NOT NULL THEN
            replace(
                replace(create_constraint_sql, 'customers_vehicle', 'customer_vehicle'),
                'uk_cv_agency_vin_client',
                'uk_customer_vehicle_agency_vin_client'
            )
        ELSE create_constraint_sql
    END
WHERE destination_table = 'customer_vehicle'
  AND create_table_sql IS NOT NULL
  AND create_table_sql LIKE '%customers_vehicle%';

-- Last customer seller: alinear nombre en DDL e índice
UPDATE object_catalog
SET
    create_table_sql = replace(
        create_table_sql,
        'CREATE TABLE IF NOT EXISTS last_customer_sale',
        'CREATE TABLE IF NOT EXISTS last_customer_seller'
    ),
    create_constraint_sql = CASE
        WHEN create_constraint_sql IS NOT NULL THEN
            replace(
                replace(create_constraint_sql, 'last_customer_sale', 'last_customer_seller'),
                'uk_lcs_agency_order',
                'uk_last_customer_seller_agency_order'
            )
        ELSE create_constraint_sql
    END
WHERE destination_table = 'last_customer_seller'
  AND create_table_sql IS NOT NULL
  AND create_table_sql LIKE '%last_customer_sale%';
