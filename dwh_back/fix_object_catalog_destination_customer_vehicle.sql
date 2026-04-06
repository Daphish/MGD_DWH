-- ============================================================================
-- fix_object_catalog_destination_customer_vehicle.sql
-- BD de configuración (mgd_dwh_config).
-- ============================================================================

UPDATE object_catalog
SET destination_table = 'customer_vehicle'
WHERE destination_table = 'customers_vehicle';

UPDATE object_catalog
SET destination_table = 'last_customer_seller'
WHERE destination_table = 'last_customer_sale';
