-- ============================================================================
-- apply_schema_notes_existing_config.sql
-- BD de configuración (mgd_dwh_config) — aplicar si YA cargaste datos antes
-- de los cambios en insert_all_dms_configs.sql.
--
-- 1) Renombra destination_table al nuevo nombre de tabla en el DWH.
-- 2) Desactiva tareas BMW (agencia 11) que apuntan a objetos inexistentes.
-- 3) Tras esto: actualiza extract_sql de Services / Invoices manualmente
--    copiando desde insert_all_dms_configs.sql, o DROP tablas + NULL last_run.
-- ============================================================================

-- Tablas DWH renombradas en catálogo
UPDATE object_catalog
SET destination_table = 'customer_vehicle'
WHERE destination_table = 'customers_vehicle';

UPDATE object_catalog
SET destination_table = 'last_customer_seller'
WHERE destination_table = 'last_customer_sale';

-- Ajustar nombres de constraint en catálogo (referencia; el índice real está en PG)
UPDATE object_catalog
SET constraint_name = 'uk_customer_vehicle_agency_vin_client'
WHERE name = 'CustomersVehicle';

UPDATE object_catalog
SET constraint_name = 'uk_last_customer_seller_agency_order'
WHERE name = 'LastCustomerSale';

-- BMW Incadea: Spares y Customer vehicle (ajusta agency_id si tu BMW no es 11)
UPDATE agency_task at
SET is_active = FALSE
FROM object_catalog oc
WHERE at.object_catalog_id = oc.id
  AND at.agency_id = 11
  AND oc.name IN ('CustomersVehicle', 'Spares');
