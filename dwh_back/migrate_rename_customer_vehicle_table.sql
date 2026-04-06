-- ============================================================================
-- migrate_rename_customer_vehicle_table.sql
-- Ejecutar contra el DWH PostgreSQL si aún tienes el nombre antiguo.
-- ============================================================================

ALTER TABLE IF EXISTS customers_vehicle RENAME TO customer_vehicle;
