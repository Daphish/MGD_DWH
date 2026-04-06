-- ============================================================================
-- fix_drop_bad_tables_dwh.sql
-- Ejecutar contra la BD DWH PostgreSQL (NO la de config).
--
-- Elimina TODAS las tablas del DWH para insertar en limpio.
-- ============================================================================

DROP TABLE IF EXISTS inventory;
DROP TABLE IF EXISTS invoices;
DROP TABLE IF EXISTS services;
DROP TABLE IF EXISTS spares;
DROP TABLE IF EXISTS vehicle_orders;
DROP TABLE IF EXISTS commissions;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS services_by_vin;
DROP TABLE IF EXISTS customer_vehicle;
DROP TABLE IF EXISTS customers_vehicle;
DROP TABLE IF EXISTS last_customer_seller;
DROP TABLE IF EXISTS last_customer_sale;
