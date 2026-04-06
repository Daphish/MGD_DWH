-- ============================================================================
-- migrate_rename_last_customer_sale_to_seller.sql
-- DWH PostgreSQL: renombrar tabla física si aún se llama last_customer_sale.
-- ============================================================================

ALTER TABLE IF EXISTS last_customer_sale RENAME TO last_customer_seller;
