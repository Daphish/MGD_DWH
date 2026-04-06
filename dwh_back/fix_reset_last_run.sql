-- ============================================================================
-- fix_reset_last_run.sql
-- Ejecutar contra la BD de configuración (mgd_dwh_config).
--
-- Resetea last_run_at de TODAS las tareas para carga completa desde origen.
-- ============================================================================

UPDATE agency_task SET last_run_at = NULL;
