-- Tarea incluida en GET /configs (token de razón social). Si FALSE, la tarea
-- solo aplica en /agency-configs o /group-configs (evita repetir el mismo extract por cada agencia).
-- Ejecutar en la BD de configuración.

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'agency_task'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'agency_task'
          AND column_name = 'run_on_company_token'
    ) THEN
        ALTER TABLE agency_task
            ADD COLUMN run_on_company_token BOOLEAN NOT NULL DEFAULT TRUE;
        COMMENT ON COLUMN agency_task.run_on_company_token IS
            'Si TRUE, la tarea se devuelve en modo token de razón social (GET /configs). Si FALSE, solo agencia/grupo.';
    END IF;
END $$;
