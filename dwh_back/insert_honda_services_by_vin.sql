-- ============================================================
-- Honda / SQLHONDA — carga de servicios desde vista view_dwh_services_by_vin
--
-- Requisitos en el origen (SQL Server):
--   - BD [SQLHONDA], vista dbo.view_dwh_services_by_vin (columnas alineadas con services_by_vin)
--   - Filtro incremental: timestamp_dms >= {last_run} (sustituido por el cliente ETL)
--
-- Cliente ETL (dwh_client):
--   - Sustituye {last_run} por last_run_at de la tarea o '1900-01-01 00:00:00' la 1ª vez
--   - Tras una carga OK, el backend actualiza last_run_at (marca de agua de ejecución)
--
-- Ajusta antes de ejecutar en la BD de CONFIGURACIÓN (mgd_dwh_config):
--   company_id, agency_id, nombres si ya existen catálogos duplicados
--   upsert_keys y el índice único si tu clave natural es otra (p. ej. incluir timestamp_hex)
-- ============================================================

-- ── 1) object_catalog (una fila por compañía que use este extract)
INSERT INTO object_catalog (
    company_id,
    name,
    description,
    destination_table,
    create_table_sql,
    upsert_keys,
    constraint_name,
    create_constraint_sql,
    is_enabled
) VALUES (
    3,
    'Services by VIN (Honda SQLHONDA)',
    'Servicios por agencia desde view_dwh_services_by_vin',
    'services_by_vin',
    $ddl$
CREATE TABLE IF NOT EXISTS services_by_vin (
    "idAgency" VARCHAR(128) NOT NULL,
    invoice TEXT,
    vin VARCHAR(128),
    order_dms VARCHAR(128) NOT NULL,
    "IdStatus" VARCHAR(32),
    "statusDescription" VARCHAR(128),
    "idServiceType" VARCHAR(128),
    "serviceType" VARCHAR(128),
    "serviceTypeDescription" TEXT,
    "serviceTypeDetail" TEXT,
    amount NUMERIC(18, 4),
    km NUMERIC(18, 2),
    "startDateTime" TIMESTAMP,
    "endDateTime" TIMESTAMP,
    "ndConsultant" VARCHAR(64),
    "consultantName" TEXT,
    consultant_phone VARCHAR(64),
    timestamp_dms TIMESTAMP,
    "timestamp" TIMESTAMP,
    timestamp_hex VARCHAR(128)
);
$ddl$,
    'idAgency,order_dms',
    'uk_services_by_vin_agency_order',
    $idx$
CREATE UNIQUE INDEX IF NOT EXISTS uk_services_by_vin_agency_order
    ON services_by_vin ("idAgency", "order_dms");
$idx$,
    TRUE
);

-- Si ya insertaste el catálogo y solo falta la tarea, usa el SELECT del paso 2 con
-- object_catalog_id = (SELECT id FROM object_catalog WHERE destination_table = 'services_by_vin' AND company_id = 1 LIMIT 1);


-- ── 2) agency_task — extract: SELECT explícito con alias = nombres de columna en PostgreSQL.
--    No uses SELECT *: la vista puede devolver Invoice/VIN y el destino espera invoice/vin.
--    Si tu vista renombra columnas, ajusta los alias (parte tras AS) para que coincidan con el DDL.

INSERT INTO agency_task (
    agency_id,
    object_catalog_id,
    extract_sql,
    schedule_seconds,
    is_active
) VALUES (
    4,
    (SELECT id FROM object_catalog WHERE destination_table = 'services_by_vin' AND company_id = 1 ORDER BY id DESC LIMIT 1),
    $extract$
SELECT
    v.idAgency AS idAgency,
    v.Invoice AS invoice,
    v.VIN AS vin,
    v.order_dms AS order_dms,
    v.IdStatus AS IdStatus,
    v.statusDescription AS statusDescription,
    v.idServiceType AS idServiceType,
    v.serviceType AS serviceType,
    v.serviceTypeDescription AS serviceTypeDescription,
    v.serviceTypeDetail AS serviceTypeDetail,
    v.amount AS amount,
    v.km AS km,
    v.startDateTime AS startDateTime,
    v.endDateTime AS endDateTime,
    v.ndConsultant AS ndConsultant,
    v.consultantName AS consultantName,
    v.consultant_phone AS consultant_phone,
    v.timestamp_dms AS timestamp_dms,
    v.[timestamp] AS [timestamp],
    v.timestamp_hex AS timestamp_hex
FROM dbo.view_dwh_services_by_vin AS v
WHERE v.timestamp_dms >= '{last_run}'
ORDER BY v.timestamp_dms ASC
$extract$,
    3600,
    TRUE
);

-- Si ves error "Incorrect syntax near '1900-01-01...'" en SQL Server, el texto guardado
-- no tenía comillas alrededor de {last_run}. Corrige la fila, p. ej.:
--   UPDATE agency_task SET extract_sql = $e$
--   SELECT v.idAgency AS idAgency, v.Invoice AS invoice, ... (mismo SELECT explícito del paso 2)
--   $e$ WHERE id = 21;

-- Opcional (si ya migraste): una sola fila en modo token de razón social sin repetir por agencia
-- UPDATE agency_task SET run_on_company_token = TRUE
-- WHERE object_catalog_id = (SELECT id FROM object_catalog WHERE destination_table = 'services_by_vin' AND company_id = 1 LIMIT 1)
--   AND agency_id = <id de la agencia “canónica”>;
