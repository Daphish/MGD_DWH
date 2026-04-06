-- ============================================================
-- BD de CONFIGURACIÓN (mgd_dwh_config): alinear extract y DDL de
-- services_by_vin con insert_honda_services_by_vin.sql
--
-- Después, en el DWH ejecuta migrate_services_by_vin_widen_and_fix.sql
-- si la tabla ya existía con VARCHAR(32).
-- ============================================================

-- 1) Catálogo: DDL actualizado (CREATE TABLE IF NOT EXISTS en cada corrida del cliente)
UPDATE object_catalog
SET create_table_sql = $ddl$
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
$ddl$
WHERE destination_table = 'services_by_vin';

UPDATE object_catalog
SET
    constraint_name = 'uk_services_by_vin_agency_order',
    create_constraint_sql = $idx$
CREATE UNIQUE INDEX IF NOT EXISTS uk_services_by_vin_agency_order
    ON services_by_vin ("idAgency", "order_dms");
$idx$
WHERE destination_table = 'services_by_vin';

-- 2) Tarea 31 solamente: SELECT explícito con alias (evita Invoice/VIN vs invoice/vin en PostgreSQL)
UPDATE agency_task
SET extract_sql = $extract$
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
$extract$
WHERE id = 31;
