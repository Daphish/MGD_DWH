-- =============================================================================
-- INSERT COMPLETO: object_catalog + agency_task para todas las entidades
-- Basado en DMSQueries.cs (excluyendo Lead y company_id = 1 / Vanguardia)
-- Ejecutar en la BD de CONFIGURACIÓN (mgd_dwh_config)
-- =============================================================================
--
-- Mapeo compañía → agencia → rama DMS:
-- ┌─────┬──────────┬────────┬──────────────────────────────┬──────────┐
-- │ co. │ company  │ ag.id  │ agency_name                  │ DMS      │
-- ├─────┼──────────┼────────┼──────────────────────────────┼──────────┤
-- │  2  │ Honda    │   3    │ Honda VANGUARDIA GALERIAS    │ Total    │
-- │  3  │ Kia      │   4    │ KIA ALTARIA                  │ Total    │
-- │  4  │ Audi     │   5    │ AUDI GALERIAS                │ Total    │
-- │  5  │ Motonova │   6    │ MOTONOVA JROMO               │ Total    │
-- │  6  │ Omoda    │   7    │ OMODA PATRIA                 │ Total    │
-- │  7  │ Geely    │   8    │ GEELY GALERIAS               │ Total    │
-- │  8  │ Chirey   │   9    │ CHIREY MANZANILLO            │ Total    │
-- │  9  │ Quiter   │  10    │ RENAULT AMERICAS             │ Quiter   │
-- │ 10  │ BMW      │  11    │ BMW VANGUARDIA MOTORS        │ Incadea  │
-- └─────┴──────────┴────────┴──────────────────────────────┴──────────┘
--
-- Convenciones:
--   @lastExecution / @start_date  →  '{last_run}'  (el cliente sustituye)
--   SPs (Total)                   →  EXEC dbo.sp '{last_run}' (posicional)
--   ON CONFLICT DO NOTHING        →  seguro ejecutar varias veces
--   create_table_sql              →  CREATE TABLE IF NOT EXISTS (no-op si existe)
--   Columnas camelCase en DDL     →  entre comillas dobles para PostgreSQL
-- =============================================================================


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  1. INVENTORY                                                           ║
-- ║  destination_table: inventory                                           ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

-- DDL: columnas desconocidas (SELECT * a vista). Si la tabla ya existe, es no-op.
-- Si NO existe, revisa las columnas de view_get_dwh_inventory y ajusta este DDL.

INSERT INTO object_catalog
    (company_id, name, description, destination_table, create_table_sql, upsert_keys, constraint_name, create_constraint_sql, is_enabled)
VALUES
    (2,  'Inventory', 'Inventario de vehículos', 'inventory', NULL, '', NULL, NULL, TRUE),
    (3,  'Inventory', 'Inventario de vehículos', 'inventory', NULL, '', NULL, NULL, TRUE),
    (4,  'Inventory', 'Inventario de vehículos', 'inventory', NULL, '', NULL, NULL, TRUE),
    (5,  'Inventory', 'Inventario de vehículos', 'inventory', NULL, '', NULL, NULL, TRUE),
    (6,  'Inventory', 'Inventario de vehículos', 'inventory', NULL, '', NULL, NULL, TRUE),
    (7,  'Inventory', 'Inventario de vehículos', 'inventory', NULL, '', NULL, NULL, TRUE),
    (8,  'Inventory', 'Inventario de vehículos', 'inventory', NULL, '', NULL, NULL, TRUE),
    (9,  'Inventory', 'Inventario de vehículos', 'inventory', NULL, '', NULL, NULL, TRUE),
    (10, 'Inventory', 'Inventario de vehículos', 'inventory', NULL, '', NULL, NULL, TRUE)
ON CONFLICT (company_id, name) DO NOTHING;

-- Total (agencies 3-9)
INSERT INTO agency_task (agency_id, object_catalog_id, extract_sql, schedule_seconds, is_active)
SELECT a_id, oc.id,
    $$SELECT * FROM view_get_dwh_inventory WHERE timestamp_dms >= '{last_run}' ORDER BY timestamp_dms ASC$$,
    3600, TRUE
FROM object_catalog oc, unnest(ARRAY[3,4,5,6,7,8,9]) AS a_id
WHERE oc.name = 'Inventory' AND oc.company_id = (SELECT company_id FROM agency WHERE id = a_id)
ON CONFLICT (agency_id, object_catalog_id) DO NOTHING;

-- Quiter (agency 10)
INSERT INTO agency_task (agency_id, object_catalog_id, extract_sql, schedule_seconds, is_active)
SELECT 10, oc.id,
    $$SELECT * FROM view_get_dwh_inventory WHERE timestamp_dms >= '{last_run}'$$,
    3600, TRUE
FROM object_catalog oc WHERE oc.company_id = 9 AND oc.name = 'Inventory'
ON CONFLICT (agency_id, object_catalog_id) DO NOTHING;

-- Incadea / BMW (agency 11)
INSERT INTO agency_task (agency_id, object_catalog_id, extract_sql, schedule_seconds, is_active)
SELECT 11, oc.id,
    $$SELECT * FROM view_get_dwh_inventory WHERE timestamp_dms >= '{last_run}' ORDER BY timestamp_dms ASC$$,
    3600, TRUE
FROM object_catalog oc WHERE oc.company_id = 10 AND oc.name = 'Inventory'
ON CONFLICT (agency_id, object_catalog_id) DO NOTHING;


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  2. CUSTOMERS                                                           ║
-- ║  destination_table: customers   (puede que ya existan filas)            ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

INSERT INTO object_catalog
    (company_id, name, description, destination_table, create_table_sql, upsert_keys, constraint_name, create_constraint_sql, is_enabled)
VALUES
    (2,  'Customers', 'Clientes', 'customers', NULL, 'idAgency,ndClientDMS', 'uk_agency_client', $$CREATE UNIQUE INDEX IF NOT EXISTS uk_agency_client ON customers ("idAgency", "ndClientDMS")$$, TRUE),
    (3,  'Customers', 'Clientes', 'customers', NULL, 'idAgency,ndClientDMS', 'uk_agency_client', $$CREATE UNIQUE INDEX IF NOT EXISTS uk_agency_client ON customers ("idAgency", "ndClientDMS")$$, TRUE),
    (4,  'Customers', 'Clientes', 'customers', NULL, 'idAgency,ndClientDMS', 'uk_agency_client', $$CREATE UNIQUE INDEX IF NOT EXISTS uk_agency_client ON customers ("idAgency", "ndClientDMS")$$, TRUE),
    (5,  'Customers', 'Clientes', 'customers', NULL, 'idAgency,ndClientDMS', 'uk_agency_client', $$CREATE UNIQUE INDEX IF NOT EXISTS uk_agency_client ON customers ("idAgency", "ndClientDMS")$$, TRUE),
    (6,  'Customers', 'Clientes', 'customers', NULL, 'idAgency,ndClientDMS', 'uk_agency_client', $$CREATE UNIQUE INDEX IF NOT EXISTS uk_agency_client ON customers ("idAgency", "ndClientDMS")$$, TRUE),
    (7,  'Customers', 'Clientes', 'customers', NULL, 'idAgency,ndClientDMS', 'uk_agency_client', $$CREATE UNIQUE INDEX IF NOT EXISTS uk_agency_client ON customers ("idAgency", "ndClientDMS")$$, TRUE),
    (8,  'Customers', 'Clientes', 'customers', NULL, 'idAgency,ndClientDMS', 'uk_agency_client', $$CREATE UNIQUE INDEX IF NOT EXISTS uk_agency_client ON customers ("idAgency", "ndClientDMS")$$, TRUE),
    (9,  'Customers', 'Clientes', 'customers', NULL, 'idAgency,ndClientDMS', 'uk_agency_client', $$CREATE UNIQUE INDEX IF NOT EXISTS uk_agency_client ON customers ("idAgency", "ndClientDMS")$$, TRUE),
    (10, 'Customers', 'Clientes', 'customers', NULL, 'idAgency,ndClientDMS', 'uk_agency_client', $$CREATE UNIQUE INDEX IF NOT EXISTS uk_agency_client ON customers ("idAgency", "ndClientDMS")$$, TRUE)
ON CONFLICT (company_id, name) DO NOTHING;

-- Total (agencies 3-9): SELECT * a la vista TD
INSERT INTO agency_task (agency_id, object_catalog_id, extract_sql, schedule_seconds, is_active)
SELECT a_id, oc.id,
    $$SELECT * FROM view_get_dwh_customers WHERE timestamp_dms >= '{last_run}' ORDER BY ndClientDMS$$,
    3600, TRUE
FROM object_catalog oc, unnest(ARRAY[3,4,5,6,7,8,9]) AS a_id
WHERE oc.name = 'Customers' AND oc.company_id = (SELECT company_id FROM agency WHERE id = a_id)
ON CONFLICT (agency_id, object_catalog_id) DO NOTHING;

-- Quiter (agency 10)
INSERT INTO agency_task (agency_id, object_catalog_id, extract_sql, schedule_seconds, is_active)
SELECT 10, oc.id,
    $$SELECT * FROM view_get_dwh_customers WHERE timestamp_dms >= '{last_run}' ORDER BY ndClientDMS$$,
    3600, TRUE
FROM object_catalog oc WHERE oc.company_id = 9 AND oc.name = 'Customers'
ON CONFLICT (agency_id, object_catalog_id) DO NOTHING;

-- Incadea / BMW (agency 11) — columnas alineadas con DDL de customers
INSERT INTO agency_task (agency_id, object_catalog_id, extract_sql, schedule_seconds, is_active)
SELECT 11, oc.id, $inc_cust$
SELECT
    idAgency, ndClientDMS, name, second_name AS paternal_surname,
    last_name AS maternal_surname, bussines_name, rfc, curp,
    phone, mobile_phone, other_phone, assitant_phone, office_phone,
    mail, activitie, street, external_number, internal_number,
    zipcode, between_streets, settlement, deputation, country, city, state,
    TRY_CONVERT(DATE, CONVERT(VARCHAR(50), birthay_date)) AS birthay_date,
    salutation, gender, costumer_type, appointment, allow_contact,
    ndSeller, seller_Name, clasification,
    CONVERT(VARCHAR(64), last_sale) AS last_sale,
    TRY_CONVERT(DATETIME2(0), CONVERT(VARCHAR(50), timestamp_dms)) AS timestamp_dms,
    TRY_CONVERT(DATETIME2(0), CONVERT(VARCHAR(50), [timestamp])) AS [timestamp],
    timestamp_hex,
    CAST(NULL AS VARCHAR(50)) AS "Est_Civil",
    CAST(NULL AS VARCHAR(256)) AS "seller_Email",
    CAST(NULL AS VARCHAR(50)) AS customer_source,
    CAST(NULL AS VARCHAR(50)) AS preferred_contact_method,
    CAST(0 AS SMALLINT) AS is_consolidated
FROM DW_Clientes
WHERE TRY_CONVERT(DATETIME2(0), CONVERT(VARCHAR(50), timestamp_dms))
      >= TRY_CONVERT(DATETIME2(0), '{last_run}')
ORDER BY timestamp_hex ASC
$inc_cust$, 3600, TRUE
FROM object_catalog oc WHERE oc.company_id = 10 AND oc.name = 'Customers'
ON CONFLICT (agency_id, object_catalog_id) DO NOTHING;


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  3. INVOICES                                                            ║
-- ║  destination_table: invoices                                            ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

INSERT INTO object_catalog
    (company_id, name, description, destination_table, create_table_sql, upsert_keys, constraint_name, create_constraint_sql, is_enabled)
VALUES
    (2,  'Invoices', 'Facturas', 'invoices', $inv_ddl$
CREATE TABLE IF NOT EXISTS invoices (
    id BIGSERIAL PRIMARY KEY,
    "idAgency" TEXT NOT NULL, order_dms TEXT, state TEXT, vin TEXT,
    warranty_init_date TEXT, plates TEXT, payment_method TEXT,
    sub_total TEXT, accesories TEXT, amount_accesories TEXT,
    financial_terms TEXT, invoice_reference TEXT, billing_date TEXT,
    amount_taxes TEXT, financial_institution TEXT, delivery_date TEXT,
    cancelation_date TEXT, stage_name TEXT, timestamp_dms TIMESTAMP,
    timestamp_hex TEXT, close_date TEXT, description TEXT,
    "timestamp" TIMESTAMP, "ndClientDMS" TEXT, client_bussines_name TEXT,
    CONSTRAINT uk_invoices_agency_order UNIQUE ("idAgency", order_dms)
)$inv_ddl$, 'idAgency,order_dms', 'uk_invoices_agency_order', NULL, TRUE),
    (3,  'Invoices', 'Facturas', 'invoices', NULL, 'idAgency,order_dms', 'uk_invoices_agency_order', NULL, TRUE),
    (4,  'Invoices', 'Facturas', 'invoices', NULL, 'idAgency,order_dms', 'uk_invoices_agency_order', NULL, TRUE),
    (5,  'Invoices', 'Facturas', 'invoices', NULL, 'idAgency,order_dms', 'uk_invoices_agency_order', NULL, TRUE),
    (6,  'Invoices', 'Facturas', 'invoices', NULL, 'idAgency,order_dms', 'uk_invoices_agency_order', NULL, TRUE),
    (7,  'Invoices', 'Facturas', 'invoices', NULL, 'idAgency,order_dms', 'uk_invoices_agency_order', NULL, TRUE),
    (8,  'Invoices', 'Facturas', 'invoices', NULL, 'idAgency,order_dms', 'uk_invoices_agency_order', NULL, TRUE),
    (9,  'Invoices', 'Facturas', 'invoices', NULL, 'idAgency,order_dms', 'uk_invoices_agency_order', NULL, TRUE),
    (10, 'Invoices', 'Facturas', 'invoices', NULL, 'idAgency,order_dms', 'uk_invoices_agency_order', NULL, TRUE)
ON CONFLICT (company_id, name) DO NOTHING;

-- Total (agencies 3-9) — SP; verifica que el parámetro posicional sea correcto
INSERT INTO agency_task (agency_id, object_catalog_id, extract_sql, schedule_seconds, is_active)
SELECT a_id, oc.id,
    $$EXEC dbo.sp_get_invoices '{last_run}'$$,
    60, TRUE
FROM object_catalog oc, unnest(ARRAY[3,4,5,6,7,8,9]) AS a_id
WHERE oc.name = 'Invoices' AND oc.company_id = (SELECT company_id FROM agency WHERE id = a_id)
ON CONFLICT (agency_id, object_catalog_id) DO NOTHING;

-- Incadea / BMW (agency 11)
INSERT INTO agency_task (agency_id, object_catalog_id, extract_sql, schedule_seconds, is_active)
SELECT 11, oc.id, $inc_inv$
SELECT
    ndPlant AS idAgency, order_dms, state, vin, warranty_init_date,
    plates, payment_method, sub_total, accesories, amount_accesories,
    financial_terms, invoice_reference, billing_date, amount_taxes,
    financial_institution, delivery_date, cancelation_date, stage_name,
    timestamp_dms, timestamp_hex, close_date, description, [timestamp],
    CustomerId AS ndClientDMS, Cliente AS client_bussines_name
FROM view_dwh_invoices
WHERE timestamp_dms >= '{last_run}'
$inc_inv$, 60, TRUE
FROM object_catalog oc WHERE oc.company_id = 10 AND oc.name = 'Invoices'
ON CONFLICT (agency_id, object_catalog_id) DO NOTHING;

-- Quiter / Renault (agency 10)
INSERT INTO agency_task (agency_id, object_catalog_id, extract_sql, schedule_seconds, is_active)
SELECT 10, oc.id, $qt_inv$
SELECT DISTINCT
    48410047 AS idAgency,
    PED.REFERENCIA AS order_dms,
    'Facturado' AS state,
    VEHI.BASTIDOR AS vin,
    FAPR.FEC_FACTURA AS warranty_init_date,
    VEHI.MATRICULA AS plates,
    PED.DES_TIPO_VENTA_DEST AS payment_method,
    PED.TOTAL_OFERTA AS sub_total,
    0 AS accesories,
    0 AS amount_accesories,
    '' AS financial_terms,
    FAPR.NUM_FACTURA AS invoice_reference,
    FAPR.FEC_FACTURA AS billing_date,
    FAPR.IMP_IVA AS amount_taxes,
    '' AS financial_institution,
    PED.FEC_FINAL AS delivery_date,
    '' AS cancelation_date,
    'Facturacion del vehiculo' AS stage_name,
    VEHI.FEC_ULTIMA_MODIFICACION AS timestamp_dms,
    CAST(VEHI.FEC_ULTIMA_MODIFICACION AS varbinary) AS timestamp_hex,
    '' AS close_date,
    '' AS description,
    GETDATE() AS [timestamp],
    PED.CLIENTE AS ndClientDMS,
    COALESCE(NULLIF(CLI.NOMBRE_COMERCIAL, ''), NULLIF(CLI.NOMBRE, ''), NULLIF(CLI.NOMBRE_PERSONAL, ''), '') AS client_bussines_name
FROM FMVEHBI_PR AS VEHI
INNER JOIN FTOFVEBI_PR AS PED ON VEHI.IDV = PED.IDV
INNER JOIN FTVENBI_PR  AS FAPR ON PED.IDV = FAPR.IDV AND FAPR.COD_CONCEPTO = 'FF'
LEFT  JOIN FMCUBI_PR   AS CLI  ON LTRIM(RTRIM(CLI.CUENTA)) = LTRIM(RTRIM(PED.CLIENTE))
WHERE VEHI.FEC_ULTIMA_MODIFICACION >= '{last_run}'
ORDER BY timestamp_dms ASC
$qt_inv$, 60, TRUE
FROM object_catalog oc WHERE oc.company_id = 9 AND oc.name = 'Invoices'
ON CONFLICT (agency_id, object_catalog_id) DO NOTHING;


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  4. SERVICES                                                            ║
-- ║  destination_table: services                                            ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

INSERT INTO object_catalog
    (company_id, name, description, destination_table, create_table_sql, upsert_keys, constraint_name, create_constraint_sql, is_enabled)
VALUES
    (2,  'Services', 'Servicios', 'services', $svc_ddl$
CREATE TABLE IF NOT EXISTS services (
    id BIGSERIAL PRIMARY KEY,
    "idAgency" TEXT NOT NULL, order_dms TEXT, service_date TIMESTAMP,
    service_type TEXT, service_to_perform TEXT, kms TEXT,
    "ndClientDMS" TEXT, "nmVendedor" TEXT, vin TEXT,
    status TEXT, stage_name TEXT, "timestamp" TIMESTAMP,
    timestamp_dms TIMESTAMP, timestamp_hex TEXT, amount TEXT,
    CONSTRAINT uk_services_agency_order UNIQUE ("idAgency", order_dms)
)$svc_ddl$, 'idAgency,order_dms', 'uk_services_agency_order',
    NULL, TRUE),
    (3,  'Services', 'Servicios', 'services', NULL, 'idAgency,order_dms', 'uk_services_agency_order', NULL, TRUE),
    (4,  'Services', 'Servicios', 'services', NULL, 'idAgency,order_dms', 'uk_services_agency_order', NULL, TRUE),
    (5,  'Services', 'Servicios', 'services', NULL, 'idAgency,order_dms', 'uk_services_agency_order', NULL, TRUE),
    (6,  'Services', 'Servicios', 'services', NULL, 'idAgency,order_dms', 'uk_services_agency_order', NULL, TRUE),
    (7,  'Services', 'Servicios', 'services', NULL, 'idAgency,order_dms', 'uk_services_agency_order', NULL, TRUE),
    (8,  'Services', 'Servicios', 'services', NULL, 'idAgency,order_dms', 'uk_services_agency_order', NULL, TRUE),
    (9,  'Services', 'Servicios', 'services', NULL, 'idAgency,order_dms', 'uk_services_agency_order', NULL, TRUE),
    (10, 'Services', 'Servicios', 'services', NULL, 'idAgency,order_dms', 'uk_services_agency_order', NULL, TRUE)
ON CONFLICT (company_id, name) DO NOTHING;

-- Total (agencies 3-9) — SP
INSERT INTO agency_task (agency_id, object_catalog_id, extract_sql, schedule_seconds, is_active)
SELECT a_id, oc.id,
    $$EXEC dbo.sp_get_services '{last_run}'$$,
    300, TRUE
FROM object_catalog oc, unnest(ARRAY[3,4,5,6,7,8,9]) AS a_id
WHERE oc.name = 'Services' AND oc.company_id = (SELECT company_id FROM agency WHERE id = a_id)
ON CONFLICT (agency_id, object_catalog_id) DO NOTHING;

-- Incadea / BMW (agency 11)
INSERT INTO agency_task (agency_id, object_catalog_id, extract_sql, schedule_seconds, is_active)
SELECT 11, oc.id, $inc_svc$
SELECT
    ndPlant AS idAgency, order_dms, service_date, service_type,
    servicer_to_performe AS service_to_perform,
    km AS kms,
    CAST(NULL AS NVARCHAR(64)) AS ndClientDMS,
    CAST(NULL AS NVARCHAR(256)) AS nmVendedor,
    vin, status, stage_name,
    [timestamp], timestamp_dms, timestamp_hex, amount
FROM DW_Servicios
WHERE timestamp_dms >= '{last_run}'
ORDER BY timestamp_hex ASC
$inc_svc$, 300, TRUE
FROM object_catalog oc WHERE oc.company_id = 10 AND oc.name = 'Services'
ON CONFLICT (agency_id, object_catalog_id) DO NOTHING;

-- Quiter / Renault (agency 10)
INSERT INTO agency_task (agency_id, object_catalog_id, extract_sql, schedule_seconds, is_active)
SELECT 10, oc.id, $qt_svc$
SELECT
    idAgency, order_dms, service_date, service_type,
    service_to_performe AS service_to_perform,
    km AS kms,
    CAST(NULL AS NVARCHAR(64)) AS ndClientDMS,
    CAST(NULL AS NVARCHAR(256)) AS nmVendedor,
    vin, status, stage_name, [timestamp], timestamp_dms, timestamp_hex, amount
FROM view_dwh_services
WHERE timestamp_dms >= '{last_run}'
ORDER BY timestamp_hex ASC
$qt_svc$, 300, TRUE
FROM object_catalog oc WHERE oc.company_id = 9 AND oc.name = 'Services'
ON CONFLICT (agency_id, object_catalog_id) DO NOTHING;


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  5. SERVICES BY VIN                                                     ║
-- ║  destination_table: services_by_vin  (puede que ya existan filas)       ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

INSERT INTO object_catalog
    (company_id, name, description, destination_table, create_table_sql, upsert_keys, constraint_name, create_constraint_sql, is_enabled)
VALUES
    (2,  'ServicesByVin', 'Servicios por VIN', 'services_by_vin', $sbv_ddl$
CREATE TABLE IF NOT EXISTS services_by_vin (
    id BIGSERIAL PRIMARY KEY,
    "idAgency" VARCHAR(128) NOT NULL, invoice TEXT, vin VARCHAR(128),
    order_dms VARCHAR(128) NOT NULL, "IdStatus" VARCHAR(32),
    "statusDescription" VARCHAR(128), "idServiceType" VARCHAR(128),
    "serviceType" VARCHAR(128), "serviceTypeDescription" TEXT,
    "serviceTypeDetail" TEXT, amount NUMERIC(18,4), km NUMERIC(18,2),
    "startDateTime" TIMESTAMP, "endDateTime" TIMESTAMP,
    "ndConsultant" VARCHAR(64), "consultantName" TEXT,
    consultant_phone VARCHAR(64), timestamp_dms TIMESTAMP,
    "timestamp" TIMESTAMP, timestamp_hex VARCHAR(128),
    CONSTRAINT uk_services_by_vin_agency_order UNIQUE ("idAgency", order_dms)
)$sbv_ddl$, 'idAgency,order_dms', 'uk_services_by_vin_agency_order', NULL, TRUE),
    (3,  'ServicesByVin', 'Servicios por VIN', 'services_by_vin', NULL, 'idAgency,order_dms', 'uk_services_by_vin_agency_order', NULL, TRUE),
    (4,  'ServicesByVin', 'Servicios por VIN', 'services_by_vin', NULL, 'idAgency,order_dms', 'uk_services_by_vin_agency_order', NULL, TRUE),
    (5,  'ServicesByVin', 'Servicios por VIN', 'services_by_vin', NULL, 'idAgency,order_dms', 'uk_services_by_vin_agency_order', NULL, TRUE),
    (6,  'ServicesByVin', 'Servicios por VIN', 'services_by_vin', NULL, 'idAgency,order_dms', 'uk_services_by_vin_agency_order', NULL, TRUE),
    (7,  'ServicesByVin', 'Servicios por VIN', 'services_by_vin', NULL, 'idAgency,order_dms', 'uk_services_by_vin_agency_order', NULL, TRUE),
    (8,  'ServicesByVin', 'Servicios por VIN', 'services_by_vin', NULL, 'idAgency,order_dms', 'uk_services_by_vin_agency_order', NULL, TRUE),
    (9,  'ServicesByVin', 'Servicios por VIN', 'services_by_vin', NULL, 'idAgency,order_dms', 'uk_services_by_vin_agency_order', NULL, TRUE),
    (10, 'ServicesByVin', 'Servicios por VIN', 'services_by_vin', NULL, 'idAgency,order_dms', 'uk_services_by_vin_agency_order', NULL, TRUE)
ON CONFLICT (company_id, name) DO NOTHING;

-- Total (agencies 3-9) — SP; si migraste a vista, cambia por SELECT
INSERT INTO agency_task (agency_id, object_catalog_id, extract_sql, schedule_seconds, is_active)
SELECT a_id, oc.id,
    $$EXEC dbo.sp_get_services_by_vin '{last_run}'$$,
    3600, TRUE
FROM object_catalog oc, unnest(ARRAY[3,4,5,6,7,8,9]) AS a_id
WHERE oc.name = 'ServicesByVin' AND oc.company_id = (SELECT company_id FROM agency WHERE id = a_id)
ON CONFLICT (agency_id, object_catalog_id) DO NOTHING;

-- Incadea / BMW (agency 11)
INSERT INTO agency_task (agency_id, object_catalog_id, extract_sql, schedule_seconds, is_active)
SELECT 11, oc.id,
    $$SELECT * FROM view_dwh_services_by_vin WHERE timestamp_dms >= '{last_run}' ORDER BY timestamp_dms ASC$$,
    3600, TRUE
FROM object_catalog oc WHERE oc.company_id = 10 AND oc.name = 'ServicesByVin'
ON CONFLICT (agency_id, object_catalog_id) DO NOTHING;

-- Quiter / Renault (agency 10)
INSERT INTO agency_task (agency_id, object_catalog_id, extract_sql, schedule_seconds, is_active)
SELECT 10, oc.id,
    $$SELECT * FROM view_dwh_services_by_vin WHERE timestamp_dms >= '{last_run}' ORDER BY timestamp_dms ASC$$,
    3600, TRUE
FROM object_catalog oc WHERE oc.company_id = 9 AND oc.name = 'ServicesByVin'
ON CONFLICT (agency_id, object_catalog_id) DO NOTHING;


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  6. SPARES                                                              ║
-- ║  destination_table: spares                                              ║
-- ║  Sin query para Quiter (vacío en DMSQueries)                            ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

INSERT INTO object_catalog
    (company_id, name, description, destination_table, create_table_sql, upsert_keys, constraint_name, create_constraint_sql, is_enabled)
VALUES
    (2,  'Spares', 'Refacciones', 'spares', $spr_ddl$
CREATE TABLE IF NOT EXISTS spares (
    id BIGSERIAL PRIMARY KEY,
    "idAgency" TEXT NOT NULL, sku TEXT, bussines_name TEXT, agency TEXT,
    warehouse_number TEXT, part_number TEXT, description TEXT,
    desctiption_ext TEXT, cost TEXT, location TEXT, family TEXT,
    available TEXT, reserved TEXT, public_cost TEXT,
    first_purchase TEXT, last_purchase TEXT, last_sale TEXT,
    timestamp_dms TIMESTAMP, timestamp_hex TEXT, "timestamp" TIMESTAMP,
    CONSTRAINT uk_spares_agency_part UNIQUE ("idAgency", part_number)
)$spr_ddl$, 'idAgency,part_number', 'uk_spares_agency_part', NULL, TRUE),
    (3,  'Spares', 'Refacciones', 'spares', NULL, 'idAgency,part_number', 'uk_spares_agency_part', NULL, TRUE),
    (4,  'Spares', 'Refacciones', 'spares', NULL, 'idAgency,part_number', 'uk_spares_agency_part', NULL, TRUE),
    (5,  'Spares', 'Refacciones', 'spares', NULL, 'idAgency,part_number', 'uk_spares_agency_part', NULL, TRUE),
    (6,  'Spares', 'Refacciones', 'spares', NULL, 'idAgency,part_number', 'uk_spares_agency_part', NULL, TRUE),
    (7,  'Spares', 'Refacciones', 'spares', NULL, 'idAgency,part_number', 'uk_spares_agency_part', NULL, TRUE),
    (8,  'Spares', 'Refacciones', 'spares', NULL, 'idAgency,part_number', 'uk_spares_agency_part', NULL, TRUE),
    -- Sin Quiter (9)
    (10, 'Spares', 'Refacciones', 'spares', NULL, 'idAgency,part_number', 'uk_spares_agency_part', NULL, TRUE)
ON CONFLICT (company_id, name) DO NOTHING;

-- Total (agencies 3-9) — SP
INSERT INTO agency_task (agency_id, object_catalog_id, extract_sql, schedule_seconds, is_active)
SELECT a_id, oc.id,
    $$EXEC dbo.sp_get_spare_parts_inventory '{last_run}'$$,
    3600, TRUE
FROM object_catalog oc, unnest(ARRAY[3,4,5,6,7,8,9]) AS a_id
WHERE oc.name = 'Spares' AND oc.company_id = (SELECT company_id FROM agency WHERE id = a_id)
ON CONFLICT (agency_id, object_catalog_id) DO NOTHING;

-- Incadea / BMW (agency 11) — desactivada: DW_Refacciones no existe en esta BD.
INSERT INTO agency_task (agency_id, object_catalog_id, extract_sql, schedule_seconds, is_active)
SELECT 11, oc.id, $inc_spr$
SELECT
    ndPlant AS idAgency, sku, bussines_name, agency,
    warehouse_number, part_number, description, desctiption_ext,
    cost, location, family, available, reserved, public_cost,
    first_purchase, last_purchase, last_sale,
    timestamp_dms, timestamp_hex, [timestamp]
FROM DW_Refacciones
WHERE timestamp_dms >= '{last_run}'
ORDER BY timestamp_hex ASC
$inc_spr$, 3600, FALSE
FROM object_catalog oc WHERE oc.company_id = 10 AND oc.name = 'Spares'
ON CONFLICT (agency_id, object_catalog_id) DO NOTHING;


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  7. CUSTOMER VEHICLE (tabla DWH: customer_vehicle)                       ║
-- ║  BMW/Incadea: DW_Vehiculos_Clientes no existe en muchas BDs — tarea OFF. ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

INSERT INTO object_catalog
    (company_id, name, description, destination_table, create_table_sql, upsert_keys, constraint_name, create_constraint_sql, is_enabled)
VALUES
    (2,  'CustomersVehicle', 'Vehículos de clientes', 'customer_vehicle', $cv_ddl$
CREATE TABLE IF NOT EXISTS customer_vehicle (
    id BIGSERIAL PRIMARY KEY,
    "idAgency" TEXT NOT NULL, "ndPlant" TEXT, "ndClientDMS" TEXT,
    version TEXT, "customerName" TEXT, vin TEXT, brand TEXT, model TEXT,
    year TEXT, plates TEXT, external_color TEXT, internal_color TEXT,
    insurance_expiration_date TEXT, insurance_number TEXT,
    insurance_company TEXT, timestamp_dms TIMESTAMP,
    timestamp_hex TEXT, "timestamp" TIMESTAMP,
    timestamp_insurance_info TEXT,
    CONSTRAINT uk_customer_vehicle_agency_vin_client UNIQUE ("idAgency", vin, "ndClientDMS")
)$cv_ddl$, 'idAgency,vin,ndClientDMS', 'uk_customer_vehicle_agency_vin_client', NULL, TRUE),
    (3,  'CustomersVehicle', 'Vehículos de clientes', 'customer_vehicle', NULL, 'idAgency,vin,ndClientDMS', 'uk_customer_vehicle_agency_vin_client', NULL, TRUE),
    (4,  'CustomersVehicle', 'Vehículos de clientes', 'customer_vehicle', NULL, 'idAgency,vin,ndClientDMS', 'uk_customer_vehicle_agency_vin_client', NULL, TRUE),
    (5,  'CustomersVehicle', 'Vehículos de clientes', 'customer_vehicle', NULL, 'idAgency,vin,ndClientDMS', 'uk_customer_vehicle_agency_vin_client', NULL, TRUE),
    (6,  'CustomersVehicle', 'Vehículos de clientes', 'customer_vehicle', NULL, 'idAgency,vin,ndClientDMS', 'uk_customer_vehicle_agency_vin_client', NULL, TRUE),
    (7,  'CustomersVehicle', 'Vehículos de clientes', 'customer_vehicle', NULL, 'idAgency,vin,ndClientDMS', 'uk_customer_vehicle_agency_vin_client', NULL, TRUE),
    (8,  'CustomersVehicle', 'Vehículos de clientes', 'customer_vehicle', NULL, 'idAgency,vin,ndClientDMS', 'uk_customer_vehicle_agency_vin_client', NULL, TRUE),
    (9,  'CustomersVehicle', 'Vehículos de clientes', 'customer_vehicle', NULL, 'idAgency,vin,ndClientDMS', 'uk_customer_vehicle_agency_vin_client', NULL, TRUE),
    (10, 'CustomersVehicle', 'Vehículos de clientes', 'customer_vehicle', NULL, 'idAgency,vin,ndClientDMS', 'uk_customer_vehicle_agency_vin_client', NULL, TRUE)
ON CONFLICT (company_id, name) DO NOTHING;

-- Total (agencies 3-9)
INSERT INTO agency_task (agency_id, object_catalog_id, extract_sql, schedule_seconds, is_active)
SELECT a_id, oc.id,
    $$SELECT * FROM view_get_customer_vehicle WHERE timestamp_dms >= '{last_run}' ORDER BY timestamp_hex ASC$$,
    3600, TRUE
FROM object_catalog oc, unnest(ARRAY[3,4,5,6,7,8,9]) AS a_id
WHERE oc.name = 'CustomersVehicle' AND oc.company_id = (SELECT company_id FROM agency WHERE id = a_id)
ON CONFLICT (agency_id, object_catalog_id) DO NOTHING;

-- Quiter / Renault (agency 10)
INSERT INTO agency_task (agency_id, object_catalog_id, extract_sql, schedule_seconds, is_active)
SELECT 10, oc.id,
    $$SELECT * FROM view_get_customer_vehicle WHERE timestamp_dms >= '{last_run}' ORDER BY timestamp_hex ASC$$,
    3600, TRUE
FROM object_catalog oc WHERE oc.company_id = 9 AND oc.name = 'CustomersVehicle'
ON CONFLICT (agency_id, object_catalog_id) DO NOTHING;

-- Incadea / BMW (agency 11) — desactivada: DW_Vehiculos_Clientes no existe en esta BD.
-- Cuando exista la vista/tabla, pon is_active TRUE y revisa el FROM.
INSERT INTO agency_task (agency_id, object_catalog_id, extract_sql, schedule_seconds, is_active)
SELECT 11, oc.id, $inc_cv$
SELECT
    idAgency, ndPlant, ndClientDMS, version, customerName, vin,
    brand, model, year, plates, external_color, internal_color,
    insurance_expiration_date, insurance_number, insurance_company,
    timestamp_dms, timestamp_hex, [timestamp], timestamp_insurance_info
FROM DW_Vehiculos_Clientes
WHERE timestamp_dms >= '{last_run}'
ORDER BY timestamp_hex ASC
$inc_cv$, 3600, FALSE
FROM object_catalog oc WHERE oc.company_id = 10 AND oc.name = 'CustomersVehicle'
ON CONFLICT (agency_id, object_catalog_id) DO NOTHING;


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  8. LAST CUSTOMER SELLER (tabla DWH: last_customer_seller)              ║
-- ║  Sin columna "timestamp" inventada: order_timestamp viene del origen.   ║
-- ║  Sin query para Quiter (vacío en DMSQueries)                            ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

INSERT INTO object_catalog
    (company_id, name, description, destination_table, create_table_sql, upsert_keys, constraint_name, create_constraint_sql, is_enabled)
VALUES
    (2,  'LastCustomerSale', 'Última venta por cliente', 'last_customer_seller', $lcs_ddl$
CREATE TABLE IF NOT EXISTS last_customer_seller (
    id BIGSERIAL PRIMARY KEY,
    "idAgency" TEXT NOT NULL, order_dms TEXT, "ndClientDms" TEXT,
    "customerName" TEXT, "ndConsultant" TEXT, "consultantName" TEXT,
    "consultantMail" TEXT, order_timestamp TIMESTAMP,
    CONSTRAINT uk_last_customer_seller_agency_order UNIQUE ("idAgency", order_dms)
)$lcs_ddl$, 'idAgency,order_dms', 'uk_last_customer_seller_agency_order', NULL, TRUE),
    (3,  'LastCustomerSale', 'Última venta por cliente', 'last_customer_seller', NULL, 'idAgency,order_dms', 'uk_last_customer_seller_agency_order', NULL, TRUE),
    (4,  'LastCustomerSale', 'Última venta por cliente', 'last_customer_seller', NULL, 'idAgency,order_dms', 'uk_last_customer_seller_agency_order', NULL, TRUE),
    (5,  'LastCustomerSale', 'Última venta por cliente', 'last_customer_seller', NULL, 'idAgency,order_dms', 'uk_last_customer_seller_agency_order', NULL, TRUE),
    (6,  'LastCustomerSale', 'Última venta por cliente', 'last_customer_seller', NULL, 'idAgency,order_dms', 'uk_last_customer_seller_agency_order', NULL, TRUE),
    (7,  'LastCustomerSale', 'Última venta por cliente', 'last_customer_seller', NULL, 'idAgency,order_dms', 'uk_last_customer_seller_agency_order', NULL, TRUE),
    (8,  'LastCustomerSale', 'Última venta por cliente', 'last_customer_seller', NULL, 'idAgency,order_dms', 'uk_last_customer_seller_agency_order', NULL, TRUE),
    -- Sin Quiter (9)
    (10, 'LastCustomerSale', 'Última venta por cliente', 'last_customer_seller', NULL, 'idAgency,order_dms', 'uk_last_customer_seller_agency_order', NULL, TRUE)
ON CONFLICT (company_id, name) DO NOTHING;

-- Total (agencies 3-9) — SP
INSERT INTO agency_task (agency_id, object_catalog_id, extract_sql, schedule_seconds, is_active)
SELECT a_id, oc.id,
    $$EXEC dbo.sp_get_last_customer_seller '{last_run}'$$,
    3600, TRUE
FROM object_catalog oc, unnest(ARRAY[3,4,5,6,7,8,9]) AS a_id
WHERE oc.name = 'LastCustomerSale' AND oc.company_id = (SELECT company_id FROM agency WHERE id = a_id)
ON CONFLICT (agency_id, object_catalog_id) DO NOTHING;

-- Incadea / BMW (agency 11) — idAgency hardcoded 10018 en el query original
INSERT INTO agency_task (agency_id, object_catalog_id, extract_sql, schedule_seconds, is_active)
SELECT 11, oc.id, $inc_lcs$
SELECT
    10018 AS idAgency,
    order_dms, ndClientDms, bussines_name AS customerName,
    ndSeller AS ndConsultant, seller_Name AS consultantName,
    seller_Mail AS consultantMail,
    last_sale AS order_timestamp
FROM [MEX-VAN-incadea].[dbo].[view_dwh_last_customer_seller]
WHERE timestamp_dms >= '{last_run}'
    AND ndClientDms IS NOT NULL AND order_dms IS NOT NULL
ORDER BY timestamp_dms ASC
$inc_lcs$, 3600, TRUE
FROM object_catalog oc WHERE oc.company_id = 10 AND oc.name = 'LastCustomerSale'
ON CONFLICT (agency_id, object_catalog_id) DO NOTHING;


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  9. VEHICLE ORDERS                                                      ║
-- ║  destination_table: vehicle_orders                                      ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

INSERT INTO object_catalog
    (company_id, name, description, destination_table, create_table_sql, upsert_keys, constraint_name, create_constraint_sql, is_enabled)
VALUES
    (2,  'VehicleOrders', 'Pedidos de vehículos', 'vehicle_orders', NULL, '', NULL, NULL, TRUE),
    (3,  'VehicleOrders', 'Pedidos de vehículos', 'vehicle_orders', NULL, '', NULL, NULL, TRUE),
    (4,  'VehicleOrders', 'Pedidos de vehículos', 'vehicle_orders', NULL, '', NULL, NULL, TRUE),
    (5,  'VehicleOrders', 'Pedidos de vehículos', 'vehicle_orders', NULL, '', NULL, NULL, TRUE),
    (6,  'VehicleOrders', 'Pedidos de vehículos', 'vehicle_orders', NULL, '', NULL, NULL, TRUE),
    (7,  'VehicleOrders', 'Pedidos de vehículos', 'vehicle_orders', NULL, '', NULL, NULL, TRUE),
    (8,  'VehicleOrders', 'Pedidos de vehículos', 'vehicle_orders', NULL, '', NULL, NULL, TRUE),
    (9,  'VehicleOrders', 'Pedidos de vehículos', 'vehicle_orders', NULL, '', NULL, NULL, TRUE),
    (10, 'VehicleOrders', 'Pedidos de vehículos', 'vehicle_orders', NULL, '', NULL, NULL, TRUE)
ON CONFLICT (company_id, name) DO NOTHING;

-- Total (agencies 3-9) — SP
INSERT INTO agency_task (agency_id, object_catalog_id, extract_sql, schedule_seconds, is_active)
SELECT a_id, oc.id,
    $$EXEC dbo.sp_get_vehicle_orders '{last_run}'$$,
    300, TRUE
FROM object_catalog oc, unnest(ARRAY[3,4,5,6,7,8,9]) AS a_id
WHERE oc.name = 'VehicleOrders' AND oc.company_id = (SELECT company_id FROM agency WHERE id = a_id)
ON CONFLICT (agency_id, object_catalog_id) DO NOTHING;

-- Incadea / BMW (agency 11) — TRY_CONVERT como filtro (tolerante a fechas malas)
INSERT INTO agency_task (agency_id, object_catalog_id, extract_sql, schedule_seconds, is_active)
SELECT 11, oc.id, $inc_vo$
SELECT * FROM view_dwh_vehicle_orders
WHERE TRY_CONVERT(datetime, timestamp_dms) >= '{last_run}'
  AND TRY_CONVERT(datetime, timestamp_dms) IS NOT NULL
$inc_vo$, 300, TRUE
FROM object_catalog oc WHERE oc.company_id = 10 AND oc.name = 'VehicleOrders'
ON CONFLICT (agency_id, object_catalog_id) DO NOTHING;

-- Quiter / Renault (agency 10)
INSERT INTO agency_task (agency_id, object_catalog_id, extract_sql, schedule_seconds, is_active)
SELECT 10, oc.id, $qt_vo$
SELECT * FROM view_dwh_vehicle_orders
WHERE TRY_CONVERT(datetime, timestamp_dms) >= '{last_run}'
  AND TRY_CONVERT(datetime, timestamp_dms) IS NOT NULL
$qt_vo$, 300, TRUE
FROM object_catalog oc WHERE oc.company_id = 9 AND oc.name = 'VehicleOrders'
ON CONFLICT (agency_id, object_catalog_id) DO NOTHING;


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  10. COMMISSIONS                                                        ║
-- ║  destination_table: commissions                                         ║
-- ║  Solo Total (Incadea y Quiter vacíos en DMSQueries)                     ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

INSERT INTO object_catalog
    (company_id, name, description, destination_table, create_table_sql, upsert_keys, constraint_name, create_constraint_sql, is_enabled)
VALUES
    (2,  'Commissions', 'Comisiones', 'commissions', NULL, '', NULL, NULL, TRUE),
    (3,  'Commissions', 'Comisiones', 'commissions', NULL, '', NULL, NULL, TRUE),
    (4,  'Commissions', 'Comisiones', 'commissions', NULL, '', NULL, NULL, TRUE),
    (5,  'Commissions', 'Comisiones', 'commissions', NULL, '', NULL, NULL, TRUE),
    (6,  'Commissions', 'Comisiones', 'commissions', NULL, '', NULL, NULL, TRUE),
    (7,  'Commissions', 'Comisiones', 'commissions', NULL, '', NULL, NULL, TRUE),
    (8,  'Commissions', 'Comisiones', 'commissions', NULL, '', NULL, NULL, TRUE)
ON CONFLICT (company_id, name) DO NOTHING;

-- Total (agencies 3-9) — SP
INSERT INTO agency_task (agency_id, object_catalog_id, extract_sql, schedule_seconds, is_active)
SELECT a_id, oc.id,
    $$EXEC dbo.sp_get_comissions '{last_run}'$$,
    3600, TRUE
FROM object_catalog oc, unnest(ARRAY[3,4,5,6,7,8,9]) AS a_id
WHERE oc.name = 'Commissions' AND oc.company_id = (SELECT company_id FROM agency WHERE id = a_id)
ON CONFLICT (agency_id, object_catalog_id) DO NOTHING;


-- =============================================================================
-- NOTAS IMPORTANTES
-- =============================================================================
--
-- 1) SPs (Total): se llaman con parámetro posicional:
--       EXEC dbo.sp_name '{last_run}'
--    Si tu SP espera un nombre distinto (p. ej. @lastExecution, @start_date),
--    el posicional funciona si es el primer parámetro. Si falla, usa:
--       EXEC dbo.sp_name @nombre_param = '{last_run}'
--
-- 2) DDL (create_table_sql):
--    - Se proporciona para invoices, services, services_by_vin, spares,
--      customer_vehicle, last_customer_seller (basado en columnas Incadea).
--    - NULL para inventory, vehicle_orders, commissions (SELECT * o SP;
--      columnas desconocidas). La tabla debe existir previamente en el DWH.
--    - CREATE TABLE IF NOT EXISTS es no-op si la tabla ya existe.
--
-- 3) Columnas camelCase en PostgreSQL:
--    Las columnas con mayúsculas (idAgency, ndClientDMS, etc.) deben estar
--    entre comillas dobles en el DDL. El cliente ETL las entrecomilla al
--    hacer INSERT basándose en cursor.description del driver.
--
-- 4) SELECT * con vistas Total Dealer:
--    Si la vista devuelve nombres distintos a los del DDL (Invoice vs invoice),
--    cambia el extract_sql a un SELECT explícito con alias (ver services_by_vin).
--
-- 5) Quiter — idAgency hardcoded:
--    Invoices.Quiter usa 48410047 AS idAgency. Es el código DMS de Renault.
--
-- 6) BMW — idAgency hardcoded en LastCustomerSale:
--    10018 AS idAgency. Si no corresponde, ajústalo.
--
-- 7) Tablas DWH renombradas: customer_vehicle (antes customers_vehicle),
--    last_customer_seller (antes last_customer_sale). BMW Spares y
--    Customer vehicle Incadea: tareas en is_active FALSE hasta existan
--    DW_Refacciones / DW_Vehiculos_Clientes.
--
-- 8) Timestamp: no inventar fechas. Usa columnas que ya venga el origen
--    (p. ej. order_timestamp, timestamp_dms). Para auditoría de carga,
--    opcional en PostgreSQL: loaded_at TIMESTAMPTZ DEFAULT now() (no
--    sustituye fecha de negocio).
--
-- 9) Total Dealer — sp_get_services / sp_get_invoices: si los nombres de
--    columna del SP no coinciden con el DWH (service_to_perform, kms,
--    client_bussines_name), crea vistas en SQL Server con alias o alinea el SP.
--
-- 7) schedule_seconds (del appSettings C#):
--    customers=3600, inventory=3600, invoices=60, services=300,
--    spares=3600, vehicle_orders=300 (original 5s, ajustado).
-- =============================================================================
