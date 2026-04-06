-- ============================================================================
-- update_agency_task_extracts_by_id.sql
-- BD de configuración (mgd_dwh_config).
--
-- Alinea extract_sql con insert_all_dms_configs.sql para los IDs que pegaste.
-- NO toca tareas 1–2 (customers con vgd_dwh_prod): son específicas de tu entorno.
-- Ejecuta en una transacción si quieres poder hacer ROLLBACK.
-- ============================================================================

-- ── 19: BMW / Incadea customers (corrige alias y comillas; igual que el insert) ──
UPDATE agency_task
SET extract_sql = $t19$
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
$t19$
WHERE id = 19;

-- ── 21,24–29: Total / services_by_vin — mismo patrón que el insert (parámetro posicional) ──
UPDATE agency_task
SET extract_sql = $sbv$
EXEC dbo.sp_get_services_by_vin '{last_run}'
$sbv$
WHERE id IN (21, 24, 25, 26, 27, 28, 29);

-- ── 57: Incadea invoices — client_bussines_name (no Client_Bussines_Name) ──
UPDATE agency_task
SET extract_sql = $inv57$
SELECT
    ndPlant AS idAgency, order_dms, state, vin, warranty_init_date,
    plates, payment_method, sub_total, accesories, amount_accesories,
    financial_terms, invoice_reference, billing_date, amount_taxes,
    financial_institution, delivery_date, cancelation_date, stage_name,
    timestamp_dms, timestamp_hex, close_date, description, [timestamp],
    CustomerId AS ndClientDMS, Cliente AS client_bussines_name
FROM view_dwh_invoices
WHERE timestamp_dms >= '{last_run}'
$inv57$
WHERE id = 57;

-- ── 58: Quiter invoices ──
UPDATE agency_task
SET extract_sql = $inv58$
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
$inv58$
WHERE id = 58;

-- ── 66: Incadea services — service_to_perform, kms, placeholders ndClientDMS / nmVendedor ──
UPDATE agency_task
SET extract_sql = $svc66$
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
$svc66$
WHERE id = 66;

-- ── 67: Quiter services ──
UPDATE agency_task
SET extract_sql = $svc67$
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
$svc67$
WHERE id = 67;

-- ============================================================================
-- Notas
-- ----------------------------------------------------------------------------
-- • Tareas 1 y 2 (customers con esquema vgd_dwh_prod): no las tocamos; si quieres
--   el texto del insert estándar, cámbialas a mano o deja las tuyas.
-- • Tareas 11–18 (Kia/Total customers): tu SELECT largo es válido si el DWH ya
--   tiene esas columnas; el insert usa SELECT * para Total. Solo actualicé 19.
-- • Tarea 30 (services_by_vin Renault) y 31 (BMW explícito): ya coinciden con lo
--   habitual; no hace falta UPDATE aquí.
-- • 50–56, 59–65, 77–83, 94–100, 102–108, 111–117: EXEC / SELECT * igual que el
--   insert; el cliente normaliza nombres de columna donde aplica.
-- • 84 y 93: inactivas; cuando existan DW_Refacciones / DW_Vehiculos_Clientes,
--   revisa extract y is_active.
-- ============================================================================
