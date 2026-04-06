-- DWH PostgreSQL: ancho de columnas y coherencia con el ETL.
-- Ejecutar si ya existe services_by_vin creada con VARCHAR(32) cortos.

ALTER TABLE services_by_vin ALTER COLUMN "idAgency" TYPE VARCHAR(128);
ALTER TABLE services_by_vin ALTER COLUMN "idServiceType" TYPE VARCHAR(128);
ALTER TABLE services_by_vin ALTER COLUMN "serviceType" TYPE VARCHAR(128);
