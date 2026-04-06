-- Ejecutar en el DWH (PostgreSQL), base donde está services_by_vin.
-- Corrige nombres: sin comillas en CREATE TABLE, PG creó columnas en minúsculas
-- pero el ETL inserta con el mismo camelCase que devuelve SQL Server.
-- Si alguna columna ya tiene el nombre correcto, ese RENAME fallará: coméntalo o ajústalo.

ALTER TABLE services_by_vin RENAME COLUMN statusdescription TO "statusDescription";
ALTER TABLE services_by_vin RENAME COLUMN idservicetype TO "idServiceType";
ALTER TABLE services_by_vin RENAME COLUMN servicetype TO "serviceType";
ALTER TABLE services_by_vin RENAME COLUMN servicetypedescription TO "serviceTypeDescription";
ALTER TABLE services_by_vin RENAME COLUMN servicetypedetail TO "serviceTypeDetail";
ALTER TABLE services_by_vin RENAME COLUMN startdatetime TO "startDateTime";
ALTER TABLE services_by_vin RENAME COLUMN enddatetime TO "endDateTime";
ALTER TABLE services_by_vin RENAME COLUMN ndconsultant TO "ndConsultant";
ALTER TABLE services_by_vin RENAME COLUMN consultantname TO "consultantName";
