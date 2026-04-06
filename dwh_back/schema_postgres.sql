-- ============================================================
-- ESQUEMA NEXUS – PostgreSQL (nombres en inglés)
-- Jerarquía: client_group > company > agency > object_catalog / agency_task
-- ============================================================

-- ============================================================
-- Crear bases de datos (ejecutar como superusuario)
-- ============================================================
-- CREATE DATABASE mgd_dwh_config;
-- CREATE DATABASE mgd_dwh_test;
-- \c mgd_dwh_config

-- ============================================================
-- Eliminar tablas si existen (en orden por dependencias)
-- ============================================================
DROP TABLE IF EXISTS activity_log;
DROP TABLE IF EXISTS client_events;
DROP TABLE IF EXISTS agency_task;
DROP TABLE IF EXISTS object_catalog;
DROP TABLE IF EXISTS agency;
DROP TABLE IF EXISTS company;
DROP TABLE IF EXISTS client_group;


-- ============================================================
-- 1. CLIENT_GROUP
--    Conexión al DWH (PostgreSQL) compartida por todas las compañías.
-- ============================================================
CREATE TABLE client_group (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(255) NOT NULL UNIQUE,
    group_token     VARCHAR(64) UNIQUE,
    warehouse_host  VARCHAR(255) NOT NULL,
    warehouse_port  INT          NOT NULL DEFAULT 5432,
    warehouse_database VARCHAR(255) NOT NULL,
    warehouse_username VARCHAR(255) NOT NULL,
    warehouse_password VARCHAR(255) NOT NULL,
    is_enabled      BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER client_group_updated_at
    BEFORE UPDATE ON client_group
    FOR EACH ROW EXECUTE PROCEDURE update_updated_at();


-- ============================================================
-- 2. COMPANY
--    Conexión a BD de origen (MySQL, SQL Server o PostgreSQL).
--    source_type: 'mysql' | 'sqlserver' | 'postgresql'
-- ============================================================
CREATE TABLE company (
    id              SERIAL PRIMARY KEY,
    group_id        INT          NOT NULL REFERENCES client_group(id) ON UPDATE CASCADE ON DELETE CASCADE,
    name            VARCHAR(255) NOT NULL,
    company_token   VARCHAR(64)  NOT NULL UNIQUE,
    source_type     VARCHAR(20)  NOT NULL DEFAULT 'sqlserver',
    source_dsn      VARCHAR(255) NOT NULL DEFAULT '',
    source_host     VARCHAR(255) NOT NULL DEFAULT '',
    source_port     INT          NOT NULL DEFAULT 1433,
    source_database VARCHAR(255) NOT NULL DEFAULT '',
    source_username VARCHAR(255) NOT NULL DEFAULT '',
    source_password VARCHAR(255) NOT NULL DEFAULT '',
    verbose_logging BOOLEAN      NOT NULL DEFAULT FALSE,
    refresh_seconds INT          NOT NULL DEFAULT 60,
    is_enabled      BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (group_id, name)
);

CREATE TRIGGER company_updated_at
    BEFORE UPDATE ON company
    FOR EACH ROW EXECUTE PROCEDURE update_updated_at();


-- ============================================================
-- 3. AGENCY
-- ============================================================
CREATE TABLE agency (
    id           SERIAL PRIMARY KEY,
    company_id   INT          NOT NULL REFERENCES company(id) ON UPDATE CASCADE ON DELETE CASCADE,
    name         VARCHAR(255) NOT NULL,
    agency_token VARCHAR(64) UNIQUE,
    is_enabled   BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (company_id, name)
);

CREATE TRIGGER agency_updated_at
    BEFORE UPDATE ON agency
    FOR EACH ROW EXECUTE PROCEDURE update_updated_at();


-- ============================================================
-- 4. OBJECT_CATALOG
--    Plantillas reutilizables. DDL usa sintaxis PostgreSQL.
-- ============================================================
CREATE TABLE object_catalog (
    id                  SERIAL PRIMARY KEY,
    company_id          INT          NOT NULL REFERENCES company(id) ON UPDATE CASCADE ON DELETE CASCADE,
    name                VARCHAR(255) NOT NULL,
    description         TEXT,
    destination_table   VARCHAR(255) NOT NULL,
    create_table_sql    TEXT,
    upsert_keys         TEXT,
    constraint_name     VARCHAR(255),
    create_constraint_sql TEXT,
    is_enabled          BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (company_id, name)
);

CREATE TRIGGER object_catalog_updated_at
    BEFORE UPDATE ON object_catalog
    FOR EACH ROW EXECUTE PROCEDURE update_updated_at();


-- ============================================================
-- 5. AGENCY_TASK (N:M agency ↔ catalog)
-- ============================================================
CREATE TABLE agency_task (
    id               SERIAL PRIMARY KEY,
    agency_id        INT       NOT NULL REFERENCES agency(id) ON UPDATE CASCADE ON DELETE CASCADE,
    object_catalog_id INT      NOT NULL REFERENCES object_catalog(id) ON UPDATE CASCADE ON DELETE CASCADE,
    extract_sql      TEXT      NOT NULL,
    schedule_seconds INT       NOT NULL DEFAULT 3600,
    last_run_at      TIMESTAMP NULL,
    is_active        BOOLEAN   NOT NULL DEFAULT TRUE,
    run_on_company_token BOOLEAN NOT NULL DEFAULT TRUE,
    created_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (agency_id, object_catalog_id)
);

CREATE TRIGGER agency_task_updated_at
    BEFORE UPDATE ON agency_task
    FOR EACH ROW EXECUTE PROCEDURE update_updated_at();


-- ============================================================
-- 6. ACTIVITY LOG
-- ============================================================
CREATE TABLE activity_log (
    id              BIGSERIAL PRIMARY KEY,
    token           VARCHAR(64)  NOT NULL DEFAULT '',
    company_name    VARCHAR(255) NOT NULL DEFAULT '',
    group_name      VARCHAR(255) NOT NULL DEFAULT '',
    method          VARCHAR(10)  NOT NULL,
    endpoint        VARCHAR(255) NOT NULL,
    status_code     INT          NOT NULL,
    response_ms     INT          NOT NULL DEFAULT 0,
    error_detail    TEXT,
    client_ip       VARCHAR(45)  NOT NULL DEFAULT '',
    created_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);


-- ============================================================
-- 7. CLIENT EVENTS
-- ============================================================
CREATE TABLE client_events (
    id             SERIAL PRIMARY KEY,
    created_at     TIMESTAMP    NOT NULL DEFAULT NOW(),
    token          VARCHAR(128)  NOT NULL DEFAULT '',
    group_name     VARCHAR(255)  NOT NULL DEFAULT '',
    company_name   VARCHAR(255)  NOT NULL DEFAULT '',
    config_id      VARCHAR(32)   NOT NULL DEFAULT '',
    task_name      VARCHAR(512)  NOT NULL DEFAULT '',
    event_type     VARCHAR(10)   NOT NULL DEFAULT 'error' CHECK (event_type IN ('ok', 'error')),
    detail         TEXT,
    rows_loaded    INT           NOT NULL DEFAULT 0,
    is_acknowledged SMALLINT     NOT NULL DEFAULT 0
);


-- ============================================================
-- ÍNDICES
-- ============================================================
CREATE INDEX idx_company_group_id       ON company(group_id);
CREATE INDEX idx_agency_company_id      ON agency(company_id);
CREATE INDEX idx_object_catalog_company_id ON object_catalog(company_id);
CREATE INDEX idx_agency_task_agency_id  ON agency_task(agency_id);
CREATE INDEX idx_agency_task_object_catalog_id ON agency_task(object_catalog_id);
CREATE INDEX idx_actlog_token   ON activity_log(token);
CREATE INDEX idx_actlog_created ON activity_log(created_at);
CREATE INDEX idx_actlog_status  ON activity_log(status_code);
CREATE INDEX idx_actlog_group_name ON activity_log(group_name);
CREATE INDEX idx_ce_token       ON client_events(token);
CREATE INDEX idx_ce_group_name  ON client_events(group_name);
CREATE INDEX idx_ce_company_name ON client_events(company_name);
CREATE INDEX idx_ce_event_type  ON client_events(event_type);
CREATE INDEX idx_ce_is_acknowledged ON client_events(is_acknowledged);
CREATE INDEX idx_ce_created_at  ON client_events(created_at);
