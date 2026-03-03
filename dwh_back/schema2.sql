-- ============================================================
-- ESQUEMA NEXUS – MySQL
-- Jerarquía: Grupo > Razón Social > Agencia > Objetos
-- ============================================================

-- ============================================================
-- Eliminar BD anterior (si se renombró)
-- ============================================================
DROP DATABASE IF EXISTS nexus_config;

-- ============================================================
-- Crear bases de datos
-- ============================================================
CREATE DATABASE IF NOT EXISTS mgd_dwh_config
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

CREATE DATABASE IF NOT EXISTS mgd_dwh_test
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE mgd_dwh_config;

-- ============================================================
-- Eliminar tablas si existen (en orden por dependencias)
-- ============================================================
DROP TABLE IF EXISTS agencia_objeto;
DROP TABLE IF EXISTS catalogo_objeto;
DROP TABLE IF EXISTS agencia;
DROP TABLE IF EXISTS razon_social;
DROP TABLE IF EXISTS grupo;


-- ============================================================
-- 1. GRUPO
--    Conexión al DWH (MySQL) compartida por todas las razones sociales.
-- ============================================================
CREATE TABLE grupo (
    id          INT          AUTO_INCREMENT PRIMARY KEY,
    nombre      VARCHAR(255) NOT NULL UNIQUE,
    dwh_host    VARCHAR(255) NOT NULL COMMENT 'IP o hostname del servidor MySQL del DWH',
    dwh_port    INT          NOT NULL DEFAULT 3306,
    dwh_db      VARCHAR(255) NOT NULL COMMENT 'Nombre de la BD destino (DWH)',
    dwh_user    VARCHAR(255) NOT NULL,
    dwh_pass    VARCHAR(255) NOT NULL,
    enabled     BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ============================================================
-- 2. RAZÓN SOCIAL
--    Conexión a BD de origen (SQL Server del cliente).
-- ============================================================
CREATE TABLE razon_social (
    id              INT          AUTO_INCREMENT PRIMARY KEY,
    id_grupo        INT          NOT NULL,
    nombre          VARCHAR(255) NOT NULL,
    token           VARCHAR(64)  NOT NULL UNIQUE,
    dsn_odbc        VARCHAR(255) NOT NULL DEFAULT '' COMMENT 'DSN ODBC opcional (si se prefiere en vez de conexión directa)',
    origen_ip       VARCHAR(255) NOT NULL DEFAULT '' COMMENT 'IP del SQL Server origen',
    origen_port     INT          NOT NULL DEFAULT 1433,
    origen_db       VARCHAR(255) NOT NULL DEFAULT '' COMMENT 'Nombre de BD en SQL Server',
    usuario_sql     VARCHAR(255) NOT NULL DEFAULT '',
    pass_sql        VARCHAR(255) NOT NULL DEFAULT '',
    log_verbose     BOOLEAN      NOT NULL DEFAULT FALSE,
    refresh_seconds INT          NOT NULL DEFAULT 60,
    enabled         BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_rs_grupo_nombre (id_grupo, nombre),
    FOREIGN KEY (id_grupo) REFERENCES grupo(id)
        ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ============================================================
-- 3. AGENCIA
-- ============================================================
CREATE TABLE agencia (
    id              INT          AUTO_INCREMENT PRIMARY KEY,
    id_razon_social INT          NOT NULL,
    nombre          VARCHAR(255) NOT NULL,
    enabled         BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_agencia_rs_nombre (id_razon_social, nombre),
    FOREIGN KEY (id_razon_social) REFERENCES razon_social(id)
        ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ============================================================
-- 4. CATÁLOGO DE OBJETOS
--    Plantillas reutilizables. DDL usa sintaxis MySQL (destino = DWH MySQL).
-- ============================================================
CREATE TABLE catalogo_objeto (
    id                  INT          AUTO_INCREMENT PRIMARY KEY,
    id_razon_social     INT          NOT NULL,
    nombre              VARCHAR(255) NOT NULL,
    descripcion         TEXT,
    tabla_destino       VARCHAR(255) NOT NULL,
    query_tabla_destino TEXT         COMMENT 'DDL MySQL para crear la tabla en el DWH',
    upsert_keys         TEXT         COMMENT 'Columnas clave separadas por coma',
    constraint_nombre   VARCHAR(255),
    query_constraint    TEXT         COMMENT 'DDL MySQL para constraint/índice en el DWH',
    enabled             BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_catalogo_rs_nombre (id_razon_social, nombre),
    FOREIGN KEY (id_razon_social) REFERENCES razon_social(id)
        ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ============================================================
-- 5. AGENCIA ↔ OBJETO (N:M)
-- ============================================================
CREATE TABLE agencia_objeto (
    id               INT       AUTO_INCREMENT PRIMARY KEY,
    id_agencia       INT       NOT NULL,
    id_objeto        INT       NOT NULL,
    extract_sql      TEXT      NOT NULL COMMENT 'Query SQL Server para extraer datos del origen',
    schedule_seconds INT       NOT NULL DEFAULT 3600,
    last_run_at      DATETIME  NULL,
    active           BOOLEAN   NOT NULL DEFAULT TRUE,
    created_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_agobj (id_agencia, id_objeto),
    FOREIGN KEY (id_agencia) REFERENCES agencia(id)
        ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (id_objeto) REFERENCES catalogo_objeto(id)
        ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ============================================================
-- ÍNDICES
-- ============================================================
CREATE INDEX idx_rs_grupo       ON razon_social(id_grupo);
CREATE INDEX idx_agencia_rs     ON agencia(id_razon_social);
CREATE INDEX idx_catalogo_rs    ON catalogo_objeto(id_razon_social);
CREATE INDEX idx_agobj_agencia  ON agencia_objeto(id_agencia);
CREATE INDEX idx_agobj_objeto   ON agencia_objeto(id_objeto);


-- ============================================================
--                    DATOS INICIALES
-- ============================================================

-- Grupo Vanguardia
-- ⚠️  Ajusta dwh_host, dwh_user y dwh_pass con tus datos reales de MySQL.
INSERT INTO grupo (nombre, dwh_host, dwh_port, dwh_db, dwh_user, dwh_pass)
VALUES ('Vanguardia', '127.0.0.1', 3306, 'mgd_dwh_test', 'root', '');

-- Honda
INSERT INTO razon_social
    (id_grupo, nombre, token, origen_ip, origen_port, origen_db, usuario_sql, pass_sql)
VALUES
    (1, 'Honda', 'tok_honda_001',
     '192.168.190.123', 1433, 'SQLHONDA', 'sa', 'TotalDealer!');
-- Kia
INSERT INTO razon_social
    (id_grupo, nombre, token, origen_ip, origen_port, origen_db, usuario_sql, pass_sql)
VALUES
    (1, 'Kia', 'tok_kia_001',
     '192.168.190.116', 1433, 'KIASQL', 'customerss', 'V4ndu4rd1a2022');



USE mgd_dwh_config;

CREATE TABLE activity_log (
    id              BIGINT       AUTO_INCREMENT PRIMARY KEY,
    token           VARCHAR(64)  NOT NULL DEFAULT '',
    razon_social    VARCHAR(255) NOT NULL DEFAULT '',
    grupo VARCHAR(255) NOT NULL DEFAULT '' COMMENT 'Nombre del grupo resuelto desde el token',
    method          VARCHAR(10)  NOT NULL,
    endpoint        VARCHAR(255) NOT NULL,
    status_code     INT          NOT NULL,
    response_ms     INT          NOT NULL DEFAULT 0 COMMENT 'Tiempo de respuesta en milisegundos',
    error_detail    TEXT,
    client_ip       VARCHAR(45)  NOT NULL DEFAULT '',
    created_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS client_events (
    id            INT UNSIGNED  NOT NULL AUTO_INCREMENT,
    created_at    DATETIME      NOT NULL DEFAULT NOW(),
    token         VARCHAR(128)  NOT NULL DEFAULT '',
    grupo         VARCHAR(255)  NOT NULL DEFAULT ''   COMMENT 'Nombre del grupo',
    razon_social  VARCHAR(255)  NOT NULL DEFAULT ''   COMMENT 'Nombre de la razón social',
    config_id     VARCHAR(32)   NOT NULL DEFAULT ''   COMMENT 'ID de agencia_objeto',
    task_name     VARCHAR(512)  NOT NULL DEFAULT ''   COMMENT 'Nombre descriptivo de la tarea',
    event_type    ENUM('ok','error') NOT NULL DEFAULT 'error',
    detail        TEXT                               COMMENT 'Resumen si ok, traceback si error',
    rows_loaded   INT UNSIGNED  NOT NULL DEFAULT 0   COMMENT 'Filas cargadas (solo en ok)',
    acknowledged  TINYINT(1)    NOT NULL DEFAULT 0   COMMENT '0=pendiente, 1=visto',
    PRIMARY KEY (id),
    INDEX idx_token        (token),
    INDEX idx_grupo        (grupo),
    INDEX idx_razon_social (razon_social),
    INDEX idx_event_type   (event_type),
    INDEX idx_acknowledged (acknowledged),
    INDEX idx_created_at   (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='Eventos de ejecución ETL reportados por los clientes';
  
CREATE INDEX idx_al_token      ON activity_log (token);
CREATE INDEX idx_al_status     ON activity_log (status_code);
CREATE INDEX idx_al_created_at ON activity_log (created_at);
CREATE INDEX idx_al_grupo      ON activity_log (grupo);