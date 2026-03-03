-- ============================================================
-- DATOS DE PRUEBA: Tablas de origen simuladas
-- Simula las tablas que existirían en la BD del cliente.
-- Ejecutar en la BD a la que apunta DSN_CLIENTE_1.
-- ============================================================

DROP TABLE IF EXISTS inventario_origen_mty_centro;
DROP TABLE IF EXISTS inventario_origen_mty_sur;


-- ============================================================
-- Tabla de origen: Sucursal Monterrey Centro
-- ============================================================
CREATE TABLE inventario_origen_mty_centro (
    vin                 TEXT NOT NULL,
    sucursal            TEXT NOT NULL DEFAULT 'Monterrey Centro',
    modelo              TEXT,
    anio                INTEGER,
    color               TEXT,
    precio              NUMERIC(12,2),
    estatus             TEXT,
    fecha_modificacion  TIMESTAMP NOT NULL DEFAULT NOW()
);

INSERT INTO inventario_origen_mty_centro (vin, sucursal, modelo, anio, color, precio, estatus, fecha_modificacion)
VALUES
    ('VIN001', 'Monterrey Centro', 'Sentra',  2024, 'Blanco', 350000.00, 'Disponible', NOW() - INTERVAL '2 hours'),
    ('VIN002', 'Monterrey Centro', 'Versa',   2024, 'Negro',  320000.00, 'Reservado',  NOW() - INTERVAL '2 hours'),
    ('VIN003', 'Monterrey Centro', 'Kicks',   2025, 'Rojo',   480000.00, 'Disponible', NOW() - INTERVAL '2 hours');


-- ============================================================
-- Tabla de origen: Sucursal Monterrey Sur
-- ============================================================
CREATE TABLE inventario_origen_mty_sur (
    vin                 TEXT NOT NULL,
    sucursal            TEXT NOT NULL DEFAULT 'Monterrey Sur',
    modelo              TEXT,
    anio                INTEGER,
    color               TEXT,
    precio              NUMERIC(12,2),
    estatus             TEXT,
    fecha_modificacion  TIMESTAMP NOT NULL DEFAULT NOW()
);

INSERT INTO inventario_origen_mty_sur (vin, sucursal, modelo, anio, color, precio, estatus, fecha_modificacion)
VALUES
    ('VIN101', 'Monterrey Sur', 'March',   2023, 'Azul', 280000.00, 'Disponible', NOW() - INTERVAL '2 hours'),
    ('VIN102', 'Monterrey Sur', 'X-Trail', 2025, 'Gris', 620000.00, 'Reservado',  NOW() - INTERVAL '2 hours');


-- ============================================================
-- Para probar extracción incremental después de la primera corrida:
--
-- 1) Insertar un registro nuevo:
--    INSERT INTO inventario_origen_mty_centro
--        (vin, modelo, anio, color, precio, estatus)
--    VALUES ('VIN004', 'Altima', 2025, 'Plata', 550000.00, 'Disponible');
--
-- 2) Modificar un registro existente (actualiza fecha_modificacion):
--    UPDATE inventario_origen_mty_centro
--    SET estatus = 'Vendido', fecha_modificacion = NOW()
--    WHERE vin = 'VIN001';
--
-- En la siguiente ejecución, solo se extraerán VIN004 y VIN001 (el modificado).
-- ============================================================

