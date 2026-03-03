"""
nexus_client.py  v3
────────────────────
Cliente ETL. Lee su configuración del backend usando su token de razón social.
Reporta cada ejecución (exitosa o fallida) al backend para que el monitor
pueda ver grupo + RS + detalle de cada evento.
"""

import configparser
import glob
import logging
import os
import sys
import time
import traceback
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence

import pymysql
import pyodbc
import requests


# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
if getattr(sys, "frozen", False):
    _APP_DIR = os.path.dirname(sys.executable)
else:
    _APP_DIR = os.path.dirname(os.path.abspath(__file__))


def load_ini() -> configparser.ConfigParser:
    ini_path = os.path.join(_APP_DIR, "config.ini")
    if not os.path.exists(ini_path):
        print(f"No se encontró config.ini en: {ini_path}")
        sys.exit(1)
    cfg = configparser.ConfigParser()
    cfg.read(ini_path)
    return cfg


_ini         = load_ini()
API_BASE_URL = _ini.get("nexus", "api_url", fallback="http://127.0.0.1:8000")
CLIENT_TOKEN = _ini.get("nexus", "token",   fallback="")

if not CLIENT_TOKEN:
    print("Falta [nexus] token en config.ini")
    sys.exit(1)


# ─────────────────────────────────────────────────────────────────────────────
# Logging  — archivo diario en ./logs/  (máx 7 archivos)
# ─────────────────────────────────────────────────────────────────────────────
_LOGS_DIR    = os.path.join(_APP_DIR, "logs")
_MAX_LOGS    = 7
os.makedirs(_LOGS_DIR, exist_ok=True)
LOG_PATH     = os.path.join(_LOGS_DIR, f"nexus_{date.today().isoformat()}.log")


def _cleanup_old_logs() -> None:
    files = sorted(glob.glob(os.path.join(_LOGS_DIR, "nexus_*.log")))
    while len(files) > _MAX_LOGS:
        try:
            os.remove(files.pop(0))
        except OSError:
            pass


_cleanup_old_logs()

log = logging.getLogger("nexus")
log.setLevel(logging.DEBUG)
_fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s",
                         datefmt="%Y-%m-%d %H:%M:%S")
_fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
_fh.setLevel(logging.DEBUG)
_fh.setFormatter(_fmt)
log.addHandler(_fh)
_ch = logging.StreamHandler(sys.stdout)
_ch.setLevel(logging.INFO)
_ch.setFormatter(_fmt)
log.addHandler(_ch)


# ─────────────────────────────────────────────────────────────────────────────
# Estado global
# ─────────────────────────────────────────────────────────────────────────────
_verbose:      bool         = False
_dwh_config:   Dict[str, Any] = {}
_source_config: Dict[str, Any] = {}


# ─────────────────────────────────────────────────────────────────────────────
# API — obtener configuraciones
# ─────────────────────────────────────────────────────────────────────────────
def fetch_configs() -> Dict[str, Any]:
    url     = f"{API_BASE_URL}/configs"
    headers = {"x-token": CLIENT_TOKEN}
    empty: Dict[str, Any] = {
        "status": "unreachable", "log_verbose": False,
        "refresh_seconds": 60,  "configs": [],
        "dwh": {},              "source": {},
    }

    try:
        r = requests.get(url, headers=headers, timeout=10)
    except requests.ConnectionError:
        return empty

    if r.status_code == 401:
        return {**empty, "status": "invalid"}
    if r.status_code == 403:
        log.warning("Servicio deshabilitado: %s", r.json().get("detail", ""))
        return {**empty, "status": "disabled"}

    r.raise_for_status()
    data = r.json()
    return {
        "status":          "ok",
        "log_verbose":     data.get("log_verbose", False),
        "refresh_seconds": data.get("refresh_seconds", 60),
        "configs":         data.get("configs", []),
        "dwh": {
            "host": data.get("dwh_host", ""), "port": data.get("dwh_port", 3306),
            "db":   data.get("dwh_db",   ""), "user": data.get("dwh_user", ""),
            "pass": data.get("dwh_pass", ""),
        },
        "source": {
            "ip":      data.get("origen_ip",   ""),  "port": data.get("origen_port", 1433),
            "db":      data.get("origen_db",   ""),  "user": data.get("origen_user", ""),
            "pass":    data.get("origen_pass", ""),  "dsn_odbc": data.get("dsn_odbc", ""),
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
# API — reportar evento al backend (éxito o error)
# ─────────────────────────────────────────────────────────────────────────────
def report_event(
    config_id: str,
    task_name: str,
    event_type: str,          # "ok" | "error"
    detail: str = "",
    rows_loaded: int = 0,
) -> None:
    """
    Informa al backend el resultado de una ejecución de tarea.
    El backend resuelve grupo + RS desde el token y los almacena.
    El monitor los mostrará agrupados por grupo y razón social.
    """
    url     = f"{API_BASE_URL}/client-event"
    headers = {"x-token": CLIENT_TOKEN}
    payload = {
        "config_id":   config_id,
        "task_name":   task_name,
        "event_type":  event_type,
        "detail":      detail,
        "rows_loaded": rows_loaded,
    }
    try:
        r = requests.post(url, json=payload, headers=headers, timeout=10)
        if r.status_code != 200:
            log.warning("No se pudo reportar evento al backend (status %d).", r.status_code)
    except requests.ConnectionError:
        log.warning("No se pudo reportar evento al backend (sin conexión).")


# ─────────────────────────────────────────────────────────────────────────────
# API — notificar last_run_at
# ─────────────────────────────────────────────────────────────────────────────
def notify_last_run(config_id: str) -> None:
    url     = f"{API_BASE_URL}/configs/{config_id}/last_run"
    headers = {"x-token": CLIENT_TOKEN}
    try:
        r = requests.put(url, headers=headers, timeout=10)
        if r.status_code != 200:
            log.warning("No se pudo actualizar last_run_at para tarea %s.", config_id)
    except requests.ConnectionError:
        log.warning("No se pudo notificar last_run_at (sin conexión).")


# ─────────────────────────────────────────────────────────────────────────────
# Conexiones
# ─────────────────────────────────────────────────────────────────────────────
def _detect_sql_server_driver() -> str:
    available = pyodbc.drivers()
    for name in [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "ODBC Driver 13 for SQL Server",
        "SQL Server Native Client 11.0",
        "SQL Server",
    ]:
        if name in available:
            return name
    return "ODBC Driver 17 for SQL Server"


def build_source_conn_str() -> str:
    dsn = _source_config.get("dsn_odbc", "").strip()
    if dsn:
        parts = [f"DSN={dsn}"]
        if _source_config.get("user"):
            parts.append(f"UID={_source_config['user']}")
        if _source_config.get("pass"):
            parts.append(f"PWD={_source_config['pass']}")
        return ";".join(parts)

    if not _source_config.get("ip"):
        raise ValueError("No hay IP de origen configurada ni DSN ODBC.")

    driver = _detect_sql_server_driver()
    server = _source_config["ip"]
    port   = _source_config.get("port", 1433)
    if port and port != 1433:
        server = f"{server},{port}"

    return (
        f"DRIVER={{{driver}}};SERVER={server};"
        f"DATABASE={_source_config['db']};"
        f"UID={_source_config['user']};PWD={_source_config['pass']};"
        f"TrustServerCertificate=yes"
    )


@contextmanager
def _dwh_connection():
    if not _dwh_config.get("host"):
        raise ValueError("No hay host DWH configurado.")
    conn = pymysql.connect(
        host=_dwh_config["host"],   port=int(_dwh_config["port"]),
        user=_dwh_config["user"],   password=_dwh_config["pass"],
        database=_dwh_config["db"], charset="utf8mb4",
    )
    try:
        yield conn
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# MySQL helpers
# ─────────────────────────────────────────────────────────────────────────────
def quote_ident(name: str) -> str:
    return f"`{name}`"


def infer_column_types(
    columns: Sequence[str], rows: Sequence[Sequence[Any]]
) -> Dict[str, str]:
    samples: List[Any] = [None] * len(columns)
    for row in rows:
        for i, val in enumerate(row):
            if samples[i] is None and val is not None:
                samples[i] = val
        if all(v is not None for v in samples):
            break

    type_map: Dict[str, str] = {}
    for i, col in enumerate(columns):
        val = samples[i]
        if isinstance(val, bool):          sql_type = "TINYINT(1)"
        elif isinstance(val, int):         sql_type = "INT"
        elif isinstance(val, Decimal):     sql_type = "DECIMAL(18,4)"
        elif isinstance(val, float):       sql_type = "DOUBLE"
        elif isinstance(val, datetime):    sql_type = "DATETIME"
        elif isinstance(val, date):        sql_type = "DATE"
        else:                              sql_type = "TEXT"
        type_map[col] = sql_type
    return type_map


def create_table_if_missing(
    cursor: Any, table: str,
    columns: Sequence[str], column_types: Dict[str, str],
    upsert_keys: Sequence[str],
) -> None:
    col_defs = []
    for col in columns:
        sql_type = column_types[col]
        if sql_type == "TEXT" and col in upsert_keys:
            sql_type = "VARCHAR(255)"
        col_defs.append(f"{quote_ident(col)} {sql_type}")

    valid_keys = [k for k in upsert_keys if k in columns]
    if valid_keys:
        pk = ", ".join(quote_ident(k) for k in valid_keys)
        col_defs.append(f"PRIMARY KEY ({pk})")

    cursor.execute(
        f"CREATE TABLE IF NOT EXISTS {quote_ident(table)} "
        f"({', '.join(col_defs)}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
    )


def build_upsert_sql(table: str, columns: Sequence[str], upsert_keys: Sequence[str]) -> str:
    cols_sql     = ", ".join(quote_ident(c) for c in columns)
    placeholders = ", ".join("%s" for _ in columns)
    valid_keys   = [k for k in upsert_keys if k in columns]

    if not valid_keys:
        return f"INSERT INTO {quote_ident(table)} ({cols_sql}) VALUES ({placeholders})"

    update_cols = [c for c in columns if c not in valid_keys]
    if not update_cols:
        return f"INSERT IGNORE INTO {quote_ident(table)} ({cols_sql}) VALUES ({placeholders})"

    set_clause = ", ".join(
        f"{quote_ident(c)} = VALUES({quote_ident(c)})" for c in update_cols
    )
    return (
        f"INSERT INTO {quote_ident(table)} ({cols_sql}) VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {set_clause}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Ejecutar tarea
# ─────────────────────────────────────────────────────────────────────────────
def prepare_extract_sql(sql: str, last_run_at: Optional[str]) -> str:
    if "{last_run}" not in sql:
        return sql
    return sql.replace("{last_run}", last_run_at or "1900-01-01 00:00:00")


def run_task(task: Dict[str, Any]) -> int:
    """
    Ejecuta la tarea ETL completa.
    Devuelve el número de filas cargadas.
    Lanza excepción si algo falla (el scheduler la captura y reporta el error).
    """
    task_id          = task["id"]
    task_name        = task["name"]
    load_table       = task["load_table"]
    upsert_keys      = task.get("upsert_keys") or []
    query_tabla      = task.get("query_tabla_destino")
    query_constraint = task.get("query_constraint")
    last_run_at      = task.get("last_run_at")

    if not _dwh_config or not _source_config:
        raise RuntimeError("Conexiones no configuradas.")

    if _verbose:
        log.info("--- Tarea %s: %s ---", task_id, task_name)

    # 1) DDL tabla destino
    if query_tabla:
        with _dwh_connection() as conn:
            cur = conn.cursor()
            cur.execute(query_tabla)
            conn.commit()
            if _verbose:
                log.info("  Tabla '%s' verificada.", load_table)

    # 2) Constraint
    if query_constraint:
        try:
            with _dwh_connection() as conn:
                cur = conn.cursor()
                cur.execute(query_constraint)
                conn.commit()
        except Exception as exc:
            log.warning("  Constraint falló (puede que ya exista): %s", exc)

    # 3) Extraer desde SQL Server
    extract_sql = prepare_extract_sql(task["extract_sql"], last_run_at)
    with pyodbc.connect(build_source_conn_str()) as conn:
        cur = conn.cursor()
        cur.execute(extract_sql)
        rows    = cur.fetchall()
        columns = [d[0] for d in (cur.description or [])]

    if not columns:
        log.warning("  Tarea %s: sin columnas, se omite.", task_id)
        return 0

    rows_tuples = [tuple(r) for r in rows]
    if _verbose:
        log.info("  Filas extraídas: %d", len(rows_tuples))

    # 4) Crear tabla inferida si no hay DDL
    if not query_tabla:
        col_types = infer_column_types(columns, rows_tuples)
        with _dwh_connection() as conn:
            cur = conn.cursor()
            create_table_if_missing(cur, load_table, columns, col_types, upsert_keys)
            conn.commit()

    # 5) Upsert
    upsert_sql = build_upsert_sql(load_table, columns, upsert_keys)
    with _dwh_connection() as conn:
        cur = conn.cursor()
        if rows_tuples:
            cur.executemany(upsert_sql, rows_tuples)
            conn.commit()
            if _verbose:
                log.info("  Filas cargadas: %d", len(rows_tuples))
        else:
            if _verbose:
                log.info("  Sin filas nuevas.")

    return len(rows_tuples)


# ─────────────────────────────────────────────────────────────────────────────
# Detección de cambios entre configuraciones
# ─────────────────────────────────────────────────────────────────────────────
_COMPARE_FIELDS = [
    "schedule_seconds", "extract_sql", "load_table", "upsert_keys",
    "query_tabla_destino", "constraint_nombre", "query_constraint", "active",
]


def detect_changes(old: List[Dict], new: List[Dict]) -> None:
    old_map = {str(t["id"]): t for t in old}
    new_map = {str(t["id"]): t for t in new}
    old_ids, new_ids = set(old_map), set(new_map)

    for tid in sorted(new_ids - old_ids):
        log.info("  [+] Nueva tarea %s: %s", tid, new_map[tid].get("name", ""))
    for tid in sorted(old_ids - new_ids):
        log.info("  [-] Tarea removida %s: %s", tid, old_map[tid].get("name", ""))
    for tid in sorted(old_ids & new_ids):
        cambios = [
            f"{f}: {old_map[tid].get(f)!r} → {new_map[tid].get(f)!r}"
            for f in _COMPARE_FIELDS
            if old_map[tid].get(f) != new_map[tid].get(f)
        ]
        if cambios:
            log.info("  [~] Tarea %s modificada:", tid)
            for c in cambios:
                log.info("      %s", c)


# ─────────────────────────────────────────────────────────────────────────────
# Scheduler
# ─────────────────────────────────────────────────────────────────────────────
_last_run_ts: Dict[str, float] = {}
TICK_SECONDS  = 5
RETRY_SECONDS = 30


def run_scheduler() -> None:
    global _verbose, _dwh_config, _source_config

    configs: List[Dict[str, Any]] = []
    last_config_refresh: float = 0
    refresh_seconds: int = 60

    log.info("Scheduler iniciado.")

    while True:
        now = time.time()

        # ── Refrescar configs ──
        if now - last_config_refresh >= refresh_seconds:
            result = fetch_configs()
            status = result["status"]

            if status == "unreachable":
                log.error("Backend no disponible. Reintentando en %ds...", RETRY_SECONDS)
                configs = []
                last_config_refresh = 0
                time.sleep(RETRY_SECONDS)
                continue

            if status == "invalid":
                log.error("Token no válido. Verifica config.ini. Deteniendo.")
                return

            if status == "disabled":
                log.warning("Servicio deshabilitado. Reintentando en %ds...", RETRY_SECONDS)
                configs = []
                last_config_refresh = 0
                time.sleep(RETRY_SECONDS)
                continue

            _verbose       = result["log_verbose"]
            _dwh_config    = result["dwh"]
            _source_config = result["source"]
            nuevas         = result["configs"]

            new_refresh = result["refresh_seconds"]
            if new_refresh != refresh_seconds:
                log.info("refresh_seconds: %ds → %ds", refresh_seconds, new_refresh)
                refresh_seconds = new_refresh

            if configs and nuevas:
                detect_changes(configs, nuevas)

            # Resetear timer si schedule_seconds cambió
            old_map = {str(t["id"]): t for t in configs}
            for t in nuevas:
                tid = str(t["id"])
                if tid in old_map:
                    if old_map[tid].get("schedule_seconds") != t.get("schedule_seconds"):
                        _last_run_ts.pop(tid, None)
                        log.info("Tarea %s: schedule reiniciado.", tid)

            configs = nuevas
            last_config_refresh = now
            log.info("Configs: %d tareas activas (refresh: %ds).", len(configs), refresh_seconds)

        # ── Ejecutar tareas ──
        executed = errors = 0

        for task in configs:
            task_id   = str(task["id"])
            task_name = task.get("name", task_id)
            schedule  = task.get("schedule_seconds", 3600)

            if not task.get("active", True):
                continue
            if now - _last_run_ts.get(task_id, 0) < schedule:
                continue

            try:
                rows_loaded = run_task(task)
                _last_run_ts[task_id] = time.time()
                executed += 1

                # Notificar last_run_at al backend
                notify_last_run(task_id)

                # Reportar ejecución exitosa al monitor
                report_event(
                    config_id=task_id,
                    task_name=task_name,
                    event_type="ok",
                    detail=f"{rows_loaded} filas cargadas en '{task.get('load_table', '')}'",
                    rows_loaded=rows_loaded,
                )

            except Exception as exc:
                _last_run_ts[task_id] = time.time()
                errors += 1
                error_msg = traceback.format_exc()
                log.error("Error en tarea %s: %s", task_id, exc, exc_info=True)

                # Reportar error al monitor (con traceback completo)
                report_event(
                    config_id=task_id,
                    task_name=task_name,
                    event_type="error",
                    detail=error_msg,
                )

        if not _verbose and (executed > 0 or errors > 0):
            log.info("Ciclo: %d ejecutadas, %d errores.", executed, errors)

        time.sleep(TICK_SECONDS)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    log.info("=== Nexus DWH Client ===")
    log.info("Servidor : %s", API_BASE_URL)
    log.info("Logs     : %s", _LOGS_DIR)
    try:
        run_scheduler()
    except KeyboardInterrupt:
        log.info("Cliente detenido por el usuario.")


if __name__ == "__main__":
    main()