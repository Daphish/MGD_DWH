"""
nexus_server.py  v3
───────────────────
El token de cada cliente identifica una razón social.
El grupo se resuelve automáticamente vía JOIN en cada petición.
Ambos (grupo + RS) se almacenan en todos los registros de eventos y logs.

Endpoints clientes ETL:
  GET  /configs                  → configuraciones de la RS
  PUT  /configs/{id}/last_run    → marcar tarea ejecutada exitosamente

Endpoints monitor (requieren header x-monitor-token):
  POST /client-event             → clientes reportan eventos (éxito o error)
  GET  /monitor/events           → historial de eventos
  GET  /monitor/clients          → resumen de todos los clientes
  GET  /monitor/activity         → log HTTP del backend
  PUT  /monitor/events/{id}/ack  → reconocer una alerta
  PUT  /monitor/events/ack-all   → reconocer todas las alertas
"""

import argparse
import configparser
import os
import sys
import time
import traceback
from typing import List, Optional

import pymysql
import uvicorn
from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

app = FastAPI(title="Nexus Config API", version="3.0.0")


# ─────────────────────────────────────────────────────────────────────────────
# Configuración
# ─────────────────────────────────────────────────────────────────────────────
if getattr(sys, "frozen", False):
    _APP_DIR = os.path.dirname(sys.executable)
else:
    _APP_DIR = os.path.dirname(os.path.abspath(__file__))

_ini = configparser.ConfigParser()
_ini.read(os.path.join(_APP_DIR, "config.ini"))

DB_HOST       = _ini.get("database", "host",     fallback="127.0.0.1")
DB_PORT       = _ini.getint("database", "port",  fallback=3306)
DB_NAME       = _ini.get("database", "db",       fallback="nexus_config")
DB_USER       = _ini.get("database", "user",     fallback="root")
DB_PASS       = _ini.get("database", "password", fallback="")
MONITOR_TOKEN = _ini.get("monitor",  "token",    fallback="")


# ─────────────────────────────────────────────────────────────────────────────
# Modelos
# ─────────────────────────────────────────────────────────────────────────────
class TaskConfig(BaseModel):
    id: str
    name: str
    schedule_seconds: int = 3600
    extract_sql: str
    load_table: str
    upsert_keys: List[str] = Field(default_factory=list)
    query_tabla_destino: Optional[str] = None
    constraint_nombre: Optional[str] = None
    query_constraint: Optional[str] = None
    active: bool = True
    last_run_at: Optional[str] = None
    updated_at: str = ""


class ConfigsResponse(BaseModel):
    log_verbose: bool = False
    refresh_seconds: int = 60
    dwh_host: str = ""
    dwh_port: int = 3306
    dwh_db: str = ""
    dwh_user: str = ""
    dwh_pass: str = ""
    origen_ip: str = ""
    origen_port: int = 1433
    origen_db: str = ""
    origen_user: str = ""
    origen_pass: str = ""
    dsn_odbc: str = ""
    configs: List[TaskConfig] = Field(default_factory=list)


# ─────────────────────────────────────────────────────────────────────────────
# Conexión BD
# ─────────────────────────────────────────────────────────────────────────────
def get_connection() -> pymysql.connections.Connection:
    return pymysql.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER,
        password=DB_PASS, database=DB_NAME, charset="utf8mb4",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Resolver identidad completa desde token
# ─────────────────────────────────────────────────────────────────────────────
def _resolve_identity(token: str) -> tuple:
    """
    Dado un token de razón social devuelve (razon_social, grupo).
    Usado por el middleware para enriquecer el activity_log.
    """
    if not token:
        return ("", "")
    try:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT rs.nombre, g.nombre
                FROM razon_social rs
                JOIN grupo g ON g.id = rs.id_grupo
                WHERE rs.token = %s
                """,
                (token,),
            )
            row = cur.fetchone()
            return (row[0], row[1]) if row else ("", "")
        finally:
            conn.close()
    except Exception:
        return ("", "")


# ─────────────────────────────────────────────────────────────────────────────
# Middleware – activity_log
# ─────────────────────────────────────────────────────────────────────────────
def _log_activity(
    token: str, razon_social: str, grupo: str,
    method: str, endpoint: str, status_code: int,
    response_ms: int, error_detail: Optional[str], client_ip: str,
) -> None:
    try:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO activity_log
                    (token, razon_social, grupo, method, endpoint,
                     status_code, response_ms, error_detail, client_ip)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (token, razon_social, grupo, method, endpoint,
                 status_code, response_ms, error_detail, client_ip),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        pass


@app.middleware("http")
async def activity_logging_middleware(request: Request, call_next):
    start     = time.time()
    token     = request.headers.get("x-token", "")
    endpoint  = request.url.path
    method    = request.method
    client_ip = request.client.host if request.client else ""

    error_detail: Optional[str] = None
    status_code = 500

    try:
        response    = await call_next(request)
        status_code = response.status_code

        if status_code >= 400:
            body = b""
            async for chunk in response.body_iterator:
                body += chunk
            error_detail = body.decode("utf-8", errors="replace")
            from starlette.responses import Response as RawResponse
            response = RawResponse(
                content=body, status_code=status_code,
                headers=dict(response.headers), media_type=response.media_type,
            )
        return response

    except Exception:
        status_code  = 500
        error_detail = traceback.format_exc()
        return JSONResponse(status_code=500, content={"detail": "Error interno."})

    finally:
        elapsed_ms          = int((time.time() - start) * 1000)
        razon_social, grupo = _resolve_identity(token)
        _log_activity(
            token, razon_social, grupo, method, endpoint,
            status_code, elapsed_ms, error_detail, client_ip,
        )


# ─────────────────────────────────────────────────────────────────────────────
# Auth helpers
# ─────────────────────────────────────────────────────────────────────────────
def _require_monitor_token(token: str) -> None:
    if not MONITOR_TOKEN:
        raise HTTPException(
            status_code=503,
            detail="Monitor no configurado: agrega [monitor] token=... en config.ini del servidor.",
        )
    if token != MONITOR_TOKEN:
        raise HTTPException(status_code=401, detail="Token de monitor no válido.")


def check_token_status(token: str) -> dict:
    """
    Resuelve el token de un cliente ETL.
    Retorna grupo + RS + configuraciones de conexión.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                rs.id,
                rs.nombre          AS rs_nombre,
                rs.enabled         AS rs_enabled,
                rs.log_verbose,
                rs.refresh_seconds,
                rs.dsn_odbc,
                rs.origen_ip,
                rs.origen_port,
                rs.origen_db,
                rs.usuario_sql     AS origen_user,
                rs.pass_sql        AS origen_pass,
                g.id               AS grupo_id,
                g.nombre           AS grupo_nombre,
                g.enabled          AS grupo_enabled,
                g.dwh_host,
                g.dwh_port,
                g.dwh_db,
                g.dwh_user,
                g.dwh_pass
            FROM razon_social rs
            JOIN grupo g ON g.id = rs.id_grupo
            WHERE rs.token = %s
            """,
            (token,),
        )
        row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        return {"found": False}

    return {
        "found":           True,
        "rs_id":           row[0],
        "rs_nombre":       row[1],
        "rs_enabled":      bool(row[2]),
        "log_verbose":     bool(row[3]),
        "refresh_seconds": row[4],
        "dsn_odbc":        row[5]  or "",
        "origen_ip":       row[6]  or "",
        "origen_port":     row[7],
        "origen_db":       row[8]  or "",
        "origen_user":     row[9]  or "",
        "origen_pass":     row[10] or "",
        "grupo_id":        row[11],
        "grupo_nombre":    row[12],
        "grupo_enabled":   bool(row[13]),
        "dwh_host":        row[14] or "",
        "dwh_port":        row[15],
        "dwh_db":          row[16] or "",
        "dwh_user":        row[17] or "",
        "dwh_pass":        row[18] or "",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Configs de tareas
# ─────────────────────────────────────────────────────────────────────────────
def fetch_configs_from_db(token: str) -> List[TaskConfig]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                ao.id,
                g.nombre,
                rs.nombre,
                a.nombre,
                co.tabla_destino,
                co.upsert_keys,
                co.query_tabla_destino,
                co.constraint_nombre,
                co.query_constraint,
                ao.extract_sql,
                ao.schedule_seconds,
                ao.active,
                ao.last_run_at,
                ao.updated_at
            FROM agencia_objeto  ao
            JOIN agencia         a  ON a.id  = ao.id_agencia
            JOIN catalogo_objeto co ON co.id = ao.id_objeto
            JOIN razon_social    rs ON rs.id = a.id_razon_social
            JOIN grupo           g  ON g.id  = rs.id_grupo
            WHERE rs.token  = %s
              AND ao.active  = 1
              AND a.enabled  = 1
              AND co.enabled = 1
              AND rs.enabled = 1
              AND g.enabled  = 1
            ORDER BY ao.id
            """,
            (token,),
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    configs: List[TaskConfig] = []
    for row in rows:
        upsert_keys = [k.strip() for k in (row[5] or "").split(",") if k.strip()]
        configs.append(TaskConfig(
            id=str(row[0]),
            name=f"{row[1]} | {row[2]} | {row[3]}",
            load_table=row[4],
            upsert_keys=upsert_keys,
            query_tabla_destino=row[6]  or None,
            constraint_nombre=row[7]    or None,
            query_constraint=row[8]     or None,
            extract_sql=row[9],
            schedule_seconds=row[10],
            active=bool(row[11]),
            last_run_at=row[12].isoformat() if row[12] else None,
            updated_at=row[13].isoformat()  if row[13] else "",
        ))
    return configs


# ─────────────────────────────────────────────────────────────────────────────
# Endpoints — clientes ETL
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/configs", response_model=ConfigsResponse)
def get_configs(x_token: str = Header(...)) -> ConfigsResponse:
    s = check_token_status(x_token)

    if not s["found"]:
        raise HTTPException(status_code=401, detail="Token no válido.")
    if not s["grupo_enabled"]:
        raise HTTPException(status_code=403, detail=f"Grupo '{s['grupo_nombre']}' deshabilitado.")
    if not s["rs_enabled"]:
        raise HTTPException(status_code=403, detail=f"Razón social '{s['rs_nombre']}' deshabilitada.")

    return ConfigsResponse(
        log_verbose=s["log_verbose"],
        refresh_seconds=s["refresh_seconds"],
        dwh_host=s["dwh_host"],       dwh_port=s["dwh_port"],
        dwh_db=s["dwh_db"],           dwh_user=s["dwh_user"],
        dwh_pass=s["dwh_pass"],
        origen_ip=s["origen_ip"],     origen_port=s["origen_port"],
        origen_db=s["origen_db"],     origen_user=s["origen_user"],
        origen_pass=s["origen_pass"], dsn_odbc=s["dsn_odbc"],
        configs=fetch_configs_from_db(x_token),
    )


@app.put("/configs/{config_id}/last_run")
def update_last_run(config_id: int, x_token: str = Header(...)) -> dict:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT ao.id
            FROM agencia_objeto ao
            JOIN agencia      a  ON a.id  = ao.id_agencia
            JOIN razon_social rs ON rs.id = a.id_razon_social
            WHERE ao.id = %s AND rs.token = %s
            """,
            (config_id, x_token),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Tarea no encontrada.")
        cur.execute(
            "UPDATE agencia_objeto SET last_run_at = NOW() WHERE id = %s",
            (config_id,),
        )
        conn.commit()
    finally:
        conn.close()
    return {"status": "ok", "config_id": config_id}


# ─────────────────────────────────────────────────────────────────────────────
# Endpoint — clientes reportan eventos (éxito O error)
# ─────────────────────────────────────────────────────────────────────────────
@app.post("/client-event")
def report_client_event(payload: dict, x_token: str = Header(...)) -> dict:
    """
    Cada vez que un cliente termina de ejecutar una tarea (bien o mal),
    reporta aquí. El backend resuelve grupo + RS desde el token y los guarda.

    Payload:
      config_id   : str  — ID de la tarea
      task_name   : str  — nombre descriptivo (grupo | RS | agencia)
      event_type  : str  — "ok" | "error"
      detail      : str  — resumen si ok, traceback si error (opcional)
      rows_loaded : int  — filas cargadas (solo en ok, opcional)
    """
    s = check_token_status(x_token)
    if not s["found"]:
        raise HTTPException(status_code=401, detail="Token no válido.")

    event_type  = str(payload.get("event_type", "error"))
    config_id   = str(payload.get("config_id", ""))
    task_name   = str(payload.get("task_name", ""))
    detail      = str(payload.get("detail", ""))
    rows_loaded = int(payload.get("rows_loaded", 0))

    # Los eventos "ok" se auto-reconocen (no generan alerta pendiente).
    # Los eventos "error" quedan acknowledged=0 hasta que el monitor los revise.
    acknowledged = 1 if event_type == "ok" else 0

    try:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO client_events
                    (token, grupo, razon_social, config_id,
                     task_name, event_type, detail, rows_loaded, acknowledged)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    x_token,
                    s["grupo_nombre"],
                    s["rs_nombre"],
                    config_id,
                    task_name,
                    event_type,
                    detail,
                    rows_loaded,
                    acknowledged,
                ),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    return {"status": "ok"}


# ─────────────────────────────────────────────────────────────────────────────
# Endpoints — monitor
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/monitor/events")
def get_events(
    x_monitor_token: str = Header(...),
    event_type: Optional[str] = Query(None, description="'ok' | 'error' | None = todos"),
    only_unacknowledged: bool = Query(False),
    limit: int = Query(200, ge=1, le=2000),
) -> dict:
    """Historial de eventos con grupo + razón social en cada registro."""
    _require_monitor_token(x_monitor_token)

    conditions: List[str] = []
    params: list = []

    if event_type in ("ok", "error"):
        conditions.append("event_type = %s")
        params.append(event_type)
    if only_unacknowledged:
        conditions.append("acknowledged = 0")

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT id, created_at, grupo, razon_social, config_id,
                   task_name, event_type, detail, rows_loaded, acknowledged
            FROM client_events
            {where}
            ORDER BY created_at DESC
            LIMIT %s
            """,
            params + [limit],
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    items = [
        {
            "id":           r[0],
            "timestamp":    r[1].isoformat() if r[1] else "",
            "grupo":        r[2],
            "razon_social": r[3],
            "config_id":    r[4],
            "task_name":    r[5],
            "event_type":   r[6],
            "detail":       r[7],
            "rows_loaded":  r[8],
            "acknowledged": bool(r[9]),
        }
        for r in rows
    ]
    return {"total": len(items), "items": items}


@app.get("/monitor/clients")
def get_clients_status(x_monitor_token: str = Header(...)) -> dict:
    """
    Resumen por cliente: grupo, RS, última conexión,
    ejecuciones totales, errores pendientes y errores en la última hora.
    """
    _require_monitor_token(x_monitor_token)

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                g.nombre                                                AS grupo,
                rs.nombre                                               AS razon_social,
                rs.token,
                rs.enabled                                              AS rs_enabled,
                g.enabled                                               AS grupo_enabled,
                MAX(al.created_at)                                      AS last_seen,
                COUNT(al.id)                                            AS requests_total,
                SUM(al.status_code >= 400)                              AS http_errors_total,
                SUM(al.created_at >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
                    AND al.status_code >= 400)                          AS http_errors_1h,
                (SELECT COUNT(*)
                 FROM client_events ce
                 WHERE ce.token = rs.token)                             AS executions_total,
                (SELECT COUNT(*)
                 FROM client_events ce
                 WHERE ce.token = rs.token
                   AND ce.event_type = 'error')                         AS exec_errors_total,
                (SELECT COUNT(*)
                 FROM client_events ce
                 WHERE ce.token = rs.token
                   AND ce.event_type = 'error'
                   AND ce.acknowledged = 0)                             AS exec_errors_pending,
                (SELECT COUNT(*)
                 FROM client_events ce
                 WHERE ce.token = rs.token
                   AND ce.event_type = 'error'
                   AND ce.created_at >= DATE_SUB(NOW(), INTERVAL 1 HOUR)) AS exec_errors_1h,
                (SELECT MAX(ce.created_at)
                 FROM client_events ce
                 WHERE ce.token = rs.token)                             AS last_execution
            FROM razon_social rs
            JOIN grupo g ON g.id = rs.id_grupo
            LEFT JOIN activity_log al ON al.token = rs.token
            GROUP BY rs.id, rs.nombre, rs.token, rs.enabled,
                     g.id, g.nombre, g.enabled
            ORDER BY g.nombre, rs.nombre
            """
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    items = [
        {
            "grupo":               r[0],
            "razon_social":        r[1],
            "token_preview":       (r[2] or "")[:8] + "...",
            "rs_enabled":          bool(r[3]),
            "grupo_enabled":       bool(r[4]),
            "last_seen":           r[5].isoformat()  if r[5]  else None,
            "requests_total":      int(r[6]  or 0),
            "http_errors_total":   int(r[7]  or 0),
            "http_errors_1h":      int(r[8]  or 0),
            "executions_total":    int(r[9]  or 0),
            "exec_errors_total":   int(r[10] or 0),
            "exec_errors_pending": int(r[11] or 0),
            "exec_errors_1h":      int(r[12] or 0),
            "last_execution":      r[13].isoformat() if r[13] else None,
        }
        for r in rows
    ]
    return {"clients": items}


@app.get("/monitor/activity")
def get_activity_log(
    x_monitor_token: str = Header(...),
    only_errors: bool = Query(True),
    limit: int = Query(200, ge=1, le=2000),
) -> dict:
    """Log de actividad HTTP del backend con grupo + RS."""
    _require_monitor_token(x_monitor_token)

    where = "WHERE status_code >= 400" if only_errors else ""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT id, created_at, grupo, razon_social, token,
                   method, endpoint, status_code, response_ms,
                   error_detail, client_ip
            FROM activity_log
            {where}
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    items = [
        {
            "id":           r[0],
            "timestamp":    r[1].isoformat() if r[1] else "",
            "grupo":        r[2],
            "razon_social": r[3],
            "token":        (r[4] or "")[:8] + "...",
            "method":       r[5],
            "endpoint":     r[6],
            "status_code":  r[7],
            "response_ms":  r[8],
            "error_detail": r[9],
            "client_ip":    r[10],
        }
        for r in rows
    ]
    return {"total": len(items), "items": items}


@app.put("/monitor/events/{event_id}/ack")
def acknowledge_event(event_id: int, x_monitor_token: str = Header(...)) -> dict:
    _require_monitor_token(x_monitor_token)
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE client_events SET acknowledged = 1 WHERE id = %s",
            (event_id,),
        )
        conn.commit()
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Evento no encontrado.")
    finally:
        conn.close()
    return {"status": "ok", "event_id": event_id}


@app.put("/monitor/events/ack-all")
def acknowledge_all_events(x_monitor_token: str = Header(...)) -> dict:
    _require_monitor_token(x_monitor_token)
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE client_events SET acknowledged = 1 "
            "WHERE acknowledged = 0 AND event_type = 'error'"
        )
        conn.commit()
        affected = cur.rowcount
    finally:
        conn.close()
    return {"status": "ok", "acknowledged": affected}


# ─────────────────────────────────────────────────────────────────────────────
# Entrypoint
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Nexus Config API Server")
    parser.add_argument("--host", default="127.0.0.1",
                        help="Interfaz de escucha. Usa 0.0.0.0 para aceptar conexiones externas.")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    print(f"Nexus Server iniciando en {args.host}:{args.port} ...")
    uvicorn.run(app, host=args.host, port=args.port)