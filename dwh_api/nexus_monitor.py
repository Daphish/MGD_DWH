"""
nexus_monitor.py  v3
─────────────────────
Monitor CLI para Nexus. Muestra en consola:
  · Errores de ejecución pendientes  (grupo + razón social)
  · Ejecuciones exitosas recientes   (grupo + razón social)
  · Estado de todos los clientes     (grupo + razón social)
  · Log de actividad HTTP del backend

config.ini requerido junto al .exe:
    [monitor]
    api_url      = http://IP_DEL_BACK:8000
    token        = TOKEN_SECRETO_MONITOR
    poll_seconds = 30
"""

import configparser
import os
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

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
        print(f"[ERROR] No se encontró config.ini en: {ini_path}")
        sys.exit(1)
    cfg = configparser.ConfigParser()
    cfg.read(ini_path)
    return cfg


_ini          = load_ini()
API_BASE_URL  = _ini.get("monitor", "api_url",      fallback="http://127.0.0.1:8000")
MONITOR_TOKEN = _ini.get("monitor", "token",         fallback="")
POLL_SECONDS  = _ini.getint("monitor", "poll_seconds", fallback=30)

if not MONITOR_TOKEN:
    print("[ERROR] Falta [monitor] token en config.ini")
    sys.exit(1)


# ─────────────────────────────────────────────────────────────────────────────
# Colores ANSI
# ─────────────────────────────────────────────────────────────────────────────
RESET  = "\033[0m"
BOLD   = "\033[1m"
RED    = "\033[91m"
YELLOW = "\033[93m"
GREEN  = "\033[92m"
CYAN   = "\033[96m"
BLUE   = "\033[94m"
GRAY   = "\033[90m"
WHITE  = "\033[97m"


def _enable_ansi_windows() -> None:
    if sys.platform == "win32":
        try:
            import ctypes
            ctypes.windll.kernel32.SetConsoleMode(
                ctypes.windll.kernel32.GetStdHandle(-11), 7
            )
        except Exception:
            pass


_enable_ansi_windows()


# ─────────────────────────────────────────────────────────────────────────────
# Helpers de impresión
# ─────────────────────────────────────────────────────────────────────────────
W = 78  # ancho de línea


def hline(char: str = "─", w: int = W) -> str:
    return char * w


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def print_header(subtitle: str = "DASHBOARD") -> None:
    os.system("cls" if sys.platform == "win32" else "clear")
    print(f"{BOLD}{CYAN}{hline('═')}{RESET}")
    print(f"{BOLD}{CYAN}  NEXUS MONITOR  ·  {subtitle}  ·  {now_str()}{RESET}")
    print(f"{BOLD}{CYAN}  Backend: {API_BASE_URL}{RESET}")
    print(f"{BOLD}{CYAN}{hline('═')}{RESET}")


def print_section(title: str) -> None:
    print(f"\n{BOLD}{WHITE}  {title}{RESET}")
    print(f"  {GRAY}{hline('-', W - 2)}{RESET}")


def col(text: str, width: int) -> str:
    """Trunca o rellena un string a un ancho fijo."""
    text = str(text or "")
    return text[:width].ljust(width)


# ─────────────────────────────────────────────────────────────────────────────
# Llamadas al backend
# ─────────────────────────────────────────────────────────────────────────────
_HEADERS = {"x-monitor-token": MONITOR_TOKEN}


def _get(path: str, params: Optional[Dict] = None) -> Optional[Any]:
    try:
        r = requests.get(
            f"{API_BASE_URL}{path}",
            headers=_HEADERS,
            params=params or {},
            timeout=10,
        )
        if r.status_code == 401:
            print(f"\n{RED}[AUTH] Token de monitor inválido.{RESET}")
            return None
        if r.status_code == 503:
            print(f"\n{YELLOW}[CONFIG] Monitor no configurado en el servidor.{RESET}")
            return None
        r.raise_for_status()
        return r.json()
    except requests.ConnectionError:
        print(f"\n{RED}[ERROR] Sin conexión con el backend ({API_BASE_URL}).{RESET}")
        return None
    except Exception as exc:
        print(f"\n{RED}[ERROR] {exc}{RESET}")
        return None


def _put(path: str) -> bool:
    try:
        r = requests.put(f"{API_BASE_URL}{path}", headers=_HEADERS, timeout=10)
        return r.status_code == 200
    except Exception:
        return False


def fetch_error_events() -> List[Dict]:
    data = _get("/monitor/events", {
        "event_type": "error",
        "only_unacknowledged": "true",
        "limit": 50,
    })
    return (data or {}).get("items", [])


def fetch_ok_events(limit: int = 20) -> List[Dict]:
    data = _get("/monitor/events", {
        "event_type": "ok",
        "limit": limit,
    })
    return (data or {}).get("items", [])


def fetch_all_events(limit: int = 60) -> List[Dict]:
    data = _get("/monitor/events", {"limit": limit})
    return (data or {}).get("items", [])


def fetch_clients() -> List[Dict]:
    data = _get("/monitor/clients")
    return (data or {}).get("clients", [])


def fetch_activity(only_errors: bool = True, limit: int = 40) -> List[Dict]:
    data = _get("/monitor/activity", {
        "only_errors": str(only_errors).lower(),
        "limit": limit,
    })
    return (data or {}).get("items", [])


def ack_event(event_id: int) -> bool:
    return _put(f"/monitor/events/{event_id}/ack")


def ack_all() -> bool:
    return _put("/monitor/events/ack-all")


# ─────────────────────────────────────────────────────────────────────────────
# Vistas
# ─────────────────────────────────────────────────────────────────────────────
def show_dashboard() -> None:
    """
    Vista principal:
      1. Errores de ejecución pendientes  (grupo + RS)
      2. Ejecuciones exitosas recientes   (grupo + RS)
      3. Estado de todos los clientes     (grupo + RS + contadores)
    """
    print_header("DASHBOARD")

    # ── 1. Errores pendientes ────────────────────────────────────────────────
    errors = fetch_error_events()
    print_section(f"ERRORES DE EJECUCIÓN PENDIENTES  [{len(errors)}]")

    if not errors:
        print(f"  {GREEN}✓  Sin errores pendientes{RESET}")
    else:
        for e in errors:
            eid   = e.get("id")
            ts    = e.get("timestamp", "")[:19]
            grp   = e.get("grupo", "?")
            rs    = e.get("razon_social", "?")
            tid   = e.get("config_id", "?")
            tname = e.get("task_name", "")
            detail = (e.get("detail") or "").strip()

            print(
                f"\n  {RED}● [{eid}]{RESET}  {GRAY}{ts}{RESET}  "
                f"{BOLD}{CYAN}{grp}{RESET}  {YELLOW}›{RESET}  {CYAN}{rs}{RESET}"
            )
            print(f"    {GRAY}Tarea #{tid}:{RESET} {tname}")
            # Mostrar las últimas 4 líneas del traceback (las más informativas)
            lines = [l for l in detail.splitlines() if l.strip()]
            for line in lines[-4:]:
                print(f"    {RED}{line}{RESET}")
            if len(lines) > 4:
                print(f"    {GRAY}... ({len(lines)} líneas — usa [v {eid}] para ver completo){RESET}")

    # ── 2. Ejecuciones exitosas recientes ────────────────────────────────────
    oks = fetch_ok_events(limit=10)
    print_section(f"ÚLTIMAS EJECUCIONES EXITOSAS  [{len(oks)}]")

    if not oks:
        print(f"  {GRAY}Sin ejecuciones recientes.{RESET}")
    else:
        hdr = (
            f"  {GRAY}"
            f"{'TIMESTAMP':<20} {'GRUPO':<18} {'RAZÓN SOCIAL':<22} "
            f"{'TAREA':<22} {'FILAS':>6}"
            f"{RESET}"
        )
        print(hdr)
        for e in oks:
            ts    = e.get("timestamp", "")[:19]
            grp   = col(e.get("grupo", ""), 18)
            rs    = col(e.get("razon_social", ""), 22)
            tname = col(e.get("task_name", "").split("|")[-1].strip(), 22)
            rows  = e.get("rows_loaded", 0)
            print(
                f"  {GRAY}{ts}{RESET}  "
                f"{CYAN}{grp}{RESET}  {CYAN}{rs}{RESET}  "
                f"{tname}  {GREEN}{rows:>6}{RESET}"
            )

    # ── 3. Estado de clientes ────────────────────────────────────────────────
    clients = fetch_clients()
    print_section(f"ESTADO DE CLIENTES  [{len(clients)}]")

    if not clients:
        print(f"  {GRAY}Sin clientes registrados.{RESET}")
    else:
        hdr = (
            f"  {GRAY}"
            f"{'GRUPO':<18} {'RAZÓN SOCIAL':<22} {'ÚLTIMA EJECUCIÓN':<20} "
            f"{'EXEC':>5} {'ERR':>5} {'PEND':>5} {'STATUS'}"
            f"{RESET}"
        )
        print(hdr)
        print(f"  {GRAY}{hline('-', W - 2)}{RESET}")

        current_group = None
        for c in clients:
            grp    = c.get("grupo", "")
            rs     = c.get("razon_social", "")
            last_x = (c.get("last_execution") or c.get("last_seen") or "—")[:19]
            exec_t = c.get("executions_total", 0)
            err_t  = c.get("exec_errors_total", 0)
            pend   = c.get("exec_errors_pending", 0)
            rs_ok  = c.get("rs_enabled", True)
            grp_ok = c.get("grupo_enabled", True)

            # Separador de grupo
            if grp != current_group:
                current_group = grp
                print(f"\n  {BOLD}{BLUE}▸ {grp}{RESET}")

            status_str = (
                f"{GREEN}activo{RESET}"   if (rs_ok and grp_ok) else
                f"{YELLOW}grupo-off{RESET}" if not grp_ok else
                f"{RED}rs-off{RESET}"
            )
            pend_str   = f"{RED}{pend:>5}{RESET}" if pend > 0 else f"{pend:>5}"
            err_str    = f"{YELLOW}{err_t:>5}{RESET}" if err_t > 0 else f"{err_t:>5}"

            print(
                f"    {col(rs, 22)}  {GRAY}{last_x:<20}{RESET}  "
                f"{exec_t:>5}  {err_str}  {pend_str}  {status_str}"
            )


def show_event_detail(event_id: int) -> None:
    """Muestra el traceback completo de un evento de error."""
    data = _get(f"/monitor/events", {
        "limit": 500,
        "event_type": "error",
    })
    if not data:
        return
    items = data.get("items", [])
    match = next((e for e in items if e.get("id") == event_id), None)

    if not match:
        print(f"\n{YELLOW}  Evento #{event_id} no encontrado.{RESET}")
        return

    print_header(f"DETALLE EVENTO #{event_id}")
    print(f"\n  {BOLD}Timestamp :{RESET} {match.get('timestamp', '')[:19]}")
    print(f"  {BOLD}Grupo     :{RESET} {CYAN}{match.get('grupo', '')}{RESET}")
    print(f"  {BOLD}RS        :{RESET} {CYAN}{match.get('razon_social', '')}{RESET}")
    print(f"  {BOLD}Tarea ID  :{RESET} {match.get('config_id', '')}")
    print(f"  {BOLD}Tarea     :{RESET} {match.get('task_name', '')}")
    print(f"\n  {BOLD}{RED}Traceback:{RESET}")
    for line in (match.get("detail") or "").splitlines():
        print(f"    {RED}{line}{RESET}")


def show_history() -> None:
    """Muestra historial mixto (ok + error) de los últimos 60 eventos."""
    print_header("HISTORIAL DE EVENTOS")
    events = fetch_all_events(limit=60)
    print_section(f"ÚLTIMOS {len(events)} EVENTOS  (ok + error)")

    if not events:
        print(f"  {GRAY}Sin eventos registrados.{RESET}")
        return

    hdr = (
        f"  {GRAY}"
        f"{'ID':>5} {'TIMESTAMP':<20} {'GRUPO':<18} {'RAZÓN SOCIAL':<20} "
        f"{'TIPO':<7} {'FILAS':>6} {'TAREA'}"
        f"{RESET}"
    )
    print(hdr)

    for e in events:
        eid    = e.get("id", "")
        ts     = e.get("timestamp", "")[:19]
        grp    = col(e.get("grupo", ""), 18)
        rs     = col(e.get("razon_social", ""), 20)
        etype  = e.get("event_type", "")
        rows   = e.get("rows_loaded", 0)
        tname  = col(e.get("task_name", "").split("|")[-1].strip(), 24)

        type_str = (
            f"{GREEN}✓ ok   {RESET}" if etype == "ok" else
            f"{RED}✗ error{RESET}"
        )
        rows_str = f"{GREEN}{rows:>6}{RESET}" if etype == "ok" else f"{GRAY}{'—':>6}{RESET}"

        print(
            f"  {GRAY}{eid:>5}{RESET}  {GRAY}{ts}{RESET}  "
            f"{CYAN}{grp}{RESET}  {CYAN}{rs}{RESET}  "
            f"{type_str}  {rows_str}  {tname}"
        )


def show_activity_log() -> None:
    """Log de actividad HTTP (errores 4xx/5xx) con grupo + RS."""
    print_header("LOG DE ACTIVIDAD HTTP")
    items = fetch_activity(only_errors=True, limit=40)
    print_section(f"ERRORES HTTP RECIENTES  [{len(items)}]")

    if not items:
        print(f"  {GREEN}✓  Sin errores HTTP recientes{RESET}")
        return

    for item in items:
        ts    = (item.get("timestamp") or "")[:19]
        grp   = item.get("grupo") or "—"
        rs    = item.get("razon_social") or item.get("token", "?")
        method = item.get("method", "?")
        ep    = item.get("endpoint", "?")
        code  = item.get("status_code", 0)
        ms    = item.get("response_ms", 0)
        ip    = item.get("client_ip", "")

        code_color = RED if code >= 500 else YELLOW
        print(
            f"\n  {GRAY}{ts}{RESET}  "
            f"{CYAN}{grp}{RESET} › {CYAN}{rs}{RESET}  "
            f"{BOLD}{method:<6}{RESET} {ep}  "
            f"{code_color}{code}{RESET}  {GRAY}{ms}ms  {ip}{RESET}"
        )
        detail = (item.get("error_detail") or "").strip()
        for line in detail.splitlines()[:3]:
            print(f"    {GRAY}{line}{RESET}")


# ─────────────────────────────────────────────────────────────────────────────
# Menú
# ─────────────────────────────────────────────────────────────────────────────
MENU = f"""
{BOLD}  Comandos:{RESET}
    {CYAN}[Enter]  {RESET}  Refrescar dashboard
    {CYAN}[h]      {RESET}  Historial completo (ok + errores)
    {CYAN}[l]      {RESET}  Log de actividad HTTP
    {CYAN}[v ID]   {RESET}  Ver detalle/traceback de un error  (ej: v 42)
    {CYAN}[r ID]   {RESET}  Reconocer error por ID             (ej: r 42)
    {CYAN}[a]      {RESET}  Reconocer TODOS los errores pendientes
    {CYAN}[q]      {RESET}  Salir
"""


def _read_cmd(timeout: float = 5.0) -> str:
    """Lee un comando de stdin con timeout sin bloquear el auto-refresh."""
    if sys.platform == "win32":
        import msvcrt
        buf      = ""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if msvcrt.kbhit():
                ch = msvcrt.getwche()
                if ch in ("\r", "\n"):
                    print()
                    break
                elif ch == "\x08":          # backspace
                    buf = buf[:-1]
                else:
                    buf += ch
            else:
                time.sleep(0.1)
        return buf.strip().lower()
    else:
        import select
        ready, _, _ = select.select([sys.stdin], [], [], timeout)
        return sys.stdin.readline().strip().lower() if ready else ""


def run_monitor() -> None:
    last_refresh = 0.0

    while True:
        now = time.time()

        if now - last_refresh >= POLL_SECONDS:
            show_dashboard()
            print(MENU)
            last_refresh = time.time()

        cmd = _read_cmd(timeout=5.0)

        if not cmd:
            continue

        if cmd == "q":
            print(f"\n{CYAN}Hasta luego.{RESET}\n")
            break

        elif cmd == "h":
            show_history()
            print(MENU)
            last_refresh = time.time()

        elif cmd == "l":
            show_activity_log()
            print(MENU)
            last_refresh = time.time()

        elif cmd.startswith("v ") and cmd[2:].isdigit():
            show_event_detail(int(cmd[2:]))
            input(f"\n  {GRAY}[Enter para volver]{RESET} ")
            show_dashboard()
            print(MENU)
            last_refresh = time.time()

        elif cmd.startswith("r ") and cmd[2:].isdigit():
            eid = int(cmd[2:])
            if ack_event(eid):
                print(f"  {GREEN}✓ Evento #{eid} reconocido.{RESET}")
            else:
                print(f"  {RED}✗ No se encontró el evento #{eid}.{RESET}")
            time.sleep(1.2)
            show_dashboard()
            print(MENU)
            last_refresh = time.time()

        elif cmd == "a":
            if ack_all():
                print(f"  {GREEN}✓ Todos los errores pendientes reconocidos.{RESET}")
            else:
                print(f"  {RED}✗ No se pudo completar la operación.{RESET}")
            time.sleep(1.2)
            show_dashboard()
            print(MENU)
            last_refresh = time.time()

        else:
            # Enter o cualquier otra cosa → refrescar dashboard
            show_dashboard()
            print(MENU)
            last_refresh = time.time()


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    print(f"{BOLD}{CYAN}=== Nexus Monitor ==={RESET}")
    print(f"Backend      : {API_BASE_URL}")
    print(f"Auto-refresh : cada {POLL_SECONDS}s")
    time.sleep(1)
    run_monitor()


if __name__ == "__main__":
    main()