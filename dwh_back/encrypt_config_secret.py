"""
Utility to encrypt or decrypt config secrets for dwh_config.

Usage examples:
  python encrypt_config_secret.py                    # cifrar: modo interactivo (entrada visible, pegado fiable en Windows)
  python encrypt_config_secret.py "my-password"    # cifrar: una sola vez
  python encrypt_config_secret.py --hide-input       # cifrar interactivo ocultando teclas (getpass; pegado puede fallar)
  python encrypt_config_secret.py -d "ENC:..."       # descifrar una cadena (con o sin prefijo ENC:)
  python encrypt_config_secret.py --decrypt          # descifrar: modo interactivo

Output format (cifrado):
  ENC:<fernet-token>

The backend (`main.py` / `main_postgres.py`) can decrypt values stored in the
config DB with this format if `config_secret_key` is configured.
"""

import argparse
import configparser
import os
import sys
from getpass import getpass

from cryptography.fernet import Fernet, InvalidToken


def _config_ini_candidates() -> list:
    """
    PyInstaller onefile: __file__ apunta al _MEI temporal, no junto al .exe.
    Hay que usar el directorio de sys.executable.
    """
    candidates = []
    if getattr(sys, "frozen", False):
        candidates.append(os.path.join(os.path.dirname(sys.executable), "config.ini"))
    candidates.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.ini"))
    candidates.append(os.path.join(os.getcwd(), "config.ini"))
    # Sin duplicados, conservando orden
    seen = set()
    out = []
    for p in candidates:
        ap = os.path.abspath(p)
        if ap not in seen:
            seen.add(ap)
            out.append(ap)
    return out


def load_secret_key() -> str:
    key = os.environ.get("NEXUS_CONFIG_SECRET_KEY", "").strip()
    if key:
        return key

    key = ""
    for ini_path in _config_ini_candidates():
        if not os.path.isfile(ini_path):
            continue
        cfg = configparser.ConfigParser()
        cfg.read(ini_path, encoding="utf-8-sig")
        if cfg.has_section("security"):
            key = cfg.get("security", "config_secret_key", fallback="").strip()
        if key:
            break

    if not key:
        print(
            "Falta [security] config_secret_key en config.ini o la variable "
            "de entorno NEXUS_CONFIG_SECRET_KEY.",
            file=sys.stderr,
        )
        print("Buscado en:", file=sys.stderr)
        for p in _config_ini_candidates():
            exists = "sí" if os.path.isfile(p) else "no"
            print(f"  [{exists}] {p}", file=sys.stderr)
        sys.exit(1)
    return key


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Cifra o descifra secretos de configuración (Fernet, mismo formato que el backend)."
    )
    parser.add_argument(
        "value",
        nargs="?",
        help="Texto en claro a cifrar, o token/cadena ENC:... a descifrar si usas --decrypt. "
        "Si se omite, entra en bucle interactivo.",
    )
    parser.add_argument(
        "-d",
        "--decrypt",
        action="store_true",
        help="Descifrar en lugar de cifrar (acepta ENC:<token> o solo el token Fernet).",
    )
    parser.add_argument(
        "--hide-input",
        action="store_true",
        help="Cifrado interactivo: oculta lo escrito (getpass). En Windows el pegado largo puede fallar.",
    )
    parser.add_argument(
        "--show-input",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    return parser


def _pause_if_frozen() -> None:
    if getattr(sys, "frozen", False):
        try:
            input("\nPulsa Enter para cerrar...")
        except EOFError:
            pass


def _should_exit_loop(line: str) -> bool:
    s = line.strip()
    if not s:
        return True
    return s.lower() in ("exit", "quit", "salir", "q")


def strip_enc_prefix(s: str) -> str:
    """Quita el prefijo ENC: si existe (mismo criterio que main_postgres / main)."""
    s = s.strip()
    if s.startswith("ENC:"):
        return s[4:].strip()
    return s


def normalize_pasted_line(s: str) -> str:
    """
    Limpia pegados desde consola/Excel/navegador: CR, BOM, espacios raros.
    getpass() en Windows a veces corrompe pegados largos; input() + esto suele bastar.
    """
    s = s.replace("\r", "").replace("\ufeff", "").strip()
    s = s.replace("\u00a0", " ").strip()
    return s


def encrypt_and_print(cipher: Fernet, plain: str) -> None:
    encrypted = cipher.encrypt(plain.encode("utf-8")).decode("utf-8")
    print("Valor cifrado:")
    print(f"ENC:{encrypted}")
    print()


def decrypt_and_print(cipher: Fernet, encrypted: str) -> None:
    token = strip_enc_prefix(normalize_pasted_line(encrypted))
    if not token:
        print("No hay token para descifrar.", file=sys.stderr)
        sys.exit(1)
    try:
        plain = cipher.decrypt(token.encode("utf-8")).decode("utf-8")
    except InvalidToken:
        print(
            "No se pudo descifrar: token inválido o clave distinta a la usada al cifrar.",
            file=sys.stderr,
        )
        sys.exit(1)
    print("Texto en claro:")
    print(plain)
    print()


def run_interactive_loop(cipher: Fernet, hide_input: bool) -> None:
    print(
        "Modo interactivo (cifrado): escribe o pega el texto en cada línea.\n"
        "La entrada se muestra por defecto para que el pegado en Windows sea fiable "
        "(getpass oculta y suele fallar con textos largos).\n"
        "Salir: Enter vacío, o salir / exit / quit / q.\n"
    )
    while True:
        prompt = "Texto a cifrar (Enter para salir): "
        line = getpass(prompt) if hide_input else input(prompt)
        if _should_exit_loop(line):
            print("Hasta luego.")
            break
        plain = normalize_pasted_line(line)
        if not plain:
            continue
        encrypt_and_print(cipher, plain)


def run_decrypt_interactive_loop(cipher: Fernet) -> None:
    print(
        "Modo interactivo (descifrado): pega el token en una línea (con o sin ENC:).\n"
        "Se muestra lo escrito para que el pegado en Windows sea fiable "
        "(getpass oculta la entrada y suele fallar con tokens largos).\n"
        "Salir: Enter vacío, o salir / exit / quit / q.\n"
    )
    while True:
        line = input("Cadena cifrada a descifrar (Enter para salir): ")
        if _should_exit_loop(line):
            print("Hasta luego.")
            break
        enc = normalize_pasted_line(line)
        if not enc:
            continue
        decrypt_and_print(cipher, enc)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    secret_key = load_secret_key()
    cipher = Fernet(secret_key.encode("utf-8"))

    if args.decrypt:
        if args.value is not None:
            enc_value = args.value.strip()
            if not enc_value:
                print("No se recibió ningún valor para descifrar.", file=sys.stderr)
                sys.exit(1)
            decrypt_and_print(cipher, enc_value)
            _pause_if_frozen()
            return
        run_decrypt_interactive_loop(cipher)
        _pause_if_frozen()
        return

    if args.value is not None:
        plain_value = normalize_pasted_line(args.value)
        if not plain_value:
            print("No se recibió ningún valor para cifrar.", file=sys.stderr)
            sys.exit(1)
        encrypt_and_print(cipher, plain_value)
        _pause_if_frozen()
        return

    if args.show_input and not args.hide_input:
        print(
            "Nota: --show-input está obsoleto (la entrada ya es visible por defecto).\n",
            file=sys.stderr,
        )
    run_interactive_loop(cipher, hide_input=bool(args.hide_input))
    _pause_if_frozen()


if __name__ == "__main__":
    main()
