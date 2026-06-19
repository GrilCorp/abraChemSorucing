"""
abraChem · Streamlit — Almacenamiento persistente (SQLite, sin Flask).
Guarda los prospectos para siempre en data/abrachem_st.db.
"""
import sqlite3
import unicodedata
from pathlib import Path
from datetime import datetime

DB_PATH = Path(__file__).resolve().parent / "data" / "abrachem_st.db"

CAMPOS = ["pais", "laboratorio", "rubro", "nombre", "apellido", "cargo",
          "email", "email_verificado", "fuente_email", "dominio",
          "apis_clave", "top_apis", "relevancia", "confianza",
          "mensaje", "notas", "creado"]


def _conn():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c


def init_db():
    with _conn() as c:
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS resultados (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                {", ".join(f'{x} TEXT' for x in CAMPOS)}
            )
        """)


def norm_lab(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s or "")).encode("ascii", "ignore").decode()
    return "".join(ch for ch in s.lower() if ch.isalnum())


def add_resultado(d: dict):
    d = {**d, "creado": datetime.now().strftime("%d/%m/%Y %H:%M")}
    cols = [k for k in CAMPOS if k in d]
    with _conn() as c:
        c.execute(
            f"INSERT INTO resultados ({', '.join(cols)}) "
            f"VALUES ({', '.join('?' for _ in cols)})",
            [d.get(k, "") for k in cols],
        )


def get_all() -> list:
    with _conn() as c:
        rows = c.execute("SELECT * FROM resultados ORDER BY id DESC").fetchall()
    return [dict(r) for r in rows]


def found_emails() -> set:
    with _conn() as c:
        rows = c.execute("SELECT email FROM resultados").fetchall()
    return {(r["email"] or "").lower().strip() for r in rows if r["email"]}


def found_labs() -> set:
    with _conn() as c:
        rows = c.execute("SELECT laboratorio FROM resultados").fetchall()
    return {norm_lab(r["laboratorio"]) for r in rows}


def delete_all():
    with _conn() as c:
        c.execute("DELETE FROM resultados")
