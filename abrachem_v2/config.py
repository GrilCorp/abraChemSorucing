"""
abraChem — Configuración global del pipeline
"""

# ── API Keys ──────────────────────────────────────────────────
import os as _os

# ──────────────────────────────────────────────────────────────
#  CLAVES API — se cargan automáticamente. NO se piden en la web.
#  Prioridad: variable de entorno  →  el valor de acá abajo.
#
#  Para producción/GitHub: definí las variables de entorno
#      export HUNTER_API_KEY="..."
#      export ROCKETREACH_API_KEY="..."
#  (o, en Streamlit, usá st.secrets — ver README).
#
#  Para uso local rápido: pegá tus claves entre las comillas.
# ──────────────────────────────────────────────────────────────
HUNTER_API_KEY      = _os.environ.get("HUNTER_API_KEY",      "c97e3aca77f4579e4f9a232e99aa815bcb8edf06")
ROCKETREACH_API_KEY = _os.environ.get("ROCKETREACH_API_KEY", "1e76c5ekcbd2ad292ea293a6ba0fe09ca0d27989")

# ── Países a procesar (en orden) ─────────────────────────────
PAISES = ["ARG", "CHL", "URY", "BRA", "MEX", "COL", "PER"]

# ── Rubros a incluir ──────────────────────────────────────────
RUBROS = [
    "farmacéutico",
    "veterinario",
    "nutracéutico",
    "cosmética",
    "dermocosmética",
    "fitoterapia",
    "homeopatía",
    "diagnóstico in vitro",
]

# ── Fuentes de datos por país ─────────────────────────────────
FUENTES = {
    "ARG": {
        "nombre": "Argentina",
        "organismo": "ANMAT",
        "url_medicamentos": "https://datos.gob.ar/dataset/salud-actualizaciones-vademecun-nacional-medicamentos-vnm",
        "url_veterinaria": "https://www.senasa.gob.ar/",
        "url_cosmetica": "https://www.anmat.gov.ar/cosmeticos/",
    },
    "CHL": {
        "nombre": "Chile",
        "organismo": "ISP",
        "url_medicamentos": "https://registrosanitario.isp.cl/",
        "url_veterinaria": "https://www.sag.gob.cl/",
    },
    "URY": {
        "nombre": "Uruguay",
        "organismo": "MSP / MGAP",
        "url_medicamentos": "https://www.gub.uy/ministerio-salud-publica/",
        "url_veterinaria": "https://www.mgap.gub.uy/",
    },
    "BRA": {
        "nombre": "Brasil",
        "organismo": "ANVISA",
        "url_medicamentos": "https://consultas.anvisa.gov.br/",
    },
    "MEX": {
        "nombre": "México",
        "organismo": "COFEPRIS",
        "url_medicamentos": "https://www.gob.mx/cofepris/",
    },
    "COL": {
        "nombre": "Colombia",
        "organismo": "INVIMA",
        "url_medicamentos": "https://www.invima.gov.co/",
    },
    "PER": {
        "nombre": "Perú",
        "organismo": "DIGEMID",
        "url_medicamentos": "https://www.digemid.minsa.gob.pe/",
    },
}

# ── Títulos de compras a buscar ───────────────────────────────
TITULOS_COMPRAS = [
    # Español — compras
    "compras", "jefe de compras", "gerente de compras", "director de compras",
    "responsable de compras", "coordinador de compras",
    "jefe de abastecimiento", "gerente de abastecimiento",
    "encargado de compras", "analista de compras",
    "jefe de aprovisionamiento", "gerente de aprovisionamiento",
    "jefe de adquisiciones", "gerente de adquisiciones",
    # Español — supply chain / logística
    "supply chain", "cadena de suministro", "cadena de abastecimiento",
    "jefe de logística", "gerente de logística", "director de logística",
    "jefe de operaciones", "gerente de operaciones",
    "materias primas", "materiales",
    # Inglés — purchasing
    "purchasing", "purchasing manager", "purchasing director",
    "head of purchasing", "vp purchasing", "vp of purchasing",
    "chief procurement officer", "cpo",
    # Inglés — procurement
    "procurement", "procurement manager", "procurement director",
    "head of procurement", "vp procurement",
    # Inglés — supply chain
    "supply chain manager", "supply chain director",
    "head of supply chain", "vp supply chain",
    "supply chain", "sourcing", "sourcing manager",
    # Inglés — otros relacionados
    "buyer", "senior buyer", "strategic buyer",
    "category manager", "materials manager",
    "logistics manager", "operations manager",
]

# ── Parámetros del pipeline ───────────────────────────────────
TOP_N_APIS = 10          # APIs a inferir por laboratorio
MIN_PRODUCTOS = 2        # Mínimo de productos para procesar un lab
DELAY_BETWEEN_REQUESTS = 1.5   # Segundos entre requests (rate limiting)
HUNTER_CREDITS_ALERT = 100     # Avisar cuando queden menos de X créditos
