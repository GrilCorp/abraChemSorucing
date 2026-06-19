"""
PASO 4 — RocketReach → búsqueda por dominio de empresa → persona de compras
PASO 5 — Hunter.io → fallback y construcción/verificación de email

abraChem Pipeline v2

Flujo correcto de RocketReach (como se usa en la UI):
    1. person.search().filter(employer_domain='roemmers.com.ar', current_title='compras')
       → Busca por DOMINIO (no por nombre de empresa) — mucho más preciso
       → NO consume créditos
    2. person.lookup(person_id=id)
       → Obtiene email verificado del candidato encontrado
       → Consume 1 crédito

Flujo completo por laboratorio:
    RocketReach con dominio + título compras → lookup email
    Si no hay email → Hunter Email Finder con patrón
    Si no hay patrón → probar candidatos email uno a uno
    Si nada → Hunter domain_search directo
    Siempre guarda algo
"""

import re
import time
import requests
from dataclasses import dataclass, field
from typing import Optional
from config import HUNTER_API_KEY, TITULOS_COMPRAS, DELAY_BETWEEN_REQUESTS
import logging

log = logging.getLogger(__name__)

HEADERS_BASE = {"User-Agent": "abraChem-Pipeline/2.0"}

JERARQUIA_CARGOS = [
    "chief executive", "ceo", "director general", "presidente",
    "director", "gerente general", "gerente", "jefe",
    "head of", "manager", "coordinador", "supervisor", "analista",
]

DOMINIOS_PERSONALES = {
    "gmail.com", "hotmail.com", "outlook.com", "yahoo.com",
    "yahoo.com.ar", "yahoo.com.mx", "yahoo.com.co",
    "protonmail.com", "icloud.com", "live.com",
}


@dataclass
class ResultadoContacto:
    nombre: str = ""
    apellido: str = ""
    cargo: str = ""
    email: str = ""
    dominio: str = ""
    fuente_nombre: str = ""
    fuente_email: str = ""
    verificado: Optional[bool] = None
    creditos_rocketreach: int = 0
    creditos_hunter: int = 0
    relevancia: str = ""      # etiqueta del tier de cargo (Compras, Supply, etc.)
    intentos: list = field(default_factory=list)
    notas: str = ""


# ═══════════════════════════════════════════════════════════════
# CLASIFICACIÓN DE CARGOS POR RELEVANCIA PARA COMPRAS
# El núcleo de la calidad: solo contactamos a quien decide o influye
# en la compra de materias primas. NUNCA marketing, arte, legal, etc.
# ═══════════════════════════════════════════════════════════════

def _norm_cargo(cargo: str) -> str:
    t = str.maketrans("áéíóúàèìòùäëïöüñç", "aeiouaeiouaeiounc")
    c = (cargo or "").lower().translate(t)
    return f" {re.sub(r'[^a-z0-9 ]', ' ', c)} "

# Áreas que NUNCA compran materias primas → exclusión dura
_EXCLUIR_CARGO = [
    "marketing", "brand", "art director", " arte ", " art ", "design", "diseñ",
    "diseno", "creative", "creativ", "publicidad", "advertising", "growth",
    "social media", "community manager", "content",
    " sales", "venta", "comercial", "business development", "account executive",
    "account manager", "key account", "ejecutiv de cuenta",
    "recursos humanos", "human resource", " hr ", " rrhh", "talent", "people ",
    "people ops", "reclutam", "recruit", "capacitacion", "nomina",
    "legal", "counsel", "abogad", "compliance", "auditor", "audit",
    "information technology", " it ", " ti ", "sistemas", "software", "developer",
    "programad", "helpdesk", "data scien", "data analyst", "infraestructura",
    "ciberseg", "cybersec", "devops", "qa engineer",
    "medical", " medic", "clinic", "scientific", "cientific", "investigacion",
    "research", " r&d", " i+d", "regulator", "asuntos regulatorios",
    "farmacovigilancia", "pharmacovigilance", "medical affairs", "market access",
    "biologics", "patient", "scientific affairs", "preclinic", "clinical",
    "comunicacion", "communication", "prensa", "public relation", " pr ",
    "press", "asuntos publicos", "relaciones institucionales", "sustainab",
    "contabilidad", "accounting", "contador", "tesoreria", "treasury", "fiscal",
    "calidad", "quality", " qa ", " qc ", "assurance", "control de calidad",
    "validacion", "validation",
    "enfermer", "nurse", "recepcion", "secretari", "asistente de direccion",
    "pasant", "intern ", "internship", "trainee", "becari", "estudiante",
    "professor", "profesor", "docente", "consultor", "freelance",
    "engineer", "ingenier de", "mantenimiento", "maintenance", "seguridad e higiene",
    "ehs", "medio ambiente", "safety",
]

# Tiers de relevancia (menor = mejor). Cada uno: lista de señales.
_TIER_COMPRAS = [
    "compras", "purchasing", "procurement", "sourcing", "abastecim",
    "adquisicion", "buyer", "comprador", "purchase", "supply management",
    "strategic sourcing", "category manager", "categoria de compra",
    "materia prima", "materias primas", "raw material",
]
_TIER_SUPPLY = [
    "supply chain", "cadena de suministro", "cadena de abastec", "scm",
    "materials manager", "materiales", "demand planning", "planificacion de",
    "s&op", "inventory", "inventario", "planning manager", "planeacion",
]
_TIER_OPS = [
    "logistic", "operations", "operacion", "production", "produccion",
    "plant manager", "gerente de planta", "director de planta", "manufactur",
    "manufactura", "warehouse", "almacen", "deposito", "distribution",
    "distribucion", "director de operaciones", "director industrial",
    "director tecnico", "director de produccion",
]
_TIER_GERENCIA = [
    "ceo", "chief executive", "general manager", "gerente general",
    "director general", "managing director", "owner", "dueñ", "dueno",
    "propietari", "founder", "co-founder", "cofounder", "fundador",
    "presidente", "president", "country manager", "chief operating",
    " coo ", "chief financial", " cfo ", "director financiero",
    "gerente financiero", "vicepresidente", "vice president", " vp ",
    "socio", "managing partner", "administracion", "gerente administrativo",
    "director administrativo", "chief procurement", " cpo ", "chief supply",
]


def clasificar_cargo(cargo: str, email: str = "") -> tuple:
    """
    Clasifica un cargo por relevancia para compras de materias primas.
    Devuelve (tier, etiqueta):
        1 = Compras / Procurement   (ideal)
        2 = Supply Chain / Materiales
        3 = Operaciones / Producción / Logística
        4 = Gerencia / Dirección general (decide en PyMEs)
        99 = NO relevante → nunca contactar
    Si no hay cargo, intenta inferir del local-part del email corporativo.
    """
    c = _norm_cargo(cargo)

    # Sin cargo → mirar el local-part del email (compras@, ventas@, info@…)
    if not cargo.strip() and email and "@" in email:
        local = email.split("@")[0].lower()
        if any(k in local for k in ("compra", "purchas", "procure", "abastec", "sourcing")):
            return (1, "Compras (email)")
        if any(k in local for k in ("venta", "sales", "marketing", "rrhh", "hr", "legal", "prensa")):
            return (99, "")
        if any(k in local for k in ("info", "contacto", "contact", "ventas", "hola", "admin")):
            return (4, "Email genérico")
        return (99, "")

    # Exclusión dura
    for x in _EXCLUIR_CARGO:
        if x in c:
            return (99, "")

    for x in _TIER_COMPRAS:
        if x in c:
            return (1, "Compras / Procurement")
    for x in _TIER_SUPPLY:
        if x in c:
            return (2, "Supply Chain")
    for x in _TIER_OPS:
        if x in c:
            return (3, "Operaciones / Logística")
    for x in _TIER_GERENCIA:
        if x in c:
            return (4, "Gerencia / Dirección")

    # Cargo no reconocido y no claramente relevante → excluir
    return (99, "")


def _seniority(cargo: str) -> int:
    """Mayor = más senior. Para desempatar dentro de un mismo tier."""
    c = _norm_cargo(cargo)
    niveles = [
        (["chief", "ceo", "presidente", "president", "owner", "dueñ", "dueno",
          "founder", "fundador", "director general", "managing director",
          "general manager", "gerente general"], 100),
        (["vicepresidente", "vice president", " vp "], 92),
        (["director", "head of", "jefatura"], 84),
        (["gerente", "manager"], 74),
        (["jefe", "lead", "supervisor", "encargad", "responsable"], 64),
        (["coordinador", "coordinator"], 52),
        (["analista", "analyst", "specialist", "especialista", "ejecutiv"], 42),
        (["asistente", "assistant", "auxiliar", "aux ", "junior", " jr "], 22),
    ]
    for señales, score in niveles:
        if any(s in c for s in señales):
            return score
    return 35


def _es_cargo_compras(cargo: str) -> bool:
    """Relevante para compras = tier 1, 2 o 3 (no gerencia genérica)."""
    return clasificar_cargo(cargo)[0] <= 3


def _rango_cargo(cargo: str) -> int:
    """Compat: menor = más senior (invertido de _seniority)."""
    return 100 - _seniority(cargo)


def _split_nombre(nombre_completo: str) -> tuple:
    partes = (nombre_completo or "").strip().split()
    if len(partes) >= 2:
        return partes[0], " ".join(partes[1:])
    return (nombre_completo or "").strip(), ""


def _email_del_dominio(email: str, dominio: str) -> bool:
    if not email or "@" not in email or not dominio:
        return False
    return email.split("@")[1].lower().strip() == dominio.lower().strip()


def _empresa_coherente(employer: str, tokens_fuertes_lab: list) -> bool:
    """
    True si el empleador del candidato contiene algún token fuerte del lab.
    Evita aceptar gente de empresas que sólo comparten una palabra genérica.
    """
    if not tokens_fuertes_lab:
        return False
    t = str.maketrans("áéíóúàèìòùäëïöüñç", "aeiouaeiouaeiounc")
    emp = (employer or "").lower().translate(t)
    emp = "".join(ch if ch.isalnum() else " " for ch in emp)
    emp_join = emp.replace(" ", "")
    for tok in tokens_fuertes_lab:
        if tok in emp.split() or tok in emp_join:
            return True
    return False


def _email_es_personal(email: str) -> bool:
    if not email or "@" not in email:
        return True
    return email.split("@")[1].lower() in DOMINIOS_PERSONALES


# ═══════════════════════════════════════════════════════════════
# ROCKETREACH — búsqueda por dominio
# ═══════════════════════════════════════════════════════════════

class RocketReachClient:
    """
    Usa employer_domain para buscar por dominio de empresa.
    Esto es equivalente a lo que se hace en la UI de RocketReach:
    pegar el dominio en el filtro Company y buscar por título.
    """

    def __init__(self, api_key: str):
        self.api_key = api_key
        self._creditos_usados = 0
        self._rr = None
        self._ultimos_logs = []
        self._rate_limited = False   # se activa al primer 429 de la hora
        self._init()

    def _init(self):
        try:
            import rocketreach
            self._rr = rocketreach.Gateway(api_key=self.api_key)
            log.info("RocketReach SDK inicializado OK")
        except ImportError:
            log.error("rocketreach no instalado. Corré: pip install rocketreach")
        except Exception as e:
            log.error(f"Error iniciando RocketReach: {e}")

    def creditos_disponibles(self) -> int:
        """Lookups restantes en la cuenta RocketReach (-1 si no se pudo)."""
        try:
            r = requests.get(
                "https://api.rocketreach.co/api/v2/account",
                headers={"Api-Key": self.api_key}, timeout=10)
            if r.status_code == 200:
                d = r.json()
                return d.get("lookup_credit_balance",
                             d.get("credits_remaining", -1))
        except Exception:
            pass
        return -1

    def _person_to_dict(self, person) -> dict:
        try:
            if hasattr(person, "to_dict"):
                d = person.to_dict()
                if d:
                    return d
        except Exception:
            pass
        return {
            "id":                 getattr(person, "id", None),
            "name":               getattr(person, "name", "") or "",
            "current_title":      getattr(person, "current_title", "") or "",
            "current_employer":   getattr(person, "current_employer", "") or "",
            "current_work_email": getattr(person, "current_work_email", "") or "",
            "emails":             getattr(person, "emails", []) or [],
            "linkedin_url":       getattr(person, "linkedin_url", "") or "",
            "status":             getattr(person, "status", "") or "",
        }

    def _extraer_emails(self, person_dict: dict, dominio: str) -> list:
        """Extrae emails del dict, priorizando los del dominio correcto."""
        emails = []

        we = (person_dict.get("current_work_email") or "").strip()
        if we and "@" in we and not _email_es_personal(we):
            emails.append(we)

        for e in (person_dict.get("emails") or []):
            if isinstance(e, dict):
                addr = (e.get("email") or e.get("smtp_address") or "").strip()
            elif hasattr(e, "email"):
                addr = (getattr(e, "email", "") or "").strip()
            else:
                addr = str(e).strip()
            if addr and "@" in addr and not _email_es_personal(addr):
                emails.append(addr)

        emails = list(dict.fromkeys(emails))
        if dominio:
            emails.sort(key=lambda e: (0 if _email_del_dominio(e, dominio) else 1))
        return emails

    def buscar_por_dominio(self, dominio: str, titulos: list, nombre_lab: str = "") -> list:
        """
        Busca TODOS los empleados del dominio, sin filtro de título.
        Luego filtra manualmente por cargo de compras.
        Incluye fallback directo via HTTP si el SDK falla.
        """
        if not dominio:
            return []

        todos = []
        logs_internos = []

        # Variantes del nombre para búsqueda — RocketReach usa current_employer.
        # Orden: nombre limpio (mejor match), palabra distintiva, nombre completo.
        nombre_limpio = self._limpiar_nombre_empresa(nombre_lab)
        try:
            from paso2_3_apis_dominio import _palabras_distintivas, tokens_fuertes
            fuertes = tokens_fuertes(nombre_lab)
            distintiva = max(_palabras_distintivas(nombre_lab), key=len, default="").title()
        except Exception:
            fuertes, distintiva = [], ""

        # Si el nombre NO tiene tokens fuertes (ej: "Laboratorios Life"),
        # buscar por nombre en RocketReach trae basura ("Village Life Homes").
        # Mejor saltear RR y dejar que Hunter trabaje el dominio correcto.
        if not fuertes:
            self._ultimos_logs = [
                f"RocketReach: '{nombre_lab}' es nombre genérico — "
                f"se omite búsqueda por nombre, Hunter trabajará {dominio}"]
            return []

        self._tokens_fuertes = fuertes  # para filtrar coherencia de empleador
        variantes = [v for v in dict.fromkeys(
            [nombre_limpio, distintiva, nombre_lab]) if v and len(v) >= 3]

        # ── Búsqueda eficiente y consciente del rate limit ───
        # Máximo 2 llamadas por laboratorio: 1 amplia por empresa y, si
        # vino vacía, 1 dirigida a "purchasing". Al primer 429 se corta
        # RocketReach para toda la sesión (el límite es por hora).
        empresa_ok = variantes[0] if variantes else nombre_lab

        gente, status = self._rr_search(empresa_ok, None, 25, logs_internos)
        if status == "ok":
            todos = gente
        if status == "ok" and not todos:
            gente2, status2 = self._rr_search(empresa_ok, "purchasing", 10, logs_internos)
            if status2 == "ok":
                todos = gente2

        self._ultimos_logs = logs_internos
        for msg in logs_internos:
            log.info(f"  {msg}")

        if not todos:
            return []

        # Filtrar por coherencia de EMPLEADOR: la persona debe trabajar
        # realmente en el laboratorio buscado (rechaza "Village Life Homes"
        # cuando se buscó un token que aparece en otras empresas).
        fuertes = getattr(self, "_tokens_fuertes", [])
        if fuertes:
            antes = len(todos)
            todos = [p for p in todos
                     if _empresa_coherente(p.get("current_employer", ""), fuertes)]
            if antes != len(todos):
                logs_internos.append(
                    f"RocketReach: {antes-len(todos)} descartados por empresa no coincidente")

        # Clasificar a cada empleado y DESCARTAR los irrelevantes (tier 99:
        # marketing, arte, legal, ventas, IT, calidad, etc.). Mejor ningún
        # contacto que un Art Director.
        relevantes = []
        descartados = 0
        for p in todos:
            tier, etiqueta = clasificar_cargo(p.get("current_title", ""))
            if tier >= 99:
                descartados += 1
                continue
            p["_tier"] = tier
            p["_tier_label"] = etiqueta
            p["_seniority"] = _seniority(p.get("current_title", ""))
            relevantes.append(p)

        # Orden: primero menor tier (compras), dentro del tier mayor seniority
        relevantes.sort(key=lambda p: (p["_tier"], -p["_seniority"]))

        n_compras = sum(1 for p in relevantes if p["_tier"] == 1)
        logs_internos.append(
            f"RocketReach: {len(relevantes)} relevantes "
            f"({n_compras} de compras), {descartados} descartados por cargo")
        return relevantes

    def _es_throttle(self, status, msg) -> bool:
        return str(status) == "429" or "throttl" in str(msg).lower() \
            or "rate limit" in str(msg).lower()

    def _rr_search(self, empresa: str, titulo, size: int, logs: list):
        """
        Una búsqueda a RocketReach (SDK, y HTTP si el SDK falla).
        Devuelve (lista_personas, status) con status: 'ok' | '429' | 'err'.
        Si detecta 429, activa self._rate_limited y NO reintenta.
        """
        if self._rate_limited:
            return [], "429"

        etiqueta = f"'{empresa}'" + (f" + '{titulo}'" if titulo else "")

        # — SDK —
        if self._rr:
            try:
                logs.append(f"RocketReach: buscando {etiqueta}...")
                q = self._rr.person.search()
                q = q.filter(current_employer=empresa, current_title=titulo) if titulo \
                    else q.filter(current_employer=empresa)
                result = q.params(start=1, size=size).execute()
                if getattr(result, "is_success", False):
                    ppl = [self._person_to_dict(p) for p in (getattr(result, "people", []) or [])]
                    logs.append(f"RocketReach: {len(ppl)} resultados")
                    return ppl, "ok"
                msg = getattr(result, "message", "") or getattr(result, "errors", "")
                status = getattr(getattr(result, "response", None), "status_code", "?")
                if self._es_throttle(status, msg):
                    self._rate_limited = True
                    logs.append("RocketReach: límite por hora alcanzado (429) — "
                                "se omite RocketReach el resto de la sesión, sigue Hunter")
                    return [], "429"
                logs.append(f"RocketReach SDK falló: HTTP {status} — {msg}")
            except Exception as e:
                if self._es_throttle("", e):
                    self._rate_limited = True
                    logs.append("RocketReach: límite por hora (429) — sigue Hunter")
                    return [], "429"
                logs.append(f"RocketReach SDK excepción: {type(e).__name__}: {e}")

        # — HTTP fallback (solo si el SDK no resolvió) —
        ppl, hlog, status = self._http_search(empresa, titulo, size)
        logs.extend(hlog)
        return ppl, status

    def _http_search(self, empresa: str, titulo, size: int):
        """HTTP directo. Devuelve (personas, logs, status)."""
        logs = []
        if self._rate_limited:
            return [], logs, "429"
        try:
            url = "https://api.rocketreach.co/api/v2/search"
            headers = {"Api-Key": self.api_key, "Content-Type": "application/json"}
            query = {"current_employer": [empresa]}
            if titulo:
                query["current_title"] = [titulo]
            r = requests.post(url, json={"query": query, "start": 1, "page_size": size},
                              headers=headers, timeout=15)
            if r.status_code == 429:
                self._rate_limited = True
                logs.append("RocketReach HTTP: límite por hora (429) — sigue Hunter")
                return [], logs, "429"
            if r.status_code != 200:
                logs.append(f"RocketReach HTTP: error {r.status_code} — {r.text[:120]}")
                return [], logs, "err"
            data = r.json()
            profs = data.get("profiles", data.get("people", data.get("results", [])))
            return [self._profile_raw_to_dict(p) for p in (profs or [])], logs, "ok"
        except Exception as e:
            logs.append(f"RocketReach HTTP excepción: {e}")
            return [], logs, "err"

    def _limpiar_nombre_empresa(self, nombre: str) -> str:
        """Simplifica el nombre del lab para mejor matching en RocketReach."""
        n = nombre.lower()
        for quitar in ["laboratorio ", "laboratorios ", "lab. ", "lab ",
                       "s.a.i.c.", "s.a.", "s.r.l.", "s.a.c.", "e.i.r.l.",
                       "ltda.", "s.p.a.", "cia.", "química "]:
            n = n.replace(quitar, "")
        import re as _re
        n = _re.sub(r"[^a-z0-9\s]", "", n).strip().title()
        return n

    def _profile_raw_to_dict(self, p: dict) -> dict:
        """Convierte un perfil raw de la API HTTP a dict estándar."""
        return {
            "id":                 p.get("id"),
            "name":               p.get("name", ""),
            "current_title":      p.get("current_title", ""),
            "current_employer":   p.get("current_employer", ""),
            "current_work_email": p.get("current_work_email", ""),
            "emails":             p.get("emails", []),
            "linkedin_url":       p.get("linkedin_url", ""),
            "status":             p.get("status", ""),
        }

    def lookup_email(self, person_dict: dict, dominio: str) -> list:
        """
        Obtiene emails verificados de un perfil. Consume 1 crédito.
        """
        if not self._rr or self._rate_limited:
            return []
        try:
            pid    = person_dict.get("id")
            nombre = person_dict.get("name", "")
            empresa = person_dict.get("current_employer", "")

            log.info(f"  RocketReach lookup: {nombre} (1 crédito)")

            if pid:
                result = self._rr.person.lookup(person_id=pid)
            else:
                result = self._rr.person.lookup(
                    name=nombre, current_employer=empresa
                )

            self._creditos_usados += 1

            if not result.is_success:
                msg = getattr(result, "message", "")
                if self._es_throttle(getattr(getattr(result,"response",None),"status_code","?"), msg):
                    self._rate_limited = True
                return []

            persona = getattr(result, "person", None)
            if not persona:
                return []

            return self._extraer_emails(self._person_to_dict(persona), dominio)

        except Exception as e:
            log.debug(f"  RocketReach lookup error: {e}")
            return []


# ═══════════════════════════════════════════════════════════════
# HUNTER API
# ═══════════════════════════════════════════════════════════════

class HunterAPI:
    BASE = "https://api.hunter.io/v2"

    def __init__(self, api_key: str = None):
        self.api_key = api_key or HUNTER_API_KEY
        self._creditos_usados = 0

    def _get(self, endpoint: str, params: dict) -> dict:
        params = {**params, "api_key": self.api_key}
        try:
            r = requests.get(
                f"{self.BASE}/{endpoint}", params=params,
                headers=HEADERS_BASE, timeout=15
            )
            if r.status_code == 429:
                log.warning("Hunter: rate limit. Esperando 60s...")
                time.sleep(60)
                return {}
            if r.status_code == 401:
                log.error("Hunter: API key inválida.")
                return {}
            r.raise_for_status()
            self._creditos_usados += 1
            return r.json().get("data", {})
        except Exception as e:
            log.error(f"Hunter /{endpoint}: {e}")
            return {}

    def creditos_disponibles(self) -> int:
        try:
            r = requests.get(
                f"{self.BASE}/account",
                params={"api_key": self.api_key}, timeout=10
            )
            data = r.json().get("data", {})
            return data.get("requests", {}).get("searches", {}).get("available", -1)
        except Exception:
            return -1

    def domain_search(self, dominio: str) -> dict:
        data = self._get("domain-search", {"domain": dominio, "limit": 100})
        return {
            "patron": data.get("pattern") if data else None,
            "emails": data.get("emails", []) if data else [],
        }

    def email_finder(self, nombre: str, apellido: str, dominio: str) -> str:
        data = self._get("email-finder", {
            "domain": dominio,
            "first_name": nombre,
            "last_name": apellido,
        })
        if not data:
            return ""
        email = data.get("email", "")
        conf  = data.get("score", 0)
        return email if (email and conf >= 40) else ""

    def email_verifier(self, email: str) -> str:
        data = self._get("email-verifier", {"email": email})
        return data.get("status", "unknown") if data else "unknown"

    def _aplicar_patron(self, nombre: str, apellido: str, patron: str) -> str:
        def limpiar(s):
            t = str.maketrans("áéíóúàèìòùäëïöüñÁÉÍÓÚÑ", "aeiouaeiouaeiounAEIOUN")
            return re.sub(r"[^a-z0-9]", "", (s or "").lower().strip().translate(t))
        n, a = limpiar(nombre), limpiar(apellido)
        if not n or not a:
            return n or a
        return {
            "first.last": f"{n}.{a}", "last.first": f"{a}.{n}",
            "firstlast":  f"{n}{a}",  "lastfirst":  f"{a}{n}",
            "flast":      f"{n[0]}{a}", "lfirst":   f"{a[0]}{n}",
            "f.last":     f"{n[0]}.{a}", "l.first": f"{a[0]}.{n}",
            "first": n,   "last": a,    "first_last": f"{n}_{a}",
            "nombre.apellido": f"{n}.{a}", "n.apellido": f"{n[0]}.{a}",
        }.get(patron, f"{n}.{a}")

    def construir_candidatos(self, nombre: str, apellido: str,
                             dominio: str, patron: str) -> list:
        def limpiar(s):
            t = str.maketrans("áéíóúàèìòùäëïöüñÁÉÍÓÚÑ", "aeiouaeiouaeiounAEIOUN")
            return re.sub(r"[^a-z0-9]", "", (s or "").lower().strip().translate(t))
        n, a = limpiar(nombre), limpiar(apellido)
        if not n or not a or not dominio:
            return []
        candidatos = []
        if patron:
            local = self._aplicar_patron(nombre, apellido, patron)
            if local:
                candidatos.append(f"{local}@{dominio}")
        for local in [
            f"{n}.{a}", f"{n[0]}.{a}", f"{n}{a}",
            f"{a}.{n}", f"{a[0]}.{n}", f"{n}_{a}",
            f"{n[0]}{a}", f"{n}", f"{a}",
        ]:
            email = f"{local}@{dominio}"
            if email not in candidatos:
                candidatos.append(email)
        return candidatos

    def emails_del_dominio_por_cargo(self, emails_list: list) -> tuple:
        """
        Clasifica los emails del dominio y devuelve (compras, fallback).
        EXCLUYE cargos irrelevantes (tier 99: marketing, arte, legal, ventas…).
        'compras' = mejor de tier 1; 'fallback' = mejor de tiers 2-4.
        Cada uno trae su etiqueta en e['_tier_label'].
        """
        clasificados = []
        for e in emails_list:
            addr = e.get("value", "")
            if not addr:
                continue
            cargo = e.get("position") or ""
            tier, etiqueta = clasificar_cargo(cargo, email=addr)
            if tier >= 99:
                continue
            e = dict(e)
            e["_tier"] = tier
            e["_tier_label"] = etiqueta
            e["_seniority"] = _seniority(cargo)
            clasificados.append(e)

        if not clasificados:
            return None, None

        clasificados.sort(key=lambda e: (e["_tier"], -e["_seniority"]))
        compras  = next((e for e in clasificados if e["_tier"] == 1), None)
        fallback = next((e for e in clasificados if e["_tier"] != 1), None)
        return compras, fallback


# ═══════════════════════════════════════════════════════════════
# FUNCIÓN PRINCIPAL
# ═══════════════════════════════════════════════════════════════

def obtener_contacto(
    nombre_lab: str,
    pais: str,
    dominio: str,
    hunter: HunterAPI,
    rocketreach: RocketReachClient,
) -> ResultadoContacto:
    """
    Busca contacto de compras usando dominio en RocketReach, luego Hunter.

    Orden de intentos:
    1. RocketReach: search por dominio + título compras → lookup email
    2. Hunter Email Finder con nombre RocketReach + patrón del dominio
    3. Candidatos email construidos + verificación uno a uno
    4. Hunter domain_search → email de compras directo
    5. Hunter domain_search → cargo más alto disponible
    """
    resultado = ResultadoContacto(dominio=dominio)
    intentos  = []

    def log_intento(paso: str, detalle: str, ok: bool):
        icono = "✅" if ok else ("❌" if ok is False else "ℹ️")
        msg = f"{icono} [{paso}] {detalle}"
        intentos.append(msg)
        log.info(f"  {msg}")

    # ── Hunter domain_search siempre al inicio (1 crédito, da patrón + emails) ──
    log.info(f"  Hunter: domain_search '{dominio}'...")
    hd            = hunter.domain_search(dominio) if dominio else {"patron": None, "emails": []}
    patron        = hd.get("patron")
    emails_hunter = hd.get("emails", [])
    email_compras_h, email_fallback_h = hunter.emails_del_dominio_por_cargo(emails_hunter)
    log.info(f"  Hunter: patrón={patron}, {len(emails_hunter)} emails en dominio")

    # Variables para guardar nombre encontrado
    nombre_enc = apellido_enc = cargo_enc = rel_enc = ""

    # ────────────────────────────────────────────────────────
    # INTENTO 1 — RocketReach por dominio + título compras
    # ────────────────────────────────────────────────────────
    if rocketreach._rr and dominio:
        titulos_compras = [
            "compras", "purchasing", "procurement",
            "abastecimiento", "sourcing", "supply chain",
            "director", "gerente", "jefe",
        ]
        candidatos_rr = rocketreach.buscar_por_dominio(dominio, titulos_compras, nombre_lab)
        resultado.creditos_rocketreach = rocketreach._creditos_usados

        # Mostrar logs internos de RocketReach en la UI
        for msg in getattr(rocketreach, "_ultimos_logs", []):
            intentos.append(f"ℹ️ {msg}")

        if candidatos_rr:
            log_intento("RocketReach",
                f"{len(candidatos_rr)} perfiles encontrados en {dominio}", True)
        else:
            log_intento("RocketReach",
                f"Sin resultados para dominio {dominio}", False)

        for candidato in candidatos_rr[:3]:
            nombre_c = candidato.get("name", "")
            cargo_c  = candidato.get("current_title", "")
            tier_lbl = candidato.get("_tier_label", "")
            log_intento("RocketReach",
                f"Candidato: {nombre_c} ({cargo_c}) → {tier_lbl}", True)

            emails_rr = rocketreach.lookup_email(candidato, dominio)
            resultado.creditos_rocketreach = rocketreach._creditos_usados

            # Prioridad: email del dominio exacto > cualquier email corporativo
            # real de RocketReach (multinacionales usan el dominio del grupo,
            # ej: Genfar → @sanofi.com — es un email REAL, no descartarlo)
            emails_ok = [e for e in emails_rr if _email_del_dominio(e, dominio)]
            email_elegido, nota_email = "", ""
            if emails_ok:
                email_elegido = emails_ok[0]
            elif emails_rr:
                email_elegido = emails_rr[0]
                nota_email = f"Email corporativo real de RocketReach (dominio del grupo, esperado @{dominio})"

            if email_elegido:
                log_intento("RocketReach lookup",
                    f"Email verificado: {email_elegido}", True)
                n, a = _split_nombre(nombre_c)
                resultado.nombre        = n
                resultado.apellido      = a
                resultado.cargo         = cargo_c
                resultado.email         = email_elegido
                resultado.fuente_nombre = "rocketreach"
                resultado.fuente_email  = "rocketreach_directo"
                resultado.verificado    = True
                resultado.notas         = nota_email
                resultado.relevancia    = candidato.get("_tier_label", "")
                resultado.intentos      = intentos
                return resultado
            else:
                log_intento("RocketReach lookup", f"Sin email para {nombre_c}", False)

            # Guardar nombre para usar con Hunter aunque no haya email
            if not nombre_enc:
                nombre_enc, apellido_enc = _split_nombre(nombre_c)
                cargo_enc = cargo_c
                rel_enc = candidato.get("_tier_label", "")

    # ────────────────────────────────────────────────────────
    # INTENTO 2 — Hunter Email Finder con nombre de RocketReach
    # ────────────────────────────────────────────────────────
    if nombre_enc and apellido_enc and dominio:
        log.info(f"  Hunter Email Finder: {nombre_enc} {apellido_enc} @{dominio}")
        ef = hunter.email_finder(nombre_enc, apellido_enc, dominio)

        if ef and _email_del_dominio(ef, dominio):
            log_intento("Hunter Email Finder", f"Encontrado: {ef}", True)
            status = hunter.email_verifier(ef)

            if status in ("valid", "accept_all"):
                resultado.nombre        = nombre_enc
                resultado.apellido      = apellido_enc
                resultado.cargo         = cargo_enc
                resultado.email         = ef
                resultado.fuente_nombre = "rocketreach"
                resultado.fuente_email  = "hunter_email_finder"
                resultado.verificado    = True if status == "valid" else None
                resultado.relevancia    = rel_enc
                resultado.intentos      = intentos
                log_intento("Verificación", f"{ef} → {status}", True)
                return resultado
            elif status == "invalid":
                log_intento("Verificación", f"{ef} → inválido", False)
            else:
                log_intento("Verificación", f"{ef} → {status}", None)
                resultado.nombre        = nombre_enc
                resultado.apellido      = apellido_enc
                resultado.cargo         = cargo_enc
                resultado.email         = ef
                resultado.fuente_nombre = "rocketreach"
                resultado.fuente_email  = "hunter_email_finder"
                resultado.verificado    = None
                resultado.relevancia    = rel_enc
                resultado.intentos      = intentos
                return resultado
        else:
            log_intento("Hunter Email Finder",
                f"Sin resultado válido para {nombre_enc} @{dominio}", False)

    # ────────────────────────────────────────────────────────
    # INTENTO 3 — Candidatos email construidos, verificar uno a uno
    # ────────────────────────────────────────────────────────
    if nombre_enc and apellido_enc and dominio:
        candidatos_email = hunter.construir_candidatos(
            nombre_enc, apellido_enc, dominio, patron
        )
        log.info(f"  Probando {len(candidatos_email)} candidatos de email...")

        for email_c in candidatos_email[:6]:
            status = hunter.email_verifier(email_c)

            if status == "valid":
                log_intento("Candidatos email", f"{email_c} → EXISTE ✅", True)
                resultado.nombre        = nombre_enc
                resultado.apellido      = apellido_enc
                resultado.cargo         = cargo_enc
                resultado.email         = email_c
                resultado.fuente_nombre = "rocketreach"
                resultado.fuente_email  = "candidato_verificado"
                resultado.verificado    = True
                resultado.relevancia    = rel_enc
                resultado.intentos      = intentos
                return resultado
            elif status == "accept_all":
                log_intento("Candidatos email", f"{email_c} → catch-all", None)
                resultado.nombre        = nombre_enc
                resultado.apellido      = apellido_enc
                resultado.cargo         = cargo_enc
                resultado.email         = email_c
                resultado.fuente_nombre = "rocketreach"
                resultado.fuente_email  = "candidato_catchall"
                resultado.verificado    = None
                resultado.relevancia    = rel_enc
                resultado.intentos      = intentos
                return resultado
            else:
                log_intento("Candidatos email", f"{email_c} → {status}", False)

    # ────────────────────────────────────────────────────────
    # INTENTO 4 — Hunter domain_search → email compras directo
    # ────────────────────────────────────────────────────────
    if email_compras_h:
        addr  = email_compras_h.get("value", "")
        cargo = email_compras_h.get("position", "")
        nom   = email_compras_h.get("first_name", "")
        ape   = email_compras_h.get("last_name", "")
        log_intento("Hunter domain_search",
            f"Email de compras: {addr} ({cargo}) → {email_compras_h.get('_tier_label','')}", True)
        resultado.nombre        = nom
        resultado.apellido      = ape
        resultado.cargo         = cargo
        resultado.email         = addr
        resultado.fuente_nombre = "hunter"
        resultado.fuente_email  = "hunter_domain_compras"
        resultado.verificado    = None
        resultado.relevancia    = email_compras_h.get("_tier_label", "")
        resultado.intentos      = intentos
        return resultado

    # ────────────────────────────────────────────────────────
    # INTENTO 5 — Hunter domain_search → cargo más alto
    # ────────────────────────────────────────────────────────
    if email_fallback_h:
        addr  = email_fallback_h.get("value", "")
        cargo = email_fallback_h.get("position", "")
        nom   = email_fallback_h.get("first_name", "")
        ape   = email_fallback_h.get("last_name", "")
        log_intento("Hunter domain_search fallback",
            f"Cargo relevante: {addr} ({cargo}) → {email_fallback_h.get('_tier_label','')}", True)
        resultado.nombre        = nom
        resultado.apellido      = ape
        resultado.cargo         = cargo
        resultado.email         = addr
        resultado.fuente_nombre = "hunter"
        resultado.fuente_email  = "hunter_domain_fallback"
        resultado.verificado    = None
        resultado.relevancia    = email_fallback_h.get("_tier_label", "")
        resultado.intentos      = intentos
        return resultado

    # (Eliminado el "último recurso" que construía emails sin verificar:
    #  un email inventado vale menos que activar el laboratorio de reserva.)

    # Sin resultado
    log_intento("Sin resultado",
        "Agotados todos los intentos", False)
    resultado.fuente_nombre = "no_encontrado"
    resultado.fuente_email  = "no_encontrado"
    resultado.intentos      = intentos
    resultado.notas         = "Sin resultado tras todos los intentos"
    return resultado
