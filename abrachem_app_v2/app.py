"""
abraChem Prospector v2
Corré con: python3 app.py  (desde cualquier directorio)
Navegador:  http://localhost:5000
"""

import json, csv, io, sys, os, re, threading, time, logging, traceback
from datetime import datetime
from pathlib import Path

# ── Encontrar config.py y los módulos del pipeline ───────────
# Funcionan tanto si están en ESTA carpeta como en la carpeta
# hermana 'abrachem_v2'. Así no importa cómo estén ubicadas.
_here = Path(__file__).resolve().parent
for _cand in (_here, _here.parent / "abrachem_v2", _here.parent,
              _here / "abrachem_v2"):
    if _cand.exists() and str(_cand) not in sys.path:
        sys.path.insert(0, str(_cand))

from flask import Flask, render_template, request, jsonify, Response
from models import db, Config, Sesion, LabQueue, Resultado, LogEntry
try:
    from config import HUNTER_API_KEY, ROCKETREACH_API_KEY
except ModuleNotFoundError:
    # Si no aparece config.py, usar variables de entorno (o vacío)
    HUNTER_API_KEY      = os.environ.get("HUNTER_API_KEY", "")
    ROCKETREACH_API_KEY = os.environ.get("ROCKETREACH_API_KEY", "")

# ── Rutas absolutas basadas en __file__ (funciona desde cualquier CWD) ──
BASE_DIR     = Path(__file__).resolve().parent
DATA_DIR     = BASE_DIR / "data"
TEMPLATE_DIR = BASE_DIR / "templates"
PIPELINE_DIR = BASE_DIR.parent / "abrachem_v2"

DATA_DIR.mkdir(exist_ok=True)

# Agregar pipeline al path de Python
sys.path.insert(0, str(PIPELINE_DIR))

# ── Flask con rutas explícitas ────────────────────────────────
app = Flask(
    __name__,
    template_folder=str(TEMPLATE_DIR),
    static_folder=str(BASE_DIR / "static"),
)
app.config["SQLALCHEMY_DATABASE_URI"] = f"sqlite:///{DATA_DIR / 'abrachem.db'}"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db.init_app(app)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# ── Estado en memoria del pipeline activo ─────────────────────
_pipeline = {
    "running":   False,
    "sesion_id": None,
    "thread":    None,
    "rondas":    {},   # sesion_id → nº de reposiciones web hechas
}

# ── Init DB ───────────────────────────────────────────────────
with app.app_context():
    db.create_all()
    if not Config.query.get(1):
        db.session.add(Config(id=1))
        db.session.commit()


# ═══════════════════════════════════════════════════════════════
# RUTAS — Vistas
# ═══════════════════════════════════════════════════════════════

@app.route("/")
def index():
    return render_template("index.html")


# ═══════════════════════════════════════════════════════════════
# RUTAS — Config
# ═══════════════════════════════════════════════════════════════

@app.route("/api/config", methods=["GET"])
def get_config():
    cfg = Config.query.get(1)
    return jsonify(cfg.to_dict())


@app.route("/api/config", methods=["POST"])
def set_config():
    data = request.json
    cfg = Config.query.get(1)
    cfg.hunter_key    = data.get("hunter_key", cfg.hunter_key)
    cfg.netrows_key   = data.get("netrows_key", cfg.netrows_key)
    cfg.max_labs      = int(data.get("max_labs", 50))
    cfg.min_productos = int(data.get("min_productos", 2))
    cfg.paises        = data.get("paises", ["ARG"])
    db.session.commit()
    return jsonify({"ok": True})


# ═══════════════════════════════════════════════════════════════
# RUTAS — Sesiones
# ═══════════════════════════════════════════════════════════════

@app.route("/api/sesiones", methods=["GET"])
def get_sesiones():
    sesiones = Sesion.query.order_by(Sesion.creada_en.desc()).all()
    return jsonify([s.to_dict() for s in sesiones])


@app.route("/api/sesiones/<int:sid>/delete", methods=["POST"])
def delete_sesion(sid):
    if _pipeline["running"] and _pipeline["sesion_id"] == sid:
        return jsonify({"error": "No podés eliminar una sesión que está corriendo"}), 400
    s = Sesion.query.get_or_404(sid)
    db.session.delete(s)
    db.session.commit()
    return jsonify({"ok": True})


@app.route("/api/sesiones/<int:sid>/logs", methods=["GET"])
def get_logs(sid):
    desde = int(request.args.get("desde", 0))
    logs = LogEntry.query\
        .filter_by(sesion_id=sid)\
        .order_by(LogEntry.id)\
        .offset(desde).all()
    return jsonify([l.to_dict() for l in logs])


@app.route("/api/resultados/all", methods=["GET"])
def get_resultados_all():
    """Todos los prospectos de TODAS las sesiones (persisten para siempre)."""
    rows = Resultado.query.order_by(Resultado.id.desc()).all()
    vistos = set()
    out = []
    for r in rows:
        em = (r.email or "").lower().strip()
        if em and em in vistos:
            continue
        if em:
            vistos.add(em)
        d = r.to_dict()
        d["relevancia_cargo"] = _relevancia_cargo(d)
        d["confianza"]        = _nivel_confianza(d)
        d["mensaje_sugerido"] = _mensaje_sugerido(d)
        out.append(d)
    return jsonify(out)


@app.route("/api/sesiones/<int:sid>/resultados", methods=["GET"])
def get_resultados(sid):
    rows = Resultado.query.filter_by(sesion_id=sid).order_by(Resultado.id.desc()).all()
    out = []
    for r in rows:
        d = r.to_dict()
        # Enriquecer con relevancia del cargo + nivel de confianza (sin CSV)
        d["relevancia_cargo"] = _relevancia_cargo(d)
        d["confianza"]        = _nivel_confianza(d)
        d["mensaje_sugerido"] = _mensaje_sugerido(d)
        out.append(d)
    return jsonify(out)


@app.route("/api/sesiones/<int:sid>/download", methods=["GET"])
def download_csv(sid):
    sesion = Sesion.query.get_or_404(sid)
    rows   = Resultado.query.filter_by(sesion_id=sid).all()
    if not rows:
        return jsonify({"error": "Sin resultados para descargar"}), 400
    return _hacer_csv(rows, f"abraChem_{sesion.nombre.replace(' ','_')}")


@app.route("/api/download/all", methods=["GET"])
def download_all():
    rows = Resultado.query.order_by(Resultado.sesion_id, Resultado.id).all()
    if not rows:
        return jsonify({"error": "Sin resultados"}), 400
    return _hacer_csv(rows, "abraChem_todos")




def _relevancia_cargo(d: dict) -> str:
    """Etiqueta de relevancia del cargo (Compras / Supply Chain / etc.)."""
    try:
        from paso4_5_linkedin_hunter import clasificar_cargo
        _, etiqueta = clasificar_cargo(d.get("cargo", "") or "", d.get("email", "") or "")
        return etiqueta or "—"
    except Exception:
        return "—"


def _nivel_confianza(d: dict) -> str:
    """
    Alta/Media/Baja combinando: verificación del email + fuente +
    relevancia del cargo. Un email verificado de un Jefe de Compras = Alta.
    Un email no verificado de gerencia general = Baja.
    """
    fuente = d.get("fuente_email", "")
    verif  = d.get("email_verificado", "")
    try:
        from paso4_5_linkedin_hunter import clasificar_cargo
        tier = clasificar_cargo(d.get("cargo", "") or "", d.get("email", "") or "")[0]
    except Exception:
        tier = 4

    verificado = (fuente == "rocketreach_directo" or verif == "válido")
    semi = fuente in ("hunter_email_finder", "candidato_verificado",
                      "hunter_domain_compras", "candidato_catchall")

    if verificado and tier <= 2:
        return "Alta"
    if verificado or (semi and tier <= 2):
        return "Media"
    if semi or tier <= 3:
        return "Media-Baja"
    return "Baja"


def _mensaje_sugerido(d: dict) -> str:
    """Borrador de email personalizado, listo para copiar y enviar."""
    nombre = (d.get("nombre") or "").strip()
    saludo = f"Estimado/a {nombre}" if nombre else "Estimados"
    apis = [a.strip() for a in (d.get("apis_clave") or "").split("|") if a.strip()][:3]
    apis_txt = ", ".join(apis) if apis else "materias primas farmacéuticas"
    lab = d.get("laboratorio", "su laboratorio")
    return (
        f"{saludo}: Mi nombre es [TU NOMBRE], de abraChem. "
        f"Somos distribuidores de materias primas farmacéuticas, nutracéuticas y veterinarias. "
        f"Vimos que {lab.title()} trabaja con productos que utilizan {apis_txt}, "
        f"y nos gustaría cotizarles estos insumos con condiciones competitivas. "
        f"¿Tendría 15 minutos esta semana para una llamada breve? Saludos cordiales."
    )

def _hacer_csv(rows, nombre_base):
    import unicodedata
    output = io.StringIO()
    campos = ["pais","laboratorio","rubro","nombre","apellido","cargo",
              "relevancia_cargo","email","confianza","email_verificado",
              "fuente_email","dominio","apis_clave","top_apis",
              "mensaje_sugerido","notas"]
    writer = csv.DictWriter(output, fieldnames=campos, extrasaction="ignore")
    writer.writeheader()
    for r in rows:
        d = r.to_dict()
        d["relevancia_cargo"] = _relevancia_cargo(d)
        d["confianza"] = _nivel_confianza(d)
        d["mensaje_sugerido"] = _mensaje_sugerido(d)
        writer.writerow(d)
    output.seek(0)
    ts = datetime.now().strftime("%Y%m%d_%H%M")

    # Limpiar nombre de archivo — solo alfanumérico y guiones
    nombre_limpio = unicodedata.normalize("NFKD", nombre_base)
    nombre_limpio = nombre_limpio.encode("ascii", "ignore").decode("ascii")
    nombre_limpio = re.sub(r"[^\w\-]", "_", nombre_limpio)
    nombre_limpio = re.sub(r"_+", "_", nombre_limpio).strip("_")[:60]
    filename = f"abraChem_{nombre_limpio}_{ts}.csv"

    csv_content = "\ufeff" + output.getvalue()

    return Response(
        csv_content,
        mimetype="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Type": "text/csv; charset=utf-8",
            "Content-Length": str(len(csv_content.encode("utf-8"))),
            "Cache-Control": "no-cache",
        }
    )


# ═══════════════════════════════════════════════════════════════
# RUTAS — Control del pipeline
# ═══════════════════════════════════════════════════════════════

@app.route("/api/start", methods=["POST"])
def start_pipeline():
    if _pipeline["running"]:
        return jsonify({"error": "Ya hay un pipeline corriendo"}), 400

    data = request.json

    # Guardar config (las claves NO vienen del cliente: se cargan del servidor)
    cfg = Config.query.get(1)
    cfg.max_labs      = int(data.get("max_labs", cfg.max_labs))
    cfg.min_productos = int(data.get("min_productos", cfg.min_productos))
    cfg.paises        = data.get("paises", cfg.paises)
    db.session.commit()

    sesion_id = data.get("sesion_id")

    if sesion_id:
        # Retomar sesión pausada
        sesion = Sesion.query.get(sesion_id)
        if not sesion:
            return jsonify({"error": "Sesión no encontrada"}), 404
        if sesion.estado == "completado":
            return jsonify({"error": "Esa sesión ya está completada"}), 400
        sesion.estado    = "corriendo"
        sesion.error_msg = ""
        db.session.commit()
        _log(sesion.id, "info", f"▶ Retomando sesión — {sesion.nombre}")
    else:
        # Nueva sesión
        paises = data.get("paises", ["ARG"])
        nombre = f"{' + '.join(paises)} — {datetime.now().strftime('%d/%m/%Y %H:%M')}"
        sesion = Sesion(
            nombre=nombre,
            paises_json=json.dumps(paises),
            estado="corriendo",
        )
        db.session.add(sesion)
        db.session.commit()
        _log(sesion.id, "info", f"▶ Nueva sesión iniciada — {sesion.nombre}")

    _pipeline["running"]   = True
    _pipeline["sesion_id"] = sesion.id

    t = threading.Thread(target=correr_pipeline, args=(sesion.id,), daemon=True)
    _pipeline["thread"] = t
    t.start()

    return jsonify({"ok": True, "sesion_id": sesion.id})


@app.route("/api/pause", methods=["POST"])
def pause_pipeline():
    if not _pipeline["running"]:
        return jsonify({"error": "No hay pipeline corriendo"}), 400
    _pipeline["running"] = False
    sid = _pipeline["sesion_id"]
    if sid:
        with app.app_context():
            sesion = Sesion.query.get(sid)
            if sesion:
                sesion.estado = "pausado"
                sesion.actualizada_en = datetime.now()
                db.session.commit()
        _log(sid, "warning", "⏸ Pausado — podés continuar mañana desde el historial")
    return jsonify({"ok": True})


@app.route("/api/status", methods=["GET"])
def get_status():
    sid = _pipeline["sesion_id"]
    if not sid:
        ultima = Sesion.query.order_by(Sesion.actualizada_en.desc()).first()
        if ultima:
            sid = ultima.id
    if not sid:
        return jsonify({"running": False, "sesion_id": None, "sesion": None})
    sesion = Sesion.query.get(sid)
    return jsonify({
        "running":   _pipeline["running"],
        "sesion_id": sid,
        "sesion":    sesion.to_dict() if sesion else None,
    })


# ═══════════════════════════════════════════════════════════════
# PIPELINE
# ═══════════════════════════════════════════════════════════════

def _log(sesion_id: int, tipo: str, msg: str):
    with app.app_context():
        db.session.add(LogEntry(
            sesion_id=sesion_id,
            tipo=tipo,
            msg=msg,
            ts=datetime.now().strftime("%H:%M:%S"),
        ))
        db.session.commit()
    print(f"[{tipo.upper()}] {msg}")


def _resolver_claves(cfg):
    """Claves desde config.py/entorno; si son placeholder, usa las guardadas."""
    def _ok(k):
        return bool(k) and "PEGA_AQUI" not in k and "TU_" not in k
    hunter = HUNTER_API_KEY if _ok(HUNTER_API_KEY) else (cfg.hunter_key or "")
    rr     = ROCKETREACH_API_KEY if _ok(ROCKETREACH_API_KEY) else (cfg.netrows_key or "")
    return hunter, rr


def _reponer_laboratorios(sesion_id: int, faltan: int) -> int:
    """
    Cuando se acaban los laboratorios y falta llegar al objetivo, busca MÁS
    en la web (con enfoques distintos cada vez), excluyendo los ya usados en
    esta sesión y los ya conseguidos en cualquier sesión. Devuelve cuántos
    agregó. Hace varios intentos con enfoques distintos antes de rendirse.
    """
    import unicodedata
    from paso1_laboratorios import descubrir_laboratorios_web

    def _norm(s):
        s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode()
        return "".join(c for c in s.lower() if c.isalnum())

    sesion = Sesion.query.get(sesion_id)
    paises = sesion.paises or []

    # Nombres ya usados (en esta sesión) o ya conseguidos (cualquier sesión)
    usados = {_norm(l.nombre) for l in LabQueue.query.filter_by(sesion_id=sesion_id).all()}
    usados |= {_norm(r.laboratorio)
               for r in Resultado.query.with_entities(Resultado.laboratorio).all()}

    # Cuántas reposiciones ya hicimos (para variar el enfoque)
    ronda = _pipeline["rondas"].get(sesion_id, 0)
    if ronda >= 12:
        return 0  # tope de seguridad: ya buscamos bastante en la web

    orden_base = (db.session.query(db.func.max(LabQueue.orden))
                  .filter_by(sesion_id=sesion_id).scalar() or 0) + 1
    agregados = 0

    # Probar hasta 2 enfoques nuevos por llamada
    for _ in range(2):
        ronda += 1
        for pais in paises:
            try:
                df = descubrir_laboratorios_web(pais, max_labs=30, enfoque=ronda)
            except Exception as e:
                _log(sesion_id, "warning", f"   ⚠️ Reposición falló para {pais}: {e}")
                continue
            for _, row in df.iterrows():
                clave = _norm(row["nombre"])
                if not clave or clave in usados:
                    continue
                usados.add(clave)
                db.session.add(LabQueue(
                    sesion_id=sesion_id, pais=pais, nombre=row["nombre"],
                    rubro=row.get("rubro", "farmacéutico"),
                    productos=str(row.get("productos", "")),
                    estado="pendiente", orden=orden_base,
                ))
                orden_base += 1
                agregados += 1
        if agregados > 0:
            break

    _pipeline["rondas"][sesion_id] = ronda

    if agregados:
        db.session.commit()
        _log(sesion_id, "info",
             f"🔄 Reposición: +{agregados} laboratorios nuevos encontrados en la web")
    return agregados


def correr_pipeline(sesion_id: int):
    with app.app_context():
        try:
            cfg    = Config.query.get(1)
            sesion = Sesion.query.get(sesion_id)

            from paso1_laboratorios      import obtener_laboratorios
            from paso2_3_apis_dominio    import inferir_apis, obtener_dominio
            from paso4_5_linkedin_hunter import HunterAPI, RocketReachClient, obtener_contacto

            k_hunter, k_rr = _resolver_claves(cfg)
            hunter     = HunterAPI(k_hunter)
            rocketreach = RocketReachClient(k_rr)

            try:
                cr_rr = rocketreach.creditos_disponibles()
                if cr_rr != -1:
                    _log(sesion_id, "info", f"🚀 Créditos RocketReach disponibles: {cr_rr}")
            except Exception:
                pass

            creditos = hunter.creditos_disponibles()
            if creditos != -1:
                _log(sesion_id, "info", f"💳 Créditos Hunter disponibles: {creditos}")
                if creditos < 50:
                    _log(sesion_id, "warning", f"⚠️ Pocos créditos ({creditos}) — considerá recargar")

            # ── Cargar o crear la cola ──────────────────────
            labs_pendientes = LabQueue.query.filter_by(
                sesion_id=sesion_id, estado="pendiente"
            ).order_by(LabQueue.orden).all()

            if not labs_pendientes:
                # Sesión nueva — scrapear y encolar laboratorios
                _log(sesion_id, "info", "🔍 Buscando laboratorios en registros públicos...")
                orden = 0
                orden_activos = 0
                for pais in sesion.paises:
                    if not _pipeline["running"]:
                        break
                    _log(sesion_id, "info", f"🌎 Scrapeando {pais}...")
                    try:
                        # Traer TODOS los disponibles (hasta 30), no solo max_labs
                        df_todos = obtener_laboratorios(pais, cache=True)
                        # Filtro de mín. productos SOLO a labs con productos conocidos.
                        # Los descubiertos en la web (productos vacíos) no se filtran:
                        # no conocemos su catálogo pero igual sirven como prospecto.
                        _prod = df_todos["productos"].astype(str).str.strip()
                        df_todos = df_todos[
                            (df_todos["n_productos"] >= cfg.min_productos) | (_prod == "")
                        ]

                        # No repetir laboratorios ya conseguidos en sesiones
                        # anteriores: saltear los que ya tienen prospecto guardado.
                        def _norm_lab(s):
                            import unicodedata
                            s = unicodedata.normalize("NFKD", str(s)).encode("ascii","ignore").decode()
                            return "".join(c for c in s.lower() if c.isalnum())
                        ya_hechos = {
                            _norm_lab(r.laboratorio)
                            for r in Resultado.query.with_entities(Resultado.laboratorio).all()
                        }

                        # Los primeros max_labs van como "activos", el resto como "reserva"
                        saltados = 0
                        for _, row in df_todos.iterrows():
                            if _norm_lab(row["nombre"]) in ya_hechos:
                                saltados += 1
                                continue
                            estado_inicial = "pendiente" if orden_activos < cfg.max_labs else "reserva"
                            db.session.add(LabQueue(
                                sesion_id=sesion_id,
                                pais=pais,
                                nombre=row["nombre"],
                                rubro=row.get("rubro", "farmacéutico"),
                                productos=str(row.get("productos", "")),
                                estado=estado_inicial,
                                orden=orden,
                            ))
                            orden += 1
                            orden_activos += 1

                        db.session.commit()
                        n_activos  = min(orden_activos, cfg.max_labs)
                        n_reserva  = max(0, orden_activos - cfg.max_labs)
                        extra_msg = f" · {saltados} ya contactados antes (omitidos)" if saltados else ""
                        _log(sesion_id, "success",
                             f"✅ {n_activos} laboratorios activos + {n_reserva} en reserva ({pais}){extra_msg}")
                    except Exception as e:
                        _log(sesion_id, "error", f"❌ Error scrapeando {pais}: {e}")

                sesion.total = LabQueue.query.filter_by(
                    sesion_id=sesion_id, estado="pendiente"
                ).count()
                db.session.commit()

                labs_pendientes = LabQueue.query.filter_by(
                    sesion_id=sesion_id, estado="pendiente"
                ).order_by(LabQueue.orden).all()
            else:
                _log(sesion_id, "info",
                     f"⏭ Retomando — {len(labs_pendientes)} laboratorios pendientes")

            # Sincronizar progreso
            sesion.progreso     = LabQueue.query.filter(
                LabQueue.sesion_id == sesion_id,
                LabQueue.estado.in_(["ok", "sin_email", "error", "procesando"])
            ).count()
            sesion.total        = LabQueue.query.filter_by(
                sesion_id=sesion_id, estado="pendiente"
            ).count()
            sesion.n_resultados = Resultado.query.filter_by(sesion_id=sesion_id).count()
            db.session.commit()

            # ── Procesar cada lab pendiente ─────────────────
            # Objetivo: conseguir exactamente cfg.max_labs prospectos con email
            objetivo    = cfg.max_labs
            conseguidos = Resultado.query.filter_by(sesion_id=sesion_id).count()

            while True:
                if not _pipeline["running"]:
                    sesion = Sesion.query.get(sesion_id)
                    sesion.estado = "pausado"
                    sesion.actualizada_en = datetime.now()
                    db.session.commit()
                    _log(sesion_id, "warning", "⏸ Pausado correctamente")
                    return

                # Tomar el siguiente pendiente
                lab = LabQueue.query.filter_by(
                    sesion_id=sesion_id, estado="pendiente"
                ).order_by(LabQueue.orden).first()

                if not lab:
                    break  # No quedan más pendientes

                sesion      = Sesion.query.get(sesion_id)
                conseguidos = Resultado.query.filter_by(sesion_id=sesion_id).count()

                _log(sesion_id, "info",
                     f"[{conseguidos+1}/{objetivo}] {lab.nombre} ({lab.pais})")

                lab.estado = "procesando"
                db.session.commit()

                productos = [p.strip() for p in lab.productos.split("|") if p.strip()]

                # Paso 2 — APIs
                try:
                    apis      = inferir_apis(productos, top_n=10)
                    apis_str  = " | ".join([a["api"] for a in apis])
                    apis_ifas = [a["api"] for a in apis if a["es_ifa"]][:5]
                    _log(sesion_id, "info", f"   🧪 {len(apis)} APIs inferidas")
                except Exception as e:
                    apis_str, apis_ifas = "", []
                    _log(sesion_id, "warning", f"   ⚠️ Error inferiendo APIs: {e}")

                # Paso 3 — Dominio
                try:
                    dominio = obtener_dominio(lab.nombre, lab.pais, hunter=hunter)
                    if dominio:
                        _log(sesion_id, "info", f"   🌐 Dominio: {dominio}")
                    else:
                        _log(sesion_id, "warning", "   ⚠️ No se encontró dominio web")
                except Exception as e:
                    dominio = ""
                    _log(sesion_id, "warning", f"   ⚠️ Error buscando dominio: {e}")

                # Pasos 4+5 — RocketReach + Hunter
                email_conseguido = False
                try:
                    contacto = obtener_contacto(
                        nombre_lab=lab.nombre,
                        pais=lab.pais,
                        dominio=dominio,
                        hunter=hunter,
                        rocketreach=rocketreach,
                    )

                    # Mostrar el paso a paso de los intentos
                    for intento in (contacto.intentos or []):
                        tipo = "success" if intento.startswith("✅") else \
                               "warning" if intento.startswith("❌") else "info"
                        _log(sesion_id, tipo, f"   {intento}")

                    # ── Deduplicación global: nunca contactar 2 veces ──
                    if contacto.email:
                        ya_existe = Resultado.query.filter(
                            Resultado.email == contacto.email,
                            Resultado.sesion_id != sesion_id,
                        ).first()
                        if ya_existe:
                            _log(sesion_id, "warning",
                                 f"   ♻️ {contacto.email} ya contactado en sesión #{ya_existe.sesion_id} — buscando otro lab")
                            contacto.email = ""

                    if contacto.email:
                        email_conseguido = True
                        if contacto.verificado is True:
                            verif = "válido"
                            _log(sesion_id, "success",
                                 f"   📧 Email final: {contacto.email} ✅")
                        elif contacto.verificado is False:
                            verif = "verificado inválido — usando igual"
                            _log(sesion_id, "warning",
                                 f"   📧 Email final (sin verificar): {contacto.email}")
                        else:
                            verif = contacto.fuente_email or "no verificable"
                            _log(sesion_id, "info",
                                 f"   📧 Email final: {contacto.email}")

                        db.session.add(Resultado(
                            sesion_id    = sesion_id,
                            pais         = lab.pais,
                            laboratorio  = lab.nombre,
                            rubro        = lab.rubro,
                            nombre       = contacto.nombre,
                            apellido     = contacto.apellido,
                            cargo        = contacto.cargo,
                            email        = contacto.email,
                            email_verificado = verif,
                            fuente_email = contacto.fuente_email,
                            dominio      = dominio,
                            apis_clave   = " | ".join(apis_ifas),
                            top_apis     = apis_str,
                            notas        = contacto.notas,
                        ))
                        lab.estado = "ok"
                        rel = getattr(contacto, "relevancia", "") or "—"
                        _log(sesion_id, "success",
                             f"   ✅ Prospecto guardado · {contacto.nombre} {contacto.apellido} · {rel}")
                    else:
                        lab.estado = "sin_email"

                except Exception as e:
                    lab.estado = "error"
                    _log(sesion_id, "error", f"   ❌ Error: {e}")

                # ── Siempre hacer commit del estado del lab ──────
                # Esto garantiza que "procesando" nunca quede atascado
                db.session.commit()

                # ── Activar reserva si no hubo email ────────────
                if lab.estado in ("sin_email", "error"):
                    _log(sesion_id, "warning",
                         "   ⚠️ Sin email — activando laboratorio de reserva...")

                    siguiente = LabQueue.query.filter_by(
                        sesion_id=sesion_id, estado="reserva"
                    ).order_by(LabQueue.orden).first()

                    if siguiente:
                        siguiente.estado = "pendiente"
                        db.session.commit()  # Commit inmediato para que el while lo vea
                        _log(sesion_id, "info",
                             f"   🔄 Continuando con: {siguiente.nombre}")
                    else:
                        _log(sesion_id, "warning",
                             "   ℹ️ Sin más laboratorios en reserva")

                sesion.progreso     = LabQueue.query.filter(
                    LabQueue.sesion_id == sesion_id,
                    LabQueue.estado.in_(["ok", "sin_email", "error"])
                ).count()
                sesion.n_resultados = Resultado.query.filter_by(sesion_id=sesion_id).count()
                sesion.total        = objetivo  # Siempre el objetivo fijo
                sesion.actualizada_en = datetime.now()
                db.session.commit()
                time.sleep(0.5)

                # Cortar solo si llegamos al objetivo
                conseguidos = Resultado.query.filter_by(sesion_id=sesion_id).count()
                if conseguidos >= objetivo:
                    _log(sesion_id, "success",
                         f"🎯 Objetivo alcanzado: {conseguidos}/{objetivo} prospectos")
                    break
                # Si no hay más pendientes ni reservas, REPONER con búsqueda web
                # (no parar hasta llegar al objetivo, salvo que la web se agote).
                quedan = LabQueue.query.filter_by(
                    sesion_id=sesion_id, estado="pendiente"
                ).count() + LabQueue.query.filter_by(
                    sesion_id=sesion_id, estado="reserva"
                ).count()
                if quedan == 0:
                    repuestos = _reponer_laboratorios(sesion_id, objetivo - conseguidos)
                    if repuestos > 0:
                        continue  # seguir procesando los nuevos
                    _log(sesion_id, "warning",
                         f"ℹ️ Se agotaron los laboratorios disponibles en la web. "
                         f"Conseguidos: {conseguidos}/{objetivo}")
                    break

            # ── Completado: resumen de calidad ───────────────
            sesion        = Sesion.query.get(sesion_id)
            sesion.estado = "completado"
            sesion.actualizada_en = datetime.now()
            db.session.commit()

            try:
                from paso4_5_linkedin_hunter import clasificar_cargo
                rows = Resultado.query.filter_by(sesion_id=sesion_id).all()
                tiers = {1: 0, 2: 0, 3: 0, 4: 0}
                conf = {"Alta": 0, "Media": 0, "Media-Baja": 0, "Baja": 0}
                verificados = 0
                for r in rows:
                    t = clasificar_cargo(r.cargo or "", r.email or "")[0]
                    if t in tiers:
                        tiers[t] += 1
                    c = _nivel_confianza({"fuente_email": r.fuente_email,
                                          "email_verificado": r.email_verificado,
                                          "cargo": r.cargo, "email": r.email})
                    conf[c] = conf.get(c, 0) + 1
                    if r.email_verificado == "válido":
                        verificados += 1
                _log(sesion_id, "success",
                     f"🎉 ¡Listo! {sesion.n_resultados} prospectos encontrados")
                _log(sesion_id, "info", "📊 Resumen de calidad:")
                _log(sesion_id, "info",
                     f"   🎯 Compras directo: {tiers[1]} · Supply Chain: {tiers[2]} · "
                     f"Operaciones: {tiers[3]} · Gerencia: {tiers[4]}")
                _log(sesion_id, "info",
                     f"   ✅ Confianza Alta: {conf.get('Alta',0)} · Media: {conf.get('Media',0)} · "
                     f"Media-Baja: {conf.get('Media-Baja',0)} · Baja: {conf.get('Baja',0)}")
                _log(sesion_id, "info",
                     f"   📧 Emails verificados por SMTP: {verificados}/{sesion.n_resultados}")
            except Exception as e:
                _log(sesion_id, "success",
                     f"🎉 ¡Listo! {sesion.n_resultados} prospectos encontrados")

        except Exception as e:
            _log(sesion_id, "error", f"❌ Error crítico: {e}")
            _log(sesion_id, "error", traceback.format_exc())
            with app.app_context():
                s = Sesion.query.get(sesion_id)
                if s:
                    s.estado    = "error"
                    s.error_msg = str(e)
                    db.session.commit()
        finally:
            _pipeline["running"]   = False
            _pipeline["sesion_id"] = None


if __name__ == "__main__":
    print(f"\n{'='*50}")
    print("  abraChem Prospector v2")
    print(f"  Archivos en: {BASE_DIR}")
    print(f"  Pipeline en: {PIPELINE_DIR}")
    print(f"  Base de datos: {DATA_DIR / 'abrachem.db'}")
    print(f"  Abriendo en: http://127.0.0.1:5000")
    print(f"{'='*50}\n")

    if not PIPELINE_DIR.exists():
        print(f"⚠️  ADVERTENCIA: No se encontró abrachem_v2 en {PIPELINE_DIR}")
        print("   Asegurate de que la estructura sea:")
        print("   abraChem_Agente/")
        print("     abrachem_v2/      ← pipeline")
        print("     abrachem_app_v2/  ← esta app")

    app.run(debug=False, port=5000, host="127.0.0.1", use_reloader=False)
