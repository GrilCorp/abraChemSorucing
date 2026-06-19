"""
abraChem · Streamlit — Motor de búsqueda.
Reutiliza los pasos 1-5 del pipeline original y emite eventos para que
la interfaz muestre el progreso en vivo. No depende de Flask.
"""
import unicodedata
from paso1_laboratorios      import obtener_laboratorios, descubrir_laboratorios_web
from paso2_3_apis_dominio    import inferir_apis, obtener_dominio
from paso4_5_linkedin_hunter import (HunterAPI, RocketReachClient,
                                      obtener_contacto, clasificar_cargo)
import store


def _norm(s):
    s = unicodedata.normalize("NFKD", str(s or "")).encode("ascii", "ignore").decode()
    return "".join(ch for ch in s.lower() if ch.isalnum())


# ── Calidad del prospecto (igual que en la app Flask) ──────────
def relevancia_cargo(cargo, email):
    try:
        return clasificar_cargo(cargo or "", email or "")[1] or "—"
    except Exception:
        return "—"


def nivel_confianza(cargo, email, fuente, verif):
    try:
        tier = clasificar_cargo(cargo or "", email or "")[0]
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


def mensaje_sugerido(nombre, laboratorio, apis_clave):
    saludo = f"Estimado/a {nombre.strip()}" if (nombre or "").strip() else "Estimados"
    apis = [a.strip() for a in (apis_clave or "").split("|") if a.strip()][:3]
    apis_txt = ", ".join(apis) if apis else "materias primas farmacéuticas"
    lab = (laboratorio or "su laboratorio").title()
    return (f"{saludo}: Mi nombre es [TU NOMBRE], de abraChem. "
            f"Somos distribuidores de materias primas farmacéuticas, nutracéuticas y "
            f"veterinarias. Vimos que {lab} trabaja con productos que utilizan {apis_txt}, "
            f"y nos gustaría cotizarles estos insumos con condiciones competitivas. "
            f"¿Tendría 15 minutos esta semana para una llamada breve? Saludos cordiales.")


def ejecutar_busqueda(paises, objetivo, min_productos, k_hunter, k_rr,
                      on_log=None, on_progress=None, on_result=None,
                      stop_flag=None):
    """
    Corre la búsqueda completa. Llama:
      on_log(tipo, msg)              — línea de actividad
      on_progress(conseguidos, obj, lab) — avance
      on_result(dict)               — prospecto nuevo guardado
    Devuelve la cantidad de prospectos conseguidos.
    """
    log  = on_log      or (lambda *a: None)
    prog = on_progress or (lambda *a: None)
    res  = on_result   or (lambda *a: None)

    hunter      = HunterAPI(k_hunter)
    rocketreach = RocketReachClient(k_rr)

    try:
        cr = rocketreach.creditos_disponibles()
        if cr != -1:
            log("info", f"🚀 Créditos RocketReach disponibles: {cr}")
    except Exception:
        pass
    try:
        ch = hunter.creditos_disponibles()
        if ch != -1:
            log("info", f"💳 Búsquedas Hunter disponibles: {ch}")
    except Exception:
        pass

    emails_prev = store.found_emails()
    labs_prev   = store.found_labs()

    # ── Armar el pozo de laboratorios ──────────────────────────
    cola, en_cola = [], set()

    def _add_labs(df, pais):
        added = 0
        for _, row in df.iterrows():
            nombre = str(row["nombre"]).strip()
            clave  = _norm(nombre)
            if not clave or clave in labs_prev or clave in en_cola:
                continue
            prod = str(row.get("productos", "")).strip()
            nprod = int(row.get("n_productos", 0) or 0)
            if prod and nprod < min_productos:
                continue
            en_cola.add(clave)
            cola.append({"pais": pais, "nombre": nombre,
                         "rubro": row.get("rubro", "farmacéutico"),
                         "productos": prod})
            added += 1
        return added

    for pais in paises:
        log("info", f"🌎 Buscando laboratorios en {pais}...")
        try:
            df = obtener_laboratorios(pais, cache=True)
            n = _add_labs(df, pais)
            log("success", f"✅ {n} laboratorios nuevos en {pais}")
        except Exception as e:
            log("error", f"❌ Error obteniendo laboratorios de {pais}: {e}")

    conseguidos = 0
    i = 0
    ronda_rep = 0

    while conseguidos < objetivo:
        if stop_flag is not None and stop_flag():
            log("warning", "⏹ Búsqueda detenida por el usuario.")
            break

        if i >= len(cola):
            # Reposición: buscar más laboratorios en la web
            if ronda_rep >= 12:
                log("warning", "ℹ️ Se agotó la búsqueda web de reservas.")
                break
            nuevos = 0
            for _ in range(2):
                ronda_rep += 1
                for pais in paises:
                    try:
                        df = descubrir_laboratorios_web(pais, max_labs=30, enfoque=ronda_rep)
                        nuevos += _add_labs(df, pais)
                    except Exception as e:
                        log("warning", f"   ⚠️ Reposición falló en {pais}: {e}")
                if nuevos:
                    break
            if nuevos:
                log("info", f"🔄 Reposición: +{nuevos} laboratorios nuevos de la web")
            else:
                log("warning", f"ℹ️ Sin más laboratorios. Conseguidos: {conseguidos}/{objetivo}")
                break

        lab = cola[i]; i += 1
        prog(conseguidos, objetivo, lab["nombre"])
        log("info", f"[{conseguidos+1}/{objetivo}] {lab['nombre']} ({lab['pais']})")

        productos = [p.strip() for p in lab["productos"].split("|") if p.strip()]

        # Paso 2 — APIs
        try:
            apis = inferir_apis(productos, top_n=10)
            apis_str  = " | ".join(a["api"] for a in apis)
            apis_ifas = " | ".join(a["api"] for a in apis if a["es_ifa"])
        except Exception:
            apis_str, apis_ifas = "", ""

        # Paso 3 — Dominio
        try:
            dominio = obtener_dominio(lab["nombre"], lab["pais"], hunter=hunter)
            if dominio:
                log("info", f"   🌐 Dominio: {dominio}")
        except Exception:
            dominio = ""

        # Pasos 4+5 — RocketReach + Hunter
        try:
            contacto = obtener_contacto(nombre_lab=lab["nombre"], pais=lab["pais"],
                                        dominio=dominio, hunter=hunter,
                                        rocketreach=rocketreach)
        except Exception as e:
            log("error", f"   ❌ Error buscando contacto: {e}")
            continue

        for intento in (contacto.intentos or []):
            tipo = ("success" if intento.startswith("✅")
                    else "warning" if intento.startswith("❌") else "info")
            log(tipo, f"   {intento}")

        email = (contacto.email or "").strip()
        if email and email.lower() in emails_prev:
            log("warning", f"   ♻️ {email} ya estaba en la base — sigo con otro lab")
            email = ""

        if not email:
            continue  # pasar a otro laboratorio (último recurso ya agotado adentro)

        verif = ("válido" if contacto.verificado is True
                 else "inválido" if contacto.verificado is False
                 else (contacto.fuente_email or "no verificable"))
        d = {
            "pais": lab["pais"], "laboratorio": lab["nombre"], "rubro": lab["rubro"],
            "nombre": contacto.nombre, "apellido": contacto.apellido,
            "cargo": contacto.cargo, "email": email, "email_verificado": verif,
            "fuente_email": contacto.fuente_email, "dominio": dominio,
            "apis_clave": apis_ifas, "top_apis": apis_str,
            "notas": contacto.notas or "",
        }
        d["relevancia"] = relevancia_cargo(d["cargo"], email)
        d["confianza"]  = nivel_confianza(d["cargo"], email, d["fuente_email"], verif)
        d["mensaje"]    = mensaje_sugerido(d["nombre"], d["laboratorio"], apis_ifas)

        store.add_resultado(d)
        emails_prev.add(email.lower())
        labs_prev.add(_norm(lab["nombre"]))
        conseguidos += 1
        log("success", f"   ✅ Prospecto guardado · {d['nombre']} {d['apellido']} · {d['relevancia']}")
        res(d)
        prog(conseguidos, objetivo, lab["nombre"])

    log("success", f"🎉 Listo. {conseguidos}/{objetivo} prospectos conseguidos.")
    return conseguidos
