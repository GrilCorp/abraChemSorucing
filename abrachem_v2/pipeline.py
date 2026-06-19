"""
abraChem — Pipeline Principal v2
Orquesta los 5 pasos completos para un país dado.

Uso:
    python pipeline.py --pais ARG
    python pipeline.py --pais CHL --max-labs 50
    python pipeline.py --pais ARG --skip-scraping
"""

import argparse
import logging
import time
import pandas as pd
from pathlib import Path
from datetime import datetime

from config import HUNTER_API_KEY, NETROWS_API_KEY, PAISES
from paso1_laboratorios import obtener_laboratorios
from paso2_3_apis_dominio import inferir_apis, obtener_dominio
from paso4_5_linkedin_hunter import HunterAPI, NetrowsAPI, obtener_contacto

# Logging
Path("logs").mkdir(exist_ok=True)
Path("data").mkdir(exist_ok=True)
Path("output").mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(f"logs/pipeline_{datetime.now():%Y%m%d_%H%M}.log"),
        logging.StreamHandler(),
    ]
)
log = logging.getLogger(__name__)


def run_pipeline(
    pais: str,
    max_labs: int = None,
    skip_scraping: bool = False,
    min_productos: int = 2,
) -> pd.DataFrame:
    """
    Pipeline completo para un país.

    Args:
        pais: Código del país ("ARG", "CHL", etc.)
        max_labs: Límite de laboratorios a procesar (None = todos)
        skip_scraping: Usar cache local si existe
        min_productos: Mínimo de productos para incluir un lab

    Returns:
        DataFrame final con toda la información lista para enviar emails
    """
    log.info("=" * 65)
    log.info(f"abraChem Pipeline — País: {pais}")
    log.info("=" * 65)

    hunter = HunterAPI(HUNTER_API_KEY)
    netrows = NetrowsAPI(NETROWS_API_KEY)

    # Verificar créditos disponibles
    creditos = hunter.creditos_disponibles()
    log.info(f"Créditos Hunter disponibles: {creditos}")
    if creditos != -1 and creditos < 10:
        log.warning(f"⚠️  Pocos créditos Hunter ({creditos}). Considerar recargar.")

    # ────────────────────────────────────────────────────────
    # PASO 1: Obtener laboratorios
    # ────────────────────────────────────────────────────────
    log.info(f"\n{'─'*40}")
    log.info("PASO 1 — Obteniendo laboratorios...")
    log.info(f"{'─'*40}")

    df_labs = obtener_laboratorios(pais, cache=skip_scraping)

    # Filtrar por mínimo de productos
    df_labs = df_labs[df_labs["n_productos"] >= min_productos].reset_index(drop=True)

    if max_labs:
        df_labs = df_labs.head(max_labs)

    log.info(f"Laboratorios a procesar: {len(df_labs)}")

    # ────────────────────────────────────────────────────────
    # PROCESAR CADA LABORATORIO
    # ────────────────────────────────────────────────────────
    filas = []
    total_creditos = 0

    for i, (_, lab) in enumerate(df_labs.iterrows()):
        nombre_lab = lab["nombre"]
        rubro = lab.get("rubro", "farmacéutico")
        productos_str = str(lab.get("productos", ""))
        productos = [p.strip() for p in productos_str.split("|") if p.strip()]

        log.info(f"\n[{i+1}/{len(df_labs)}] {nombre_lab} ({rubro})")

        # ── PASO 2: Top 10 APIs del laboratorio ──────────────
        apis = inferir_apis(productos, top_n=10)
        apis_str = " | ".join([a["api"] for a in apis])
        apis_ifas = [a["api"] for a in apis if a["es_ifa"]][:5]
        log.info(f"  APIs inferidas: {len(apis)}")

        # ── PASO 3: Dominio web ───────────────────────────────
        dominio = obtener_dominio(nombre_lab, pais)
        time.sleep(1)

        # ── PASOS 4+5: LinkedIn + Hunter ──────────────────────
        contacto = obtener_contacto(
            nombre_lab=nombre_lab,
            pais=pais,
            dominio=dominio,
            hunter=hunter,
            netrows=netrows,
        )
        total_creditos += contacto.creditos_usados

        # ── Construir fila del resultado ──────────────────────
        # Solo incluir si encontramos email
        if not contacto.email:
            log.info(f"  ⚠️  Sin email — laboratorio omitido del output final")
            # Igual guardarlo con estado incompleto
            fila = _construir_fila(
                pais=pais,
                nombre_lab=nombre_lab,
                rubro=rubro,
                dominio=dominio,
                contacto=contacto,
                apis_str=apis_str,
                apis_ifas=apis_ifas,
                incluido=False,
            )
        else:
            # Verificación: ¿incluir en tabla final?
            incluir = True
            if contacto.verificado is False:
                # Email verificado como INVÁLIDO → no incluir
                log.info(f"  ❌ Email inválido, omitido del output")
                incluir = False

            fila = _construir_fila(
                pais=pais,
                nombre_lab=nombre_lab,
                rubro=rubro,
                dominio=dominio,
                contacto=contacto,
                apis_str=apis_str,
                apis_ifas=apis_ifas,
                incluido=incluir,
            )

        filas.append(fila)
        log.info(f"  Créditos Hunter usados en este lab: {contacto.creditos_usados}")
        log.info(f"  Créditos totales usados hasta ahora: {total_creditos}")

        # Guardar progreso parcial cada 10 laboratorios
        if (i + 1) % 10 == 0:
            _guardar_parcial(filas, pais, i + 1)

        time.sleep(1)

    # ────────────────────────────────────────────────────────
    # OUTPUT FINAL
    # ────────────────────────────────────────────────────────
    df_completo = pd.DataFrame(filas)
    df_final = df_completo[df_completo["incluido"] == True].copy()
    df_final = df_final.drop(columns=["incluido"])

    # Ordenar por confianza del email
    orden_confianza = {
        "hunter_directo": 1,
        "hunter_formato": 2,
        "hunter_fallback": 3,
        "no_encontrado": 4,
    }
    df_final["_orden"] = df_final["fuente_email"].map(orden_confianza).fillna(5)
    df_final = df_final.sort_values("_orden").drop(columns=["_orden"]).reset_index(drop=True)

    # Guardar
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    output_path = Path("output") / f"prospectos_{pais}_{ts}.csv"
    df_final.to_csv(output_path, index=False, encoding="utf-8-sig")  # utf-8-sig para Excel

    # También guardar el completo (incluyendo omitidos)
    completo_path = Path("output") / f"prospectos_{pais}_{ts}_completo.csv"
    df_completo.to_csv(completo_path, index=False, encoding="utf-8-sig")

    # Resumen final
    log.info(f"\n{'='*65}")
    log.info("RESUMEN FINAL")
    log.info(f"{'='*65}")
    log.info(f"País:                      {pais}")
    log.info(f"Laboratorios procesados:   {len(df_labs)}")
    log.info(f"Con email válido:          {len(df_final)}")
    log.info(f"  → Email Hunter directo:  {(df_final['fuente_email'] == 'hunter_directo').sum()}")
    log.info(f"  → Email por formato:     {(df_final['fuente_email'] == 'hunter_formato').sum()}")
    log.info(f"  → Email fallback:        {(df_final['fuente_email'] == 'hunter_fallback').sum()}")
    log.info(f"Emails verificados ✅:     {df_final['email_verificado'].eq('válido').sum()}")
    log.info(f"Emails no verificables:    {df_final['email_verificado'].eq('no_verificable').sum()}")
    log.info(f"Créditos Hunter usados:    {total_creditos}")
    log.info(f"Output: {output_path}")

    return df_final


def _construir_fila(
    pais, nombre_lab, rubro, dominio, contacto, apis_str, apis_ifas, incluido
) -> dict:
    """Construye la fila del DataFrame de resultado."""

    # Estado de verificación legible
    if contacto.verificado is True:
        verif_str = "válido"
    elif contacto.verificado is False:
        verif_str = "inválido"
    elif contacto.fuente_email in ("hunter_directo", "hunter_fallback"):
        verif_str = "no verificado (Hunter)"
    else:
        verif_str = "no_verificable"

    return {
        # ── Identificación ──
        "pais": pais,
        "laboratorio": nombre_lab,
        "rubro": rubro,
        "dominio": dominio,
        # ── Contacto ──
        "nombre": contacto.nombre,
        "apellido": contacto.apellido,
        "cargo": contacto.cargo,
        "email": contacto.email,
        # ── Calidad del dato ──
        "fuente_nombre": contacto.fuente_nombre,
        "fuente_email": contacto.fuente_email,
        "email_verificado": verif_str,
        # ── APIs (el valor comercial) ──
        "top_apis": apis_str,
        "apis_clave": " | ".join(apis_ifas),
        # ── Meta ──
        "notas": contacto.notas,
        "incluido": incluido,
    }


def _guardar_parcial(filas: list, pais: str, n: int):
    """Guarda progreso parcial para no perder trabajo."""
    df = pd.DataFrame(filas)
    path = Path("data") / f"parcial_{pais}_{n}labs.csv"
    df.to_csv(path, index=False)
    log.info(f"  💾 Progreso parcial guardado: {path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="abraChem Pipeline v2")
    parser.add_argument("--pais", required=True, choices=PAISES,
                        help="País a procesar")
    parser.add_argument("--max-labs", type=int, default=None,
                        help="Límite de laboratorios (para pruebas)")
    parser.add_argument("--skip-scraping", action="store_true",
                        help="Usar cache local del scraping anterior")
    parser.add_argument("--min-productos", type=int, default=2,
                        help="Mínimo de productos registrados para incluir un lab")
    args = parser.parse_args()

    df = run_pipeline(
        pais=args.pais,
        max_labs=args.max_labs,
        skip_scraping=args.skip_scraping,
        min_productos=args.min_productos,
    )

    # Preview del resultado
    print(f"\n{'='*65}")
    print("TABLA FINAL — Lista para enviar emails")
    print(f"{'='*65}")
    cols_preview = ["laboratorio", "nombre", "apellido", "cargo", "email", "email_verificado", "apis_clave"]
    cols_disponibles = [c for c in cols_preview if c in df.columns]
    print(df[cols_disponibles].head(20).to_string(index=False))
