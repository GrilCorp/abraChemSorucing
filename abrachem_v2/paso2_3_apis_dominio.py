"""
PASO 2 — Top 10 APIs por laboratorio (según sus productos)
PASO 3 — Dominio exacto de cada laboratorio

abraChem Pipeline v2
"""

import re
import time
import requests
import pandas as pd
from pathlib import Path
from difflib import SequenceMatcher
from bs4 import BeautifulSoup
import logging

log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

# ═══════════════════════════════════════════════════════════════
# PASO 2 — BASE DE CONOCIMIENTO + INFERENCIA DE APIs
# ═══════════════════════════════════════════════════════════════

KNOWLEDGE_BASE: dict[str, list[str]] = {
    # ── Analgésicos / AINEs ──────────────────────────────────
    "ibuprofeno":           ["Ibuprofeno API", "Celulosa microcristalina", "Almidón de maíz", "Estearato de magnesio", "Croscarmelosa sódica"],
    "paracetamol":          ["Paracetamol API", "Almidón de maíz", "Povidona K30", "Estearato de magnesio", "Talco farmacéutico"],
    "diclofenac":           ["Diclofenac sódico", "Celulosa microcristalina", "Povidona K30", "Croscarmelosa sódica", "Estearato de magnesio"],
    "naproxeno":            ["Naproxeno API", "Povidona K30", "Talco farmacéutico", "Estearato de magnesio", "Celulosa microcristalina"],
    "ketoprofeno":          ["Ketoprofeno API", "Celulosa microcristalina", "Estearato de magnesio", "Propilenglicol"],
    "meloxicam":            ["Meloxicam API", "Lactosa monohidrato", "Povidona K30", "Crospovidona"],
    "ketorolac":            ["Ketorolac trometamina", "Cloruro de sodio", "Alcohol bencílico", "Agua para inyección"],
    "tramadol":             ["Tramadol clorhidrato", "Celulosa microcristalina", "Estearato de magnesio", "Dióxido de silicio"],
    "metamizol":            ["Metamizol sódico", "Celulosa microcristalina", "Almidón de maíz", "Estearato de magnesio"],
    "aspirina":             ["Ácido acetilsalicílico", "Almidón de maíz", "Celulosa microcristalina", "Estearato de magnesio"],
    "codeina":              ["Codeína fosfato", "Lactosa monohidrato", "Almidón de maíz"],
    "morfina":              ["Morfina sulfato", "Cloruro de sodio", "Agua para inyección"],

    # ── Antibióticos ─────────────────────────────────────────
    "amoxicilina":          ["Amoxicilina trihidrato", "Celulosa microcristalina", "Estearato de magnesio", "Dióxido de silicio coloidal"],
    "ampicilina":           ["Ampicilina trihidrato", "Almidón de maíz", "Estearato de magnesio"],
    "ciprofloxacino":       ["Ciprofloxacino clorhidrato", "Celulosa microcristalina", "Almidón de maíz", "Estearato de magnesio"],
    "azitromicina":         ["Azitromicina dihidrato", "Celulosa microcristalina", "Almidón de maíz", "Estearato de magnesio"],
    "cefalexina":           ["Cefalexina monohidrato", "Almidón de maíz", "Estearato de magnesio", "Dióxido de silicio"],
    "claritromicina":       ["Claritromicina API", "Celulosa microcristalina", "Povidona K30", "Croscarmelosa sódica"],
    "doxiciclina":          ["Doxiciclina hiclato", "Lactosa monohidrato", "Estearato de magnesio", "Celulosa microcristalina"],
    "metronidazol":         ["Metronidazol API", "Celulosa microcristalina", "Almidón de maíz", "Estearato de magnesio"],
    "levofloxacino":        ["Levofloxacino hemimonohidrato", "Celulosa microcristalina", "Croscarmelosa sódica", "Estearato de magnesio"],
    "amoxicilina clavulanico": ["Amoxicilina trihidrato", "Clavulanato de potasio", "Celulosa microcristalina", "Croscarmelosa sódica"],
    "clindamicina":         ["Clindamicina clorhidrato", "Lactosa monohidrato", "Almidón de maíz", "Estearato de magnesio"],
    "ceftriaxona":          ["Ceftriaxona sódica", "Carbonato de sodio", "Agua para inyección"],
    "cefuroxima":           ["Cefuroxima axetilo", "Celulosa microcristalina", "Croscarmelosa sódica"],
    "trimetoprima sulfametoxazol": ["Trimetoprima API", "Sulfametoxazol", "Almidón de maíz", "Povidona K30"],
    "eritromicina":         ["Eritromicina estearato", "Celulosa microcristalina", "Estearato de magnesio"],
    "vancomicina":          ["Vancomicina clorhidrato", "Agua para inyección"],
    "meropenem":            ["Meropenem trihidrato", "Carbonato de sodio", "Agua para inyección"],
    "nitrofurantoina":      ["Nitrofurantoína API", "Lactosa monohidrato", "Almidón de maíz"],

    # ── Cardiovascular ───────────────────────────────────────
    "enalapril":            ["Enalapril maleato", "Lactosa monohidrato", "Almidón de maíz", "Estearato de magnesio"],
    "losartan":             ["Losartán potásico", "Celulosa microcristalina", "Lactosa monohidrato", "Estearato de magnesio"],
    "amlodipino":           ["Amlodipino besilato", "Celulosa microcristalina", "Estearato de magnesio", "Lactosa monohidrato"],
    "atorvastatina":        ["Atorvastatina cálcica", "Celulosa microcristalina", "Lactosa monohidrato", "Croscarmelosa sódica"],
    "metoprolol":           ["Metoprolol tartrato", "Celulosa microcristalina", "Lactosa monohidrato", "Estearato de magnesio"],
    "carvedilol":           ["Carvedilol API", "Lactosa monohidrato", "Celulosa microcristalina", "Crospovidona"],
    "furosemida":           ["Furosemida API", "Lactosa monohidrato", "Almidón de maíz", "Estearato de magnesio"],
    "espironolactona":      ["Espironolactona API", "Celulosa microcristalina", "Estearato de magnesio"],
    "simvastatina":         ["Simvastatina API", "Celulosa microcristalina", "Lactosa monohidrato", "Butilhidroxianisol"],
    "valsartan":            ["Valsartán API", "Celulosa microcristalina", "Crospovidona", "Estearato de magnesio"],
    "ramipril":             ["Ramipril API", "Almidón de maíz", "Lactosa monohidrato", "Estearato de magnesio"],
    "bisoprolol":           ["Bisoprolol fumarato", "Celulosa microcristalina", "Lactosa monohidrato"],
    "digoxina":             ["Digoxina API", "Lactosa monohidrato", "Almidón de maíz"],
    "clopidogrel":          ["Clopidogrel bisulfato", "Celulosa microcristalina", "Crospovidona", "Estearato de magnesio"],
    "warfarina":            ["Warfarina sódica", "Lactosa monohidrato", "Almidón de maíz"],

    # ── Sistema Nervioso ─────────────────────────────────────
    "alprazolam":           ["Alprazolam API", "Lactosa monohidrato", "Celulosa microcristalina", "Estearato de magnesio"],
    "diazepam":             ["Diazepam API", "Lactosa monohidrato", "Almidón de maíz", "Estearato de magnesio"],
    "fluoxetina":           ["Fluoxetina clorhidrato", "Almidón de maíz", "Celulosa microcristalina", "Estearato de magnesio"],
    "sertralina":           ["Sertralina clorhidrato", "Celulosa microcristalina", "Almidón de maíz", "Estearato de magnesio"],
    "clonazepam":           ["Clonazepam API", "Lactosa monohidrato", "Celulosa microcristalina", "Estearato de magnesio"],
    "risperidona":          ["Risperidona API", "Lactosa monohidrato", "Celulosa microcristalina", "Estearato de magnesio"],
    "carbamazepina":        ["Carbamazepina API", "Celulosa microcristalina", "Almidón de maíz", "Propilenglicol"],
    "pregabalina":          ["Pregabalina API", "Lactosa monohidrato", "Almidón de maíz", "Talco farmacéutico"],
    "gabapentina":          ["Gabapentina API", "Lactosa monohidrato", "Almidón de maíz", "Estearato de magnesio"],
    "levodopa":             ["Levodopa API", "Carbidopa API", "Celulosa microcristalina", "Almidón de maíz"],
    "olanzapina":           ["Olanzapina API", "Lactosa monohidrato", "Crospovidona", "Estearato de magnesio"],
    "quetiapina":           ["Quetiapina fumarato", "Celulosa microcristalina", "Lactosa monohidrato"],
    "escitalopram":         ["Escitalopram oxalato", "Celulosa microcristalina", "Croscarmelosa sódica"],
    "venlafaxina":          ["Venlafaxina clorhidrato", "Celulosa microcristalina", "Lactosa monohidrato"],

    # ── Gastrointestinal ─────────────────────────────────────
    "omeprazol":            ["Omeprazol API", "Celulosa microcristalina", "Manitol", "Povidona K30", "Hipromelosa"],
    "pantoprazol":          ["Pantoprazol sódico", "Manitol", "Crospovidona", "Estearato de magnesio"],
    "ranitidina":           ["Ranitidina clorhidrato", "Celulosa microcristalina", "Almidón de maíz"],
    "domperidona":          ["Domperidona API", "Lactosa monohidrato", "Almidón de maíz", "Estearato de magnesio"],
    "metoclopramida":       ["Metoclopramida clorhidrato", "Celulosa microcristalina", "Lactosa monohidrato"],
    "loperamida":           ["Loperamida clorhidrato", "Lactosa monohidrato", "Almidón de maíz"],
    "mesalazina":           ["Mesalazina API", "Celulosa microcristalina", "Croscarmelosa sódica", "Hipromelosa"],
    "esomeprazol":          ["Esomeprazol magnesio", "Celulosa microcristalina", "Manitol", "Hipromelosa"],
    "lansoprazol":          ["Lansoprazol API", "Celulosa microcristalina", "Manitol", "Hipromelosa"],

    # ── Respiratorio / Alergias ──────────────────────────────
    "salbutamol":           ["Salbutamol sulfato", "Lactosa monohidrato", "HFA 134a (propelente)"],
    "budesonida":           ["Budesonida API", "Lactosa monohidrato", "Polisorbato 80"],
    "montelukast":          ["Montelukast sódico", "Celulosa microcristalina", "Croscarmelosa sódica", "Hidroxipropilcelulosa"],
    "cetirizina":           ["Cetirizina dihidroclorhidrato", "Lactosa monohidrato", "Celulosa microcristalina"],
    "loratadina":           ["Loratadina API", "Lactosa monohidrato", "Almidón de maíz", "Estearato de magnesio"],
    "fexofenadina":         ["Fexofenadina clorhidrato", "Celulosa microcristalina", "Croscarmelosa sódica"],
    "bromhexina":           ["Bromhexina clorhidrato", "Lactosa monohidrato", "Almidón de maíz"],
    "ambroxol":             ["Ambroxol clorhidrato", "Celulosa microcristalina", "Lactosa monohidrato"],
    "formoterol":           ["Formoterol fumarato", "Lactosa monohidrato"],
    "salmeterol":           ["Salmeterol xinafoato", "Lactosa monohidrato"],
    "fluticasona":          ["Fluticasona propionato", "Lactosa monohidrato"],
    "ipratropio":           ["Bromuro de ipratropio", "Cloruro de sodio", "Agua para inyección"],

    # ── Diabetes ─────────────────────────────────────────────
    "metformina":           ["Metformina clorhidrato", "Povidona K30", "Estearato de magnesio", "Celulosa microcristalina"],
    "glibenclamida":        ["Glibenclamida API", "Lactosa monohidrato", "Almidón de maíz", "Estearato de magnesio"],
    "glimepirida":          ["Glimepirida API", "Lactosa monohidrato", "Povidona K30", "Croscarmelosa sódica"],
    "sitagliptina":         ["Sitagliptina fosfato", "Celulosa microcristalina", "Croscarmelosa sódica"],
    "empagliflozina":       ["Empagliflozina API", "Celulosa microcristalina", "Lactosa monohidrato"],
    "insulina":             ["Insulina humana rDNA", "Cresol", "Glicerina", "Agua para inyección"],
    "pioglitazona":         ["Pioglitazona clorhidrato", "Lactosa monohidrato", "Celulosa microcristalina"],

    # ── Hormonas / Endocrinología ────────────────────────────
    "levotiroxina":         ["Levotiroxina sódica", "Lactosa monohidrato", "Almidón de maíz", "Estearato de magnesio"],
    "estradiol":            ["Estradiol API", "Lactosa monohidrato", "Celulosa microcristalina"],
    "progesterona":         ["Progesterona micronizada", "Aceite de maní refinado", "Gelatina", "Glicerina"],
    "testosterona":         ["Testosterona undecanoato", "Aceite de ricino", "Alcohol bencílico"],
    "medroxiprogesterona":  ["Medroxiprogesterona acetato", "Celulosa microcristalina", "Polisorb 80"],
    "dexametasona":         ["Dexametasona fosfato sódico", "Agua para inyección", "Cloruro de sodio"],
    "prednisona":           ["Prednisona API", "Lactosa monohidrato", "Almidón de maíz"],
    "hidrocortisona":       ["Hidrocortisona succinato sódico", "Agua para inyección"],
    "betametasona":         ["Betametasona valerato", "Alcohol cetílico", "Vaselina blanca", "Propilenglicol"],

    # ── Dermatología ─────────────────────────────────────────
    "clotrimazol":          ["Clotrimazol API", "Alcohol cetílico", "Polisorbato 60", "Propilenglicol"],
    "terbinafina":          ["Terbinafina clorhidrato", "Celulosa microcristalina", "Croscarmelosa sódica"],
    "ketoconazol":          ["Ketoconazol API", "Celulosa microcristalina", "Almidón de maíz"],
    "tretinoina":           ["Tretinoína API", "Alcohol isopropílico", "BHT", "Carbómero"],
    "adapaleno":            ["Adapaleno API", "Carbómero", "Propilenglicol", "Poloxámero 182"],
    "permetrina":           ["Permetrina API", "Alcohol cetílico", "Propilenglicol"],
    "mupirocina":           ["Mupirocina API", "Polietilenglicol 400", "Polietilenglicol 3350"],
    "aciclovir":            ["Aciclovir API", "Propilenglicol", "Polietilenglicol"],

    # ── Nutraceuticos / Suplementos ──────────────────────────
    "creatina":             ["Creatina monohidrato", "Celulosa microcristalina", "Estearato de magnesio", "Dióxido de silicio"],
    "vitamina c":           ["Ácido ascórbico", "Celulosa microcristalina", "Almidón de maíz", "Estearato de magnesio"],
    "vitamina d":           ["Colecalciferol", "Aceite de girasol", "Gelatina", "Glicerina", "Tocoferol"],
    "vitamina b12":         ["Cianocobalamina", "Celulosa microcristalina", "Lactosa monohidrato"],
    "acido folico":         ["Ácido fólico", "Lactosa monohidrato", "Almidón de maíz", "Estearato de magnesio"],
    "omega 3":              ["Aceite de pescado EPA/DHA", "Gelatina", "Glicerina", "Tocoferol alfa"],
    "magnesio":             ["Óxido de magnesio", "Celulosa microcristalina", "Almidón de maíz"],
    "zinc":                 ["Sulfato de zinc monohidrato", "Celulosa microcristalina", "Almidón de maíz"],
    "hierro":               ["Sulfato ferroso", "Celulosa microcristalina", "Ácido ascórbico"],
    "calcio":               ["Carbonato de calcio", "Celulosa microcristalina", "Almidón de maíz", "Estearato de magnesio"],
    "coenzima q10":         ["Ubidecarenona (CoQ10)", "Aceite de girasol", "Gelatina", "Glicerina"],
    "melatonina":           ["Melatonina API", "Celulosa microcristalina", "Manitol", "Estearato de magnesio"],
    "colágeno":             ["Colágeno hidrolizado", "Ácido ascórbico", "Maltodextrina"],
    "probióticos":          ["Lactobacillus acidophilus", "Maltodextrina", "FOS (fructooligosacáridos)"],
    "proteina whey":        ["Concentrado de proteína de suero", "Lecitina de soya", "Sucralosa"],
    "glucosamina":          ["Glucosamina sulfato", "Celulosa microcristalina", "Croscarmelosa sódica"],
    "acido hialuronico":    ["Ácido hialurónico sódico", "Celulosa microcristalina", "Manitol"],

    # ── Cosmética / Dermocosmética ───────────────────────────
    "retinoide cosmético":  ["Retinol (Vitamina A)", "Propilenglicol", "BHT", "Carbómero"],
    "niacinamida":          ["Niacinamida (Vitamina B3)", "Propilenglicol", "Carbómero", "Glicerina"],
    "acido hialuronico cosmetico": ["Ácido hialurónico sódico LMW", "Glicerina", "Carbómero", "Alantoína"],
    "vitamina c cosmética": ["Ácido L-ascórbico", "Propilenglicol", "Glicerina", "Tocoferol acetato"],
    "filtro solar":         ["Octinoxato", "Avobenzona", "Oxibenzona", "Dióxido de titanio", "Óxido de zinc"],
    "acido salicilico":     ["Ácido salicílico", "Alcohol isopropílico", "Propilenglicol", "Carbómero"],
    "azelaic acid":         ["Ácido azelaico", "Propilenglicol", "Glicerina", "Carbómero"],
    "ceramidas":            ["Ceramida NP", "Ceramida AP", "Colesterol", "Ácido linoleico"],
    "acido glicolico":      ["Ácido glicólico", "Glicerina", "Carbómero", "Propilenglicol"],

    # ── Veterinaria ──────────────────────────────────────────
    "ivermectina":          ["Ivermectina API", "Glicol propilénico", "Alcohol bencílico", "Glicerina formal"],
    "praziquantel":         ["Praziquantel API", "Celulosa microcristalina", "Almidón de maíz", "Estearato de magnesio"],
    "levamisol":            ["Levamisol clorhidrato", "Celulosa microcristalina", "Estearato de magnesio"],
    "doramectina":          ["Doramectina API", "Aceite de sésamo", "Alcohol bencílico"],
    "oxitetraciclina":      ["Oxitetraciclina API", "Propilenglicol", "Povidona K30"],
    "enrofloxacino":        ["Enrofloxacino API", "Celulosa microcristalina", "Almidón de maíz"],
    "florfenicol":          ["Florfenicol API", "Propilenglicol", "Polietilenglicol 400"],
    "tilmicosin":           ["Tilmicosina fosfato", "Propilenglicol", "Alcohol bencílico"],
    "closantel":            ["Closantel API", "Aceite de sésamo", "Benzyl alcohol"],
    "albendazol":           ["Albendazol API", "Celulosa microcristalina", "Almidón de maíz", "Estearato de magnesio"],
    "fipronil":             ["Fipronil API", "Alcohol isopropílico", "BHT"],
    "fluazuron":            ["Fluazuron API", "Aceite mineral", "Poloxámero 401"],
    "marbofloxacino":       ["Marbofloxacino API", "Celulosa microcristalina", "Almidón de maíz"],

    # ── Oncología ────────────────────────────────────────────
    "metotrexato":          ["Metotrexato API", "Cloruro de sodio", "Agua para inyección"],
    "ciclofosfamida":       ["Ciclofosfamida monohidrato", "Manitol", "Agua para inyección"],
    "tamoxifeno":           ["Tamoxifeno citrato", "Celulosa microcristalina", "Almidón de maíz"],

    # ── Antiparasitarios humanos ──────────────────────────────
    "metronidazol antiparasitario": ["Metronidazol API", "Celulosa microcristalina", "Almidón de maíz"],
    "albendazol humano":    ["Albendazol API", "Celulosa microcristalina", "Almidón de maíz", "Laurilsulfato de sodio"],
    "mebendazol":           ["Mebendazol API", "Celulosa microcristalina", "Almidón de maíz"],

    # ── Excipientes universales (referencia) ─────────────────
    "_excipientes_comunes": [
        "Celulosa microcristalina (MCC)", "Lactosa monohidrato", "Almidón de maíz",
        "Estearato de magnesio", "Dióxido de silicio coloidal", "Povidona K30",
        "Croscarmelosa sódica", "Crospovidona", "Hipromelosa (HPMC)",
        "Manitol", "Talco farmacéutico", "Propilenglicol", "Glicerina",
    ],
}

EXCIPIENTES_COMUNES = set(KNOWLEDGE_BASE.get("_excipientes_comunes", []))


def _normalizar(texto: str) -> str:
    """Normaliza texto para matching: minúsculas, sin concentraciones ni formas."""
    texto = texto.lower().strip()
    texto = re.sub(r"\d+[\.,]?\d*\s*(mg|ml|g|mcg|ui|iu|%|µg)", "", texto)
    texto = re.sub(
        r"\b(comprimido|tableta|capsula|cápsula|jarabe|solucion|solución|"
        r"inyectable|crema|gel|suspension|suspensión|gotas|parche|"
        r"supositorio|ovulo|óvulo|polvo|spray|aerosol|colirio|ungüento|"
        r"pomada|locion|loción|champú|shampoo|reconstitui|inyeccion|"
        r"recubierto|masticable|efervescente|sublingual|transdérmico)\b",
        "", texto
    )
    texto = re.sub(r"\s+", " ", texto).strip()
    return texto


def _similitud(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()



# ── Extracción directa del principio activo desde el nombre ──
FORMAS_FARMACEUTICAS = {
    "comprimidos", "comprimido", "capsulas", "capsula", "tabletas", "tableta",
    "jarabe", "suspension", "inyectable", "gotas", "crema", "gel", "aerosol",
    "inhalador", "polvo", "solucion", "unguento", "spray", "parche", "ovulos",
    "supositorios", "masticables", "efervescente", "sublingual", "pour-on",
    "spot-on", "oral", "nasal", "facial", "champu", "retard", "xarope",
    "injetavel", "po", "veterinario", "pediatrico", "intraarticular",
    "nebulizacion", "la",
}
SALES_QUIMICAS = {
    "trihidrato", "clorhidrato", "sodico", "sodica", "potasico", "potasica",
    "sulfato", "fosfato", "acetato", "maleato", "tartrato", "citrato",
    "fumarato", "bisglicinato", "quelado", "micronizada", "monohidrato",
    "hidrocloruro", "mesilato", "besilato", "succinato", "valerato",
    "propionato", "dipropionato", "furoato", "carbonato", "hbr",
}


def extraer_apis_directas(producto: str) -> list:
    """
    Extrae el/los principios activos directamente del nombre del producto.
    "Amoxicilina + Clavulanico 875mg comprimidos" → ["Amoxicilina", "Clavulanico"]
    No infiere ni inventa: solo lo que está escrito.
    """
    t = str.maketrans("áéíóúàèìòùäëïöüñç", "aeiouaeiouaeiounc")
    p = producto.lower().translate(t)
    p = re.sub(r"\(.*?\)|\[.*?\]", " ", p)               # quitar paréntesis
    p = re.sub(r"\d+[.,]?\d*\s*(mg|mcg|ug|g|ml|l|ui|%|x)\b", " ", p)  # dosis
    p = re.sub(r"\b\d+[.,]?\d*\b", " ", p)               # números sueltos
    p = re.sub(r"/\s*\d*\s*(ml|5ml|semana)?", " ", p)

    apis = []
    for parte in re.split(r"\+|,| con | y ", p):
        palabras = [w for w in re.split(r"[^a-z]+", parte) if w]
        nucleo = [w for w in palabras
                  if w not in FORMAS_FARMACEUTICAS
                  and w not in SALES_QUIMICAS
                  and len(w) >= 4]
        if nucleo:
            # Hasta 3 palabras forman el nombre del API (ej: "acido valproico")
            api = " ".join(nucleo[:3]).strip().title()
            if len(api) >= 4:
                apis.append(api)
    return apis


def inferir_apis(productos: list[str], top_n: int = 10) -> list[dict]:
    """
    Dado el listado de productos de un laboratorio, devuelve
    las top N APIs que probablemente necesita.

    Returns:
        Lista de dicts: [{api, frecuencia, es_ifa, productos_origen}]
    """
    from collections import Counter
    api_counter: Counter = Counter()
    api_productos: dict[str, list[str]] = {}
    apis_directas: set = set()

    for producto in productos:
        # ── Extracción DIRECTA (no inventa: lee el nombre del producto) ──
        for api_d in extraer_apis_directas(producto):
            api_counter[api_d] += 1
            apis_directas.add(api_d)
            api_productos.setdefault(api_d, []).append(producto[:60])

        norm = _normalizar(producto)
        apis_encontradas = []

        # 1. Exacto
        if norm in KNOWLEDGE_BASE:
            apis_encontradas = KNOWLEDGE_BASE[norm]
        else:
            # 2. Keyword matching
            mejor_score = 0
            mejor_apis = []
            for key, apis in KNOWLEDGE_BASE.items():
                if key.startswith("_"):
                    continue
                # El nombre del principio activo aparece en el producto
                if key in norm:
                    score = len(key) / max(len(norm), 1)
                    if score > mejor_score:
                        mejor_score = score
                        mejor_apis = apis
                # Similitud fuzzy para variantes
                elif len(key) > 5:
                    sim = _similitud(norm[:len(key)+5], key)
                    if sim > 0.75 and sim > mejor_score:
                        mejor_score = sim
                        mejor_apis = apis

            if mejor_apis:
                apis_encontradas = mejor_apis

        for api in apis_encontradas:
            api_counter[api] += 1
            if api not in api_productos:
                api_productos[api] = []
            api_productos[api].append(producto[:60])

    if not api_counter:
        return []

    # Deduplicar: si la KB sugirió "Amoxicilina trihidrato" y la extracción
    # directa ya tiene "Amoxicilina", quedarse con la directa
    directas_lower = {d.lower() for d in apis_directas}
    resultados = []
    for api, freq in api_counter.most_common():
        al = api.lower()
        if api not in apis_directas and any(
                d in al or al in d for d in directas_lower):
            continue
        es_ifa   = api not in EXCIPIENTES_COMUNES
        directa  = api in apis_directas
        score = freq * (3.0 if directa else (2.0 if es_ifa else 0.4))
        resultados.append({
            "api": api,
            "frecuencia": freq,
            "es_ifa": es_ifa or directa,
            "directa": directa,
            "score": score,
            "productos_origen": " | ".join(api_productos[api][:3]),
        })

    # Ordenar: APIs extraídas directamente del producto SIEMPRE primero,
    # luego por score. Así las primeras columnas del CSV son datos reales.
    resultados.sort(key=lambda x: (not x.get("directa", False), -x["score"]))
    return resultados[:top_n]


# ═══════════════════════════════════════════════════════════════
# PASO 3 — DOMINIO WEB DE CADA LABORATORIO
# ═══════════════════════════════════════════════════════════════


# ── Dominios conocidos de laboratorios grandes ────────────────
DOMINIOS_CONOCIDOS = {
    # Argentina
    "ROEMMERS":       "roemmers.com.ar",
    "BAGO":           "bago.com.ar",
    "ELEA":           "elea.com.ar",
    "PHOENIX":        "elea.com.ar",
    "CASASCO":        "casasco.com.ar",
    "MONTPELLIER":    "montpellier.com.ar",
    "GADOR":          "gador.com.ar",
    "SIDUS":          "sidus.com.ar",
    "CASSARÁ":       "cassara.com.ar",
    "CASSARA":       "cassara.com.ar",
    "NATURAL HERBS": "naturalherbs.com.ar",
    "RAFFO":          "raffo.com.ar",
    "VARIFARMA":      "varifarma.com.ar",
    "VETANCO":        "vetanco.com.ar",
    "HOLLIDAY":       "holliday-scott.com.ar",
    "BERNABO":        "bernabo.com.ar",
    "LKM":            "lkm.com.ar",
    "EUFAR":          "eufar.com.ar",
    "TEMIS":          "temislostaló.com.ar",
    "PHARMOS":        "pharmos.com.ar",
    "FARMACOOP":      "farmacoop.com.ar",
    "OVER":           "over.com.ar",
    "INDUFAR":        "indufar.com.ar",
    "PRATER":         "prater.com.ar",
    "LAZAR":          "lazar.com.ar",
    "DRUG PHARMA":    "drugpharma.com.ar",
    # Chile
    "BESTPHARMA":     "bestpharma.cl",
    "MAVER":          "maver.cl",
    "RECALCINE":      "recalcine.cl",
    "SAVAL":          "saval.cl",
    "BIOSANO":        "biosano.cl",
    "COFAR":          "cofar.cl",
    "ROCNARF":        "rocnarf.cl",
    "KNOP":           "knop.cl",
    # Brasil
    "EMS":            "ems.com.br",
    "MEDLEY":         "medley.com.br",
    "EUROFARMA":      "eurofarma.com.br",
    "TEUTO":          "teuto.com.br",
    "ACHÉ":           "ache.com.br",
    "ACHE":           "ache.com.br",
    "BIOLAB":         "biolab.com.br",
    "CRISTÁLIA":      "cristalia.com.br",
    "CRISTALIA":      "cristalia.com.br",
    "LIBBS":          "libbs.com.br",
    "OUROFINO":       "ourofino.com.br",
    # Colombia
    "GENFAR":         "genfar.com.co",
    "LAPROFF":        "laproff.com.co",
    "TECNOQUÍMICAS":  "tecnoquimicas.com",
    "TECNOQUIMICAS":  "tecnoquimicas.com",
    "NOVAMED":        "novamed.com.co",
    "PROCAPS":        "procaps.com",
    # México
    "PISA":           "pisa.com.mx",
    "SILANES":        "silanes.com.mx",
    "CHINOIN":        "chinoin.com",
    "ARMSTRONG":      "armstrong.com.mx",
    "SENOSIAN":       "senosian.com.mx",
    "BRULUART":       "bruluart.com.mx",
    "LIOMONT":        "liomont.com.mx",
    # Perú
    "AC FARMA":       "acfarma.com.pe",
    "ACFARMA":        "acfarma.com.pe",
    "MEDIFARMA":      "medifarma.com.pe",
    "HERSIL":         "hersil.com.pe",
    "QUÍMICA SUIZA":  "qsuiza.com",
    "AGROVET":        "agrovetmarket.com",
    # Uruguay
    "CELSIUS":        "celsius.com.uy",
    "CLAUSEN":        "clausen.com.uy",
    "URUFARMA":       "urufarma.com.uy",
    "PANALAB":        "panalab.com",
}


def _nombre_a_dominio_candidatos(nombre_lab: str, pais: str) -> list:
    """
    Genera candidatos de dominio a partir del nombre del laboratorio.
    Evita usar nombres propios comunes como dominio.
    """
    PALABRAS_IGNORAR = {
        # Palabras genéricas
        "laboratorio", "laboratorios", "lab", "quimica", "química",
        "farmaceutica", "farmacéutica", "pharmaceutical", "pharma",
        "natural", "herbs", "health", "salud", "vida", "bio",
        # Nombres propios que no sirven como dominio
        "pablo", "juan", "pedro", "jose", "maria", "carlos",
        "luis", "miguel", "antonio", "francisco", "jorge",
        "argentina", "chile", "uruguay", "peru", "colombia", "mexico",
        "brasil", "brazil",
        # Palabras cortas
        "the", "los", "las", "del", "de", "san",
    }

    nombre = nombre_lab.lower()
    for quitar in [
        "s.a.i.c.", "s.a.", "s.r.l.", "s.a.c.", "e.i.r.l.",
        "ltda.", "s.p.a.", "cia.", "inc.", "corp.", "s.r.l",
        "saic", "srl", "sac",
    ]:
        nombre = nombre.replace(quitar, " ")

    nombre = re.sub(r"[^a-z0-9\s]", "", nombre)
    nombre = re.sub(r"\s+", " ", nombre).strip()

    # Filtrar palabras ignoradas y muy cortas
    palabras = [
        p for p in nombre.split()
        if len(p) > 3 and p not in PALABRAS_IGNORAR
    ]

    if not palabras:
        return []

    # TLD por país — resuelve código o nombre libre ("Japón"→.jp)
    ext_pais = {
        "ARG": [".com.ar", ".ar"], "CHL": [".cl"], "URY": [".com.uy", ".uy"],
        "BRA": [".com.br", ".br"], "COL": [".com.co", ".co"], "MEX": [".com.mx", ".mx"],
        "PER": [".com.pe", ".pe"], "ECU": [".com.ec", ".ec"], "BOL": [".com.bo", ".bo"],
        "PRY": [".com.py", ".py"], "VEN": [".com.ve", ".ve"], "CRI": [".co.cr", ".cr"],
        "PAN": [".com.pa", ".pa"], "GTM": [".com.gt", ".gt"], "DOM": [".com.do", ".do"],
        "SLV": [".com.sv", ".sv"], "HND": [".hn"], "NIC": [".com.ni", ".ni"],
        "ESP": [".es"], "USA": [".com", ".us"], "PRT": [".pt"], "ITA": [".it"],
        "JPN": [".co.jp", ".jp"], "DEU": [".de"], "FRA": [".fr"], "GBR": [".co.uk", ".uk"],
        "CHN": [".cn", ".com.cn"], "IND": [".in", ".co.in"], "KOR": [".co.kr", ".kr"],
        "CAN": [".ca"], "AUS": [".com.au", ".au"], "ZAF": [".co.za"],
    }
    try:
        from paso1_laboratorios import resolver_pais
        cod, _ = resolver_pais(pais)
    except Exception:
        cod = pais if pais in ext_pais else None
    # mapa de nombres comunes no curados → TLD
    if not cod:
        _t = str.maketrans("áéíóúàèìòùäëïöüñç", "aeiouaeiouaeiounc")
        n = re.sub(r"[^a-z]", "", (pais or "").lower().translate(_t))
        alias = {"japon": "JPN", "japan": "JPN", "alemania": "DEU", "germany": "DEU",
                 "francia": "FRA", "france": "FRA", "españa": "ESP", "espana": "ESP",
                 "spain": "ESP", "italia": "ITA", "italy": "ITA", "portugal": "PRT",
                 "reinounido": "GBR", "inglaterra": "GBR", "uk": "GBR",
                 "china": "CHN", "india": "IND", "corea": "KOR", "coreadelsur": "KOR",
                 "canada": "CAN", "australia": "AUS", "sudafrica": "ZAF",
                 "estadosunidos": "USA", "usa": "USA"}
        cod = alias.get(n)
    extensiones = ext_pais.get(cod, []) + [".com", ".net"]

    candidatos = []
    primera = palabras[0]
    junto   = "".join(palabras[:2]) if len(palabras) > 1 else primera
    guion   = "-".join(palabras[:2]) if len(palabras) > 1 else primera

    for ext in extensiones:
        candidatos.append(f"{primera}{ext}")
        if junto != primera:
            candidatos.append(f"{junto}{ext}")
            candidatos.append(f"{guion}{ext}")

    return candidatos


# ═══════════════════════════════════════════════════════════════
# COHERENCIA NOMBRE ↔ DOMINIO
# ═══════════════════════════════════════════════════════════════

GENERICAS_DOMINIO = {
    "laboratorio", "laboratorios", "lab", "labs", "pharma", "farma", "pharm",
    "quimica", "farmaceutica", "farmaceutico", "industria", "industrias", "grupo",
    "salud", "health", "natural", "naturals", "the", "del", "los", "las", "san",
    "argentina", "chile", "uruguay", "peru", "colombia", "mexico", "brasil",
    # comunes en inglés/genéricas que NO identifican una empresa
    "life", "home", "homes", "house", "care", "med", "medical", "medic",
    "bio", "vida", "global", "international", "intl", "group", "company",
    "solutions", "services", "products", "trading", "import", "export",
    "del", "san", "santa", "nueva", "new", "best", "prime", "first",
    "world", "global", "corp", "co", "sa", "inc", "and",
}

NOMBRES_PROPIOS = {
    "pablo", "juan", "pedro", "jose", "maria", "carlos", "luis",
    "miguel", "antonio", "francisco", "jorge", "pierre", "paul",
}


def _palabras_distintivas(nombre_lab: str) -> list:
    """Palabras del nombre que identifican a la empresa (ej: 'genfar', 'roemmers', 'cassara')."""
    t = str.maketrans("áéíóúàèìòùäëïöüñç", "aeiouaeiouaeiounc")
    n = nombre_lab.lower().translate(t)
    n = re.sub(r"[^a-z0-9\s]", " ", n)
    todas = [p for p in n.split() if len(p) >= 2 and p not in
             {"sa", "srl", "saic", "sac", "spa", "sas", "ltda", "eirl", "cia", "inc", "de", "del", "la", "el", "y"}]
    palabras = [p for p in todas if len(p) >= 4 and p not in GENERICAS_DOMINIO]
    distintivas = [p for p in palabras if p not in NOMBRES_PROPIOS]
    resultado = distintivas or palabras or todas
    # Agregar combinaciones de palabras consecutivas (ac+farma → acfarma)
    for i in range(len(todas) - 1):
        combo = todas[i] + todas[i + 1]
        if len(combo) >= 5:
            resultado = resultado + [combo]
    return resultado


def tokens_fuertes(nombre_lab: str) -> list:
    """
    Tokens que identifican REALMENTE a la empresa (excluye genéricas
    y nombres propios). Si está vacío, el nombre es demasiado genérico
    (ej: "Laboratorios Life") y no se puede confiar en búsquedas por nombre.
    """
    t = str.maketrans("áéíóúàèìòùäëïöüñç", "aeiouaeiouaeiounc")
    n = re.sub(r"[^a-z0-9\s]", " ", nombre_lab.lower().translate(t))
    out = []
    for p in n.split():
        if len(p) >= 4 and p not in GENERICAS_DOMINIO and p not in NOMBRES_PROPIOS:
            out.append(p)
    return out


def _dominio_coherente(dominio: str, nombre_lab: str) -> bool:
    """
    Valida que el dominio tenga sentido con el nombre de la empresa.
    'pablo.com' para 'LABORATORIO PABLO CASSARÁ' → False.
    'cassara.com.ar' → True. 'genfar.com.co' para GENFAR → True.
    """
    if not dominio or "." not in dominio:
        return False
    base = dominio.split(".")[0].replace("-", "").replace("_", "")
    if len(base) < 3:
        return False
    distintivas = _palabras_distintivas(nombre_lab)
    if not distintivas:
        return False
    for p in distintivas:
        pc = p.replace("-", "")
        if not pc:
            continue
        # Igualdad exacta (genfar == genfar)
        if pc == base:
            return True
        # Palabra al inicio/fin del dominio (bago en bagoarg, si es token fuerte)
        if (base.startswith(pc) or base.endswith(pc)) and \
           (len(pc) >= 5 or len(pc) / len(base) >= 0.5):
            return True
        # Substring sólo si cubre buena parte del dominio
        # ("life"(4) dentro de "villagelifehomes"(16) = 0.25 → RECHAZA)
        if pc in base and len(pc) / len(base) >= 0.55:
            return True
        # Dominio dentro de la palabra, si cubre la mayor parte
        if base in pc and len(base) / len(pc) >= 0.6:
            return True
        # Similitud alta (typos, plurales, abreviaciones largas)
        if len(pc) >= 5 and SequenceMatcher(None, base, pc).ratio() >= 0.8:
            return True
    return False


def _verificar_dominio_vivo(dominio: str) -> bool:
    """HEAD/GET rápido para confirmar que el dominio responde."""
    for url in (f"https://www.{dominio}", f"https://{dominio}",
                f"http://www.{dominio}", f"http://{dominio}"):
        try:
            r = requests.head(url, headers=HEADERS, timeout=4, allow_redirects=True)
            if r.status_code < 400:
                return True
            if r.status_code in (403, 405):  # bloquean HEAD pero existen
                return True
        except Exception:
            continue
    return False


def _buscar_dominio_web(nombre_lab: str, pais: str) -> str:
    """
    Busca el dominio en buscadores web (sin base de datos — sirve
    para cualquier país). Extrae emails y URLs de los resultados y
    devuelve el primer dominio COHERENTE con el nombre.
    """
    EXCLUIR = (
        "linkedin.", "wikipedia.", "facebook.", "instagram.", "twitter.",
        "x.com", "youtube.", "gob.", "gov.", "gub.", "google.", "bing.",
        "duckduckgo.", "mojeek.", "mercadolibre.", "mercadolibre",
        "kompass.", "dnb.com", "bloomberg.", "crunchbase.", "zoominfo.",
        "rocketreach.", "apollo.io", "indeed.", "computrabajo.",
        "glassdoor.", "paginasamarillas", "guiamed", "vademecum",
        "gmail.", "hotmail.", "outlook.", "yahoo.",
    )
    nombre_busqueda = " ".join(_palabras_distintivas(nombre_lab)[:3]) or nombre_lab
    queries = [
        f"{nombre_busqueda} laboratorio sitio oficial contacto email",
        f'"{nombre_lab}" contacto',
        f"{nombre_busqueda} laboratorio {pais} website",
    ]
    buscadores = [
        "https://html.duckduckgo.com/html/?q={}",
        "https://www.bing.com/search?q={}",
        "https://www.mojeek.com/search?q={}",
        "https://www.google.com/search?q={}",
    ]
    candidatos_vistos = []
    for query in queries:
        for buscador in buscadores:
            try:
                url = buscador.format(requests.utils.quote(query))
                r = requests.get(url, headers=HEADERS, timeout=10)
                if r.status_code != 200:
                    continue
                texto = r.text

                # 1) Emails en los resultados → el dominio del email es oro
                for dom in re.findall(
                        r"[a-zA-Z0-9._%+-]+@([a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)+)", texto):
                    dom = dom.lower()
                    if any(e in dom for e in EXCLUIR):
                        continue
                    if _dominio_coherente(dom, nombre_lab):
                        log.info(f"  Dominio via email en web: {dom}")
                        return dom

                # 2) URLs en los resultados
                for dom in re.findall(
                        r"https?://(?:www\.)?([a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)+)", texto):
                    dom = dom.lower()
                    if any(e in dom for e in EXCLUIR):
                        continue
                    if dom in candidatos_vistos:
                        continue
                    candidatos_vistos.append(dom)
                    if _dominio_coherente(dom, nombre_lab):
                        if _verificar_dominio_vivo(dom):
                            log.info(f"  Dominio via búsqueda web: {dom}")
                            return dom
                time.sleep(0.8)
            except Exception as e:
                log.debug(f"  buscador error: {e}")
    return ""


def obtener_dominio(nombre_lab: str, pais: str, hunter=None) -> str:
    """
    Dominio del laboratorio — 4 capas, válido para CUALQUIER país.
    Todo dominio devuelto pasa el chequeo de coherencia con el nombre.

    1. Diccionario de conocidos (instantáneo)
    2. Heurística nombre→dominio + verificación que responde
    3. Búsqueda web (DuckDuckGo/Bing/Mojeek/Google) — emails y URLs
    4. Validación Hunter (si hay candidato dudoso, Hunter confirma
       que el dominio tiene emails corporativos reales)
    """
    # ── Capa 1: conocidos ─────────────────────────────────────
    nombre_upper = nombre_lab.upper()
    for clave, dominio in DOMINIOS_CONOCIDOS.items():
        if clave in nombre_upper:
            log.info(f"  Dominio conocido: {dominio}")
            return dominio

    # ── Capa 2: heurística + HEAD + coherencia ────────────────
    for cand in _nombre_a_dominio_candidatos(nombre_lab, pais):
        if not _dominio_coherente(cand, nombre_lab):
            continue
        if _verificar_dominio_vivo(cand):
            log.info(f"  Dominio heurístico verificado: {cand}")
            return cand

    # ── Capa 3: búsqueda web ──────────────────────────────────
    dom = _buscar_dominio_web(nombre_lab, pais)
    if dom:
        return dom

    # ── Capa 4: heurística + validación Hunter ────────────────
    # (para cuando la red bloquea HEAD pero el dominio existe)
    if hunter is not None:
        for cand in _nombre_a_dominio_candidatos(nombre_lab, pais)[:3]:
            if not _dominio_coherente(cand, nombre_lab):
                continue
            try:
                hd = hunter.domain_search(cand)
                if hd.get("patron") or hd.get("emails"):
                    log.info(f"  Dominio validado por Hunter: {cand}")
                    return cand
            except Exception:
                pass

    log.info(f"  Sin dominio coherente para: {nombre_lab}")
    return ""
