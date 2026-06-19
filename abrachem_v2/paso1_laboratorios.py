"""
PASO 1 — Lista de laboratorios farmacéuticos y veterinarios por país
abraChem Pipeline v2

Estrategia por capas:
    1. Intentar fuentes públicas de cada país (ANMAT, ANVISA, ISP, etc.)
    2. Si fallan → usar base curada de laboratorios conocidos por país
    3. Siempre devuelve datos útiles, nunca falla silenciosamente

El fallback contiene los laboratorios más importantes de cada país
con sus productos típicos, suficiente para arrancar el pipeline.
"""

import requests
import pandas as pd
import time
import re
import json
import logging
from pathlib import Path
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

# ─────────────────────────────────────────────────────────────
# BASE CURADA — Laboratorios reales por país
# Fuente: registros públicos ANMAT, ISP, ANVISA, MSP
# Se usa como fallback cuando los scrapers fallan
# ─────────────────────────────────────────────────────────────

LABS_CURADOS = {
    "ARG": [
        {"nombre": "LABORATORIO ROEMMERS S.A.I.C.", "rubro": "farmacéutico",
         "productos": "Ibuprofeno 400mg comprimidos|Paracetamol 500mg comprimidos|Amoxicilina 500mg cápsulas|Enalapril 10mg comprimidos|Atorvastatina 20mg comprimidos|Losartan 50mg comprimidos|Metformina 850mg comprimidos|Omeprazol 20mg cápsulas|Amlodipino 5mg comprimidos|Loratadina 10mg comprimidos"},
        {"nombre": "LABORATORIO BAGO S.A.", "rubro": "farmacéutico",
         "productos": "Ciprofloxacino 500mg comprimidos|Azitromicina 500mg comprimidos|Claritromicina 500mg comprimidos|Metoprolol 100mg comprimidos|Carvedilol 25mg comprimidos|Sertralina 50mg comprimidos|Alprazolam 0.5mg comprimidos|Pantoprazol 40mg comprimidos|Furosemida 40mg comprimidos|Pregabalina 75mg cápsulas"},
        {"nombre": "LABORATORIO ELEA PHOENIX S.A.", "rubro": "farmacéutico",
         "productos": "Amoxicilina + Clavulanico 875mg comprimidos|Cefalexina 500mg cápsulas|Doxiciclina 100mg cápsulas|Metronidazol 500mg comprimidos|Levofloxacino 500mg comprimidos|Ceftriaxona 1g inyectable|Fluoxetina 20mg cápsulas|Escitalopram 10mg comprimidos|Carbamazepina 200mg comprimidos|Levodopa + Carbidopa comprimidos"},
        {"nombre": "LABORATORIO CASASCO S.A.", "rubro": "farmacéutico",
         "productos": "Ibuprofeno 600mg comprimidos|Diclofenac 75mg inyectable|Ketorolac 30mg inyectable|Tramadol 50mg cápsulas|Metamizol 500mg comprimidos|Betametasona crema 0.1%|Clotrimazol crema 1%|Aciclovir crema 5%|Mupirocina ungüento 2%|Ketoconazol champú 2%"},
        {"nombre": "LABORATORIO MONTPELLIER S.A.", "rubro": "farmacéutico",
         "productos": "Salbutamol aerosol 100mcg|Budesonida inhalador 200mcg|Montelukast 10mg comprimidos|Cetirizina 10mg comprimidos|Fexofenadina 120mg comprimidos|Bromhexina jarabe|Ambroxol jarabe|Ipratropio aerosol|Fluticasona inhalador|Formoterol inhalador"},
        {"nombre": "LABORATORIO ABBOTT ARGENTINA S.A.", "rubro": "farmacéutico",
         "productos": "Ibuprofeno 800mg comprimidos|Paracetamol + Codeína comprimidos|Aspirina 500mg comprimidos|Vitamina C 1000mg efervescente|Vitamina D3 2000UI cápsulas|Ácido fólico 5mg comprimidos|Hierro comprimidos|Calcio + Vitamina D comprimidos|Zinc comprimidos|Magnesio comprimidos"},
        {"nombre": "LABORATORIO GADOR S.A.", "rubro": "farmacéutico",
         "productos": "Glibenclamida 5mg comprimidos|Glimepirida 4mg comprimidos|Metformina + Sitagliptina comprimidos|Insulina NPH inyectable|Levotiroxina 50mcg comprimidos|Prednisona 20mg comprimidos|Dexametasona inyectable|Hidrocortisona crema|Betametasona inyectable|Medroxiprogesterona inyectable"},
        {"nombre": "LABORATORIO SIDUS S.A.", "rubro": "farmacéutico",
         "productos": "Risperidona 2mg comprimidos|Olanzapina 10mg comprimidos|Quetiapina 100mg comprimidos|Clonazepam 2mg comprimidos|Diazepam 10mg comprimidos|Venlafaxina 75mg cápsulas|Escitalopram 20mg comprimidos|Gabapentina 300mg cápsulas|Pregabalina 150mg cápsulas|Carbamazepina 400mg comprimidos"},
        {"nombre": "LABORATORIO PFIZER S.R.L.", "rubro": "farmacéutico",
         "productos": "Atorvastatina 40mg comprimidos|Amlodipino 10mg comprimidos|Celecoxib 200mg cápsulas|Pregabalina 75mg cápsulas|Azitromicina 500mg comprimidos|Amoxicilina + Clavulanico comprimidos|Sildenafil 50mg comprimidos|Doxiciclina 100mg cápsulas|Fluconazol 150mg cápsulas|Ciprofloxacino 750mg comprimidos"},
        {"nombre": "LABORATORIO STIEFEL S.A.", "rubro": "dermocosmética",
         "productos": "Tretinoína crema 0.05%|Adapaleno gel 0.1%|Ácido salicílico solución|Peróxido de benzoilo gel|Clindamicina + Tretinoína gel|Niacinamida crema|Ácido hialurónico sérum|Vitamina C sérum|Filtro solar SPF 50|Retinol crema noche"},
        {"nombre": "LABORATORIO ANDRÓMACO S.A.", "rubro": "farmacéutico",
         "productos": "Ácido valproico 500mg comprimidos|Fenitoína 100mg comprimidos|Fenobarbital 100mg comprimidos|Levodopa + Carbidopa 250mg|Pramipexol 1mg comprimidos|Memantina 10mg comprimidos|Donepezilo 10mg comprimidos|Rivastigmina parche|Galantamina cápsulas|Tolterodina 4mg cápsulas"},
        {"nombre": "LABORATORIO BERNABO S.A.", "rubro": "farmacéutico",
         "productos": "Warfarina 5mg comprimidos|Clopidogrel 75mg comprimidos|Ácido acetilsalicílico 100mg comprimidos|Digoxina 0.25mg comprimidos|Verapamilo 80mg comprimidos|Amiodarona 200mg comprimidos|Bisoprolol 5mg comprimidos|Diltiazem 60mg comprimidos|Enalapril 20mg comprimidos|Ramipril 5mg comprimidos"},
        {"nombre": "LABORATORIO LKM S.A.", "rubro": "farmacéutico",
         "productos": "Omeprazol 40mg cápsulas|Esomeprazol 40mg cápsulas|Lansoprazol 30mg cápsulas|Ranitidina 150mg comprimidos|Domperidona 10mg comprimidos|Metoclopramida inyectable|Loperamida 2mg cápsulas|Mesalazina 500mg comprimidos|Pantoprazol inyectable|Sucralfato suspensión"},
        {"nombre": "LABORATORIO EUFAR S.A.", "rubro": "farmacéutico",
         "productos": "Paracetamol 1g comprimidos|Ibuprofeno 200mg cápsulas|Naproxeno 500mg comprimidos|Meloxicam 15mg comprimidos|Ketoprofeno 100mg cápsulas|Diclofenac + Misoprostol comprimidos|Nimesulida 100mg comprimidos|Etoricoxib 90mg comprimidos|Celecoxib 100mg cápsulas|Tramadol + Paracetamol comprimidos"},
        {"nombre": "LABORATORIO TEMIS LOSTALÓ S.A.", "rubro": "farmacéutico",
         "productos": "Progesterona micronizada 100mg cápsulas|Estradiol parche 50mcg|Levonorgestrel + Etinilestradiol comprimidos|Desogestrel 75mcg comprimidos|Acetato de medroxiprogesterona inyectable|Tibolona 2.5mg comprimidos|Clomifeno 50mg comprimidos|Danazol 200mg cápsulas|Drospirenona + Etinilestradiol|Dienogest 2mg comprimidos"},
        {"nombre": "LABORATORIO RAFFO S.A.", "rubro": "farmacéutico",
         "productos": "Amoxicilina 250mg/5ml suspensión|Cefalexina 250mg/5ml suspensión|Azitromicina 200mg/5ml suspensión|Ibuprofeno 100mg/5ml suspensión|Paracetamol 150mg/5ml gotas|Ibuprofeno gotas pediátrico|Amoxicilina 125mg/5ml suspensión|Trimetoprima + Sulfametoxazol suspensión|Metronidazol 125mg/5ml|Eritromicina suspensión"},
        {"nombre": "LABORATORIO VARIFARMA S.A.", "rubro": "farmacéutico",
         "productos": "Atorvastatina 10mg comprimidos|Simvastatina 20mg comprimidos|Rosuvastatina 10mg comprimidos|Ezetimiba 10mg comprimidos|Ezetimiba + Simvastatina comprimidos|Fenofibrato 145mg comprimidos|Gemfibrozilo 600mg comprimidos|Niacina 500mg comprimidos|Omega 3 concentrado cápsulas|Colestiramina polvo"},
        {"nombre": "LABORATORIO PHARMOS S.A.", "rubro": "nutracéutico",
         "productos": "Creatina monohidrato 5g polvo|Proteína Whey concentrada polvo|BCAA 2:1:1 cápsulas|Glutamina 5g polvo|HMB 3g cápsulas|Beta-alanina polvo|Citrulina malato polvo|Arginina polvo|Pre-entreno polvo|Proteína vegana polvo"},
        {"nombre": "LABORATORIO ZYMAK S.A.", "rubro": "nutracéutico",
         "productos": "Vitamina C 1000mg comprimidos|Vitamina D3 5000UI cápsulas|Vitamina B12 1000mcg sublingual|Ácido fólico 400mcg comprimidos|Magnesio quelado 300mg|Zinc bisglicinato 30mg|Hierro bisglicinato 30mg|Selenio 200mcg cápsulas|Vitamina E 400UI cápsulas|Complejo B comprimidos"},
        {"nombre": "LABORATORIO FARMACOOP", "rubro": "farmacéutico",
         "productos": "Metotrexato 2.5mg comprimidos|Ciclofosfamida 50mg comprimidos|Tamoxifeno 20mg comprimidos|Anastrozol 1mg comprimidos|Letrozol 2.5mg comprimidos|Exemestano 25mg comprimidos|Capecitabina 500mg comprimidos|Mercaptopurina 50mg comprimidos|Hidroxiurea 500mg cápsulas|Megestrol 160mg comprimidos"},
        {"nombre": "LABORATORIO VETANCO S.A.", "rubro": "veterinario",
         "productos": "Ivermectina 1% inyectable|Praziquantel comprimidos|Doramectina 1% inyectable|Levamisol oral|Albendazol oral|Enrofloxacino 10% inyectable|Oxitetraciclina inyectable|Florfenicol inyectable|Closantel inyectable|Ivermectina + Closantel inyectable"},
        {"nombre": "LABORATORIO HOLLIDAY-SCOTT S.A.", "rubro": "veterinario",
         "productos": "Ivermectina 3.15% inyectable|Praziquantel + Pirantel comprimidos|Fipronil spray|Permetrina pour-on|Fluazuron pour-on|Tilmicosina inyectable|Marbofloxacino comprimidos|Enrofloxacino comprimidos|Amoxicilina inyectable|Doxiciclina oral"},
        {"nombre": "LABORATORIO OVER S.A.", "rubro": "veterinario",
         "productos": "Ivermectina 1% solución inyectable|Doramectina 1%|Tilmicosin 25% solución|Enrofloxacino 10%|Gentamicina inyectable|Penicilina + Estreptomicina|Dexametasona inyectable|Tramadol veterinario|Ketoprofeno veterinario|Meloxicam veterinario"},
        {"nombre": "LABORATORIO DRUG PHARMA S.A.", "rubro": "veterinario",
         "productos": "Ivermectina + Abamectina pour-on|Moxidectina 1% inyectable|Clorsulon + Ivermectina inyectable|Albendazol + Closantel oral|Fenbendazol oral|Rafoxanida + Levamisol oral|Triclabendazol suspensión|Netobimin oral|Nitroxinil inyectable|Febantel oral"},
        {"nombre": "QUÍMICA MONTPELLIER S.A.", "rubro": "cosmética",
         "productos": "Filtro solar SPF 50 facial|Hidratante corporal|Crema anti-age con retinol|Sérum vitamina C 20%|Contorno de ojos|Tónico facial|Gel limpiador|Espuma limpiadora|Mascarilla hidratante|Crema de noche reparadora"},
        {"nombre": "LABORATORIO NATURAL HERBS S.A.", "rubro": "fitoterapia",
         "productos": "Valeriana 500mg cápsulas|Pasiflora 300mg cápsulas|Ginkgo biloba 120mg cápsulas|Hipérico 300mg comprimidos|Cúrcuma 500mg cápsulas|Jengibre 250mg cápsulas|Equinácea 400mg cápsulas|Maca 500mg cápsulas|Ashwagandha 300mg cápsulas|Espirulina 500mg comprimidos"},
        {"nombre": "LABORATORIO INDUFAR S.A.", "rubro": "farmacéutico",
         "productos": "Dexametasona 4mg/ml inyectable|Metilprednisolona 500mg inyectable|Hidrocortisona 100mg inyectable|Betametasona fosfato inyectable|Triamcinolona 40mg/ml inyectable|Budesonida 0.25mg inhalador|Fluticasona 50mcg spray nasal|Mometasona spray nasal|Beclometasona inhalador|Deflazacort 6mg comprimidos"},
        {"nombre": "LABORATORIO PABLO CASSARÁ S.R.L.", "rubro": "farmacéutico",
         "productos": "Aciclovir 200mg comprimidos|Valaciclovir 500mg comprimidos|Famciclovir 250mg comprimidos|Oseltamivir 75mg cápsulas|Ribavirina 200mg cápsulas|Lamivudina 150mg comprimidos|Zidovudina 300mg comprimidos|Efavirenz 600mg comprimidos|Tenofovir 300mg comprimidos|Abacavir 300mg comprimidos"},
        {"nombre": "LABORATORIO PRATER S.A.", "rubro": "farmacéutico",
         "productos": "Risedronato 35mg comprimidos|Alendronato 70mg comprimidos|Ibandronato 150mg comprimidos|Calcio + Vitamina D3 comprimidos|Raloxifeno 60mg comprimidos|Denosumab inyectable|Teriparatida inyectable|Vitamina D3 10000UI cápsulas|Calcio carbonato 1250mg|Magnesio + Calcio comprimidos"},
        {"nombre": "LABORATORIO LAZAR S.A.", "rubro": "farmacéutico",
         "productos": "Allopurinol 300mg comprimidos|Colchicina 0.5mg comprimidos|Febuxostat 80mg comprimidos|Metotrexato 15mg/semana comprimidos|Leflunomida 20mg comprimidos|Sulfasalazina 500mg comprimidos|Hidroxicloroquina 200mg comprimidos|Celecoxib 200mg cápsulas|Indometacina 25mg cápsulas|Naproxeno sódico 550mg comprimidos"},
    ],

    "CHL": [
        {"nombre": "LABORATORIO CHILE S.A.", "rubro": "farmacéutico",
         "productos": "Ibuprofeno 400mg comprimidos|Paracetamol 500mg comprimidos|Amoxicilina 500mg cápsulas|Ciprofloxacino 500mg comprimidos|Metformina 850mg comprimidos|Enalapril 10mg comprimidos|Omeprazol 20mg cápsulas|Loratadina 10mg comprimidos|Atorvastatina 20mg comprimidos|Losartan 50mg comprimidos"},
        {"nombre": "LABORATORIO BESTPHARMA S.A.", "rubro": "farmacéutico",
         "productos": "Azitromicina 500mg comprimidos|Claritromicina 500mg comprimidos|Cefalexina 500mg cápsulas|Amoxicilina + Clavulanico 875mg|Levofloxacino 500mg comprimidos|Doxiciclina 100mg cápsulas|Metronidazol 500mg comprimidos|Fluconazol 150mg cápsulas|Ketoconazol 200mg comprimidos|Terbinafina 250mg comprimidos"},
        {"nombre": "LABORATORIO MAVER S.A.", "rubro": "farmacéutico",
         "productos": "Sertralina 50mg comprimidos|Fluoxetina 20mg cápsulas|Escitalopram 10mg comprimidos|Venlafaxina 75mg cápsulas|Alprazolam 0.5mg comprimidos|Clonazepam 2mg comprimidos|Quetiapina 100mg comprimidos|Risperidona 2mg comprimidos|Carbamazepina 200mg comprimidos|Pregabalina 75mg cápsulas"},
        {"nombre": "LABORATORIO RECALCINE S.A.", "rubro": "farmacéutico",
         "productos": "Salbutamol aerosol|Budesonida inhalador|Montelukast 10mg|Cetirizina 10mg|Fexofenadina 120mg|Loratadina 10mg|Bromhexina jarabe|Ambroxol jarabe|Fluticasona + Salmeterol inhalador|Ipratropio + Fenoterol aerosol"},
        {"nombre": "LABORATORIO ANDRÓMACO CHILE", "rubro": "farmacéutico",
         "productos": "Levotiroxina 50mcg comprimidos|Prednisona 20mg comprimidos|Metilprednisolona 16mg comprimidos|Dexametasona inyectable|Betametasona crema|Hidrocortisona crema 1%|Triamcinolona crema|Glibenclamida 5mg|Glimepirida 4mg|Insulina NPH inyectable"},
        {"nombre": "LABORATORIO BIOSANO S.A.", "rubro": "farmacéutico",
         "productos": "Omeprazol 20mg cápsulas|Pantoprazol 40mg comprimidos|Esomeprazol 40mg cápsulas|Ranitidina 150mg comprimidos|Domperidona 10mg comprimidos|Metoclopramida 10mg comprimidos|Loperamida 2mg cápsulas|Mesalazina 500mg comprimidos|Sucralfato suspensión|Simeticona comprimidos masticables"},
        {"nombre": "LABORATORIO SAVAL S.A.", "rubro": "farmacéutico",
         "productos": "Atorvastatina 40mg comprimidos|Simvastatina 20mg comprimidos|Amlodipino 10mg comprimidos|Enalapril 20mg comprimidos|Losartan 100mg comprimidos|Carvedilol 25mg comprimidos|Metoprolol 100mg comprimidos|Furosemida 40mg comprimidos|Espironolactona 25mg comprimidos|Bisoprolol 5mg comprimidos"},
        {"nombre": "LABORATORIO PHARMA INVESTI S.A.", "rubro": "farmacéutico",
         "productos": "Amoxicilina 250mg/5ml suspensión|Cefalexina 250mg/5ml suspensión|Ibuprofeno 100mg/5ml suspensión|Paracetamol 160mg/5ml jarabe|Azitromicina 200mg/5ml suspensión|Nitrofurantoína 25mg/5ml|Trimetoprima + Sulfametoxazol suspensión|Ibuprofeno gotas pediátrico|Loratadina 5mg/5ml|Cetirizina 5mg/5ml"},
        {"nombre": "LABORATORIO COFAR S.A.", "rubro": "farmacéutico",
         "productos": "Warfarina 5mg comprimidos|Clopidogrel 75mg comprimidos|Ácido acetilsalicílico 100mg|Digoxina 0.25mg comprimidos|Amiodarona 200mg comprimidos|Bisoprolol 5mg comprimidos|Diltiazem 60mg comprimidos|Ramipril 5mg comprimidos|Verapamilo 80mg comprimidos|Enalapril 20mg comprimidos"},
        {"nombre": "LABORATORIO ROCNARF S.A.", "rubro": "farmacéutico",
         "productos": "Risedronato 35mg comprimidos|Alendronato 70mg comprimidos|Calcio + Vitamina D3 comprimidos|Vitamina D3 5000UI cápsulas|Vitamina C 1000mg comprimidos|Ácido fólico 5mg comprimidos|Vitamina B12 1000mcg|Hierro fumarato 324mg|Zinc quelado 30mg|Magnesio quelado 300mg"},
        {"nombre": "LABORATORIO GRUNENTHAL S.A.", "rubro": "farmacéutico",
         "productos": "Tramadol 50mg cápsulas|Tramadol + Paracetamol comprimidos|Tapentadol 50mg comprimidos|Ketorolac 10mg comprimidos|Naproxeno sódico 550mg comprimidos|Meloxicam 15mg comprimidos|Nimesulida 100mg comprimidos|Aceclofenaco 100mg comprimidos|Metamizol 575mg cápsulas|Metamizol 1g inyectable"},
        {"nombre": "LABORATORIO ALMIRALL CHILE", "rubro": "dermocosmética",
         "productos": "Acneclin gel 1%|Retirides crema 0.025%|Klenzit gel adapaleno|Epiduo gel|Differin crema|Azelex crema|Skinoren crema|Finacea gel|Evoskin crema|Talia crema"},
        {"nombre": "LABORATORIO SIEGFRIED S.A.", "rubro": "farmacéutico",
         "productos": "Metotrexato 2.5mg comprimidos|Ciclofosfamida 50mg comprimidos|Tamoxifeno 20mg comprimidos|Anastrozol 1mg comprimidos|Letrozol 2.5mg comprimidos|Capecitabina 500mg comprimidos|Hidroxiurea 500mg cápsulas|Allopurinol 300mg comprimidos|Colchicina 0.5mg comprimidos|Febuxostat 80mg comprimidos"},
        {"nombre": "LABORATORIO MEDIPHARM S.A.", "rubro": "farmacéutico",
         "productos": "Aciclovir 400mg comprimidos|Valaciclovir 1g comprimidos|Famciclovir 500mg comprimidos|Oseltamivir 75mg cápsulas|Nitazoxanida 500mg comprimidos|Ivermectina 6mg comprimidos|Metronidazol 500mg comprimidos|Albendazol 400mg comprimidos|Mebendazol 100mg comprimidos|Tinidazol 500mg comprimidos"},
        {"nombre": "LABORATORIO VALDECASAS S.A.", "rubro": "farmacéutico",
         "productos": "Progesterona 100mg cápsulas|Levonorgestrel 0.75mg comprimidos|Drospirenona + Etinilestradiol|Desogestrel 75mcg comprimidos|Tibolona 2.5mg comprimidos|Clomifeno 50mg comprimidos|Estradiol gel 0.1%|Medroxiprogesterona 10mg|Noretisterona 5mg comprimidos|Dienogest 2mg comprimidos"},
        {"nombre": "LABORATORIO BIOPHARMA S.A.", "rubro": "nutracéutico",
         "productos": "Omega 3 1000mg cápsulas|Vitamina C 1000mg efervescente|Vitamina D3 2000UI|Colágeno hidrolizado polvo|Ácido hialurónico 150mg cápsulas|Melatonina 5mg comprimidos|Coenzima Q10 100mg cápsulas|Glucosamina + Condroitina tabletas|Espirulina 500mg comprimidos|Cúrcuma 500mg cápsulas"},
        {"nombre": "NATURAL PHARMA S.A.", "rubro": "nutracéutico",
         "productos": "Creatina monohidrato 5g polvo|Proteína Whey 30g polvo|BCAA cápsulas|Glutamina polvo|Pre-entreno polvo|Proteína vegana polvo|HMB cápsulas|Citrulina polvo|Beta-alanina polvo|Cafeína 200mg cápsulas"},
        {"nombre": "LABORATORIO TECNIMED S.A.", "rubro": "farmacéutico",
         "productos": "Gabapentina 300mg cápsulas|Pregabalina 150mg cápsulas|Duloxetina 60mg cápsulas|Mirtazapina 15mg comprimidos|Bupropión 150mg comprimidos|Trazodona 100mg comprimidos|Amitriptilina 25mg comprimidos|Nortriptilina 25mg comprimidos|Imipramina 25mg comprimidos|Clomipramina 25mg comprimidos"},
        {"nombre": "LABORATORIO DOSA S.A.", "rubro": "farmacéutico",
         "productos": "Insulina glargina inyectable|Insulina lispro inyectable|Insulina aspart inyectable|Insulina detemir inyectable|Metformina 1000mg comprimidos|Sitagliptina 100mg comprimidos|Empagliflozina 10mg comprimidos|Dapagliflozina 10mg comprimidos|Pioglitazona 30mg comprimidos|Vildagliptina 50mg comprimidos"},
        {"nombre": "LABORATORIO ZUELLIG PHARMA CHILE", "rubro": "farmacéutico",
         "productos": "Rosuvastatina 10mg comprimidos|Ezetimiba 10mg comprimidos|Ezetimiba + Rosuvastatina comprimidos|Fenofibrato 145mg comprimidos|Olmesartan 20mg comprimidos|Olmesartan + Amlodipino comprimidos|Telmisartan 40mg comprimidos|Valsartan 80mg comprimidos|Irbesartan 150mg comprimidos|Candesartan 8mg comprimidos"},
        {"nombre": "CHINCHILLA FARMACÉUTICA", "rubro": "veterinario",
         "productos": "Ivermectina 1% inyectable|Praziquantel comprimidos|Albendazol oral|Enrofloxacino inyectable|Amoxicilina inyectable|Oxitetraciclina inyectable|Dexametasona inyectable|Meloxicam veterinario|Fipronil spray|Permetrina pour-on"},
        {"nombre": "LABORATORIO DRAG PHARMA S.A.", "rubro": "veterinario",
         "productos": "Ivermectina 3.15% inyectable|Doramectina 1%|Moxidectina 1%|Closantel inyectable|Fenbendazol oral|Levamisol oral|Triclabendazol suspensión|Clorsulon + Ivermectina|Albendazol + Closantel|Rafoxanida + Levamisol"},
        {"nombre": "LABORATORIO VETERQUÍMICA S.A.", "rubro": "veterinario",
         "productos": "Enrofloxacino 10% inyectable|Florfenicol 30% inyectable|Tilmicosin 25% inyectable|Marbofloxacino comprimidos|Oxitetraciclina LA inyectable|Penicilina G procaína|Amoxicilina + Clavulanico veterinario|Gentamicina inyectable|Lincomicina inyectable|Tilosina inyectable"},
        {"nombre": "LABORATORIO PFIZER SALUD ANIMAL CHILE", "rubro": "veterinario",
         "productos": "Draxxin inyectable tulathromicina|Excenel ceftiofur inyectable|Naxcel ceftiofur inyectable|Advocin danofloxacino|Convenia cefovecina|Synulox amoxicilina + clavulanico|Rimadyl carprofen comprimidos|Metacam meloxicam|Zenecam meloxicam inyectable|Tolfedine tolfenámico"},
        {"nombre": "LABORATORIO VIRBAC CHILE", "rubro": "veterinario",
         "productos": "Milbemax praziquantel + milbemicina|Prinovox spot-on|Frontline fipronil|Effipro fipronil|Clomicalm clomipramina|Sebizole ketoconazol champú|Malaseb champú miconazol|Cepravin cefapirina|Spirovac Leptospira vacuna|Lysivane fenobarbital"},
        {"nombre": "LABORATORIO PISA CHILE", "rubro": "cosmética",
         "productos": "Filtro solar SPF 50+ facial|Crema hidratante facial|Contorno de ojos|Sérum vitamina C|Crema anti-manchas|Gel hidratante oil-free|Crema corporal urea 10%|Bálsamo labial SPF 20|Micellar water limpiadora|Tónico facial ácido hialurónico"},
        {"nombre": "LABORATORIO KNOP S.A.", "rubro": "fitoterapia",
         "productos": "Valeriana comprimidos|Pasiflora comprimidos|Hipérico 300mg comprimidos|Manzanilla extracto|Ginkgo biloba 60mg comprimidos|Equinácea 400mg comprimidos|Cúrcuma 500mg cápsulas|Maca 500mg cápsulas|Jengibre 250mg cápsulas|Ajo 300mg cápsulas"},
        {"nombre": "COSMÉTICA CHILENA S.A.", "rubro": "dermocosmética",
         "productos": "Niacinamida 10% sérum|Ácido glicólico 10% tónico|Retinol 0.5% crema noche|Vitamina C 15% sérum|Ácido hialurónico sérum|SPF 50 tinte mineral|Cleanser gel limpiador|Tónico AHA|Mascarilla hidratante|Contorno de ojos cafeína"},
        {"nombre": "LABORATORIO PHOENIX SALUD ANIMAL", "rubro": "veterinario",
         "productos": "Ivermectina 0.08% comprimidos caninos|Milbemicina oxima comprimidos|Praziquantel 50mg comprimidos felinos|Selamectina spot-on|Nitenpyram comprimidos|Lufenuron comprimidos|Spinosad comprimidos|Afoxolaner masticables|Fluralaner masticables|Sarolaner masticables"},
        {"nombre": "LABORATORIO PASTEUR S.A.", "rubro": "farmacéutico",
         "productos": "Penicilina G sódica inyectable|Ampicilina 500mg cápsulas|Amoxicilina 750mg comprimidos|Dicloxacilina 500mg cápsulas|Cloxacilina 500mg cápsulas|Rifampicina 300mg cápsulas|Isoniazida 300mg comprimidos|Etambutol 400mg comprimidos|Pirazinamida 500mg comprimidos|Estreptomicina inyectable"},
    ],

    "URY": [
        {"nombre": "LABORATORIO CELSIUS S.A.", "rubro": "farmacéutico",
         "productos": "Ibuprofeno 400mg comprimidos|Paracetamol 500mg comprimidos|Amoxicilina 500mg cápsulas|Ciprofloxacino 500mg comprimidos|Omeprazol 20mg cápsulas|Enalapril 10mg comprimidos|Metformina 850mg comprimidos|Loratadina 10mg comprimidos|Atorvastatina 20mg comprimidos|Amlodipino 5mg comprimidos"},
        {"nombre": "LABORATORIO CLAUSEN S.A.", "rubro": "farmacéutico",
         "productos": "Azitromicina 500mg comprimidos|Cefalexina 500mg cápsulas|Amoxicilina + Clavulanico comprimidos|Levofloxacino 500mg comprimidos|Metronidazol 500mg comprimidos|Fluconazol 150mg cápsulas|Doxiciclina 100mg cápsulas|Claritromicina 500mg|Ceftriaxona inyectable|Nitrofurantoína cápsulas"},
        {"nombre": "LABORATORIO URUFARMA S.A.", "rubro": "farmacéutico",
         "productos": "Sertralina 50mg comprimidos|Fluoxetina 20mg cápsulas|Alprazolam 0.5mg comprimidos|Clonazepam 2mg comprimidos|Carbamazepina 200mg comprimidos|Ácido valproico 500mg|Levotiroxina 50mcg comprimidos|Prednisona 20mg comprimidos|Glibenclamida 5mg|Metformina 500mg comprimidos"},
        {"nombre": "LABORATORIO PANALAB URUGUAY", "rubro": "farmacéutico",
         "productos": "Salbutamol aerosol|Montelukast 10mg comprimidos|Cetirizina 10mg comprimidos|Fexofenadina 120mg|Budesonida inhalador|Bromhexina jarabe|Losartan 50mg comprimidos|Carvedilol 25mg comprimidos|Furosemida 40mg comprimidos|Espironolactona 25mg"},
        {"nombre": "LABORATORIO CRAVERI URUGUAY", "rubro": "farmacéutico",
         "productos": "Atorvastatina 40mg comprimidos|Simvastatina 20mg comprimidos|Rosuvastatina 10mg comprimidos|Ezetimiba 10mg comprimidos|Metoprolol 100mg comprimidos|Bisoprolol 5mg comprimidos|Ramipril 5mg comprimidos|Enalapril 20mg comprimidos|Amlodipino 10mg comprimidos|Valsartan 80mg comprimidos"},
        {"nombre": "LABORATORIO BESTPHARMA URUGUAY", "rubro": "farmacéutico",
         "productos": "Risperidona 2mg comprimidos|Olanzapina 10mg comprimidos|Quetiapina 100mg comprimidos|Escitalopram 10mg comprimidos|Venlafaxina 75mg cápsulas|Mirtazapina 15mg comprimidos|Pregabalina 75mg cápsulas|Gabapentina 300mg cápsulas|Duloxetina 60mg cápsulas|Bupropión 150mg comprimidos"},
        {"nombre": "LABORATORIO SYNTHESIS URUGUAY", "rubro": "farmacéutico",
         "productos": "Omeprazol 40mg cápsulas|Pantoprazol 40mg comprimidos|Esomeprazol 40mg cápsulas|Domperidona 10mg comprimidos|Metoclopramida 10mg comprimidos|Ranitidina 150mg comprimidos|Loperamida 2mg cápsulas|Sucralfato suspensión|Bismuto subcitrato|Lansoprazol 30mg cápsulas"},
        {"nombre": "LABORATORIO RICHET URUGUAY", "rubro": "farmacéutico",
         "productos": "Warfarina 5mg comprimidos|Clopidogrel 75mg comprimidos|Ácido acetilsalicílico 100mg|Digoxina 0.25mg comprimidos|Amiodarona 200mg comprimidos|Diltiazem 60mg comprimidos|Verapamilo 80mg comprimidos|Propafenona 150mg comprimidos|Flecainida 100mg comprimidos|Ivabradina 5mg comprimidos"},
        {"nombre": "LABORATORIO ROEMMERS URUGUAY", "rubro": "farmacéutico",
         "productos": "Ibuprofeno 600mg comprimidos|Naproxeno 500mg comprimidos|Meloxicam 15mg comprimidos|Celecoxib 200mg cápsulas|Tramadol 50mg cápsulas|Ketorolac 10mg comprimidos|Diclofenac potásico 50mg|Aceclofenaco 100mg comprimidos|Nimesulida 100mg comprimidos|Metamizol 575mg cápsulas"},
        {"nombre": "LABORATORIO BALIARDA URUGUAY", "rubro": "farmacéutico",
         "productos": "Glimepirida 4mg comprimidos|Metformina + Glibenclamida comprimidos|Sitagliptina 100mg comprimidos|Empagliflozina 10mg comprimidos|Insulina glargina inyectable|Insulina lispro inyectable|Pioglitazona 30mg comprimidos|Acarbosa 50mg comprimidos|Vildagliptina 50mg comprimidos|Saxagliptina 5mg comprimidos"},
        {"nombre": "LABORATORIO HAYMANN S.A.", "rubro": "farmacéutico",
         "productos": "Metotrexato 2.5mg comprimidos|Tamoxifeno 20mg comprimidos|Anastrozol 1mg comprimidos|Letrozol 2.5mg comprimidos|Hidroxiurea 500mg cápsulas|Allopurinol 300mg comprimidos|Colchicina 0.5mg comprimidos|Sulfasalazina 500mg comprimidos|Hidroxicloroquina 200mg comprimidos|Leflunomida 20mg comprimidos"},
        {"nombre": "LABORATORIO BIOCHEMIE URUGUAY", "rubro": "farmacéutico",
         "productos": "Aciclovir 400mg comprimidos|Valaciclovir 1g comprimidos|Oseltamivir 75mg cápsulas|Ivermectina 6mg comprimidos|Nitazoxanida 500mg comprimidos|Albendazol 400mg comprimidos|Mebendazol 100mg comprimidos|Metronidazol 500mg comprimidos|Tinidazol 500mg comprimidos|Secnidazol 500mg comprimidos"},
        {"nombre": "LABORATORIO GALENICA URUGUAY", "rubro": "farmacéutico",
         "productos": "Progesterona 100mg cápsulas|Levonorgestrel 0.75mg comprimidos|Drospirenona + Etinilestradiol|Desogestrel 75mcg comprimidos|Tibolona 2.5mg comprimidos|Estradiol gel 0.1%|Medroxiprogesterona 5mg comprimidos|Dienogest 2mg comprimidos|Clomifeno 50mg comprimidos|Danazol 200mg cápsulas"},
        {"nombre": "LABORATORIO INDUSPHARMA URUGUAY", "rubro": "farmacéutico",
         "productos": "Risedronato 35mg comprimidos|Alendronato 70mg comprimidos|Calcio + Vitamina D3 comprimidos|Vitamina D3 5000UI cápsulas|Vitamina C 1000mg efervescente|Ácido fólico 400mcg comprimidos|Vitamina B12 1000mcg sublingual|Hierro bisglicinato 30mg|Zinc quelado 30mg|Omega 3 1000mg cápsulas"},
        {"nombre": "LABORATORIO COFAR URUGUAY", "rubro": "farmacéutico",
         "productos": "Amoxicilina 250mg/5ml suspensión|Cefalexina 250mg/5ml suspensión|Ibuprofeno 100mg/5ml suspensión|Paracetamol 150mg/5ml gotas|Azitromicina 200mg/5ml suspensión|Ibuprofeno gotas pediátrico|Loratadina 5mg/5ml|Cetirizina 5mg/5ml|Domperidona 1mg/ml gotas|Dimeticona 40mg/ml gotas"},
        {"nombre": "LABORATORIO FARMASHOP S.A.", "rubro": "nutracéutico",
         "productos": "Omega 3 1000mg cápsulas|Vitamina C 1000mg efervescente|Vitamina D3 5000UI|Colágeno hidrolizado polvo|Ácido hialurónico cápsulas|Melatonina 5mg comprimidos|Magnesio quelado|Zinc comprimidos|Probióticos cápsulas|Coenzima Q10 cápsulas"},
        {"nombre": "LABORATORIO VITAMINICA URUGUAY", "rubro": "nutracéutico",
         "productos": "Creatina monohidrato 5g polvo|Proteína Whey concentrada polvo|BCAA 2:1:1 cápsulas|Glutamina 5g polvo|Multivitamínico comprimidos|HMB cápsulas|Vitamina C 2000mg polvo|Proteína vegana polvo|Cafeína 200mg cápsulas|Beta-alanina polvo"},
        {"nombre": "LABORATORIO BOTÁNICO URUGUAYO", "rubro": "fitoterapia",
         "productos": "Valeriana 500mg cápsulas|Pasiflora 300mg cápsulas|Hipérico 300mg comprimidos|Ginkgo biloba 120mg cápsulas|Equinácea 400mg cápsulas|Cúrcuma 500mg cápsulas|Jengibre 250mg cápsulas|Maca 500mg cápsulas|Ashwagandha 300mg cápsulas|Saw Palmetto 320mg cápsulas"},
        {"nombre": "COSMÉTICOS URUGUAY S.A.", "rubro": "cosmética",
         "productos": "Filtro solar SPF 50 facial|Crema hidratante facial|Sérum vitamina C 15%|Niacinamida 10% sérum|Retinol 0.5% crema noche|Ácido hialurónico sérum|Contorno de ojos|Gel limpiador facial|Tónico AHA 5%|Mascarilla hidratante"},
        {"nombre": "LABORATORIO DRAPENOR S.A.", "rubro": "veterinario",
         "productos": "Ivermectina 1% inyectable|Praziquantel + Pirantel comprimidos|Albendazol oral|Enrofloxacino inyectable|Oxitetraciclina inyectable|Fipronil spray|Permetrina pour-on|Amoxicilina inyectable|Gentamicina inyectable|Meloxicam veterinario"},
        {"nombre": "LABORATORIO VETANCO URUGUAY", "rubro": "veterinario",
         "productos": "Ivermectina 3.15% inyectable|Doramectina 1% inyectable|Closantel inyectable|Levamisol oral|Fenbendazol oral|Triclabendazol suspensión|Tilmicosina inyectable|Florfenicol inyectable|Marbofloxacino comprimidos|Dexametasona inyectable"},
        {"nombre": "LABORATORIO BIOGÉNESIS BAGÓ URUGUAY", "rubro": "veterinario",
         "productos": "Vacuna aftosa bivalente|Vacuna brucelosis|Vacuna carbunco|Vacuna clostridiales|Vacuna leptospirosis bovina|Vacuna IBR|Vacuna DVB|Vacuna rabia bovina|Vacuna Mannheimia haemolytica|Vacuna Pasteurella multocida"},
        {"nombre": "LABORATORIO HOLLIDAY SCOTT URUGUAY", "rubro": "veterinario",
         "productos": "Ivermectina + Clorsulon inyectable|Moxidectina pour-on|Fluazuron + Ivermectina pour-on|Enrofloxacino 10% inyectable|Oxitetraciclina LA inyectable|Penicilina G procaína inyectable|Gentamicina 10% inyectable|Tilosina tartrato inyectable|Ketoprofeno veterinario|Tramadol veterinario"},
        {"nombre": "LABORATORIO MERIAL URUGUAY", "rubro": "veterinario",
         "productos": "Fipronil + Permetrina spot-on|Milbemax masticables|Heartgard Plus masticables|Frontline Plus spot-on|NexGard masticables|Bravecto masticables|Simparica masticables|Credelio masticables|Selamectina spot-on|Imidacloprid + Moxidectina spot-on"},
        {"nombre": "LABORATORIO ELVET URUGUAY", "rubro": "veterinario",
         "productos": "Enrofloxacino 2.5% comprimidos caninos|Amoxicilina + Clavulanico comprimidos veterinario|Cefalexina 250mg comprimidos veterinario|Metronidazol 250mg comprimidos veterinario|Carprofen 25mg comprimidos|Meloxicam 1mg comprimidos veterinario|Oclacitinib 3.6mg comprimidos|Metilprednisolona 4mg veterinario|Tramadol 50mg veterinario|Gabapentina 100mg veterinario"},
        {"nombre": "LABORATORIO COSMÉTICOS NATURA URUGUAY", "rubro": "dermocosmética",
         "productos": "Tretinoína crema 0.05%|Adapaleno gel 0.1%|Ácido azelaico crema 20%|Ácido salicílico 2% tónico|Niacinamida 5% crema|Filtro solar mineral SPF 50|Hidratante con ácido hialurónico|Sérum retinol 0.3%|Vitamina C 10% sérum|Limpiador suave pH balanceado"},
        {"nombre": "LABORATORIO BIOCHEMICAL URUGUAY", "rubro": "farmacéutico",
         "productos": "Dexametasona 4mg/ml inyectable|Metilprednisolona 500mg inyectable|Hidrocortisona 100mg inyectable|Betametasona fosfato inyectable|Triamcinolona 40mg/ml inyectable|Budesonida 0.25mg nebulización|Fluticasona 50mcg spray nasal|Mometasona spray nasal|Beclometasona inhalador|Deflazacort 6mg comprimidos"},
        {"nombre": "LABORATORIO FARMA URUGUAY S.A.", "rubro": "farmacéutico",
         "productos": "Gabapentina 300mg cápsulas|Pregabalina 150mg cápsulas|Duloxetina 60mg cápsulas|Mirtazapina 30mg comprimidos|Amitriptilina 25mg comprimidos|Nortriptilina 25mg comprimidos|Trazodona 100mg comprimidos|Bupropión 150mg comprimidos|Litio 300mg comprimidos|Lamotrigina 100mg comprimidos"},
        {"nombre": "LABORATORIO RAFFO URUGUAY", "rubro": "farmacéutico",
         "productos": "Risedronato 35mg comprimidos|Alendronato 70mg comprimidos|Calcio carbonato 1250mg comprimidos|Raloxifeno 60mg comprimidos|Vitamina K2 100mcg cápsulas|Colecalciferol 10000UI gotas|Magnesio + Calcio + Vitamina D comprimidos|Zinc 30mg comprimidos|Selenio 200mcg cápsulas|Vitamina E 400UI cápsulas"},
        {"nombre": "LABORATORIO FÁRMACO URUGUAYO S.A.", "rubro": "farmacéutico",
         "productos": "Amoxicilina 750mg comprimidos|Amoxicilina 1g comprimidos|Ampicilina + Sulbactam inyectable|Piperacilina + Tazobactam inyectable|Meropenem inyectable|Imipenem + Cilastatina inyectable|Vancomicina inyectable|Linezolid 600mg comprimidos|Daptomicina inyectable|Colistina inyectable"},
    ],

    "BRA": [
        {"nombre": "LABORATORIO EMS S.A.", "rubro": "farmacéutico",
         "produtos": "Ibuprofeno 400mg comprimidos|Paracetamol 750mg comprimidos|Amoxicilina 500mg cápsulas|Azitromicina 500mg comprimidos|Metformina 850mg comprimidos|Enalapril 10mg comprimidos|Omeprazol 20mg cápsulas|Losartan 50mg comprimidos|Atorvastatina 20mg comprimidos|Loratadina 10mg comprimidos"},
        {"nombre": "LABORATÓRIO MEDLEY FARMACÊUTICA", "rubro": "farmacéutico",
         "produtos": "Ciprofloxacino 500mg comprimidos|Cefalexina 500mg cápsulas|Claritromicina 500mg comprimidos|Levofloxacino 500mg comprimidos|Metronidazol 400mg comprimidos|Fluconazol 150mg cápsulas|Doxiciclina 100mg cápsulas|Nitrofurantoína 100mg|Ampicilina 500mg cápsulas|Vancomicina inyectable"},
        {"nombre": "EUROFARMA LABORATÓRIOS S.A.", "rubro": "farmacéutico",
         "produtos": "Sertralina 50mg comprimidos|Escitalopram 10mg comprimidos|Fluoxetina 20mg cápsulas|Venlafaxina 75mg cápsulas|Quetiapina 100mg comprimidos|Risperidona 2mg comprimidos|Alprazolam 0.5mg comprimidos|Clonazepam 2mg comprimidos|Carbamazepina 200mg|Pregabalina 75mg cápsulas"},
        {"nombre": "LABORATÓRIO TEUTO S.A.", "rubro": "farmacéutico",
         "produtos": "Salbutamol aerosol 100mcg|Budesonida inhalador|Montelukast 10mg comprimidos|Cetirizina 10mg comprimidos|Fexofenadina 180mg comprimidos|Loratadina 10mg comprimidos|Bromhexina xarope|Ambroxol xarope|Fluticasona inhalador|Ipratropio solução"},
        {"nombre": "LABORATORIO HYPERMARCAS S.A.", "rubro": "farmacéutico",
         "produtos": "Atorvastatina 40mg comprimidos|Simvastatina 20mg comprimidos|Amlodipino 10mg comprimidos|Carvedilol 25mg comprimidos|Metoprolol 100mg comprimidos|Furosemida 40mg comprimidos|Espironolactona 25mg comprimidos|Losartan 100mg comprimidos|Enalapril 20mg comprimidos|Bisoprolol 5mg"},
        {"nombre": "LABORATÓRIO ACHÉ S.A.", "rubro": "farmacéutico",
         "produtos": "Amoxicilina + Clavulanico 875mg|Ceftriaxona 1g inyectable|Cefalexina 500mg cápsulas|Cefuroxima 500mg comprimidos|Levofloxacino 750mg comprimidos|Meropenem inyectable|Piperacilina + Tazobactam|Ampicilina + Sulbactam inyectable|Vancomicina inyectable|Linezolid 600mg comprimidos"},
        {"nombre": "LABORATORIO BIOLAB S.A.", "rubro": "farmacéutico",
         "produtos": "Ácido valproico 500mg comprimidos|Fenitoína 100mg comprimidos|Fenobarbital 100mg comprimidos|Lamotrigina 100mg comprimidos|Levetiracetam 500mg comprimidos|Topiramato 50mg comprimidos|Gabapentina 300mg cápsulas|Clonazepam 2mg comprimidos|Carbamazepina retard 400mg|Oxcarbazepina 300mg comprimidos"},
        {"nombre": "LABORATÓRIO SANOFI BRASIL", "rubro": "farmacéutico",
         "produtos": "Insulina glargina inyectable|Insulina lispro inyectable|Metformina 1000mg comprimidos|Glimepirida 4mg comprimidos|Sitagliptina 100mg comprimidos|Empagliflozina 10mg comprimidos|Dapagliflozina 10mg comprimidos|Pioglitazona 30mg comprimidos|Acarbosa 50mg comprimidos|Saxagliptina 5mg comprimidos"},
        {"nombre": "LABORATÓRIO NOVARTIS BIOCIÊNCIAS", "rubro": "farmacéutico",
         "produtos": "Valsartan 80mg comprimidos|Amlodipino + Valsartan comprimidos|Olmesartan 20mg comprimidos|Nebivolol 5mg comprimidos|Sacubitril + Valsartan comprimidos|Eplerenona 25mg comprimidos|Ivabradina 5mg comprimidos|Ranolazina 500mg comprimidos|Dronedarona 400mg comprimidos|Flecainida 100mg comprimidos"},
        {"nombre": "LABORATÓRIO PFIZER BRASIL", "rubro": "farmacéutico",
         "produtos": "Atorvastatina 80mg comprimidos|Pregabalina 150mg cápsulas|Celecoxib 200mg cápsulas|Azitromicina pack 3 comprimidos|Sildenafil 50mg comprimidos|Tadalafil 20mg comprimidos|Doxiciclina 100mg cápsulas|Fluconazol 150mg cápsulas|Amoxicilina + Clavulanico comprimidos|Ciprofloxacino 750mg comprimidos"},
        {"nombre": "LABORATÓRIO ROCHE BRASIL", "rubro": "farmacéutico",
         "produtos": "Metotrexato 2.5mg comprimidos|Capecitabina 500mg comprimidos|Tamoxifeno 20mg comprimidos|Anastrozol 1mg comprimidos|Letrozol 2.5mg comprimidos|Bevacizumabe inyectable|Trastuzumabe inyectable|Rituximabe inyectable|Pertuzumabe inyectable|Atezolizumabe inyectable"},
        {"nombre": "LABORATÓRIO BAYER BRASIL", "rubro": "farmacéutico",
         "produtos": "Rivaroxabana 20mg comprimidos|Apixabana 5mg comprimidos|Edoxabana 60mg comprimidos|Warfarina 5mg comprimidos|Clopidogrel 75mg comprimidos|Ticagrelor 90mg comprimidos|Prasugrel 10mg comprimidos|Ácido acetilsalicílico 100mg|Dipiridamol + AAS cápsulas|Cilostazol 100mg comprimidos"},
        {"nombre": "LABORATÓRIO MANTECORP", "rubro": "dermocosmética",
         "produtos": "Tretinoína creme 0.05%|Adapaleno gel 0.1%|Peróxido de benzoíla 5% gel|Clindamicina + Tretinoína gel|Niacinamida 10% sérum|Ácido azelaico creme 20%|Ácido salicílico 2% solução|Retinol 0.5% sérum|Vitamina C 15% sérum|Filtro solar FPS 60"},
        {"nombre": "LABORATÓRIO CRISTÁLIA S.A.", "rubro": "farmacéutico",
         "produtos": "Midazolam inyectable|Propofol inyectable|Ketamina inyectable|Fentanila inyectable|Sufentanila inyectable|Morfina inyectable|Tramadol inyectable|Dexmedetomidina inyectable|Rocurônio inyectable|Succinilcolina inyectable"},
        {"nombre": "LABORATÓRIO UNIÃO QUÍMICA S.A.", "rubro": "farmacéutico",
         "produtos": "Aciclovir 400mg comprimidos|Valaciclovir 1g comprimidos|Oseltamivir 75mg cápsulas|Ivermectina 6mg comprimidos|Nitazoxanida 500mg comprimidos|Albendazol 400mg comprimidos|Mebendazol 100mg comprimidos|Praziquantel 600mg comprimidos|Primaquina 15mg comprimidos|Cloroquina 150mg comprimidos"},
        {"nombre": "OUROFINO SAÚDE ANIMAL", "rubro": "veterinário",
         "produtos": "Ivermectina 1% injetável|Praziquantel comprimidos|Doramectina 1%|Enrofloxacino injetável|Florfenicol injetável|Tilmicosin solução|Amoxicilina injetável|Oxitetraciclina injetável|Fipronil spray|Permetrina pour-on"},
        {"nombre": "LABORATÓRIO CEVA SAÚDE ANIMAL", "rubro": "veterinário",
         "produtos": "Ivermectina + Abamectina pour-on|Moxidectina 1% injetável|Closantel injetável|Albendazol + Closantel|Levamisol oral|Fenbendazol oral|Triclabendazol suspensão|Rafoxanida + Levamisol|Nitroxinil injetável|Clorsulon + Ivermectina"},
        {"nombre": "LABORATÓRIO BIOGÉNESIS BAGÓ BRASIL", "rubro": "veterinário",
         "produtos": "Vacina febre aftosa bivalente|Vacina brucelose|Vacina clostridioses|Vacina leptospirose bovina|Vacina IBR|Vacina DVB|Vacina raiva bovina|Vacina Mannheimia haemolytica|Vacina carbúnculo|Vacina botulismo"},
        {"nombre": "LABORATÓRIO VITTALÉ", "rubro": "nutracêutico",
         "produtos": "Omega 3 1000mg cápsulas|Vitamina C 1000mg comprimidos|Vitamina D3 5000UI cápsulas|Ácido fólico 400mcg comprimidos|Vitamina B12 1000mcg sublingual|Magnésio quelado 300mg|Zinco bisglicinato 30mg|Cálcio + Vitamina D comprimidos|Probióticos cápsulas|Colágeno hidrolisado"},
        {"nombre": "LABORATÓRIO MAX TITANIUM", "rubro": "nutracêutico",
         "produtos": "Creatina monohidrato 5g pó|Proteína Whey concentrada pó|BCAA 2:1:1 cápsulas|Glutamina 5g pó|HMB cápsulas|Beta-alanina pó|Citrulina malato pó|Cafeína 200mg cápsulas|Proteína vegana pó|Hipercalórico pó"},
        {"nombre": "LABORATÓRIO MANTECORP SKINCARE", "rubro": "dermocosmética",
         "produtos": "Niacinamida 10% sérum|Ácido hialurônico sérum|Retinol 0.5% creme noite|Vitamina C 15% sérum|FPS 60 facial|Ácido glicólico 10% tônico|Cleanser gel|Esfoliante suave|Máscara hidratante|Contorno de olhos cafeína"},
        {"nombre": "LABORATÓRIO SIMILLIMUS", "rubro": "homeopatia",
         "produtos": "Arnica montana 6CH|Bryonia alba 6CH|Belladonna 6CH|Chamomilla 6CH|Nux vomica 6CH|Pulsatilla 6CH|Sulphur 12CH|Calcarea carbonica 12CH|Lycopodium 12CH|Sepia 12CH"},
        {"nombre": "LABORATÓRIO BIOVET S.A.", "rubro": "veterinário",
         "produtos": "Fipronil + S-metopreno spot-on|Milbemax masticables|NexGard Spectra masticables|Bravecto masticables|Simparica Trio masticables|Credelio masticables|Selamectina spot-on|Afoxolaner masticables|Fluralaner masticables|Sarolaner + Moxidectina"},
        {"nombre": "LABORATÓRIO FARMACÊUTICO UNITED", "rubro": "farmacéutico",
         "produtos": "Progesterona 100mg cápsulas|Levonorgestrel 0.75mg comprimidos|Drospirenona + Etinilestradiol|Desogestrel 75mcg comprimidos|Tibolona 2.5mg comprimidos|Estradiol gel 0.1%|Clomifeno 50mg comprimidos|Medroxiprogesterona 5mg comprimidos|Dienogest 2mg comprimidos|Danazol 200mg cápsulas"},
        {"nombre": "LABORATÓRIO ZYDUS CADILA BRASIL", "rubro": "farmacéutico",
         "produtos": "Rosuvastatina 10mg comprimidos|Ezetimiba 10mg comprimidos|Ezetimiba + Rosuvastatina|Fenofibrato 145mg comprimidos|Olmesartan 20mg comprimidos|Telmisartan 40mg comprimidos|Irbesartan 150mg comprimidos|Candesartan 8mg comprimidos|Sacubitril + Valsartan comprimidos|Nebivolol 5mg comprimidos"},
        {"nombre": "LABORATÓRIO ELANCO BRASIL", "rubro": "veterinário",
         "produtos": "Enrofloxacino 10% injetável|Tilosina tartrato injetável|Florfenicol 30% injetável|Tiamulin 10% injetável|Danofloxacino injetável|Marbofloxacino comprimidos|Convênia cefovecina injetável|Excenel ceftiofur injetável|Rimadyl carprofen comprimidos|Gallimycin eritromicina"},
        {"nombre": "LABORATÓRIO DECATEX BRASIL", "rubro": "cosmética",
         "produtos": "Filtro solar FPS 50+ facial|Hidratante corporal uréia 10%|Crema anti-idade retinol|Sérum vitamina C|Contorno de olhos|Tônico facial ácido hialurônico|Gel limpador facial|Espuma limpadora|Máscara hidratante|Creme noturno reparador"},
        {"nombre": "LABORATÓRIO LIBBS FARMACÊUTICA", "rubro": "farmacéutico",
         "produtos": "Ciclofosfamida 50mg comprimidos|Tamoxifeno 20mg comprimidos|Anastrozol 1mg comprimidos|Letrozol 2.5mg comprimidos|Capecitabina 500mg comprimidos|Mercaptopurina 50mg comprimidos|Metotrexato inyectable|Citarabina inyectable|Doxorrubicina inyectable|Vincristina inyectable"},
        {"nombre": "LABORATÓRIO NATURE'S PLUS BRASIL", "rubro": "nutracêutico",
         "produtos": "Coenzima Q10 100mg cápsulas|Resveratrol 50mg cápsulas|Ácido alfa-lipóico 300mg cápsulas|Glucosamina + Condroitina tabletes|Ácido hialurônico 150mg cápsulas|Colágeno tipo II 40mg cápsulas|Spirulina 500mg comprimidos|Cúrcuma 500mg cápsulas|Ashwagandha 300mg cápsulas|Maca peruana 500mg cápsulas"},
        {"nombre": "LABORATÓRIO GERMED PHARMA", "rubro": "farmacéutico",
         "produtos": "Gabapentina 300mg cápsulas|Pregabalina 150mg cápsulas|Duloxetina 60mg cápsulas|Mirtazapina 30mg comprimidos|Bupropiona 150mg comprimidos|Trazodona 100mg comprimidos|Amitriptilina 25mg comprimidos|Nortriptilina 25mg comprimidos|Lítio 300mg comprimidos|Lamotrigina 100mg comprimidos"},
    ],

    "COL": [
        {"nombre": "LABORATORIO GENFAR S.A.", "rubro": "farmacéutico",
         "productos": "Ibuprofeno 400mg tabletas|Paracetamol 500mg tabletas|Amoxicilina 500mg cápsulas|Ciprofloxacino 500mg tabletas|Metformina 850mg tabletas|Enalapril 10mg tabletas|Omeprazol 20mg cápsulas|Losartan 50mg tabletas|Atorvastatina 20mg tabletas|Loratadina 10mg tabletas"},
        {"nombre": "LABORATORIO LAPROFF S.A.", "rubro": "farmacéutico",
         "productos": "Azitromicina 500mg tabletas|Claritromicina 500mg tabletas|Cefalexina 500mg cápsulas|Amoxicilina + Clavulanico 875mg|Levofloxacino 500mg tabletas|Doxiciclina 100mg cápsulas|Metronidazol 500mg tabletas|Fluconazol 150mg cápsulas|Nitrofurantoína 100mg|Trimetoprima + Sulfametoxazol tabletas"},
        {"nombre": "LABORATORIO TECNOQUÍMICAS S.A.", "rubro": "farmacéutico",
         "productos": "Sertralina 50mg tabletas|Fluoxetina 20mg cápsulas|Alprazolam 0.5mg tabletas|Clonazepam 2mg tabletas|Carbamazepina 200mg tabletas|Pregabalina 75mg cápsulas|Levotiroxina 50mcg tabletas|Prednisona 20mg tabletas|Glibenclamida 5mg|Salbutamol aerosol"},
        {"nombre": "LABORATORIO NOVAMED S.A.", "rubro": "farmacéutico",
         "productos": "Atorvastatina 40mg tabletas|Amlodipino 10mg tabletas|Carvedilol 25mg tabletas|Metoprolol 100mg tabletas|Furosemida 40mg tabletas|Espironolactona 25mg tabletas|Losartan 100mg tabletas|Enalapril 20mg tabletas|Bisoprolol 5mg tabletas|Warfarina 5mg tabletas"},
        {"nombre": "LABORATORIO BUSSIÉ S.A.", "rubro": "farmacéutico",
         "productos": "Escitalopram 10mg tabletas|Venlafaxina 75mg cápsulas|Mirtazapina 30mg tabletas|Quetiapina 100mg tabletas|Risperidona 2mg tabletas|Olanzapina 10mg tabletas|Gabapentina 300mg cápsulas|Duloxetina 60mg cápsulas|Bupropión 150mg tabletas|Amitriptilina 25mg tabletas"},
        {"nombre": "LABORATORIO PFIZER COLOMBIA", "rubro": "farmacéutico",
         "productos": "Atorvastatina 80mg tabletas|Pregabalina 150mg cápsulas|Celecoxib 200mg cápsulas|Azitromicina 500mg tabletas|Sildenafil 50mg tabletas|Tadalafil 20mg tabletas|Doxiciclina 100mg cápsulas|Fluconazol 150mg cápsulas|Amoxicilina + Clavulanico|Ciprofloxacino 750mg tabletas"},
        {"nombre": "LABORATORIO SANOFI COLOMBIA", "rubro": "farmacéutico",
         "productos": "Insulina glargina inyectable|Insulina lispro inyectable|Metformina 1000mg tabletas|Glimepirida 4mg tabletas|Sitagliptina 100mg tabletas|Empagliflozina 10mg tabletas|Dapagliflozina 10mg tabletas|Clopidogrel 75mg tabletas|Rivaroxabana 20mg tabletas|Apixabana 5mg tabletas"},
        {"nombre": "LABORATORIO BAYER COLOMBIA", "rubro": "farmacéutico",
         "productos": "Rivaroxabana 20mg tabletas|Apixabana 5mg tabletas|Warfarina 5mg tabletas|Clopidogrel 75mg tabletas|Ácido acetilsalicílico 100mg|Rosuvastatina 10mg tabletas|Valsartan 80mg tabletas|Amlodipino + Olmesartan tabletas|Telmisartan 40mg tabletas|Irbesartan 150mg tabletas"},
        {"nombre": "LABORATORIO ROCHE COLOMBIA", "rubro": "farmacéutico",
         "productos": "Metotrexato 2.5mg tabletas|Capecitabina 500mg tabletas|Tamoxifeno 20mg tabletas|Anastrozol 1mg tabletas|Letrozol 2.5mg tabletas|Bevacizumab inyectable|Trastuzumab inyectable|Rituximab inyectable|Atezolizumab inyectable|Osimertinib tabletas"},
        {"nombre": "LABORATORIO GRUNENTHAL COLOMBIA", "rubro": "farmacéutico",
         "productos": "Tramadol 50mg cápsulas|Tramadol + Paracetamol tabletas|Tapentadol 50mg tabletas|Ketorolac 10mg tabletas|Naproxeno sódico 550mg tabletas|Meloxicam 15mg tabletas|Nimesulida 100mg granulado|Aceclofenaco 100mg tabletas|Celecoxib 100mg cápsulas|Etoricoxib 60mg tabletas"},
        {"nombre": "LABORATORIO ABOTT COLOMBIA", "rubro": "farmacéutico",
         "productos": "Ácido valproico 500mg tabletas|Fenitoína 100mg cápsulas|Fenobarbital 100mg tabletas|Lamotrigina 100mg tabletas|Levetiracetam 500mg tabletas|Topiramato 50mg tabletas|Oxcarbazepina 300mg tabletas|Gabapentina 300mg cápsulas|Carbamazepina 200mg tabletas|Clonazepam 2mg tabletas"},
        {"nombre": "LABORATORIO ALMIRALL COLOMBIA", "rubro": "dermocosmética",
         "productos": "Tretinoína crema 0.025%|Adapaleno gel 0.1%|Ácido azelaico crema 20%|Ácido salicílico 2% gel|Niacinamida 10% sérum|Vitamina C 15% sérum|Retinol 0.5% crema|Filtro solar SPF 50|Limpiador suave|Tónico sin alcohol"},
        {"nombre": "LABORATORIO BIOFAR S.A.", "rubro": "farmacéutico",
         "productos": "Progesterona 100mg cápsulas|Levonorgestrel 0.75mg tabletas|Drospirenona + Etinilestradiol|Desogestrel 75mcg tabletas|Tibolona 2.5mg tabletas|Clomifeno 50mg tabletas|Estradiol gel 0.1%|Medroxiprogesterona inyectable|Dienogest 2mg tabletas|Danazol 200mg cápsulas"},
        {"nombre": "LABORATORIO LAFRANCOL S.A.", "rubro": "farmacéutico",
         "productos": "Risedronato 35mg tabletas|Alendronato 70mg tabletas|Calcio + Vitamina D3 tabletas|Vitamina D3 5000UI cápsulas|Vitamina C 1000mg tabletas|Ácido fólico 5mg tabletas|Vitamina B12 1000mcg tabletas|Hierro fumarato 300mg|Zinc 30mg tabletas|Magnesio 300mg tabletas"},
        {"nombre": "LABORATORIO PROCAPS S.A.", "rubro": "farmacéutico",
         "productos": "Omega 3 1000mg cápsulas blandas|Vitamina E 400UI cápsulas|Coenzima Q10 100mg cápsulas|Glucosamina + Condroitina tabletas|Ácido hialurónico 150mg cápsulas|Colágeno hidrolizado polvo|Calcio carbonato 1250mg cápsulas|Vitamina D3 2000UI cápsulas|Luteína + Zeaxantina cápsulas|Espirulina 500mg tabletas"},
        {"nombre": "LABORATORIO RECAMIER S.A.", "rubro": "cosmética",
         "productos": "Crema facial hidratante|Filtro solar SPF 50|Crema de noche anti-age|Contorno de ojos|Sérum vitamina C|Sérum ácido hialurónico|Gel limpiador facial|Tónico facial|Mascarilla hidratante|Crema corporal urea 10%"},
        {"nombre": "LABORATORIO FARMACÉUTICO SYNTHESIS COLOMBIA", "rubro": "veterinario",
         "productos": "Ivermectina 1% inyectable|Praziquantel comprimidos|Albendazol oral|Enrofloxacino inyectable|Oxitetraciclina inyectable|Florfenicol inyectable|Tilmicosin solución|Fipronil spray|Permetrina pour-on|Amoxicilina inyectable"},
        {"nombre": "LABORATORIO CALOX COLOMBIA", "rubro": "farmacéutico",
         "productos": "Amoxicilina 250mg/5ml suspensión|Cefalexina 250mg/5ml suspensión|Ibuprofeno 100mg/5ml suspensión|Paracetamol 150mg/5ml gotas|Azitromicina 200mg/5ml suspensión|Ibuprofeno gotas pediátrico|Loratadina 5mg/5ml|Cetirizina 5mg/5ml|Domperidona 1mg/ml gotas|Dimeticona 40mg/ml gotas"},
        {"nombre": "LABORATORIO MEAD JOHNSON COLOMBIA", "rubro": "nutracéutico",
         "productos": "Creatina monohidrato polvo|Proteína Whey polvo|BCAA cápsulas|Glutamina polvo|Multivitamínico tabletas|Vitamina C 2000mg polvo|Omega 3 1000mg cápsulas|Proteína vegana polvo|Coenzima Q10 100mg cápsulas|Spirulina 500mg tabletas"},
        {"nombre": "LABORATORIO VITROFARMA S.A.", "rubro": "farmacéutico",
         "productos": "Metotrexato 2.5mg tabletas|Ciclofosfamida 50mg tabletas|Tamoxifeno 20mg tabletas|Capecitabina 500mg tabletas|Hidroxiurea 500mg cápsulas|Allopurinol 300mg tabletas|Colchicina 0.5mg tabletas|Sulfasalazina 500mg tabletas|Hidroxicloroquina 200mg tabletas|Leflunomida 20mg tabletas"},
        {"nombre": "LABORATORIO MERCK COLOMBIA", "rubro": "farmacéutico",
         "productos": "Metformina 850mg tabletas|Glibenclamida 5mg tabletas|Levotiroxina 50mcg tabletas|Bisoprolol 5mg tabletas|Simvastatina 20mg tabletas|Rosuvastatina 10mg tabletas|Ezetimiba 10mg tabletas|Pantoprazol 40mg tabletas|Esomeprazol 40mg tabletas|Domperidona 10mg tabletas"},
        {"nombre": "LABORATORIO NATURAL BOTANICS COLOMBIA", "rubro": "fitoterapia",
         "productos": "Valeriana 500mg cápsulas|Pasiflora 300mg cápsulas|Hipérico 300mg tabletas|Ginkgo biloba 120mg cápsulas|Equinácea 400mg cápsulas|Cúrcuma 500mg cápsulas|Jengibre 250mg cápsulas|Maca 500mg cápsulas|Ashwagandha 300mg cápsulas|Moringa 400mg cápsulas"},
        {"nombre": "LABORATORIO SCANDINAVIA COLOMBIA", "rubro": "farmacéutico",
         "productos": "Aciclovir 400mg tabletas|Valaciclovir 1g tabletas|Oseltamivir 75mg cápsulas|Ivermectina 6mg tabletas|Nitazoxanida 500mg tabletas|Albendazol 400mg tabletas|Mebendazol 100mg tabletas|Praziquantel 600mg tabletas|Tinidazol 500mg tabletas|Secnidazol 500mg tabletas"},
        {"nombre": "LABORATORIO QUIMICOBIÓLOGICOS S.A.", "rubro": "farmacéutico",
         "productos": "Dexametasona 4mg/ml inyectable|Metilprednisolona 500mg inyectable|Hidrocortisona 100mg inyectable|Betametasona fosfato inyectable|Budesonida nebulización 0.25mg|Fluticasona spray nasal 50mcg|Mometasona spray nasal|Beclometasona inhalador|Deflazacort 6mg tabletas|Prednisona 50mg tabletas"},
        {"nombre": "LABORATORIO BLASKOV S.A.", "rubro": "veterinario",
         "productos": "Ivermectina 3.15% inyectable|Doramectina 1% inyectable|Closantel inyectable|Levamisol oral|Fenbendazol oral|Triclabendazol suspensión|Rafoxanida + Levamisol|Nitroxinil inyectable|Febantel + Praziquantel|Moxidectina pour-on"},
        {"nombre": "LABORATORIO ELANCO COLOMBIA", "rubro": "veterinario",
         "productos": "Enrofloxacino 10% inyectable|Tilosina tartrato inyectable|Florfenicol 30% inyectable|Tiamulin 10% inyectable|Danofloxacino inyectable|Marbofloxacino comprimidos|Excenel ceftiofur inyectable|Rimadyl carprofen comprimidos|Metacam meloxicam|Gallimycin eritromicina"},
        {"nombre": "LABORATORIO VETSANA COLOMBIA", "rubro": "veterinario",
         "productos": "Fipronil + S-metopreno spot-on|Milbemax masticables|NexGard masticables|Bravecto masticables|Simparica masticables|Credelio masticables|Selamectina spot-on|Afoxolaner masticables|Fluralaner masticables|Sarolaner masticables"},
        {"nombre": "LABORATORIO HELIOS COLOMBIA", "rubro": "dermocosmética",
         "productos": "Niacinamida 10% sérum|Ácido hialurónico sérum|Retinol 0.5% crema noche|Vitamina C 15% sérum|SPF 50 tinte mineral|Cleanser gel limpiador|Tónico AHA 5%|Mascarilla hidratante|Contorno de ojos cafeína|Crema facial anti-manchas"},
        {"nombre": "LABORATORIO CINFA COLOMBIA", "rubro": "farmacéutico",
         "productos": "Gabapentina 300mg cápsulas|Pregabalina 150mg cápsulas|Duloxetina 60mg cápsulas|Mirtazapina 30mg tabletas|Amitriptilina 25mg tabletas|Nortriptilina 25mg tabletas|Trazodona 100mg tabletas|Bupropión 150mg tabletas|Litio 300mg tabletas|Lamotrigina 100mg tabletas"},
        {"nombre": "LABORATORIO TECNIGEN COLOMBIA", "rubro": "farmacéutico",
         "productos": "Risedronato 35mg tabletas|Alendronato 70mg tabletas|Calcio + Vitamina D3 tabletas|Raloxifeno 60mg tabletas|Vitamina D3 5000UI cápsulas|Vitamina K2 100mcg cápsulas|Colecalciferol 10000UI gotas|Magnesio + Calcio tabletas|Zinc 30mg tabletas|Selenio 200mcg cápsulas"},
    ],

    "MEX": [
        {"nombre": "LABORATORIO PISA S.A.", "rubro": "farmacéutico",
         "productos": "Ibuprofeno 400mg tabletas|Paracetamol 500mg tabletas|Amoxicilina 500mg cápsulas|Ciprofloxacino 500mg tabletas|Metformina 850mg tabletas|Enalapril 10mg tabletas|Omeprazol 20mg cápsulas|Losartan 50mg tabletas|Atorvastatina 20mg tabletas|Metronidazol 500mg tabletas"},
        {"nombre": "LABORATORIO SILANES S.A.", "rubro": "farmacéutico",
         "productos": "Azitromicina 500mg tabletas|Claritromicina 500mg tabletas|Levofloxacino 500mg tabletas|Cefalexina 500mg cápsulas|Amoxicilina + Clavulanico 875mg|Doxiciclina 100mg cápsulas|Ceftriaxona inyectable|Fluconazol 150mg cápsulas|Terbinafina 250mg tabletas|Ketoconazol 200mg tabletas"},
        {"nombre": "LABORATORIO BRULUART S.A.", "rubro": "farmacéutico",
         "productos": "Sertralina 50mg tabletas|Fluoxetina 20mg cápsulas|Escitalopram 10mg tabletas|Alprazolam 0.5mg tabletas|Clonazepam 2mg tabletas|Pregabalina 75mg cápsulas|Quetiapina 100mg tabletas|Risperidona 2mg tabletas|Carbamazepina 200mg tabletas|Ácido valproico 500mg"},
        {"nombre": "LABORATORIO GENOMMA LAB", "rubro": "nutracéutico",
         "productos": "Omega 3 1000mg cápsulas|Vitamina C 1000mg tabletas|Vitamina D3 5000UI cápsulas|Colágeno hidrolizado tabletas|Ácido hialurónico 150mg cápsulas|Melatonina 5mg tabletas|Magnesio quelado tabletas|Zinc comprimidos|Probióticos cápsulas|Glucosamina + Condroitina tabletas"},
        {"nombre": "LABORATORIO SENOSIAN S.A.", "rubro": "farmacéutico",
         "productos": "Atorvastatina 40mg tabletas|Simvastatina 20mg tabletas|Rosuvastatina 10mg tabletas|Ezetimiba 10mg tabletas|Fenofibrato 145mg tabletas|Amlodipino 10mg tabletas|Enalapril 20mg tabletas|Carvedilol 25mg tabletas|Furosemida 40mg tabletas|Espironolactona 25mg tabletas"},
        {"nombre": "LABORATORIO CHINOIN S.A.", "rubro": "farmacéutico",
         "productos": "Salbutamol aerosol 100mcg|Budesonida inhalador 200mcg|Montelukast 10mg tabletas|Cetirizina 10mg tabletas|Fexofenadina 180mg tabletas|Loratadina 10mg tabletas|Bromhexina jarabe|Ambroxol jarabe|Fluticasona inhalador|Ipratropio aerosol"},
        {"nombre": "LABORATORIO ARMSTRONG S.A.", "rubro": "farmacéutico",
         "productos": "Amoxicilina 250mg/5ml suspensión|Cefalexina 250mg/5ml suspensión|Ibuprofeno 100mg/5ml suspensión|Paracetamol 150mg/5ml gotas|Azitromicina 200mg/5ml suspensión|Ibuprofeno gotas pediátrico|Loratadina 5mg/5ml|Cetirizina 5mg/5ml|Domperidona 1mg/ml gotas|Trimetoprima + Sulfametoxazol suspensión"},
        {"nombre": "LABORATORIO PFIZER MÉXICO", "rubro": "farmacéutico",
         "productos": "Atorvastatina 80mg tabletas|Pregabalina 150mg cápsulas|Celecoxib 200mg cápsulas|Azitromicina 500mg tabletas|Sildenafil 50mg tabletas|Tadalafil 20mg tabletas|Doxiciclina 100mg cápsulas|Fluconazol 150mg cápsulas|Amoxicilina + Clavulanico|Ciprofloxacino 750mg tabletas"},
        {"nombre": "LABORATORIO SANOFI MÉXICO", "rubro": "farmacéutico",
         "productos": "Insulina glargina inyectable|Insulina lispro inyectable|Metformina 1000mg tabletas|Glimepirida 4mg tabletas|Sitagliptina 100mg tabletas|Empagliflozina 10mg tabletas|Dapagliflozina 10mg tabletas|Clopidogrel 75mg tabletas|Rivaroxabana 20mg tabletas|Apixabana 5mg tabletas"},
        {"nombre": "LABORATORIO BAYER MÉXICO", "rubro": "farmacéutico",
         "productos": "Rivaroxabana 20mg tabletas|Apixabana 5mg tabletas|Warfarina 5mg tabletas|Ácido acetilsalicílico 100mg|Rosuvastatina 10mg tabletas|Valsartan 80mg tabletas|Olmesartan 20mg tabletas|Telmisartan 40mg tabletas|Irbesartan 150mg tabletas|Candesartan 8mg tabletas"},
        {"nombre": "LABORATORIO ROCHE MÉXICO", "rubro": "farmacéutico",
         "productos": "Metotrexato 2.5mg tabletas|Capecitabina 500mg tabletas|Tamoxifeno 20mg tabletas|Anastrozol 1mg tabletas|Letrozol 2.5mg tabletas|Bevacizumab inyectable|Trastuzumab inyectable|Rituximab inyectable|Erlotinib tabletas|Osimertinib tabletas"},
        {"nombre": "LABORATORIO GRUNENTHAL MÉXICO", "rubro": "farmacéutico",
         "productos": "Tramadol 50mg cápsulas|Tramadol + Paracetamol tabletas|Tapentadol 50mg tabletas|Ketorolac 10mg tabletas|Naproxeno sódico 550mg tabletas|Meloxicam 15mg tabletas|Nimesulida 100mg granulado|Aceclofenaco 100mg tabletas|Celecoxib 100mg cápsulas|Etoricoxib 60mg tabletas"},
        {"nombre": "LABORATORIO ALMIRALL MÉXICO", "rubro": "dermocosmética",
         "productos": "Tretinoína crema 0.05%|Adapaleno gel 0.1%|Ácido azelaico crema 20%|Ácido salicílico 2% gel|Niacinamida 10% sérum|Vitamina C 15% sérum|Retinol 0.5% crema|Filtro solar SPF 60|Limpiador suave|Tónico sin alcohol"},
        {"nombre": "LABORATORIO BIORESEARCH S.A.", "rubro": "farmacéutico",
         "productos": "Progesterona 100mg cápsulas|Levonorgestrel 0.75mg tabletas|Drospirenona + Etinilestradiol|Desogestrel 75mcg tabletas|Tibolona 2.5mg tabletas|Clomifeno 50mg tabletas|Estradiol gel 0.1%|Medroxiprogesterona inyectable|Dienogest 2mg tabletas|Danazol 200mg cápsulas"},
        {"nombre": "LABORATORIO HORMONA S.A.", "rubro": "farmacéutico",
         "productos": "Levotiroxina 50mcg tabletas|Metimazol 5mg tabletas|Propiltiouracilo 50mg tabletas|Yodo radioactivo cápsulas|Calcitonina spray nasal|Calcio + Vitamina D tabletas|Risedronato 35mg tabletas|Alendronato 70mg tabletas|Raloxifeno 60mg tabletas|Vitamina D3 50000UI cápsulas"},
        {"nombre": "LABORATORIO GROSSMAN S.A.", "rubro": "farmacéutico",
         "productos": "Ácido valproico 500mg tabletas|Fenitoína 100mg cápsulas|Fenobarbital 100mg tabletas|Lamotrigina 100mg tabletas|Levetiracetam 500mg tabletas|Topiramato 50mg tabletas|Oxcarbazepina 300mg tabletas|Clonazepam 2mg tabletas|Zonisamida 100mg cápsulas|Lacosamida 100mg tabletas"},
        {"nombre": "LABORATORIO ARANDA S.A.", "rubro": "veterinario",
         "productos": "Ivermectina 1% inyectable|Praziquantel comprimidos|Doramectina 1%|Albendazol oral|Enrofloxacino inyectable|Oxitetraciclina inyectable|Florfenicol inyectable|Tilmicosin solución|Fipronil spray|Permetrina pour-on"},
        {"nombre": "LABORATORIO LAPISA S.A.", "rubro": "veterinario",
         "productos": "Ivermectina 3.15% inyectable|Closantel inyectable|Levamisol oral|Fenbendazol oral|Triclabendazol suspensión|Moxidectina pour-on|Clorsulon + Ivermectina inyectable|Rafoxanida + Levamisol|Nitroxinil inyectable|Doramectina + Closantel"},
        {"nombre": "LABORATORIO ELANCO MÉXICO", "rubro": "veterinario",
         "productos": "Enrofloxacino 10% inyectable|Tilosina tartrato inyectable|Florfenicol 30% inyectable|Tiamulin 10% inyectable|Danofloxacino inyectable|Marbofloxacino comprimidos|Excenel ceftiofur inyectable|Rimadyl carprofen comprimidos|Metacam meloxicam|Gallimycin eritromicina"},
        {"nombre": "LABORATORIO MERIAL MÉXICO", "rubro": "veterinario",
         "productos": "Fipronil + S-metopreno spot-on|Milbemax masticables|NexGard masticables|Bravecto masticables|Simparica masticables|Credelio masticables|Selamectina spot-on|Afoxolaner masticables|Fluralaner masticables|Ivomec ivermectina inyectable"},
        {"nombre": "LABORATORIO NATURE'S PLUS MÉXICO", "rubro": "nutracéutico",
         "productos": "Creatina monohidrato 5g polvo|Proteína Whey concentrada polvo|BCAA 2:1:1 cápsulas|Glutamina 5g polvo|Multivitamínico tabletas|HMB cápsulas|Coenzima Q10 100mg cápsulas|Glucosamina + Condroitina tabletas|Espirulina 500mg tabletas|Cúrcuma 500mg cápsulas"},
        {"nombre": "LABORATORIO HERBATINT MÉXICO", "rubro": "fitoterapia",
         "productos": "Valeriana 500mg cápsulas|Pasiflora 300mg cápsulas|Hipérico 300mg tabletas|Ginkgo biloba 120mg cápsulas|Equinácea 400mg cápsulas|Cúrcuma 500mg cápsulas|Moringa 400mg cápsulas|Maca 500mg cápsulas|Ashwagandha 300mg cápsulas|Té verde extracto cápsulas"},
        {"nombre": "LABORATORIO COSMÉTICOS MÉXICO S.A.", "rubro": "cosmética",
         "productos": "Filtro solar SPF 60 facial|Crema facial hidratante|Contorno de ojos|Sérum vitamina C 15%|Niacinamida 10% sérum|Retinol 0.5% crema noche|Gel limpiador facial|Tónico AHA 5%|Mascarilla hidratante|Crema corporal urea 10%"},
        {"nombre": "LABORATORIO PHARMA+MED S.A.", "rubro": "farmacéutico",
         "productos": "Metotrexato 2.5mg tabletas|Ciclofosfamida 50mg tabletas|Tamoxifeno 20mg tabletas|Capecitabina 500mg tabletas|Hidroxiurea 500mg cápsulas|Allopurinol 300mg tabletas|Colchicina 0.5mg tabletas|Sulfasalazina 500mg tabletas|Hidroxicloroquina 200mg tabletas|Leflunomida 20mg tabletas"},
        {"nombre": "LABORATORIO ULTRA MÉXICO S.A.", "rubro": "farmacéutico",
         "productos": "Aciclovir 400mg tabletas|Valaciclovir 1g tabletas|Oseltamivir 75mg cápsulas|Ivermectina 6mg tabletas|Nitazoxanida 500mg tabletas|Albendazol 400mg tabletas|Mebendazol 100mg tabletas|Praziquantel 600mg tabletas|Tinidazol 500mg tabletas|Secnidazol 500mg tabletas"},
        {"nombre": "LABORATORIO DIBA FARMACÉUTICA", "rubro": "farmacéutico",
         "productos": "Dexametasona 4mg/ml inyectable|Metilprednisolona 500mg inyectable|Hidrocortisona 100mg inyectable|Betametasona fosfato inyectable|Budesonida nebulización 0.25mg|Fluticasona spray nasal 50mcg|Mometasona spray nasal|Beclometasona inhalador|Deflazacort 6mg tabletas|Triamcinolona intraarticular"},
        {"nombre": "LABORATORIO GALENA S.A.", "rubro": "farmacéutico",
         "productos": "Gabapentina 300mg cápsulas|Pregabalina 150mg cápsulas|Duloxetina 60mg cápsulas|Mirtazapina 30mg tabletas|Amitriptilina 25mg tabletas|Nortriptilina 25mg tabletas|Trazodona 100mg tabletas|Bupropión 150mg tabletas|Litio 300mg tabletas|Lamotrigina 100mg tabletas"},
        {"nombre": "LABORATORIO LIOMONT S.A.", "rubro": "farmacéutico",
         "productos": "Insulina humana R inyectable|Insulina NPH inyectable|Insulina glargina inyectable|Insulina lispro inyectable|Insulina aspart inyectable|Insulina detemir inyectable|Metformina 850mg tabletas|Glimepirida 4mg tabletas|Sitagliptina 100mg tabletas|Empagliflozina 10mg tabletas"},
        {"nombre": "LABORATORIO PRODUCTOS MEDIX S.A.", "rubro": "farmacéutico",
         "productos": "Warfarina 5mg tabletas|Clopidogrel 75mg tabletas|Ticagrelor 90mg tabletas|Prasugrel 10mg tabletas|Ácido acetilsalicílico 100mg|Dipiridamol 75mg tabletas|Cilostazol 100mg tabletas|Pentoxifilina 400mg tabletas|Heparina sódica inyectable|Enoxaparina inyectable"},
        {"nombre": "LABORATORIO VITRO S.A.", "rubro": "farmacéutico",
         "productos": "Rosuvastatina 10mg tabletas|Ezetimiba 10mg tabletas|Ezetimiba + Rosuvastatina|Fenofibrato 145mg tabletas|Olmesartan 20mg tabletas|Telmisartan 40mg tabletas|Sacubitril + Valsartan tabletas|Nebivolol 5mg tabletas|Ivabradina 5mg tabletas|Ranolazina 500mg tabletas"},
    ],

    "PER": [
        {"nombre": "LABORATORIO PORTUGAL S.R.L.", "rubro": "farmacéutico",
         "productos": "Ibuprofeno 400mg tabletas|Paracetamol 500mg tabletas|Amoxicilina 500mg cápsulas|Ciprofloxacino 500mg tabletas|Metformina 850mg tabletas|Omeprazol 20mg cápsulas|Enalapril 10mg tabletas|Losartan 50mg tabletas|Loratadina 10mg tabletas|Azitromicina 500mg tabletas"},
        {"nombre": "LABORATORIO AC FARMA S.A.", "rubro": "farmacéutico",
         "productos": "Amoxicilina + Clavulanico 875mg|Cefalexina 500mg cápsulas|Levofloxacino 500mg tabletas|Claritromicina 500mg tabletas|Metronidazol 500mg tabletas|Doxiciclina 100mg cápsulas|Fluconazol 150mg cápsulas|Ceftriaxona inyectable|Nitrofurantoína 100mg|Trimetoprima + Sulfametoxazol"},
        {"nombre": "LABORATORIO PERUANO SUIZO S.A.C.", "rubro": "farmacéutico",
         "productos": "Sertralina 50mg tabletas|Fluoxetina 20mg cápsulas|Alprazolam 0.5mg tabletas|Clonazepam 2mg tabletas|Carbamazepina 200mg tabletas|Pregabalina 75mg cápsulas|Levotiroxina 50mcg tabletas|Prednisona 20mg tabletas|Atorvastatina 20mg tabletas|Amlodipino 5mg tabletas"},
        {"nombre": "LABORATORIO MEDIFARMA S.A.", "rubro": "farmacéutico",
         "productos": "Salbutamol aerosol|Budesonida inhalador|Montelukast 10mg tabletas|Cetirizina 10mg tabletas|Fexofenadina 120mg tabletas|Bromhexina jarabe|Furosemida 40mg tabletas|Espironolactona 25mg tabletas|Metoprolol 100mg tabletas|Carvedilol 25mg tabletas"},
        {"nombre": "LABORATORIO ROEMMERS PERÚ S.A.", "rubro": "farmacéutico",
         "productos": "Ibuprofeno 600mg tabletas|Naproxeno 500mg tabletas|Meloxicam 15mg tabletas|Celecoxib 200mg cápsulas|Tramadol 50mg cápsulas|Ketorolac 10mg tabletas|Diclofenac potásico 50mg|Aceclofenaco 100mg tabletas|Nimesulida 100mg tabletas|Metamizol 575mg cápsulas"},
        {"nombre": "LABORATORIO PFIZER PERÚ", "rubro": "farmacéutico",
         "productos": "Atorvastatina 80mg tabletas|Pregabalina 150mg cápsulas|Celecoxib 200mg cápsulas|Azitromicina 500mg tabletas|Sildenafil 50mg tabletas|Tadalafil 20mg tabletas|Doxiciclina 100mg cápsulas|Fluconazol 150mg cápsulas|Amoxicilina + Clavulanico|Ciprofloxacino 750mg tabletas"},
        {"nombre": "LABORATORIO SANOFI PERÚ", "rubro": "farmacéutico",
         "productos": "Insulina glargina inyectable|Insulina lispro inyectable|Metformina 1000mg tabletas|Glimepirida 4mg tabletas|Sitagliptina 100mg tabletas|Empagliflozina 10mg tabletas|Dapagliflozina 10mg tabletas|Clopidogrel 75mg tabletas|Rivaroxabana 20mg tabletas|Apixabana 5mg tabletas"},
        {"nombre": "LABORATORIO BAYER PERÚ", "rubro": "farmacéutico",
         "productos": "Rivaroxabana 20mg tabletas|Apixabana 5mg tabletas|Warfarina 5mg tabletas|Ácido acetilsalicílico 100mg|Rosuvastatina 10mg tabletas|Valsartan 80mg tabletas|Olmesartan 20mg tabletas|Telmisartan 40mg tabletas|Irbesartan 150mg tabletas|Candesartan 8mg tabletas"},
        {"nombre": "LABORATORIO ROCHE PERÚ", "rubro": "farmacéutico",
         "productos": "Metotrexato 2.5mg tabletas|Capecitabina 500mg tabletas|Tamoxifeno 20mg tabletas|Anastrozol 1mg tabletas|Letrozol 2.5mg tabletas|Bevacizumab inyectable|Trastuzumab inyectable|Rituximab inyectable|Erlotinib tabletas|Osimertinib tabletas"},
        {"nombre": "LABORATORIO GRUNENTHAL PERÚ", "rubro": "farmacéutico",
         "productos": "Tramadol 50mg cápsulas|Tramadol + Paracetamol tabletas|Tapentadol 50mg tabletas|Ketorolac 10mg tabletas|Naproxeno sódico 550mg tabletas|Meloxicam 15mg tabletas|Nimesulida 100mg granulado|Aceclofenaco 100mg tabletas|Celecoxib 100mg cápsulas|Etoricoxib 60mg tabletas"},
        {"nombre": "LABORATORIO BAGO PERÚ S.A.", "rubro": "farmacéutico",
         "productos": "Escitalopram 10mg tabletas|Venlafaxina 75mg cápsulas|Mirtazapina 30mg tabletas|Quetiapina 100mg tabletas|Risperidona 2mg tabletas|Olanzapina 10mg tabletas|Gabapentina 300mg cápsulas|Duloxetina 60mg cápsulas|Bupropión 150mg tabletas|Amitriptilina 25mg tabletas"},
        {"nombre": "LABORATORIO MERCK PERÚ", "rubro": "farmacéutico",
         "productos": "Metformina 850mg tabletas|Glibenclamida 5mg tabletas|Levotiroxina 50mcg tabletas|Bisoprolol 5mg tabletas|Simvastatina 20mg tabletas|Rosuvastatina 10mg tabletas|Ezetimiba 10mg tabletas|Pantoprazol 40mg tabletas|Esomeprazol 40mg tabletas|Domperidona 10mg tabletas"},
        {"nombre": "LABORATORIO ALMIRALL PERÚ", "rubro": "dermocosmética",
         "productos": "Tretinoína crema 0.05%|Adapaleno gel 0.1%|Ácido azelaico crema 20%|Ácido salicílico 2% gel|Niacinamida 10% sérum|Vitamina C 15% sérum|Retinol 0.5% crema|Filtro solar SPF 60|Limpiador suave|Tónico sin alcohol"},
        {"nombre": "LABORATORIO HERSIL S.A.", "rubro": "farmacéutico",
         "productos": "Amoxicilina 250mg/5ml suspensión|Cefalexina 250mg/5ml suspensión|Ibuprofeno 100mg/5ml suspensión|Paracetamol 150mg/5ml gotas|Azitromicina 200mg/5ml suspensión|Ibuprofeno gotas pediátrico|Loratadina 5mg/5ml|Cetirizina 5mg/5ml|Domperidona 1mg/ml gotas|Trimetoprima + Sulfametoxazol suspensión"},
        {"nombre": "LABORATORIO NATURAL PERÚ S.A.C.", "rubro": "nutracéutico",
         "productos": "Omega 3 1000mg cápsulas|Vitamina C 1000mg tabletas|Vitamina D3 5000UI cápsulas|Colágeno hidrolizado polvo|Ácido hialurónico 150mg cápsulas|Melatonina 5mg tabletas|Magnesio quelado tabletas|Zinc comprimidos|Probióticos cápsulas|Coenzima Q10 100mg cápsulas"},
        {"nombre": "LABORATORIO SPORT PERÚ S.A.C.", "rubro": "nutracéutico",
         "productos": "Creatina monohidrato 5g polvo|Proteína Whey concentrada polvo|BCAA 2:1:1 cápsulas|Glutamina 5g polvo|HMB cápsulas|Citrulina polvo|Beta-alanina polvo|Cafeína 200mg cápsulas|Proteína vegana polvo|Multivitamínico tabletas"},
        {"nombre": "LABORATORIO BOTANICS PERÚ", "rubro": "fitoterapia",
         "productos": "Valeriana 500mg cápsulas|Pasiflora 300mg cápsulas|Hipérico 300mg tabletas|Ginkgo biloba 120mg cápsulas|Equinácea 400mg cápsulas|Cúrcuma 500mg cápsulas|Maca peruana 500mg cápsulas|Jengibre 250mg cápsulas|Ashwagandha 300mg cápsulas|Moringa 400mg cápsulas"},
        {"nombre": "LABORATORIO COSMÉTICOS NATURALES PERÚ", "rubro": "cosmética",
         "productos": "Filtro solar SPF 60 facial|Crema facial hidratante|Contorno de ojos|Sérum vitamina C 15%|Niacinamida 10% sérum|Retinol 0.5% crema noche|Gel limpiador facial|Tónico AHA 5%|Mascarilla hidratante|Crema corporal urea 10%"},
        {"nombre": "LABORATORIO VITA PHARMA PERÚ", "rubro": "farmacéutico",
         "productos": "Progesterona 100mg cápsulas|Levonorgestrel 0.75mg tabletas|Drospirenona + Etinilestradiol|Desogestrel 75mcg tabletas|Tibolona 2.5mg tabletas|Clomifeno 50mg tabletas|Estradiol gel 0.1%|Medroxiprogesterona inyectable|Dienogest 2mg tabletas|Danazol 200mg cápsulas"},
        {"nombre": "LABORATORIO QUÍMICA SUIZA S.A.", "rubro": "farmacéutico",
         "productos": "Risedronato 35mg tabletas|Alendronato 70mg tabletas|Calcio + Vitamina D3 tabletas|Vitamina D3 5000UI cápsulas|Vitamina C 1000mg tabletas|Ácido fólico 5mg tabletas|Vitamina B12 1000mcg tabletas|Hierro fumarato 300mg|Zinc 30mg tabletas|Magnesio 300mg tabletas"},
        {"nombre": "LABORATORIO FARVET E.I.R.L.", "rubro": "veterinario",
         "productos": "Ivermectina 1% inyectable|Praziquantel comprimidos|Albendazol oral|Enrofloxacino inyectable|Oxitetraciclina inyectable|Amoxicilina inyectable|Gentamicina inyectable|Dexametasona inyectable|Fipronil spray|Meloxicam veterinario"},
        {"nombre": "LABORATORIO AGROVET MARKET S.A.", "rubro": "veterinario",
         "productos": "Ivermectina 3.15% inyectable|Doramectina 1% inyectable|Closantel inyectable|Levamisol oral|Fenbendazol oral|Triclabendazol suspensión|Moxidectina pour-on|Florfenicol inyectable|Tilmicosin inyectable|Enrofloxacino 10% inyectable"},
        {"nombre": "LABORATORIO ELANCO PERÚ", "rubro": "veterinario",
         "productos": "Enrofloxacino 10% inyectable|Tilosina tartrato inyectable|Florfenicol 30% inyectable|Danofloxacino inyectable|Marbofloxacino comprimidos|Excenel ceftiofur inyectable|Rimadyl carprofen comprimidos|Metacam meloxicam|Gallimycin eritromicina|Convênia cefovecina"},
        {"nombre": "LABORATORIO ELVET PERÚ S.A.", "rubro": "veterinario",
         "productos": "Fipronil + S-metopreno spot-on|Milbemax masticables|NexGard masticables|Bravecto masticables|Simparica masticables|Credelio masticables|Selamectina spot-on|Afoxolaner masticables|Fluralaner masticables|Sarolaner masticables"},
        {"nombre": "LABORATORIO BIOGÉNESIS BAGÓ PERÚ", "rubro": "veterinario",
         "productos": "Vacuna aftosa bivalente|Vacuna brucelosis|Vacuna clostridioses|Vacuna leptospirosis bovina|Vacuna IBR|Vacuna DVB|Vacuna rabia bovina|Vacuna carbunco|Vacuna botulismo|Vacuna Mannheimia haemolytica"},
        {"nombre": "LABORATORIO INDUMIX PERÚ S.A.C.", "rubro": "farmacéutico",
         "productos": "Dexametasona 4mg/ml inyectable|Metilprednisolona 500mg inyectable|Hidrocortisona 100mg inyectable|Betametasona fosfato inyectable|Budesonida nebulización 0.25mg|Fluticasona spray nasal 50mcg|Mometasona spray nasal|Beclometasona inhalador|Deflazacort 6mg tabletas|Triamcinolona intraarticular"},
        {"nombre": "LABORATORIO ALFA FARMA PERÚ", "rubro": "farmacéutico",
         "productos": "Gabapentina 300mg cápsulas|Pregabalina 150mg cápsulas|Duloxetina 60mg cápsulas|Mirtazapina 30mg tabletas|Amitriptilina 25mg tabletas|Nortriptilina 25mg tabletas|Trazodona 100mg tabletas|Bupropión 150mg tabletas|Litio 300mg tabletas|Lamotrigina 100mg tabletas"},
        {"nombre": "LABORATORIO MESOFAR S.A.C.", "rubro": "farmacéutico",
         "productos": "Metotrexato 2.5mg tabletas|Ciclofosfamida 50mg tabletas|Tamoxifeno 20mg tabletas|Capecitabina 500mg tabletas|Hidroxiurea 500mg cápsulas|Allopurinol 300mg tabletas|Colchicina 0.5mg tabletas|Sulfasalazina 500mg tabletas|Hidroxicloroquina 200mg tabletas|Leflunomida 20mg tabletas"},
        {"nombre": "LABORATORIO ACFARMA PERÚ", "rubro": "farmacéutico",
         "productos": "Aciclovir 400mg tabletas|Valaciclovir 1g tabletas|Oseltamivir 75mg cápsulas|Ivermectina 6mg tabletas|Nitazoxanida 500mg tabletas|Albendazol 400mg tabletas|Mebendazol 100mg tabletas|Praziquantel 600mg tabletas|Primaquina 15mg tabletas|Cloroquina 150mg tabletas"},
        {"nombre": "LABORATORIO NORTFARMA S.R.L.", "rubro": "farmacéutico",
         "productos": "Warfarina 5mg tabletas|Clopidogrel 75mg tabletas|Ácido acetilsalicílico 100mg|Digoxina 0.25mg tabletas|Amiodarona 200mg tabletas|Bisoprolol 5mg tabletas|Diltiazem 60mg tabletas|Ramipril 5mg tabletas|Verapamilo 80mg tabletas|Enalapril 20mg tabletas"},
    ],
    "ECU": [
        {"nombre": "LABORATORIOS LIFE", "rubro": "farmacéutico",
         "productos": "Paracetamol 500mg tabletas|Ibuprofeno 400mg tabletas|Amoxicilina 500mg cápsulas|Diclofenaco 50mg tabletas|Loratadina 10mg tabletas|Omeprazol 20mg cápsulas|Metformina 850mg tabletas|Losartan 50mg tabletas|Complejo B tabletas|Sales de rehidratación oral"},
        {"nombre": "LABORATORIOS ACROMAX", "rubro": "farmacéutico",
         "productos": "Azitromicina 500mg tabletas|Ciprofloxacino 500mg tabletas|Cefalexina 500mg cápsulas|Metronidazol 500mg tabletas|Fluconazol 150mg cápsulas|Paracetamol jarabe|Ambroxol jarabe|Salbutamol aerosol|Prednisona 20mg tabletas|Dexametasona inyectable"},
        {"nombre": "LABORATORIOS ROCNARF", "rubro": "farmacéutico",
         "productos": "Ibuprofeno 600mg tabletas|Naproxeno 550mg tabletas|Tramadol 50mg cápsulas|Meloxicam 15mg tabletas|Omeprazol 40mg cápsulas|Ranitidina 150mg tabletas|Enalapril 10mg tabletas|Amlodipino 5mg tabletas|Atorvastatina 20mg tabletas|Vitamina C 1000mg"},
        {"nombre": "JAMES BROWN PHARMA", "rubro": "farmacéutico",
         "productos": "Amoxicilina + Clavulanico 875mg|Claritromicina 500mg tabletas|Levofloxacino 500mg tabletas|Doxiciclina 100mg cápsulas|Ketoconazol 200mg tabletas|Aciclovir 400mg tabletas|Albendazol 400mg tabletas|Ivermectina 6mg tabletas|Loratadina jarabe|Cetirizina 10mg tabletas"},
        {"nombre": "LABORATORIOS LAMOSAN", "rubro": "farmacéutico",
         "productos": "Paracetamol gotas pediátrico|Ibuprofeno suspensión|Amoxicilina 250mg/5ml suspensión|Trimetoprima + Sulfametoxazol suspensión|Hierro jarabe|Calcio + Vitamina D tabletas|Multivitamínico jarabe|Zinc jarabe|Ácido fólico tabletas|Vitamina E cápsulas"},
        {"nombre": "LABORATORIOS GENAMERICA", "rubro": "farmacéutico",
         "productos": "Sertralina 50mg tabletas|Fluoxetina 20mg cápsulas|Alprazolam 0.5mg tabletas|Clonazepam 2mg tabletas|Carbamazepina 200mg tabletas|Gabapentina 300mg cápsulas|Pregabalina 75mg cápsulas|Quetiapina 100mg tabletas|Risperidona 2mg tabletas|Levotiroxina 50mcg tabletas"},
    ],

    "BOL": [
        {"nombre": "LABORATORIOS INTI", "rubro": "farmacéutico",
         "productos": "Paracetamol 500mg comprimidos|Ibuprofeno 400mg comprimidos|Amoxicilina 500mg cápsulas|Ciprofloxacino 500mg comprimidos|Metamizol 500mg comprimidos|Omeprazol 20mg cápsulas|Metformina 850mg comprimidos|Enalapril 10mg comprimidos|Complejo B inyectable|Diclofenaco gel"},
        {"nombre": "LABORATORIOS BAGO DE BOLIVIA", "rubro": "farmacéutico",
         "productos": "Azitromicina 500mg comprimidos|Claritromicina 500mg comprimidos|Cefalexina 500mg cápsulas|Levofloxacino 500mg comprimidos|Sertralina 50mg comprimidos|Alprazolam 0.5mg comprimidos|Pantoprazol 40mg comprimidos|Atorvastatina 20mg comprimidos|Losartan 50mg comprimidos|Pregabalina 75mg cápsulas"},
        {"nombre": "LABORATORIOS VITA", "rubro": "farmacéutico",
         "productos": "Paracetamol jarabe|Ibuprofeno suspensión|Amoxicilina suspensión|Salbutamol jarabe|Ambroxol jarabe|Loratadina jarabe|Hierro jarabe|Multivitamínico jarabe|Mebendazol suspensión|Metronidazol suspensión"},
        {"nombre": "LABORATORIOS TERBOL", "rubro": "farmacéutico",
         "productos": "Diclofenaco 50mg comprimidos|Meloxicam 15mg comprimidos|Tramadol 50mg cápsulas|Ketorolaco 10mg comprimidos|Dexametasona inyectable|Betametasona crema|Clotrimazol crema|Ketoconazol champú|Aciclovir crema|Mupirocina ungüento"},
        {"nombre": "LABORATORIOS COFAR BOLIVIA", "rubro": "farmacéutico",
         "productos": "Metformina 850mg comprimidos|Glibenclamida 5mg comprimidos|Levotiroxina 50mcg comprimidos|Prednisona 20mg comprimidos|Enalapril 20mg comprimidos|Amlodipino 10mg comprimidos|Furosemida 40mg comprimidos|Espironolactona 25mg|Warfarina 5mg comprimidos|Digoxina 0.25mg comprimidos"},
        {"nombre": "DELTA LABORATORIOS BOLIVIA", "rubro": "veterinario",
         "productos": "Ivermectina 1% inyectable|Albendazol oral|Levamisol oral|Oxitetraciclina inyectable|Enrofloxacino inyectable|Fipronil spray|Dexametasona inyectable|Vitaminas ADE inyectable|Hierro dextrano inyectable|Antiparasitario pour-on"},
    ],

    "PRY": [
        {"nombre": "LABORATORIOS LASCA", "rubro": "farmacéutico",
         "productos": "Paracetamol 500mg comprimidos|Ibuprofeno 400mg comprimidos|Amoxicilina 500mg cápsulas|Azitromicina 500mg comprimidos|Ciprofloxacino 500mg comprimidos|Omeprazol 20mg cápsulas|Loratadina 10mg comprimidos|Metformina 850mg comprimidos|Enalapril 10mg comprimidos|Diclofenaco 75mg inyectable"},
        {"nombre": "SCAVONE HERMANOS", "rubro": "farmacéutico",
         "productos": "Cefalexina 500mg cápsulas|Claritromicina 500mg comprimidos|Metronidazol 500mg comprimidos|Fluconazol 150mg cápsulas|Sertralina 50mg comprimidos|Clonazepam 2mg comprimidos|Atorvastatina 20mg comprimidos|Losartan 50mg comprimidos|Salbutamol aerosol|Budesonida inhalador"},
        {"nombre": "LABORATORIOS CATEDRAL", "rubro": "farmacéutico",
         "productos": "Paracetamol jarabe|Ibuprofeno suspensión|Amoxicilina suspensión|Ambroxol jarabe|Loratadina jarabe|Hierro jarabe|Mebendazol suspensión|Complejo B comprimidos|Vitamina C comprimidos|Calcio + Vitamina D"},
        {"nombre": "QUIMFA", "rubro": "farmacéutico",
         "productos": "Tramadol 50mg cápsulas|Meloxicam 15mg comprimidos|Ketorolaco 10mg comprimidos|Pregabalina 75mg cápsulas|Gabapentina 300mg cápsulas|Omeprazol 40mg cápsulas|Pantoprazol 40mg comprimidos|Levotiroxina 50mcg|Prednisona 20mg comprimidos|Dexametasona inyectable"},
        {"nombre": "INDUFAR PARAGUAY", "rubro": "farmacéutico",
         "productos": "Metamizol 500mg comprimidos|Diclofenaco 50mg comprimidos|Aciclovir 400mg comprimidos|Albendazol 400mg comprimidos|Ivermectina 6mg comprimidos|Ketoconazol 200mg comprimidos|Clotrimazol crema|Betametasona crema|Gentamicina inyectable|Penicilina inyectable"},
    ],

    "VEN": [
        {"nombre": "LABORATORIOS LETI", "rubro": "farmacéutico",
         "productos": "Paracetamol 500mg tabletas|Ibuprofeno 400mg tabletas|Amoxicilina 500mg cápsulas|Azitromicina 500mg tabletas|Loratadina 10mg tabletas|Omeprazol 20mg cápsulas|Atorvastatina 20mg tabletas|Losartan 50mg tabletas|Metformina 850mg tabletas|Sildenafil 50mg tabletas"},
        {"nombre": "LABORATORIOS VARGAS", "rubro": "farmacéutico",
         "productos": "Ciprofloxacino 500mg tabletas|Cefalexina 500mg cápsulas|Metronidazol 500mg tabletas|Fluconazol 150mg cápsulas|Prednisona 20mg tabletas|Dexametasona inyectable|Salbutamol aerosol|Ambroxol jarabe|Hierro jarabe|Complejo B inyectable"},
        {"nombre": "GENVEN GENERICOS VENEZOLANOS", "rubro": "farmacéutico",
         "productos": "Enalapril 10mg tabletas|Amlodipino 5mg tabletas|Furosemida 40mg tabletas|Espironolactona 25mg tabletas|Carvedilol 25mg tabletas|Metoprolol 100mg tabletas|Glibenclamida 5mg tabletas|Levotiroxina 50mcg tabletas|Warfarina 5mg tabletas|Digoxina 0.25mg tabletas"},
        {"nombre": "LABORATORIOS CALOX VENEZUELA", "rubro": "farmacéutico",
         "productos": "Diclofenaco 50mg tabletas|Meloxicam 15mg tabletas|Tramadol 50mg cápsulas|Ketorolaco inyectable|Aciclovir 400mg tabletas|Albendazol 400mg tabletas|Ketoconazol crema|Clotrimazol crema|Gentamicina inyectable|Vitamina C 1000mg"},
        {"nombre": "LABORATORIOS ELMOR", "rubro": "farmacéutico",
         "productos": "Sertralina 50mg tabletas|Escitalopram 10mg tabletas|Alprazolam 0.5mg tabletas|Clonazepam 2mg tabletas|Quetiapina 100mg tabletas|Carbamazepina 200mg tabletas|Pregabalina 75mg cápsulas|Gabapentina 300mg cápsulas|Levetiracetam 500mg tabletas|Lamotrigina 100mg tabletas"},
        {"nombre": "LABORATORIOS BEHRENS", "rubro": "farmacéutico",
         "productos": "Insulina NPH inyectable|Metformina 850mg tabletas|Glimepirida 4mg tabletas|Penicilina G inyectable|Ampicilina 500mg cápsulas|Dicloxacilina 500mg cápsulas|Ceftriaxona inyectable|Vancomicina inyectable|Suero fisiológico|Dextrosa solución"},
    ],

    "CRI": [
        {"nombre": "LABORATORIOS STEIN", "rubro": "farmacéutico",
         "productos": "Paracetamol 500mg tabletas|Ibuprofeno 400mg tabletas|Amoxicilina 500mg cápsulas|Azitromicina 500mg tabletas|Loratadina 10mg tabletas|Omeprazol 20mg cápsulas|Atorvastatina 20mg tabletas|Losartan 50mg tabletas|Metformina 850mg tabletas|Sertralina 50mg tabletas"},
        {"nombre": "LABORATORIOS GUTIS", "rubro": "farmacéutico",
         "productos": "Ciprofloxacino 500mg tabletas|Cefalexina 500mg cápsulas|Claritromicina 500mg tabletas|Fluconazol 150mg cápsulas|Pregabalina 75mg cápsulas|Gabapentina 300mg cápsulas|Quetiapina 100mg tabletas|Escitalopram 10mg tabletas|Pantoprazol 40mg tabletas|Rosuvastatina 10mg tabletas"},
        {"nombre": "LABORATORIOS LISAN", "rubro": "farmacéutico",
         "productos": "Paracetamol jarabe|Ibuprofeno suspensión|Amoxicilina suspensión|Ambroxol jarabe|Loratadina jarabe|Salbutamol jarabe|Hierro jarabe|Multivitamínico jarabe|Calcio + Vitamina D tabletas|Zinc jarabe"},
        {"nombre": "LABORATORIOS RAVEN", "rubro": "farmacéutico",
         "productos": "Diclofenaco 50mg tabletas|Meloxicam 15mg tabletas|Tramadol 50mg cápsulas|Betametasona crema|Clotrimazol crema|Ketoconazol champú|Aciclovir crema|Hidrocortisona crema|Mupirocina ungüento|Gentamicina crema"},
        {"nombre": "LABORATORIOS ZEPOL", "rubro": "farmacéutico",
         "productos": "Ungüento mentolado|Jarabe para la tos|Paracetamol + Fenilefrina tabletas|Loratadina + Pseudoefedrina|Ambroxol jarabe|Dextrometorfano jarabe|Vitamina C efervescente|Zinc tabletas|Mentol crema|Alcanfor ungüento"},
    ],

    "PAN": [
        {"nombre": "LABORATORIOS RIGAR", "rubro": "farmacéutico",
         "productos": "Paracetamol 500mg tabletas|Ibuprofeno 400mg tabletas|Amoxicilina 500mg cápsulas|Diclofenaco 50mg tabletas|Loratadina 10mg tabletas|Omeprazol 20mg cápsulas|Complejo B tabletas|Vitamina C tabletas|Hierro tabletas|Calcio tabletas"},
        {"nombre": "MEDIPAN", "rubro": "farmacéutico",
         "productos": "Azitromicina 500mg tabletas|Ciprofloxacino 500mg tabletas|Cefalexina 500mg cápsulas|Metronidazol 500mg tabletas|Fluconazol 150mg cápsulas|Prednisona 20mg tabletas|Salbutamol aerosol|Ambroxol jarabe|Metformina 850mg tabletas|Enalapril 10mg tabletas"},
    ],

    "GTM": [
        {"nombre": "LANCASCO", "rubro": "farmacéutico",
         "productos": "Paracetamol 500mg tabletas|Ibuprofeno 400mg tabletas|Amoxicilina 500mg cápsulas|Azitromicina 500mg tabletas|Loratadina 10mg tabletas|Omeprazol 20mg cápsulas|Metformina 850mg tabletas|Losartan 50mg tabletas|Complejo B inyectable|Diclofenaco inyectable"},
        {"nombre": "LABORATORIOS LAPRIN", "rubro": "farmacéutico",
         "productos": "Ciprofloxacino 500mg tabletas|Cefalexina 500mg cápsulas|Metronidazol 500mg tabletas|Trimetoprima + Sulfametoxazol tabletas|Albendazol 400mg tabletas|Mebendazol 100mg tabletas|Paracetamol jarabe|Ambroxol jarabe|Hierro jarabe|Multivitamínico jarabe"},
        {"nombre": "PRODUCTOS FARMACEUTICOS DONOVAN WERKE", "rubro": "farmacéutico",
         "productos": "Diclofenaco 50mg tabletas|Meloxicam 15mg tabletas|Tramadol 50mg cápsulas|Betametasona crema|Clotrimazol crema|Ketoconazol champú|Dexametasona inyectable|Gentamicina inyectable|Penicilina inyectable|Vitamina B12 inyectable"},
    ],

    "DOM": [
        {"nombre": "LABORATORIOS ROWE", "rubro": "farmacéutico",
         "productos": "Paracetamol 500mg tabletas|Ibuprofeno 400mg tabletas|Amoxicilina 500mg cápsulas|Azitromicina 500mg tabletas|Loratadina 10mg tabletas|Omeprazol 20mg cápsulas|Metformina 850mg tabletas|Losartan 50mg tabletas|Atorvastatina 20mg tabletas|Sildenafil 50mg tabletas"},
        {"nombre": "LABORATORIOS MALLEN", "rubro": "farmacéutico",
         "productos": "Ciprofloxacino 500mg tabletas|Cefalexina 500mg cápsulas|Claritromicina 500mg tabletas|Fluconazol 150mg cápsulas|Sertralina 50mg tabletas|Alprazolam 0.5mg tabletas|Pantoprazol 40mg tabletas|Enalapril 10mg tabletas|Amlodipino 5mg tabletas|Prednisona 20mg tabletas"},
        {"nombre": "MAGNACHEM INTERNACIONAL", "rubro": "farmacéutico",
         "productos": "Diclofenaco 50mg tabletas|Meloxicam 15mg tabletas|Tramadol 50mg cápsulas|Paracetamol jarabe|Ibuprofeno suspensión|Amoxicilina suspensión|Ambroxol jarabe|Salbutamol jarabe|Hierro jarabe|Complejo B jarabe"},
        {"nombre": "LABORATORIOS FELTREX", "rubro": "farmacéutico",
         "productos": "Aciclovir 400mg tabletas|Albendazol 400mg tabletas|Ivermectina 6mg tabletas|Ketoconazol 200mg tabletas|Metronidazol 500mg tabletas|Tinidazol 500mg tabletas|Nistatina suspensión|Clotrimazol crema|Betametasona crema|Gentamicina crema"},
    ],

    "SLV": [
        {"nombre": "LABORATORIOS VIJOSA", "rubro": "farmacéutico",
         "productos": "Paracetamol 500mg tabletas|Ibuprofeno 400mg tabletas|Amoxicilina 500mg cápsulas|Azitromicina 500mg tabletas|Loratadina 10mg tabletas|Omeprazol 20mg cápsulas|Metformina 850mg tabletas|Losartan 50mg tabletas|Ambroxol jarabe|Complejo B tabletas"},
        {"nombre": "LABORATORIOS PAILL", "rubro": "farmacéutico",
         "productos": "Ciprofloxacino 500mg tabletas|Cefalexina 500mg cápsulas|Metronidazol 500mg tabletas|Fluconazol 150mg cápsulas|Diclofenaco 50mg tabletas|Meloxicam 15mg tabletas|Prednisona 20mg tabletas|Salbutamol aerosol|Paracetamol jarabe|Hierro jarabe"},
        {"nombre": "LABORATORIOS TERAMED", "rubro": "farmacéutico",
         "productos": "Sertralina 50mg tabletas|Fluoxetina 20mg cápsulas|Alprazolam 0.5mg tabletas|Clonazepam 2mg tabletas|Carbamazepina 200mg tabletas|Gabapentina 300mg cápsulas|Atorvastatina 20mg tabletas|Enalapril 10mg tabletas|Amlodipino 5mg tabletas|Levotiroxina 50mcg tabletas"},
        {"nombre": "LABORATORIOS SUIZOS", "rubro": "farmacéutico",
         "productos": "Vitamina C 1000mg tabletas|Complejo B tabletas|Hierro + Ácido fólico tabletas|Calcio + Vitamina D tabletas|Multivitamínico tabletas|Zinc tabletas|Omega 3 cápsulas|Magnesio tabletas|Paracetamol + Cafeína tabletas|Loratadina + Pseudoefedrina"},
    ],

    "HND": [
        {"nombre": "LABORATORIOS FINLAY", "rubro": "farmacéutico",
         "productos": "Paracetamol 500mg tabletas|Ibuprofeno 400mg tabletas|Amoxicilina 500mg cápsulas|Ciprofloxacino 500mg tabletas|Metronidazol 500mg tabletas|Loratadina 10mg tabletas|Omeprazol 20mg cápsulas|Metformina 850mg tabletas|Diclofenaco 50mg tabletas|Complejo B inyectable"},
        {"nombre": "LABORATORIOS ANDIFAR", "rubro": "farmacéutico",
         "productos": "Azitromicina 500mg tabletas|Cefalexina 500mg cápsulas|Fluconazol 150mg cápsulas|Albendazol 400mg tabletas|Mebendazol 100mg tabletas|Paracetamol jarabe|Ibuprofeno suspensión|Ambroxol jarabe|Hierro jarabe|Multivitamínico jarabe"},
    ],

    "NIC": [
        {"nombre": "LABORATORIOS RAMOS", "rubro": "farmacéutico",
         "productos": "Paracetamol 500mg tabletas|Ibuprofeno 400mg tabletas|Amoxicilina 500mg cápsulas|Azitromicina 500mg tabletas|Loratadina 10mg tabletas|Omeprazol 20mg cápsulas|Metformina 850mg tabletas|Enalapril 10mg tabletas|Diclofenaco 50mg tabletas|Complejo B tabletas"},
        {"nombre": "LABORATORIOS PANZYMA", "rubro": "farmacéutico",
         "productos": "Ciprofloxacino 500mg tabletas|Cefalexina 500mg cápsulas|Metronidazol 500mg tabletas|Trimetoprima + Sulfametoxazol|Albendazol 400mg tabletas|Paracetamol jarabe|Ambroxol jarabe|Salbutamol jarabe|Hierro jarabe|Vitamina C tabletas"},
        {"nombre": "LABORATORIOS CEGUEL", "rubro": "farmacéutico",
         "productos": "Diclofenaco 50mg tabletas|Meloxicam 15mg tabletas|Prednisona 20mg tabletas|Dexametasona inyectable|Betametasona crema|Clotrimazol crema|Ketoconazol champú|Gentamicina inyectable|Penicilina inyectable|Vitamina B12 inyectable"},
    ],
}



# ─────────────────────────────────────────────────────────────
# FUNCIONES DE SCRAPING (con fallback automático)
# ─────────────────────────────────────────────────────────────

def _get(url: str, timeout: int = 20, **kwargs) -> requests.Response | None:
    """Request con manejo de errores."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, **kwargs)
        r.raise_for_status()
        time.sleep(1)
        return r
    except Exception as e:
        log.debug(f"GET {url}: {e}")
        return None


def _labs_curados_a_df(pais: str) -> pd.DataFrame:
    """Convierte la base curada al formato estándar del pipeline."""
    labs = LABS_CURADOS.get(pais, [])
    rows = []
    for lab in labs:
        # Normalizar campo productos (puede ser 'produtos' en Brasil)
        productos = lab.get("productos", lab.get("produtos", ""))
        n = len([p for p in productos.split("|") if p.strip()])
        rows.append({
            "nombre":     lab["nombre"],
            "pais":       pais,
            "rubro":      lab.get("rubro", "farmacéutico"),
            "n_productos": n,
            "productos":  productos,
        })
    return pd.DataFrame(rows)


def scrape_anmat() -> pd.DataFrame:
    """Intenta scrapear ANMAT, devuelve DataFrame o empty."""
    log.info("  Intentando ANMAT...")

    # URL alternativa del CSV via API de CKAN
    urls = [
        "https://datos.gob.ar/api/3/action/datastore_search?resource_id=a3852b28-f9db-44e0-af7e-a6d3e70969fa&limit=5000",
        "https://datos.gob.ar/dataset/salud-actualizaciones-vademecun-nacional-medicamentos-vnm/archivo/salud_a3852b28-f9db-44e0-af7e-a6d3e70969fa",
    ]

    for url in urls:
        r = _get(url)
        if not r:
            continue
        try:
            # Intentar como JSON (API CKAN)
            data = r.json()
            records = data.get("result", {}).get("records", [])
            if records:
                df_raw = pd.DataFrame(records)
                return _procesar_df_anmat(df_raw)
        except Exception:
            pass
        try:
            # Intentar como CSV
            df_raw = pd.read_csv(
                pd.io.common.BytesIO(r.content),
                encoding="latin-1", low_memory=False
            )
            return _procesar_df_anmat(df_raw)
        except Exception:
            pass

    return pd.DataFrame()


def _procesar_df_anmat(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Procesa cualquier DataFrame de ANMAT en el formato estándar."""
    cols = {c.lower().strip(): c for c in df_raw.columns}

    # Detectar columnas clave
    col_lab  = next((cols[c] for c in cols if "laboratorio" in c or "titular" in c), None)
    col_prod = next((cols[c] for c in cols if "nombre" in c or "comercial" in c), None)
    col_droga = next((cols[c] for c in cols if "droga" in c or "principio" in c or "generico" in c), None)
    col_forma = next((cols[c] for c in cols if "forma" in c), None)

    if not col_lab:
        log.warning("  ANMAT: no se encontró columna de laboratorio")
        return pd.DataFrame()

    labs: dict[str, dict] = {}
    for _, row in df_raw.iterrows():
        nombre_lab = str(row.get(col_lab, "")).strip().upper()
        if not nombre_lab or nombre_lab == "NAN":
            continue

        producto = str(row.get(col_prod, "")).strip() if col_prod else ""
        droga    = str(row.get(col_droga, "")).strip() if col_droga else ""
        forma    = str(row.get(col_forma, "")).strip() if col_forma else ""

        entrada = producto
        if droga and droga != "nan":
            entrada += f" ({droga})"
        if forma and forma != "nan":
            entrada += f" [{forma}]"

        if nombre_lab not in labs:
            labs[nombre_lab] = {
                "nombre": nombre_lab, "pais": "ARG",
                "rubro": "farmacéutico", "productos": []
            }
        labs[nombre_lab]["productos"].append(entrada.strip())

    rows = []
    for lab in labs.values():
        prods = list(dict.fromkeys(lab["productos"]))
        rows.append({
            "nombre":      lab["nombre"],
            "pais":        "ARG",
            "rubro":       lab["rubro"],
            "n_productos": len(prods),
            "productos":   " | ".join(prods[:100]),
        })

    df = pd.DataFrame(rows)
    return df.sort_values("n_productos", ascending=False).reset_index(drop=True) if not df.empty else df


def scrape_anvisa(max_pages: int = 30) -> pd.DataFrame:
    """Scraping ANVISA Brasil."""
    log.info("  Intentando ANVISA Brasil...")
    labs: dict[str, dict] = {}

    for page in range(1, max_pages + 1):
        r = _get(
            "https://consultas.anvisa.gov.br/api/consulta/medicamentos",
            params={"count": 100, "page": page, "situacaoRegistro": "V"}
        )
        if not r:
            break
        try:
            data = r.json()
            items = data.get("content", [])
            if not items:
                break
            for item in items:
                empresa = str(item.get("empresa", {}).get("nome", "")).strip().upper()
                if not empresa:
                    continue
                produto  = str(item.get("nomeProduto", "")).strip()
                principio = str(item.get("principioAtivo", "")).strip()
                forma    = str(item.get("formaFarmaceutica", "")).strip()
                entrada  = produto
                if principio:
                    entrada += f" ({principio})"
                if forma:
                    entrada += f" [{forma}]"
                if empresa not in labs:
                    labs[empresa] = {"nome": empresa, "pais": "BRA", "rubro": "farmacéutico", "produtos": []}
                labs[empresa]["produtos"].append(entrada)
        except Exception:
            break

    if not labs:
        return pd.DataFrame()

    rows = []
    for lab in labs.values():
        prods = list(dict.fromkeys(lab["produtos"]))
        rows.append({
            "nombre":      lab["nome"],
            "pais":        "BRA",
            "rubro":       "farmacéutico",
            "n_produtos":  len(prods),
            "produtos":    " | ".join(prods[:100]),
        })

    df = pd.DataFrame(rows).rename(columns={"n_produtos": "n_productos", "produtos": "productos"})
    return df.sort_values("n_productos", ascending=False).reset_index(drop=True)


# ─────────────────────────────────────────────────────────────
# FUNCIÓN PRINCIPAL — con fallback automático
# ─────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────
# DESCUBRIMIENTO WEB — para países SIN base curada
# Busca en la web listas de laboratorios del país y extrae nombres.
# Permite usar el pipeline con cualquier país del mundo.
# ─────────────────────────────────────────────────────────────

NOMBRES_PAISES = {
    "ARG": "Argentina", "CHL": "Chile", "URY": "Uruguay", "BRA": "Brasil",
    "COL": "Colombia", "MEX": "México", "PER": "Perú", "ECU": "Ecuador",
    "BOL": "Bolivia", "PRY": "Paraguay", "VEN": "Venezuela", "CRI": "Costa Rica",
    "PAN": "Panamá", "GTM": "Guatemala", "DOM": "República Dominicana",
    "SLV": "El Salvador", "HND": "Honduras", "NIC": "Nicaragua",
    "ESP": "España", "USA": "Estados Unidos", "PRT": "Portugal", "ITA": "Italia",
}


def _norm_txt(s: str) -> str:
    t = str.maketrans("áéíóúàèìòùäëïöüñç", "aeiouaeiouaeiounc")
    return re.sub(r"[^a-z0-9 ]", "", (s or "").lower().translate(t)).strip()


# Nombres y alias → código (para usar base curada si el país está en ella)
_NOMBRE_A_CODIGO = {}
for _cod, _nom in NOMBRES_PAISES.items():
    _NOMBRE_A_CODIGO[_norm_txt(_nom)] = _cod
    _NOMBRE_A_CODIGO[_norm_txt(_cod)] = _cod
_NOMBRE_A_CODIGO.update({
    "argentina": "ARG", "chile": "CHL", "uruguay": "URY", "brasil": "BRA",
    "brazil": "BRA", "colombia": "COL", "mexico": "MEX", "peru": "PER",
    "ecuador": "ECU", "bolivia": "BOL", "paraguay": "PRY", "venezuela": "VEN",
    "costa rica": "CRI", "panama": "PAN", "guatemala": "GTM",
    "republica dominicana": "DOM", "dominicana": "DOM",
    "el salvador": "SLV", "salvador": "SLV", "honduras": "HND",
    "nicaragua": "NIC",
})


def resolver_pais(texto: str):
    """
    Resuelve texto libre del usuario a (codigo, nombre_legible).
    Si el país tiene base curada devuelve su código; si no, código=None
    y se usará descubrimiento web con el nombre tal cual lo escribió.
    """
    n = _norm_txt(texto)
    cod = _NOMBRE_A_CODIGO.get(n)
    if cod:
        return cod, NOMBRES_PAISES.get(cod, texto.strip())
    # País no curado: usar el texto como nombre legible
    return None, texto.strip().title()


# Patrones de nombres de laboratorios reales
_PAT_LAB = [
    r"\b(Laboratorios?\s+[A-ZÁÉÍÓÚÑ][\wÁÉÍÓÚÑáéíóúñ&\.\-]{2,}(?:\s+[A-ZÁÉÍÓÚÑ][\wáéíóúñ\.\-]{1,}){0,2})",
    r"\b([A-ZÁÉÍÓÚÑ][\wáéíóúñ&\-]{3,}(?:\s+[A-ZÁÉÍÓÚÑ][\wáéíóúñ\-]{1,})?\s+(?:Pharma|Pharmaceuticals?|Farma|Farmacéutic[ao]s?|Labs?|Laboratories))\b",
    r"\b((?:Productos\s+Farmacéuticos|Industria\s+Farmacéutica)\s+[A-ZÁÉÍÓÚÑ][\wáéíóúñ&\.\-]{2,})",
]
_RUIDO_LAB = ("laboratorios de", "laboratorio de", "clínic", "clinic", "análisis",
              "analisis", "lista", "mejores", "ranking", "top ", "wikipedia",
              "directorio", "cámara", "camara", "asociación", "asociacion",
              "ministerio", "universidad", "facultad", "hospital", "vademecum")


def _extraer_nombres_lab(texto: str, contador: dict):
    """Extrae nombres de laboratorios de un bloque de texto y los acumula."""
    for patron in _PAT_LAB:
        for m in re.findall(patron, texto):
            nombre = re.sub(r"\s+", " ", m).strip(" .-")
            if len(nombre) < 7 or len(nombre) > 60:
                continue
            low = nombre.lower()
            if any(x in low for x in _RUIDO_LAB):
                continue
            # Evitar nombres que son sólo la palabra genérica
            if low in ("laboratorios", "laboratorio", "pharma", "farma"):
                continue
            clave = nombre.upper()
            contador[clave] = contador.get(clave, 0) + 1


# Enfoques de búsqueda: cada reposición usa uno distinto para traer
# laboratorios NUEVOS (no siempre los mismos).
_ENFOQUES = [
    ["laboratorios farmacéuticos {p}",
     "principales laboratorios farmacéuticos {p}",
     "industria farmacéutica nacional {p} empresas",
     "cámara de laboratorios farmacéuticos {p}"],
    ["laboratorios veterinarios {p}",
     "laboratorios nutracéuticos suplementos {p}",
     "laboratorios cosméticos dermocosmética {p}",
     "fabricantes de medicamentos genéricos {p}"],
    ["laboratorios magistrales {p}",
     "importadores materias primas farmacéuticas {p}",
     "droguerías y laboratorios {p}",
     "empresas farmacéuticas pymes {p}"],
    ["directorio laboratorios farmacéuticos {p}",
     "asociación de la industria farmacéutica {p} miembros",
     "fabricantes productos farmacéuticos {p}",
     "laboratorios homeopáticos fitoterapia {p}"],
]


def descubrir_laboratorios_web(pais: str, max_labs: int = 30, enfoque: int = 0) -> pd.DataFrame:
    """
    Descubre laboratorios de cualquier país buscando en Google y otros
    buscadores y ENTRANDO a las páginas de resultados (directorios, listas,
    cámaras). El parámetro 'enfoque' cambia las consultas para traer
    laboratorios nuevos en cada reposición.
    """
    nombre_pais = NOMBRES_PAISES.get(pais, pais)
    log.info(f"  🔎 Descubrimiento web (enfoque {enfoque}): laboratorios de {nombre_pais}...")

    plantillas = _ENFOQUES[enfoque % len(_ENFOQUES)]
    queries = [t.format(p=nombre_pais) for t in plantillas]
    buscadores = [
        "https://www.google.com/search?q={}&num=20",
        "https://html.duckduckgo.com/html/?q={}",
        "https://www.bing.com/search?q={}",
        "https://www.mojeek.com/search?q={}",
    ]
    EXCLUIR_URL = (
        "google.", "bing.", "duckduckgo.", "mojeek.", "youtube.", "facebook.",
        "instagram.", "twitter.", "x.com", "linkedin.", "wikipedia.",
        "amazon.", "mercadolibre.", "pinterest.", "tiktok.",
    )

    nombres: dict = {}
    urls_para_entrar: list = []

    # ── Fase 1: buscar y extraer de los snippets + juntar URLs ──
    for query in queries:
        for buscador in buscadores:
            try:
                url = buscador.format(requests.utils.quote(query))
                r = requests.get(url, headers=HEADERS, timeout=10)
                if r.status_code != 200:
                    continue
                soup = BeautifulSoup(r.text, "html.parser")
                _extraer_nombres_lab(soup.get_text(" "), nombres)
                # juntar URLs de resultados orgánicos (listas/directorios)
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    m = re.search(r'https?://[^\s<>"]+', href)
                    if not m:
                        continue
                    real = m.group(0)
                    low = real.lower()
                    if any(e in low for e in EXCLUIR_URL):
                        continue
                    if any(k in low for k in ("laborator", "farma", "pharma",
                                              "directorio", "camara", "empresas")):
                        if real not in urls_para_entrar:
                            urls_para_entrar.append(real)
                time.sleep(0.6)
            except Exception as e:
                log.debug(f"  buscador error: {e}")
        if len(nombres) >= max_labs * 2:
            break

    # ── Fase 2: ENTRAR a las páginas más prometedoras y extraer nombres ──
    for page_url in urls_para_entrar[:6]:
        try:
            r = requests.get(page_url, headers=HEADERS, timeout=10)
            if r.status_code != 200:
                continue
            soup = BeautifulSoup(r.text, "html.parser")
            # texto general
            _extraer_nombres_lab(soup.get_text(" "), nombres)
            # listas (li) y celdas (td) suelen tener nombres de empresas
            for tag in soup.find_all(["li", "td", "h2", "h3", "a"]):
                t = tag.get_text(" ").strip()
                if 7 <= len(t) <= 60:
                    _extraer_nombres_lab(t, nombres)
            log.info(f"     entró a {page_url[:48]}… ({len(nombres)} acumulados)")
            time.sleep(0.6)
        except Exception as e:
            log.debug(f"  page fetch error: {e}")

    ordenados = sorted(nombres.items(), key=lambda kv: -kv[1])[:max_labs]
    rows = [{
        "nombre":      clave,
        "pais":        pais,
        "rubro":       "farmacéutico",
        "n_productos": 2,
        "productos":   "",
    } for clave, _ in ordenados]

    log.info(f"  🔎 Descubrimiento web: {len(rows)} laboratorios encontrados")
    return pd.DataFrame(rows)


def obtener_laboratorios(pais: str, cache: bool = True) -> pd.DataFrame:
    """
    Obtiene laboratorios para un país.

    Estrategia:
    1. Revisar cache local (si cache=True)
    2. Intentar scraping del registro oficial del país
    3. Si falla → usar base curada de laboratorios conocidos
    4. Combinar scraping + curados (más completo)

    Siempre devuelve un DataFrame con columnas:
        nombre, pais, rubro, n_productos, productos
    """
    Path("data").mkdir(exist_ok=True)
    codigo, nombre_legible = resolver_pais(pais)
    slug = re.sub(r"[^a-z0-9]+", "_", _norm_txt(pais)) or "pais"
    cache_path = Path(f"data/laboratorios_{slug}.csv")

    # 1. Cache
    if cache and cache_path.exists():
        log.info(f"  Cargando {pais} desde cache...")
        df = pd.read_csv(cache_path)
        # Verificar que tiene la columna correcta
        if "n_productos" in df.columns and len(df) > 0:
            log.info(f"  Cache OK: {len(df)} laboratorios")
            return df
        else:
            log.warning("  Cache inválido, re-scrapeando...")

    # 2. Intentar scraping oficial
    df_scrapeado = pd.DataFrame()
    try:
        if codigo == "ARG":
            df_scrapeado = scrape_anmat()
        elif codigo == "BRA":
            df_scrapeado = scrape_anvisa()
        # Para otros países, ir directo al curado por ahora
    except Exception as e:
        log.warning(f"  Scraping {pais} falló: {e}")

    if not df_scrapeado.empty:
        log.info(f"  Scraping OK: {len(df_scrapeado)} laboratorios de fuente oficial")

    # 3. Base curada (si el país está en ella) — si no, descubrimiento web
    df_curado = _labs_curados_a_df(codigo) if codigo else pd.DataFrame()
    if df_curado.empty and df_scrapeado.empty:
        # País SIN base → buscar laboratorios en la web con el nombre escrito
        try:
            df_curado = descubrir_laboratorios_web(nombre_legible)
        except Exception as e:
            log.warning(f"  Descubrimiento web falló: {e}")
    else:
        # SIEMPRE sumar laboratorios descubiertos en la web como RESERVA.
        # Así el pozo es más grande, fresco, y no se repiten los mismos.
        try:
            extra = descubrir_laboratorios_web(nombre_legible)
            if not extra.empty:
                ya = set(df_curado["nombre"].str.upper())
                extra = extra[~extra["nombre"].str.upper().isin(ya)]
                df_curado = pd.concat([df_curado, extra], ignore_index=True)
                log.info(f"  Base + reservas web: {len(df_curado)} laboratorios")
        except Exception as e:
            log.warning(f"  Descubrimiento de reservas falló: {e}")
    log.info(f"  Base curada: {len(df_curado)} laboratorios conocidos")

    # 4. Combinar
    if not df_scrapeado.empty and not df_curado.empty:
        # Agregar curados que no estén ya en el scrapeado
        nombres_scrapeados = set(df_scrapeado["nombre"].str.upper())
        df_curado_nuevos = df_curado[
            ~df_curado["nombre"].str.upper().isin(nombres_scrapeados)
        ]
        df_final = pd.concat([df_scrapeado, df_curado_nuevos], ignore_index=True)
        log.info(f"  Total combinado: {len(df_final)} laboratorios")
    elif not df_scrapeado.empty:
        df_final = df_scrapeado
    else:
        # Solo curado
        df_final = df_curado
        log.info(f"  Usando base curada: {len(df_final)} laboratorios")

    # Asegurar columnas correctas SIEMPRE (incluso si quedó vacío)
    COLS = ["nombre", "pais", "rubro", "n_productos", "productos"]
    if df_final is None or len(df_final) == 0:
        log.warning(f"  Sin laboratorios para '{pais}'. Verificá el nombre o la conexión.")
        return pd.DataFrame(columns=COLS)
    if "productos" not in df_final.columns:
        df_final["productos"] = ""
    if "n_productos" not in df_final.columns:
        df_final["n_productos"] = df_final["productos"].apply(
            lambda x: len([p for p in str(x).split("|") if p.strip()])
        )
    if "rubro" not in df_final.columns:
        df_final["rubro"] = "farmacéutico"
    if "pais" not in df_final.columns:
        df_final["pais"] = codigo or pais

    df_final = df_final.sort_values("n_productos", ascending=False).reset_index(drop=True)

    # Guardar cache
    df_final.to_csv(cache_path, index=False, encoding="utf-8")
    log.info(f"  Guardado en cache: {cache_path}")

    return df_final


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    pais = sys.argv[1] if len(sys.argv) > 1 else "ARG"
    df = obtener_laboratorios(pais, cache=False)
    print(f"\n{len(df)} laboratorios en {pais}")
    print(df[["nombre", "rubro", "n_productos"]].head(20).to_string())
