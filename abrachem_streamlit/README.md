# abraChem · Sourcing Intelligence (Streamlit)

App web para encontrar contactos de compras de laboratorios farmacéuticos,
nutracéuticos y veterinarios en cualquier país.

## Probar en tu compu (local)

```bash
pip install -r requirements.txt
streamlit run streamlit_app.py
```
Se abre en http://localhost:8501. Las claves ya están en
`.streamlit/secrets.toml` para uso local.

## Publicar como página web (gratis) — Streamlit Community Cloud

1. **Subí el proyecto a GitHub** (repositorio nuevo). El archivo
   `.streamlit/secrets.toml` NO se sube (está en `.gitignore`): tus claves
   quedan privadas.
2. Entrá a **https://share.streamlit.io** e iniciá sesión con GitHub.
3. **New app** → elegí tu repositorio → *Main file path*: `streamlit_app.py`.
4. En **Advanced settings → Secrets**, pegá:
   ```toml
   HUNTER_API_KEY = "tu_clave_hunter"
   ROCKETREACH_API_KEY = "tu_clave_rocketreach"
   ```
5. **Deploy**. En un par de minutos tenés una URL pública (podés compartirla).

## Notas importantes

- **Las claves nunca se ven en la página.** Se cargan desde *Secrets* (nube)
  o `.streamlit/secrets.toml` (local).
- **Persistencia:** los prospectos se guardan en `data/abrachem_st.db`. En tu
  compu quedan para siempre. En Streamlit Cloud gratis el disco se reinicia al
  redeployar/reiniciar; si querés persistencia permanente en la nube, conviene
  una base externa (Postgres de Railway/Supabase) — te lo puedo armar después.
- Si RocketReach devuelve 429 (límite por hora), la app sigue con Hunter sola.

## Estructura
```
streamlit_app.py   interfaz (sidebar, progreso en vivo, resultados + buscador)
engine.py          motor de búsqueda (pasos 1–5) con eventos en vivo
store.py           guardado persistente (SQLite)
config.py          parámetros (claves por entorno/secrets)
paso1/2_3/4_5...   lógica del pipeline (laboratorios, APIs, dominio, contacto)
```
