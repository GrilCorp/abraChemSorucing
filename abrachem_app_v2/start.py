"""
abraChem Prospector v2 — Arranque
Doble click o: python3 start.py
"""
import subprocess, sys, os, webbrowser, time, threading
from pathlib import Path

def main():
    print("\n" + "="*50)
    print("  abraChem Prospector v2")
    print("="*50)

    # Instalar dependencias si faltan
    for pkg in ["flask", "flask_sqlalchemy", "requests", "pandas", "beautifulsoup4"]:
        try:
            __import__(pkg.replace("-","_"))
        except ImportError:
            print(f"  Instalando {pkg}...")
            subprocess.run([sys.executable, "-m", "pip", "install", pkg, "-q"], check=True)

    # Cambiar al directorio de app.py (crucial para que Flask encuentre templates)
    app_dir = Path(__file__).resolve().parent
    os.chdir(app_dir)
    sys.path.insert(0, str(app_dir))

    print(f"\n  Directorio: {app_dir}")
    print("  Abriendo en http://localhost:5000")
    print("  Presioná Ctrl+C para cerrar\n")

    # Abrir navegador después de que Flask arranque
    def abrir():
        time.sleep(2)
        webbrowser.open("http://127.0.0.1:5000")
    threading.Thread(target=abrir, daemon=True).start()

    # Importar y correr desde el directorio correcto
    from app import app
    app.run(debug=False, port=5000, host="127.0.0.1", use_reloader=False)

if __name__ == "__main__":
    main()
