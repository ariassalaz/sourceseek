#!/usr/bin/env python3
"""
SourceSeek - Setup y arranque rapido
Ejecutar: python setup_and_run.py
"""
import subprocess
import sys
import os
import shutil
from pathlib import Path

os.chdir(Path(__file__).parent)


def find_python_with_pip():
    """
    Encuentra el Python que realmente tiene pip y puede instalar paquetes.
    Prioriza el Python de AppData (instalacion estandar de Windows).
    """
    candidates = []

    if sys.platform == "win32":
        # 1. Buscar en AppData del usuario (instalacion tipica de python.org)
        appdata = os.environ.get("LOCALAPPDATA", "")
        if appdata:
            for version in ["Python313", "Python312", "Python311", "Python310", "Python39"]:
                p = Path(appdata) / "Programs" / "Python" / version / "python.exe"
                if p.exists():
                    candidates.append(str(p))

        # 2. Buscar en Program Files
        for pf in [os.environ.get("PROGRAMFILES", ""), os.environ.get("PROGRAMFILES(X86)", "")]:
            if pf:
                for version in ["Python313", "Python312", "Python311", "Python310"]:
                    p = Path(pf) / "Python" / version / "python.exe"
                    if p.exists():
                        candidates.append(str(p))

        # 3. Buscar en PATH (evitar MSYS2)
        for name in ["python", "python3"]:
            path = shutil.which(name)
            if path:
                path_lower = path.lower()
                if not any(x in path_lower for x in ["msys", "ucrt", "mingw", "cygwin", "py.exe"]):
                    candidates.append(path)

    # 4. El ejecutable actual como ultimo recurso
    candidates.append(sys.executable)

    # Probar cada candidato
    for py in candidates:
        result = subprocess.run(
            [py, "-c", "import requests; import flask; import faiss; print('OK')"],
            capture_output=True, text=True
        )
        if result.returncode == 0 and "OK" in result.stdout:
            print(f"  Python (con paquetes): {py}")
            return py

    # Ninguno tiene los paquetes aun — usar el que tenga pip para instalar
    for py in candidates:
        result = subprocess.run(
            [py, "-m", "pip", "--version"],
            capture_output=True, text=True
        )
        if result.returncode == 0:
            print(f"  Python (para instalar): {py}")
            return py

    print()
    print("=" * 55)
    print("  ERROR: No se encontro Python con pip.")
    print()
    print("  Instala Python desde: https://python.org/downloads")
    print("  Marca 'Add Python to PATH' durante la instalacion")
    print("=" * 55)
    sys.exit(1)


def run_cmd(cmd, check=True):
    print(f"\n$ {cmd}")
    result = subprocess.run(cmd, shell=True)
    if check and result.returncode != 0:
        print(f"ERROR: fallo con codigo {result.returncode}")
        sys.exit(1)
    return result.returncode == 0


def main():
    print("=" * 50)
    print("   SourceSeek - Futbol Search Setup")
    print("=" * 50)
    print()

    PYTHON = find_python_with_pip()

    # 1. Instalar dependencias con el Python correcto
    print("\n-- [1/4] Instalando dependencias --")
    run_cmd(f'"{PYTHON}" -m pip install -r requirements.txt -q --no-warn-script-location')

    # 2. Ejecutar ingesta, indexacion y servidor usando el MISMO Python
    # (subprocess en lugar de import directo para evitar conflictos de entorno)
    print("\n-- [2/4] Ingesta de datos de futbol --")
    result = subprocess.run(
        [PYTHON, "-c",
         "import sys; sys.path.insert(0, '.'); "
         "from src.ingest import run_ingestion; "
         "import json; "
         "docs = run_ingestion(use_live=True); "
         "json.dump(docs, open('data/documents.json','w',encoding='utf-8'), ensure_ascii=False); "
         "print(f'OK {len(docs)} documentos')"],
        cwd=str(Path(__file__).parent)
    )
    if result.returncode != 0:
        print("ERROR en ingesta")
        sys.exit(1)

    print("\n-- [3/4] Indexacion + embeddings + RDF --")
    print("  (puede tardar 2-5 min descargando el modelo de embeddings)")
    result = subprocess.run(
        [PYTHON, "-c",
         "import sys; sys.path.insert(0, '.'); "
         "import json; "
         "from src.indexer import run_indexing; "
         "docs = json.load(open('data/documents.json', encoding='utf-8')); "
         "index, chunks = run_indexing(docs); "
         "print(f'OK {index.ntotal} vectores FAISS'); "
         "print('OK N-Quads: data/football_knowledge.nq')"],
        cwd=str(Path(__file__).parent)
    )
    if result.returncode != 0:
        print("ERROR en indexacion")
        sys.exit(1)

    # 4. Servidor
    print("\n-- [4/4] Iniciando servidor Flask --")
    print("  http://localhost:5000")
    print("  Ctrl+C para detener")
    print()

    try:
        import requests as req
        r = req.get("http://localhost:11434/api/tags", timeout=2)
        if r.status_code == 200:
            print("  Ollama detectado - modo LLM activo (llama3)")
        else:
            raise Exception()
    except Exception:
        print("  Ollama no disponible - app funciona en modo directo")
        print("  Para LLM: https://ollama.com -> ollama pull llama3")

    print()
    subprocess.run([PYTHON, "app.py"], cwd=str(Path(__file__).parent))


if __name__ == "__main__":
    main()
