
import os
import json
import threading
import time
from typing import Callable, Iterable, Dict, Any

from flask import Flask, request, jsonify, render_template, send_from_directory, abort
from werkzeug.utils import secure_filename

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.auth.transport.requests import Request
from googleapiclient.errors import HttpError

# =========================
# Configuración general
# =========================

SCOPES = ["https://www.googleapis.com/auth/drive.file"]
CHUNK_SIZE = 256 * 1024  # 256 KiB

UPLOAD_DIR = os.environ.get("UPLOAD_DIR", "temp_uploads")
TEMPLATES_AUTO_RELOAD = True
CONFIG_FILE = os.environ.get("CONFIG_FILE", "config.json")
DEFAULT_CREDENTIALS_FILE = os.environ.get("OAUTH_CLIENT_FILE", "credentials.json")
MAX_CONTENT_LENGTH = int(os.environ.get("MAX_CONTENT_LENGTH", 1024 * 1024 * 1024))  # 1GB por defecto

# Crear carpeta de uploads si no existe
os.makedirs(UPLOAD_DIR, exist_ok=True)

# =========================
# Cargar configuración
# =========================
def load_config() -> Dict[str, Any]:
    if not os.path.exists(CONFIG_FILE):
        raise FileNotFoundError(f"No se encontró {CONFIG_FILE}")
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    if "cuentas" not in cfg:
        raise KeyError("config.json debe contener la clave 'cuentas'")
    return cfg

CONFIG = load_config()
CUENTAS = CONFIG["cuentas"]
CREDENTIALS_FILE = CONFIG.get("oauth_client", DEFAULT_CREDENTIALS_FILE)

# =========================
# Helpers Google Drive
# =========================
def is_transient(err: Exception) -> bool:
    s = str(err).lower()
    return any(sig in s for sig in (
        "eof occurred", "connection reset", "broken pipe",
        "timed out", "timeout", "ssl", "tls",
        "reset by peer", "transport closed",
        "503", "500", "429",
    ))

def next_chunk_with_retry(request, max_retries: int = 7, base_delay: float = 1.2):
    intento = 0
    while True:
        try:
            return request.next_chunk()
        except (HttpError, OSError) as e:
            if intento < max_retries and is_transient(e):
                sleep_s = base_delay * (2 ** intento) * (1 + 0.12 * (intento % 3))
                print(f"[upload] Retry {intento+1} tras error transitorio: {e}")
                time.sleep(sleep_s)
                intento += 1
                continue
            raise

def get_service(cuenta: str):
    if cuenta not in CUENTAS:
        raise KeyError(f"Cuenta desconocida: {cuenta}")
    token_file = CUENTAS[cuenta]["credenciales"]
    creds = None

    if os.path.exists(token_file):
        creds = Credentials.from_authorized_user_file(token_file, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print("[auth] Refrescando token…")
            creds.refresh(Request())
        else:
            # ⚠️ Solo desarrollo local
            print("[auth] Ejecutando flujo local para obtener token")
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)

        with open(token_file, "w", encoding="utf-8") as f:
            f.write(creds.to_json())

    return build("drive", "v3", credentials=creds, cache_discovery=False)

def upload_files(
    cuenta: str,
    rutas: Iterable[str],
    progress_cb: Callable[[int], None],
    status_cb: Callable[[str], None],
):
    try:
        service = get_service(cuenta)
        carpeta_id = CUENTAS[cuenta]["carpeta"]
        rutas = list(rutas)
        total = len(rutas)

        for i, ruta in enumerate(rutas, start=1):
            nombre = os.path.basename(ruta)
            status_cb(f"Subiendo ({i}/{total}): {nombre}")
            progress_cb(0)

            metadata = {"name": nombre, "parents": [carpeta_id]}
            media = MediaFileUpload(ruta, chunksize=CHUNK_SIZE, resumable=True)
            request = service.files().create(body=metadata, media_body=media, fields="id")

            while True:
                status, done = next_chunk_with_retry(request)
                if status:
                    progress_cb(int(status.progress() * 100))
                if done:
                    break

        progress_cb(100)
        status_cb("✅ Subida completada")
    except Exception as e:
        status_cb(f"❌ Error: {type(e).__name__}: {e}")
    finally:
        # Limpiar archivos temporales
        for ruta in rutas:
            try:
                os.remove(ruta)
            except Exception:
                pass

# =========================
# Flask app
# =========================
app = Flask(__name__, template_folder="templates")
app.config["MAX_CONTENT_LENGTH"] = MAX_CONTENT_LENGTH
app.config["TEMPLATES_AUTO_RELOAD"] = TEMPLATES_AUTO_RELOAD

@app.route("/")
def home():
    return render_template("index.html", cuentas=list(CUENTAS.keys()))

@app.get("/healthz")
def healthz():
    return jsonify({"ok": True})

@app.post("/upload")
def upload():
    cuenta = request.form.get("cuenta")
    files = request.files.getlist("archivos")

    if not cuenta or not files:
        return jsonify({"error": "Faltan parámetros: cuenta y archivos"}), 400
    if cuenta not in CUENTAS:
        return jsonify({"error": f"Cuenta desconocida: {cuenta}"}), 400

    rutas = []
    for f in files:
        filename = secure_filename(f.filename or "archivo")
        if not filename:
            continue
        ruta = os.path.join(UPLOAD_DIR, filename)
        f.save(ruta)
        rutas.append(ruta)

    if not rutas:
        return jsonify({"error": "No se recibieron archivos válidos"}), 400

    # Subida en segundo plano
    threading.Thread(
        target=upload_files,
        args=(cuenta, rutas, lambda v: print(f"[progress] {v}%"), lambda s: print(f"[status] {s}")),
        daemon=True
    ).start()

    return jsonify({"status": "Subida iniciada"}), 202

@app.get("/.well-known/assetlinks.json")
def assetlinks():
    path = os.path.join(app.root_path, ".well-known")
    file = os.path.join(path, "assetlinks.json")
    if os.path.exists(file):
        return send_from_directory(path, "assetlinks.json", mimetype="application/json")
    return abort(404)

# =========================
# Arranque local (desarrollo)
# =========================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=True)
