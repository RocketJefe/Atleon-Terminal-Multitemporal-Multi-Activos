import os
import time
import logging
from threading import Thread
from http.server import HTTPServer, BaseHTTPRequestHandler
from dotenv import load_dotenv

import pandas as pd
from iqoptionapi.stable_api import IQ_Option
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    ContextTypes,
    CommandHandler,
)

# ================= 1. VARIABLES DE ENTORNO =================
load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
BROKER_EMAIL = os.getenv("BROKER_EMAIL")
BROKER_PASSWORD = os.getenv("BROKER_PASSWORD")
BROKER_PLATFORM = os.getenv("BROKER_PLATFORM", "IQOPTION").upper()
EXECUTOR_CHAT_ID = os.getenv("EXECUTOR_CHAT_ID")

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

# ================= 2. SERVIDOR KEEPALIVE HTTP =================
class RenderKeepAliveHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"Atleon Terminal Multitemporal Live")

    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()

def run_web_server():
    port = int(os.environ.get("PORT", 8080))
    server = HTTPServer(("0.0.0.0", port), RenderKeepAliveHandler)
    server.serve_forever()

# ================= 3. CONEXIÓN PERSISTENTE =================
API = None

def conectar_broker():
    global API
    if not BROKER_EMAIL or not BROKER_PASSWORD:
        logging.error("Variables BROKER_EMAIL o BROKER_PASSWORD ausentes.")
        return False

    try:
        cliente = IQ_Option(BROKER_EMAIL.strip(), BROKER_PASSWORD.strip())
        if BROKER_PLATFORM == "EXNOVA":
            cliente.https_url = "https://exnova.com/api"
            cliente.wss_url = "wss://ws.exnova.com/echo/websocket"

        ok, reason = cliente.connect()
        if ok:
            API = cliente
            logging.info(f"✅ Conectado a {BROKER_PLATFORM} con éxito.")
            return True
        else:
            logging.error(f"❌ Error al conectar: {reason}")
            return False
    except Exception as e:
        logging.error(f"❌ Excepción en conexión: {e}")
        return False

def asegurar_conexion():
    global API
    if API is None:
        return conectar_broker()
    try:
        if not API.check_connect():
            ok, _ = API.connect()
            return ok
        return True
    except Exception:
        return conectar_broker()

# ================= 4. EXTRACCIÓN SEGURA DE VELAS =================
def obtener_velas_stream(activo, tamaño=60, cantidad=5):
    """
    Obtiene velas usando el flujo en tiempo real (stream) sin saturar get_candles.
    """
    global API
    if not asegurar_conexion():
        return None

    try:
        API.start_candles_stream(activo, tamaño, cantidad)
        time.sleep(0.3)
        candles_dict = API.get_realtime_candles(activo, tamaño)
        API.stop_candles_stream(activo, tamaño)

        if candles_dict and isinstance(candles_dict, dict) and len(candles_dict) > 0:
            lista_velas = list(candles_dict.values())
            lista_velas.sort(key=lambda x: x.get("from", 0))
            return lista_velas
    except Exception:
        pass

    # Fallback con timestamp exacto
    try:
        velas = API.get_candles(activo, tamaño, cantidad, int(time.time()))
        if velas and isinstance(velas, list) and len(velas) > 0:
            return velas
    except Exception:
        pass

    return None

def analizar_activo(activo):
    try:
        velas_1m = obtener_velas_stream(activo, 60, 5)
        if not velas_1m or len(velas_1m) < 3:
            return None

        df = pd.DataFrame(velas_1m)
        df["close"] = df["close"].astype(float)
        df["open"] = df["open"].astype(float)

        c_act = df.iloc[-1]["close"]
        c_prev = df.iloc[-2]["close"]
        o_act = df.iloc[-1]["open"]

        dir_tendencia = "CALL" if c_act > c_prev else ("PUT" if c_act < c_prev else "NEUTRAL")
        dir_micro = "CALL" if c_act > o_act else ("PUT" if c_act < o_act else "NEUTRAL")

        # Confirmación de dirección
        if dir_tendencia == dir_micro and dir_tendencia != "NEUTRAL":
            return {
                "activo": activo,
                "direccion": dir_tendencia,
                "fuerza": "2/2 Confirmado",
                "precio": c_act,
                "detalle": f"Tendencia:{dir_tendencia} | Impulso:{dir_micro}"
            }
    except Exception as e:
        logging.debug(f"Aviso en {activo}: {e}")

    return None

# ================= 5. CATÁLOGO DE ACTIVOS =================
def obtener_activos():
    if not asegurar_conexion():
        return []

    activos = set()
    try:
        init_data = API.get_all_init()
        turbo = init_data.get("result", {}).get("turbo", {}).get("actives", {})
        for _, info in turbo.items():
            if info.get("enabled", False) and not info.get("is_suspended", True):
                nombre = info.get("name", "").replace("front.", "").replace("/", "").strip()
                if nombre and len(nombre) >= 4:
                    activos.add(nombre)
    except Exception:
        pass

    respaldo = [
        "EURUSD-OTC", "GBPUSD-OTC", "USDJPY-OTC", "EURJPY-OTC",
        "XAUUSD-OTC", "PEPEUSD-OTC", "TRUMPUSD-OTC", "BTCUSD-OTC"
    ]
    for r in respaldo:
        activos.add(r)

    return list(activos)

# ================= 6. CONTROLADORES TELEGRAM =================
async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conectado = asegurar_conexion()
    estado = "🟢 Conectado" if conectado else "🔴 Desconectado"
    saldo = f"${API.get_balance():.2f}" if conectado and API else "$0.00"

    await update.message.reply_text(
        f"📊 Atleon Terminal - Diagnóstico\n"
        f"• Plataforma: {BROKER_PLATFORM}\n"
        f"• Estado: {estado}\n"
        f"• Saldo: {saldo}\n"
        f"• Modo: Confluencia Rápida Stream\n"
        f"• Catálogo: Multi-Activos (Forex, Crypto, Acciones)"
    )

async def activos_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("🔍 Consultando activos...")
    lista = obtener_activos()
    if lista:
        resumen = ", ".join(lista[:20])
        await msg.edit_text(f"⚡ Activos disponibles ({len(lista)}):\n\n{resumen}")
    else:
        await msg.edit_text("⚠️ No se pudieron obtener activos en este instante.")

async def escanear_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("🛰️ Escaneando mercado...")
    activos = obtener_activos()[:8]

    senales = []
    for act in activos:
        res = analizar_activo(act)
        if res:
            senales.append(res)
            if EXECUTOR_CHAT_ID:
                try:
                    await context.bot.send_message(
                        chat_id=EXECUTOR_CHAT_ID,
                        text=f"{res['activo']} {res['direccion']}"
                    )
                except Exception as e:
                    logging.warning(f"Error despachando: {e}")
        time.sleep(0.15)

    if senales:
        lineas = [
            f"🎯 {s['activo']} ➔ {s['direccion']}\n   └ {s['detalle']}"
            for s in senales
        ]
        await msg.edit_text("⚡ Señales Detectadas:\n\n" + "\n\n".join(lineas))
    else:
        await msg.edit_text("⚪ Sin confluencia clara en este momento.")

async def analizar_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Uso: /analizar EURUSD-OTC")
        return

    par = context.args[0].upper().replace("/", "").strip()
    res = analizar_activo(par)

    if res:
        await update.message.reply_text(
            f"🔍 Análisis: {par}\n\n"
            f"• Señal: {res['direccion']}\n"
            f"• Precio: {res['precio']}\n"
            f"• Detalle: {res['detalle']}\n\n"
            f"Comando sugerido: {par} {res['direccion']}"
        )
    else:
        await update.message.reply_text(f"⚪ {par} sin señal definida o datos insuficientes.")

# ================= 7. ARRANQUE =================
if __name__ == "__main__":
    Thread(target=run_web_server, daemon=True).start()
    conectar_broker()

    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", status_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("activos", activos_cmd))
    app.add_handler(CommandHandler("escanear", escanear_cmd))
    app.add_handler(CommandHandler("analizar", analizar_cmd))

    app.run_polling(drop_pending_updates=True)
