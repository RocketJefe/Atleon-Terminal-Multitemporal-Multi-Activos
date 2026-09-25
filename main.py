import os
import time
import logging
import asyncio
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
logging.getLogger("iqoptionapi").setLevel(logging.CRITICAL)

# ================= 2. SERVIDOR KEEPALIVE HTTP =================
class RenderKeepAliveHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"Atleon Terminal Live")

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
        return False
    try:
        cliente = IQ_Option(BROKER_EMAIL.strip(), BROKER_PASSWORD.strip())
        if BROKER_PLATFORM == "EXNOVA":
            cliente.https_url = "https://exnova.com/api"
            cliente.wss_url = "wss://ws.exnova.com/echo/websocket"

        ok, _ = cliente.connect()
        if ok:
            API = cliente
            logging.info(f"✅ Conectado a {BROKER_PLATFORM}")
            return True
        return False
    except Exception:
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

# ================= 4. DETECCIÓN DINÁMICA DE ACTIVOS VIVOS =================
ALIAS_MAP = {
    "PEPE": "PEPEUSD-OTC", "TRUMP": "TRUMPUSD-OTC", "LUNA": "LUNAUSD-OTC",
    "BTC": "BTCUSD-OTC", "BITCOIN": "BTCUSD-OTC", "DOGE": "DOGEUSD-OTC",
    "NIKE": "NKE-OTC", "NKE": "NKE-OTC", "TESLA": "TSLA-OTC", "TSLA": "TSLA-OTC",
    "APPLE": "AAPL-OTC", "AAPL": "AAPL-OTC", "COCACOLA": "KO-OTC", "KO": "KO-OTC",
    "ORO": "XAUUSD-OTC", "GOLD": "XAUUSD-OTC"
}

def obtener_activos_vivos_ahora():
    """Extrae únicamente los activos que IQ Option tiene abiertos y con liquidez en este instante."""
    if not asegurar_conexion():
        return []

    abiertos = []
    try:
        init_data = API.get_all_init()
        turbo = init_data.get("result", {}).get("turbo", {}).get("actives", {})
        binary = init_data.get("result", {}).get("binary", {}).get("actives", {})

        for data in [turbo, binary]:
            for _, info in data.items():
                if info.get("enabled", False) and not info.get("is_suspended", True):
                    raw = info.get("name", "").replace("front.", "").replace("/", "").strip()
                    if raw and raw not in abiertos:
                        abiertos.append(raw)
    except Exception:
        pass

    if not abiertos:
        # Respaldo confiable si get_all_init no devuelve datos temporales
        abiertos = ["EURUSD-OTC", "GBPUSD-OTC", "USDJPY-OTC", "EURJPY-OTC", "XAUUSD-OTC"]

    return abiertos

def _analizar_par_rapido(par):
    global API
    try:
        # Se solicitan solo 3 velas con timeout instantáneo
        velas = API.get_candles(par, 60, 3, int(time.time()))
        if not velas or not isinstance(velas, list) or len(velas) < 3:
            return None

        c_act = float(velas[-1]["close"])
        c_prev = float(velas[-2]["close"])
        o_act = float(velas[-1]["open"])

        tendencia = "CALL" if c_act > c_prev else ("PUT" if c_act < c_prev else None)
        impulso = "CALL" if c_act > o_act else ("PUT" if c_act < o_act else None)

        if tendencia and tendencia == impulso:
            return {
                "activo": par,
                "direccion": tendencia,
                "precio": c_act,
                "detalle": f"Tendencia:{tendencia} | Impulso:{impulso}"
            }
    except Exception:
        pass
    return None

async def analizar_par_seguro(par):
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(_analizar_par_rapido, par),
            timeout=1.2
        )
    except Exception:
        return None

# ================= 5. CONTROLADORES TELEGRAM =================
async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conectado = asegurar_conexion()
    saldo = f"${API.get_balance():.2f}" if conectado and API else "$0.00"
    canal_info = f"`{EXECUTOR_CHAT_ID}`" if EXECUTOR_CHAT_ID else "⚠️ No configurado"
    
    await update.message.reply_text(
        f"📊 **Atleon Terminal - Estado**\n"
        f"• Broker: {'🟢 Conectado' if conectado else '🔴 Desconectado'}\n"
        f"• Saldo: {saldo}\n"
        f"• Canal Destino: {canal_info}\n"
        f"• Motor: Detección dinámica en vivo (Forex, Crypto, Stocks)"
    )

async def activos_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("🔍 Consultando activos vivos en el broker...")
    vivos = obtener_activos_vivos_ahora()
    if vivos:
        resumen = ", ".join(vivos[:25])
        await msg.edit_text(f"⚡ **Activos Abiertos Ahora ({len(vivos)}):**\n\n`{resumen}`")
    else:
        await msg.edit_text("⚠️ No se pudieron obtener los activos abiertos en este momento.")

async def analizar_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Uso: `/analizar EURUSD-OTC` o `/analizar PEPE`")
        return

    entrada = context.args[0].upper().replace("/", "").strip()
    par = ALIAS_MAP.get(entrada, entrada)
    if "-OTC" not in par and not any(k in par for k in ["USD", "EUR", "GBP", "JPY"]):
        par = f"{par}-OTC"

    if not asegurar_conexion():
        await update.message.reply_text("❌ Sin conexión con el broker.")
        return

    msg = await update.message.reply_text(f"🔍 Evaluando `{par}`...")
    res = await analizar_par_seguro(par)

    if res:
        texto = (
            f"🔍 **Análisis: {res['activo']}**\n\n"
            f"• Señal: **{res['direccion']}**\n"
            f"• Precio: `{res['precio']}`\n"
            f"• Detalle: {res['detalle']}\n\n"
            f"Disparo sugerido: `{res['activo']} {res['direccion']}`"
        )
        await msg.edit_text(texto)
    else:
        await msg.edit_text(f"⚪ `{par}` mercado cerrado o sin liquidez suficiente en este segundo.")

async def escanear_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not asegurar_conexion():
        await update.message.reply_text("❌ Sin conexión con el broker.")
        return

    msg = await update.message.reply_text("🛰️ Escaneando los activos activos de mayor liquidez...")
    
    # Tomamos los activos que el broker reporta abiertos
    candidatos = obtener_activos_vivos_ahora()[:8]  # Limite de 8 para responder en <4 segundos

    senales = []
    for par in candidatos:
        res = await analizar_par_seguro(par)
        if res:
            senales.append(res)
            if EXECUTOR_CHAT_ID:
                try:
                    await context.bot.send_message(
                        chat_id=EXECUTOR_CHAT_ID,
                        text=f"{res['activo']} {res['direccion']}"
                    )
                except Exception as e:
                    logging.warning(f"Error publicando en canal: {e}")

    if senales:
        lineas = [f"🎯 `{s['activo']}` ➔ **{s['direccion']}**" for s in senales]
        await msg.edit_text("⚡ **Señales Detectadas y Despachadas:**\n\n" + "\n".join(lineas))
    else:
        await msg.edit_text("⚪ Sin señales técnicas en este instante en los activos abiertos. Reintenta en 30s.")

# ================= 6. ARRANQUE CON TIMEOUTS AMPLIADOS =================
if __name__ == "__main__":
    Thread(target=run_web_server, daemon=True).start()
    conectar_broker()

    # Construcción de la aplicación con timeouts extendidos para evitar 'TimedOut'
    app = (
        ApplicationBuilder()
        .token(TELEGRAM_BOT_TOKEN)
        .connect_timeout(30.0)
        .read_timeout(30.0)
        .write_timeout(30.0)
        .build()
    )

    app.add_handler(CommandHandler("start", status_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("activos", activos_cmd))
    app.add_handler(CommandHandler("analizar", analizar_cmd))
    app.add_handler(CommandHandler("escanear", escanear_cmd))

    app.run_polling(drop_pending_updates=True)
