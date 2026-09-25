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
EXECUTOR_CHAT_ID = os.getenv("EXECUTOR_CHAT_ID")  # Ej: -1003994540922

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

# Silenciar mensajes verbosos de la librería
logging.getLogger("iqoptionapi").setLevel(logging.CRITICAL)

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

# ================= 4. CATÁLOGO MAESTRO MULTI-ACTIVOS =================
PARES_ROBUSTOS = [
    # --- FOREX Y COMMODITIES OTC ---
    "EURUSD-OTC", "GBPUSD-OTC", "USDJPY-OTC", "EURJPY-OTC", "XAUUSD-OTC", "AUDCAD-OTC",

    # --- CRYPTO & MEMES OTC ---
    "PEPEUSD-OTC", "TRUMPUSD-OTC", "LUNAUSD-OTC", "BTCUSD-OTC", "DOGEUSD-OTC",

    # --- ACCIONES GLOBALES OTC (Tickers Wall Street) ---
    "TSLA-OTC",   # Tesla
    "NKE-OTC",    # Nike
    "KO-OTC",     # Coca-Cola
    "AAPL-OTC",   # Apple
    "AMZN-OTC",   # Amazon
    "NVDA-OTC"    # Nvidia
]

ALIAS_ACTIVOS = {
    "PEPE": "PEPEUSD-OTC",
    "TRUMP": "TRUMPUSD-OTC",
    "LUNA": "LUNAUSD-OTC",
    "BITCOIN": "BTCUSD-OTC",
    "BTC": "BTCUSD-OTC",
    "DOGE": "DOGEUSD-OTC",
    "TESLA": "TSLA-OTC",
    "TSLA": "TSLA-OTC",
    "NIKE": "NKE-OTC",
    "NKE": "NKE-OTC",
    "COCACOLA": "KO-OTC",
    "COCA-COLA": "KO-OTC",
    "KO": "KO-OTC",
    "APPLE": "AAPL-OTC",
    "AAPL": "AAPL-OTC",
    "AMAZON": "AMZN-OTC",
    "AMZN": "AMZN-OTC",
    "NVIDIA": "NVDA-OTC",
    "NVDA": "NVDA-OTC",
    "ORO": "XAUUSD-OTC",
    "GOLD": "XAUUSD-OTC"
}

def _analizar_par_seguro(par):
    global API
    try:
        velas = API.get_candles(par, 60, 4, int(time.time()))
        if not velas or not isinstance(velas, list) or len(velas) < 3:
            return None

        df = pd.DataFrame(velas)
        c_act = float(df.iloc[-1]["close"])
        c_prev = float(df.iloc[-2]["close"])
        o_act = float(df.iloc[-1]["open"])

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

async def analizar_par_con_timeout(par):
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(_analizar_par_seguro, par),
            timeout=1.5
        )
    except Exception:
        return None

# ================= 5. COMANDOS TELEGRAM =================
async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conectado = asegurar_conexion()
    saldo = f"${API.get_balance():.2f}" if conectado and API else "$0.00"
    canal_info = f"`{EXECUTOR_CHAT_ID}`" if EXECUTOR_CHAT_ID else "⚠️ No configurado"
    
    await update.message.reply_text(
        f"📊 **Atleon Terminal - Multi-Activos**\n"
        f"• Broker: {'🟢 Conectado' if conectado else '🔴 Desconectado'}\n"
        f"• Saldo: {saldo}\n"
        f"• Canal Destino: {canal_info}\n"
        f"• Catálogo: Forex, Oro, Cripto (Pepe, Luna, Trump) y Acciones (Nike, Tesla, Coca-Cola)"
    )

async def analizar_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Uso: `/analizar PEPE`, `/analizar NIKE`, `/analizar TESLA`, `/analizar EURUSD-OTC`")
        return

    entrada = context.args[0].upper().replace("/", "").strip()
    par = ALIAS_ACTIVOS.get(entrada, entrada)
    if not "-OTC" in par and not any(f in par for f in ["USD", "EUR", "GBP"]):
        par = f"{par}-OTC"

    if not asegurar_conexion():
        await update.message.reply_text("❌ Error de conexión con el broker.")
        return

    msg = await update.message.reply_text(f"🔍 Analizando `{par}` ({entrada})...")
    res = await analizar_par_con_timeout(par)

    if res:
        texto = (
            f"🔍 **Análisis: {res['activo']}**\n\n"
            f"• Señal: **{res['direccion']}**\n"
            f"• Precio: `{res['precio']}`\n"
            f"• Detalle: {res['detalle']}\n\n"
            f"Comando sugerido: `{res['activo']} {res['direccion']}`"
        )
        await msg.edit_text(texto)
    else:
        await msg.edit_text(f"⚪ `{par}` mercado cerrado o sin liquidez de velas en este segundo.")

async def escanear_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not asegurar_conexion():
        await update.message.reply_text("❌ Sin conexión con el broker.")
        return

    msg = await update.message.reply_text("🛰️ Escaneando multi-activos (Forex, Cripto y Acciones)...")
    senales = []

    for par in PARES_ROBUSTOS:
        res = await analizar_par_con_timeout(par)
        if res:
            senales.append(res)
            if EXECUTOR_CHAT_ID:
                try:
                    await context.bot.send_message(
                        chat_id=EXECUTOR_CHAT_ID,
                        text=f"{res['activo']} {res['direccion']}"
                    )
                except Exception as e:
                    logging.warning(f"Error al publicar en canal: {e}")

    if senales:
        lineas = [f"🎯 `{s['activo']}` ➔ **{s['direccion']}**" for s in senales]
        resultado = "⚡ **Señales Detectadas y Despachadas:**\n\n" + "\n".join(lineas)
        await msg.edit_text(resultado)
    else:
        await msg.edit_text("⚪ Sin señales claras en este instante. Reintenta en unos segundos.")

# ================= 6. ARRANQUE =================
if __name__ == "__main__":
    Thread(target=run_web_server, daemon=True).start()
    conectar_broker()

    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", status_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("analizar", analizar_cmd))
    app.add_handler(CommandHandler("escanear", escanear_cmd))

    app.run_polling(drop_pending_updates=True)
