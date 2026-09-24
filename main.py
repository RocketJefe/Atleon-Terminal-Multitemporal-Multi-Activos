import os
import time
import logging
import asyncio
from threading import Thread
from http.server import HTTPServer, BaseHTTPRequestHandler
from dotenv import load_dotenv

import pandas as pd
from iqoptionapi.stable_api import IQ_Option
from telegram import Update, constants
from telegram.ext import (
    ApplicationBuilder,
    ContextTypes,
    CommandHandler,
    MessageHandler,
    filters,
)

# ================= 1. VARIABLES DE ENTORNO =================
load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
BROKER_EMAIL = os.getenv("BROKER_EMAIL")
BROKER_PASSWORD = os.getenv("BROKER_PASSWORD")
BROKER_PLATFORM = os.getenv("BROKER_PLATFORM", "IQOPTION").upper()  # "IQOPTION" o "EXNOVA"
EXECUTOR_CHAT_ID = os.getenv("EXECUTOR_CHAT_ID")  # Chat ID del ejecutor o canal compartido

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

# ================= 2. SERVIDOR KEEPALIVE HTTP (RENDER) =================
class RenderKeepAliveHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"Atleon Terminal Multitemporal & Multi-Activos Live!")

    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()

def run_web_server():
    port = int(os.environ.get("PORT", 8080))
    server = HTTPServer(("0.0.0.0", port), RenderKeepAliveHandler)
    server.serve_forever()

# ================= 3. CONEXIÓN ADAPTABLE (IQ OPTION / EXNOVA) =================
API = None
ultimo_error_broker = "Sin intento de conexion aun"

def conectar_broker():
    global API, ultimo_error_broker
    if not BROKER_EMAIL or not BROKER_PASSWORD:
        ultimo_error_broker = "Variables BROKER_EMAIL o BROKER_PASSWORD no configuradas"
        logging.error(ultimo_error_broker)
        return False, ultimo_error_broker

    try:
        logging.info(f"Conectando a {BROKER_PLATFORM} con usuario {BROKER_EMAIL}...")
        cliente = IQ_Option(BROKER_EMAIL.strip(), BROKER_PASSWORD.strip())

        # Redirección de endpoints si la plataforma elegida es Exnova
        if BROKER_PLATFORM == "EXNOVA":
            cliente.https_url = "https://exnova.com/api"
            cliente.wss_url = "wss://ws.exnova.com/echo/websocket"

        check, reason = cliente.connect()
        if check:
            API = cliente
            ultimo_error_broker = "Conectado"
            logging.info(f"✅ Conectado exitosamente a {BROKER_PLATFORM}.")
            return True, "OK"
        else:
            ultimo_error_broker = str(reason)
            logging.error(f"❌ Error conectando a {BROKER_PLATFORM}: {reason}")
            return False, str(reason)
    except Exception as e:
        ultimo_error_broker = str(e)
        return False, str(e)

def asegurar_conexion():
    global API, ultimo_error_broker
    if API is None:
        ok, _ = conectar_broker()
        return ok
    try:
        if not API.check_connect():
            check, reason = API.connect()
            if not check:
                ultimo_error_broker = str(reason)
            return check
        return True
    except Exception as e:
        ultimo_error_broker = str(e)
        return False

# ================= 4. ESCANEO UNIVERSAL MULTI-ACTIVOS =================
def obtener_catalogo_activos():
    """
    Rastrea activos vivos en la plataforma:
    Forex, Crypto (Pepe, Trump), Acciones (Nike, Apple), Metales y OTC.
    """
    if not asegurar_conexion():
        return []

    activos_detectados = set()
    try:
        init_data = API.get_all_init()
        turbo_data = init_data.get("result", {}).get("turbo", {}).get("actives", {})
        binary_data = init_data.get("result", {}).get("binary", {}).get("actives", {})

        for conjunto in [turbo_data, binary_data]:
            for _, info in conjunto.items():
                if info.get("enabled", False) and not info.get("is_suspended", True):
                    raw_name = info.get("name", "")
                    clean_name = raw_name.replace("front.", "").replace("/", "").strip()
                    if clean_name:
                        activos_detectados.add(clean_name)
    except Exception as e:
        logging.warning(f"Aviso al extraer catálogo dinámico: {e}")

    # Fallback con canasta multiactivos de alta demanda
    canasta_respaldo = [
        "EURUSD", "EURUSD-OTC", "GBPUSD", "GBPUSD-OTC", "USDJPY", "USDJPY-OTC",
        "XAUUSD", "XAUUSD-OTC", "PEPEUSD-OTC", "TRUMPUSD-OTC", "BTCUSD-OTC",
        "AAPL-OTC", "NKE-OTC", "TSLA-OTC"
    ]
    for activo in canasta_respaldo:
        activos_detectados.add(activo)

    return list(activos_detectados)

# ================= 5. MOTOR MULTITEMPORAL CONFLUENTE =================
def evaluar_tendencia(velas):
    if not velas or len(velas) < 3:
        return "NEUTRAL", 0.0

    df = pd.DataFrame(velas)
    df["close"] = df["close"].astype(float)
    df["open"] = df["open"].astype(float)

    c_actual = df.iloc[-1]["close"]
    c_prev = df.iloc[-2]["close"]

    if c_actual > c_prev:
        return "CALL", c_actual
    elif c_actual < c_prev:
        return "PUT", c_actual
    return "NEUTRAL", c_actual

def analizar_multitemporal(activo):
    """
    Evalúa el activo en 30s, 1m (60s) y 2m (120s).
    Aplica filtro de confluencia: emite señal si al menos 2 temporalidades coinciden.
    """
    if not asegurar_conexion():
        return None

    ahora = time.time()
    try:
        velas_30s = API.get_candles(activo, 30, 5, ahora)
        velas_1m = API.get_candles(activo, 60, 5, ahora)
        velas_2m = API.get_candles(activo, 120, 5, ahora)

        dir_30s, _ = evaluar_tendencia(velas_30s)
        dir_1m, precio = evaluar_tendencia(velas_1m)
        dir_2m, _ = evaluar_tendencia(velas_2m)

        votos = [dir_30s, dir_1m, dir_2m]
        calls = votos.count("CALL")
        puts = votos.count("PUT")

        confluencia = None
        fuerza = 0
        if calls >= 2:
            confluencia = "CALL"
            fuerza = calls
        elif puts >= 2:
            confluencia = "PUT"
            fuerza = puts

        if confluencia:
            return {
                "activo": activo,
                "direccion": confluencia,
                "fuerza": f"{fuerza}/3",
                "precio": precio,
                "detalle": f"30s:{dir_30s} | 1m:{dir_1m} | 2m:{dir_2m}"
            }
    except Exception as e:
        logging.debug(f"Aviso al analizar {activo}: {e}")

    return None

# ================= 6. CONTROLADORES TELEGRAM =================
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"🧠 Atleon Terminal Multitemporal & Multi-Activos\n"
        f"• Broker configurado: {BROKER_PLATFORM}\n\n"
        f"Comandos:\n"
        f"• /status - Diagnóstico y saldo del broker.\n"
        f"• /activos - Lista de activos abiertos en vivo.\n"
        f"• /escanear - Escaneo confluente (30s, 1m, 2m) en todo el catálogo.\n"
        f"• /analizar <PAR> - Análisis específico (ej: /analizar EURUSD-OTC).\n"
        f"• /disparar <PAR> <CALL/PUT> - Envío manual directo al Ejecutor."
    )

async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global ultimo_error_broker
    conectado = asegurar_conexion()
    estado = "🟢 Conectado" if conectado else f"🔴 Desconectado ({ultimo_error_broker})"
    saldo = f"${API.get_balance():.2f}" if conectado and API else "$0.00"

    await update.message.reply_text(
        f"📊 Atleon Terminal - Diagnóstico\n"
        f"• Plataforma: {BROKER_PLATFORM}\n"
        f"• Estado: {estado}\n"
        f"• Saldo: {saldo}\n"
        f"• Análisis: 30s / 1M / 2M Confluente\n"
        f"• Multi-Activos: Forex, Crypto, Stocks e Índices"
    )

async def activos_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("🔍 Escaneando activos disponibles...")
    lista = obtener_catalogo_activos()
    if lista:
        muestra = ", ".join(lista[:25])
        await msg.edit_text(f"⚡ Catálogo Detectado ({len(lista)} activos):\n\n{muestra}...")
    else:
        await msg.edit_text("⚠️ No se detectaron activos en este instante.")

async def escanear_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("🛰️ Escaneando confluencia multitemporal en el mercado...")
    activos = obtener_catalogo_activos()[:15]

    senales = []
    for act in activos:
        res = analizar_multitemporal(act)
        if res:
            senales.append(res)
            # Reenvío automático al Ejecutor
            if EXECUTOR_CHAT_ID:
                try:
                    await context.bot.send_message(
                        chat_id=EXECUTOR_CHAT_ID,
                        text=f"{res['activo']} {res['direccion']}"
                    )
                except Exception as e:
                    logging.warning(f"Error despachando señal al ejecutor: {e}")

    if senales:
        lineas = [
            f"🎯 {s['activo']} ➔ {s['direccion']} ({s['fuerza']})\n   └ {s['detalle']}"
            for s in senales
        ]
        await msg.edit_text("⚡ Señales con Confluencia Detectadas:\n\n" + "\n\n".join(lineas))
    else:
        await msg.edit_text("⚪ Mercado sin confluencia clara en este segundo.")

async def analizar_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Uso: /analizar EURUSD-OTC o /analizar PEPEUSD-OTC")
        return

    par = context.args[0].upper().replace("/", "").strip()
    res = analizar_multitemporal(par)

    if res:
        await update.message.reply_text(
            f"🔍 Análisis Confluente: {par}\n\n"
            f"• Señal: {res['direccion']}\n"
            f"• Confluencia: {res['fuerza']}\n"
            f"• Precio: {res['precio']}\n"
            f"• Desglose: {res['detalle']}\n\n"
            f"Disparo: {par} {res['direccion']}"
        )
    else:
        await update.message.reply_text(f"⚪ {par} sin dirección definida o velas insuficientes.")

async def disparar_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2:
        await update.message.reply_text("Uso: /disparar EURUSD CALL")
        return

    par = context.args[0].upper().replace("/", "").strip()
    direccion = context.args[1].upper().strip()

    if EXECUTOR_CHAT_ID:
        await context.bot.send_message(chat_id=EXECUTOR_CHAT_ID, text=f"{par} {direccion}")
        await update.message.reply_text(f"🚀 Señal {par} {direccion} despachada al Ejecutor.")
    else:
        await update.message.reply_text(f"⚠️ EXECUTOR_CHAT_ID no configurada. Señal: {par} {direccion}")

# ================= 7. ARRANQUE =================
if __name__ == "__main__":
    Thread(target=run_web_server, daemon=True).start()
    conectar_broker()

    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("activos", activos_cmd))
    app.add_handler(CommandHandler("escanear", escanear_cmd))
    app.add_handler(CommandHandler("analizar", analizar_cmd))
    app.add_handler(CommandHandler("disparar", disparar_cmd))

    app.run_polling(drop_pending_updates=True)
