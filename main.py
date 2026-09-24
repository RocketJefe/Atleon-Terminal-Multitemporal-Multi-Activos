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
    MessageHandler,
    filters,
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
        self.wfile.write(b"Atleon Terminal Multitemporal Live!")

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
        logging.error("Variables BROKER_EMAIL o BROKER_PASSWORD vacías.")
        return False

    try:
        cliente = IQ_Option(BROKER_EMAIL.strip(), BROKER_PASSWORD.strip())
        if BROKER_PLATFORM == "EXNOVA":
            cliente.https_url = "https://exnova.com/api"
            cliente.wss_url = "wss://ws.exnova.com/echo/websocket"

        check, reason = cliente.connect()
        if check:
            API = cliente
            logging.info(f"✅ Conectado a {BROKER_PLATFORM} con éxito.")
            return True
        else:
            logging.error(f"❌ Error al conectar a {BROKER_PLATFORM}: {reason}")
            return False
    except Exception as e:
        logging.error(f"❌ Excepción conectando al broker: {e}")
        return False

def asegurar_conexion():
    global API
    if API is None:
        return conectar_broker()
    try:
        if not API.check_connect():
            check, _ = API.connect()
            return check
        return True
    except Exception:
        return conectar_broker()

# ================= 4. CATÁLOGO DINÁMICO DE ACTIVOS =================
def obtener_activos_disponibles():
    if not asegurar_conexion():
        return []

    activos = set()
    try:
        init_data = API.get_all_init()
        turbo = init_data.get("result", {}).get("turbo", {}).get("actives", {})
        binary = init_data.get("result", {}).get("binary", {}).get("actives", {})

        for data in [turbo, binary]:
            for _, info in data.items():
                if info.get("enabled", False) and not info.get("is_suspended", True):
                    nombre = info.get("name", "").replace("front.", "").replace("/", "").strip()
                    if nombre and len(nombre) >= 4:
                        activos.add(nombre)
    except Exception as e:
        logging.warning(f"Error escaneando catálogo: {e}")

    # Fallback seguro con activos líquidos principales
    base = [
        "EURUSD", "EURUSD-OTC", "GBPUSD", "GBPUSD-OTC", "USDJPY", "USDJPY-OTC",
        "XAUUSD", "XAUUSD-OTC", "PEPEUSD-OTC", "TRUMPUSD-OTC", "BTCUSD-OTC"
    ]
    for b in base:
        activos.add(b)

    return list(activos)

# ================= 5. MOTOR DE ANÁLISIS MULTITEMPORAL =================
def obtener_velas_seguras(activo, duracion, cantidad):
    global API
    try:
        velas = API.get_candles(activo, duracion, cantidad, time.time())
        if velas and isinstance(velas, list) and len(velas) > 0 and "close" in velas[0]:
            return velas
    except Exception:
        pass
    return None

def analizar_activo(activo):
    if not asegurar_conexion():
        return None

    try:
        # Consulta segura en 60s (1M) y 120s (2M)
        velas_1m = obtener_velas_seguras(activo, 60, 5)
        if not velas_1m or len(velas_1m) < 3:
            return None

        time.sleep(0.05)
        velas_2m = obtener_velas_seguras(activo, 120, 5)
        if not velas_2m or len(velas_2m) < 3:
            return None

        df_1m = pd.DataFrame(velas_1m)
        df_2m = pd.DataFrame(velas_2m)

        # Análisis de dirección 1M
        c_1m_act = float(df_1m.iloc[-1]["close"])
        c_1m_prev = float(df_1m.iloc[-2]["close"])
        dir_1m = "CALL" if c_1m_act > c_1m_prev else ("PUT" if c_1m_act < c_1m_prev else "NEUTRAL")

        # Análisis de dirección 2M
        c_2m_act = float(df_2m.iloc[-1]["close"])
        c_2m_prev = float(df_2m.iloc[-2]["close"])
        dir_2m = "CALL" if c_2m_act > c_2m_prev else ("PUT" if c_2m_act < c_2m_prev else "NEUTRAL")

        # Micro-impulso (momentum de la última vela en 1M)
        o_1m_act = float(df_1m.iloc[-1]["open"])
        dir_micro = "CALL" if c_1m_act > o_1m_act else ("PUT" if c_1m_act < o_1m_act else "NEUTRAL")

        votos = [dir_1m, dir_2m, dir_micro]
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
                "precio": c_1m_act,
                "detalle": f"1M:{dir_1m} | 2M:{dir_2m} | Micro:{dir_micro}"
            }
    except Exception as e:
        logging.debug(f"Aviso en análisis de {activo}: {e}")

    return None

# ================= 6. CONTROLADORES TELEGRAM =================
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"🧠 Atleon Terminal Multitemporal & Multi-Activos\n"
        f"• Plataforma: {BROKER_PLATFORM}\n\n"
        f"Comandos:\n"
        f"• /status - Diagnóstico y saldo.\n"
        f"• /activos - Catálogo de activos abiertos.\n"
        f"• /escanear - Escaneo confluente multi-activo.\n"
        f"• /analizar <ACTIVO> - Análisis individual (ej: /analizar EURUSD-OTC).\n"
        f"• /disparar <ACTIVO> <CALL/PUT> - Enviar orden manual al Ejecutor."
    )

async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conectado = asegurar_conexion()
    estado = "🟢 Conectado" if conectado else "🔴 Desconectado"
    saldo = f"${API.get_balance():.2f}" if conectado and API else "$0.00"

    await update.message.reply_text(
        f"📊 Atleon Terminal - Diagnóstico\n"
        f"• Plataforma: {BROKER_PLATFORM}\n"
        f"• Estado: {estado}\n"
        f"• Saldo: {saldo}\n"
        f"• Modo: Confluencia Multitemporal (1M / 2M / Micro)\n"
        f"• Catálogo: Forex, Crypto, Acciones e Índices"
    )

async def activos_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("🔍 Consultando activos en vivo...")
    lista = obtener_activos_disponibles()
    if lista:
        muestra = ", ".join(lista[:25])
        await msg.edit_text(f"⚡ Catálogo Activo ({len(lista)} activos detectados):\n\n{muestra}...")
    else:
        await msg.edit_text("⚠️ No se detectaron activos en este instante.")

async def escanear_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("🛰️ Escaneando confluencia multitemporal...")
    activos = obtener_activos_disponibles()[:12]

    senales = []
    for act in activos:
        res = analizar_activo(act)
        if res:
            senales.append(res)
            # Si está configurado el chat del Ejecutor, despacha la orden automáticamente
            if EXECUTOR_CHAT_ID:
                try:
                    await context.bot.send_message(
                        chat_id=EXECUTOR_CHAT_ID,
                        text=f"{res['activo']} {res['direccion']}"
                    )
                except Exception as e:
                    logging.warning(f"Error despachando al ejecutor: {e}")
        time.sleep(0.1)

    if senales:
        lineas = [
            f"🎯 {s['activo']} ➔ {s['direccion']} ({s['fuerza']})\n   └ {s['detalle']}"
            for s in senales
        ]
        await msg.edit_text("⚡ Señales con Confluencia Detectadas:\n\n" + "\n\n".join(lineas))
    else:
        await msg.edit_text("⚪ Mercado sin confluencia clara en este instante.")

async def analizar_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Uso: /analizar EURUSD o /analizar PEPEUSD-OTC")
        return

    par = context.args[0].upper().replace("/", "").strip()
    res = analizar_activo(par)

    if res:
        await update.message.reply_text(
            f"🔍 Análisis: {par}\n\n"
            f"• Señal: {res['direccion']}\n"
            f"• Confluencia: {res['fuerza']}\n"
            f"• Precio: {res['precio']}\n"
            f"• Desglose: {res['detalle']}\n\n"
            f"Disparo sugerido: {par} {res['direccion']}"
        )
    else:
        await update.message.reply_text(f"⚪ {par} sin señal clara o velas insuficientes.")

async def disparar_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2:
        await update.message.reply_text("Uso: /disparar EURUSD CALL")
        return

    par = context.args[0].upper().replace("/", "").strip()
    direccion = context.args[1].upper().strip()

    if EXECUTOR_CHAT_ID:
        await context.bot.send_message(chat_id=EXECUTOR_CHAT_ID, text=f"{par} {direccion}")
        await update.message.reply_text(f"🚀 Señal enviada al Ejecutor: {par} {direccion}")
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
