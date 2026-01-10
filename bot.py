import os
import json
from datetime import datetime

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# ============ ENV ============
BOT_TOKEN = os.getenv("BOT_TOKEN")
SHEET_ID = os.getenv("SHEET_ID")
GOOGLE_SA_JSON = os.getenv("GOOGLE_SA_JSON")

TAB_USERS = "Users"
TAB_SHIFTS = "Shifts"

# Listas simples (depois podemos ler isto do Sheet)
TEAMS = ["Equipa A", "Equipa B", "Equipa C"]
FIELDS = ["Arroz - Parcela 12", "Batata Doce - Vale Sul", "Morango - Estufa 3"]

# Estados para o fluxo do ON
STATE_PICK_TEAM = "pick_team"
STATE_PICK_FIELD = "pick_field"
STATE_WAIT_WORKERS = "wait_workers"


# ============ Sheets helpers ============
def _sheets_service():
    if not GOOGLE_SA_JSON:
        raise RuntimeError("GOOGLE_SA_JSON não definido no Render")
    if not SHEET_ID:
        raise RuntimeError("SHEET_ID não definido no Render")

    info = json.loads(GOOGLE_SA_JSON)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def _get_values(range_a1: str):
    svc = _sheets_service()
    resp = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=range_a1
    ).execute()
    return resp.get("values", [])


def _append_values(range_a1: str, values: list[list]):
    svc = _sheets_service()
    svc.spreadsheets().values().append(
        spreadsheetId=SHEET_ID,
        range=range_a1,
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": values},
    ).execute()


def _update_values(range_a1: str, values: list[list]):
    svc = _sheets_service()
    svc.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range=range_a1,
        valueInputOption="USER_ENTERED",
        body={"values": values},
    ).execute()


# ============ Auth / roles ============
def _find_user_row_by_telegram_id(telegram_id: int):
    rows = _get_values(f"{TAB_USERS}!A:D")
    if not rows or len(rows) < 2:
        return None

    headers = rows[0]
    data = rows[1:]

    def idx(col, default):
        return headers.index(col) if col in headers else default

    idx_id = idx("telegram_id", 0)
    idx_name = idx("name", 1)
    idx_role = idx("role", 2)

    for sheet_row, r in enumerate(data, start=2):
        if len(r) <= idx_id:
            continue
        if str(r[idx_id]).strip() == str(telegram_id):
            name = r[idx_name] if len(r) > idx_name else ""
            role = r[idx_role] if len(r) > idx_role else ""
            return {"sheet_row": sheet_row, "name": name, "role": role}

    return None


def _get_user_role_and_name(telegram_id: int):
    u = _find_user_row_by_telegram_id(telegram_id)
    if not u:
        return None, ""
    role = (u["role"] or "").strip().lower()
    name = (u["name"] or "").strip()
    return role, name


def _can_manage_shifts(role: str) -> bool:
    # pode abrir/fechar
    return role in ("admin", "lead")


def _can_view_all(role: str) -> bool:
    # vê tudo como admin, mas sem modificar
    return role in ("admin", "viewer")


# ============ Time helpers ============
def _today_str():
    return datetime.now().strftime("%Y-%m-%d")


def _time_str():
    return datetime.now().strftime("%H:%M")


def _make_shift_id(date_str: str, team: str, field: str):
    t = team.replace(" ", "").upper()[:10]
    f = field.replace(" ", "").upper()[:10]
    return f"{date_str}_{t}_{f}"


def _calc_hh_total(start_time: str, end_time: str, workers: int) -> float:
    fmt = "%H:%M"
    s = datetime.strptime(start_time, fmt)
    e = datetime.strptime(end_time, fmt)
    delta_hours = (e - s).total_seconds() / 3600.0
    if delta_hours < 0:
        delta_hours = 0
    return round(delta_hours * workers, 2)


# ============ Shift queries ============
def _find_open_shift_for_lead_today(lead_telegram_id: int):
    rows = _get_values(f"{TAB_SHIFTS}!A:J")
    if not rows or len(rows) < 2:
        return None

    headers = rows[0]
    data = rows[1:]

    def idx(col, default):
        return headers.index(col) if col in headers else default

    idx_shift_id = idx("shift_id", 0)
    idx_date = idx("date", 1)
    idx_lead = idx("lead_telegram_id", 4)
    idx_status = idx("status", 8)

    for sheet_row, r in enumerate(data, start=2):
        lead = r[idx_lead] if len(r) > idx_lead else ""
        status = (r[idx_status] if len(r) > idx_status else "").strip().upper()
        date_str = r[idx_date] if len(r) > idx_date else ""
        if str(lead).strip() == str(lead_telegram_id) and status == "OPEN" and date_str == _today_str():
            shift_id = r[idx_shift_id] if len(r) > idx_shift_id else ""
            return {"sheet_row": sheet_row, "shift_id": shift_id, "row": r, "headers": headers}

    return None


def _list_shifts_today():
    rows = _get_values(f"{TAB_SHIFTS}!A:J")
    if not rows or len(rows) < 2:
        return []

    headers = rows[0]
    data = rows[1:]

    def idx(col, default):
        return headers.index(col) if col in headers else default

    idx_date = idx("date", 1)
    idx_team = idx("team", 2)
    idx_field = idx("field", 3)
    idx_start = idx("start_time", 5)
    idx_end = idx("end_time", 6)
    idx_workers = idx("workers_start", 7)
    idx_status = idx("status", 8)
    idx_hh = idx("hh_total", 9)

    out = []
    for r in data:
        date_str = r[idx_date] if len(r) > idx_date else ""
        if date_str != _today_str():
            continue
        out.append({
            "team": r[idx_team] if len(r) > idx_team else "",
            "field": r[idx_field] if len(r) > idx_field else "",
            "start": r[idx_start] if len(r) > idx_start else "",
            "end": r[idx_end] if len(r) > idx_end else "",
            "workers": r[idx_workers] if len(r) > idx_workers else "",
            "status": r[idx_status] if len(r) > idx_status else "",
            "hh": r[idx_hh] if len(r) > idx_hh else "",
        })
    return out


# ============ Telegram UI ============
def _teams_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton(t, callback_data=f"TEAM::{t}")] for t in TEAMS])


def _fields_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton(f, callback_data=f"FIELD::{f}")] for f in FIELDS])


def _main_keyboard_for_role(role: str):
    # Viewer vê tudo como admin, mas sem modificar
    if role in ("admin", "viewer"):
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("📅 Hoje (Resumo)", callback_data="TODAY")],
            [InlineKeyboardButton("📋 Estado", callback_data="STATUS")],
        ])

    # Lead tem ON/OFF
    if role == "lead":
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("🟢 ON (Abrir turno)", callback_data="ON")],
            [InlineKeyboardButton("🔴 OFF (Fechar turno)", callback_data="OFF")],
            [InlineKeyboardButton("📋 Estado", callback_data="STATUS")],
        ])

    return InlineKeyboardMarkup([[InlineKeyboardButton("⛔ Sem acesso", callback_data="NOACCESS")]])


# ============ Handlers ============
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    role, name = _get_user_role_and_name(update.effective_user.id)
    if not role:
        await update.message.reply_text(
            "⛔ Sem autorização.\nFala com o administrador para te adicionar na aba Users."
        )
        return

    await update.message.reply_text(
        f"🧑‍🌾 ANF Labour Bot ativo!\nOlá {name}.\nEscolhe uma opção:",
        reply_markup=_main_keyboard_for_role(role)
    )


async def myid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"🆔 O teu telegram_id é: {update.effective_user.id}")


async def today_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    role, name = _get_user_role_and_name(query.from_user.id)
    if not role or not (role in ("admin", "viewer")):
        await query.edit_message_text("⛔ Sem permissão.", reply_markup=_main_keyboard_for_role(role or ""))
        return

    shifts = _list_shifts_today()
    if not shifts:
        await query.edit_message_text("📅 Hoje: sem turnos registados.", reply_markup=_main_keyboard_for_role(role))
        return

    lines = [f"📅 Hoje ({_today_s]()
