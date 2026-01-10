import os
import json
import math
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

# ============ ENV (aceita maiúsculas e minúsculas) ============
BOT_TOKEN = os.getenv("BOT_TOKEN") or os.getenv("bot_token")
SHEET_ID = os.getenv("SHEET_ID") or os.getenv("sheet_id") or os.getenv("GSHEET_ID")
GOOGLE_SA_JSON = os.getenv("GOOGLE_SA_JSON") or os.getenv("google_sa_json")

TAB_USERS = "Users"
TAB_SHIFTS = "Shifts"
TAB_FIELDS = "Fields"

# Estados para o fluxo do ON (GPS)
STATE_PICK_TEAM = "pick_team"
STATE_PICK_FIELD = "pick_field"
STATE_WAIT_WORKERS = "wait_workers"
STATE_WAIT_LOCATION_ON = "wait_location_on"
STATE_WAIT_LOCATION_OFF = "wait_location_off"

# Listas simples para Equipas (podes depois ler do sheet se quiseres)
TEAMS = ["Equipa A", "Equipa B", "Equipa C"]


# ============ Sheets helpers ============
def _sheets_service():
    if not GOOGLE_SA_JSON:
        raise RuntimeError("GOOGLE_SA_JSON/google_sa_json não definido no Render")
    if not SHEET_ID:
        raise RuntimeError("SHEET_ID/sheet_id não definido no Render")

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
    return role in ("admin", "lead")


# ============ Time helpers ============
def _today_str():
    return datetime.now().strftime("%Y-%m-%d")


def _time_str():
    return datetime.now().strftime("%H:%M")


def _datetime_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _make_shift_id(date_str: str, team: str, field_id: str):
    t = team.replace(" ", "").upper()[:10]
    f = field_id.replace(" ", "").upper()[:10]
    return f"{date_str}_{t}_{f}"


def _calc_hh_total(start_time: str, end_time: str, workers: int) -> float:
    fmt = "%H:%M"
    s = datetime.strptime(start_time, fmt)
    e = datetime.strptime(end_time, fmt)
    delta_hours = (e - s).total_seconds() / 3600.0
    if delta_hours < 0:
        delta_hours = 0
    return round(delta_hours * workers, 2)


# ============ GPS helpers ============
def _haversine_m(lat1, lon1, lat2, lon2):
    R = 6371000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dl/2)**2
    return 2 * R * math.asin(math.sqrt(a))


def _get_field_by_id(field_id: str):
    rows = _get_values(f"{TAB_FIELDS}!A:E")
    if not rows or len(rows) < 2:
        return None

    headers = rows[0]
    data = rows[1:]

    def idx(col, default):
        return headers.index(col) if col in headers else default

    i_id = idx("field_id", 0)
    i_name = idx("field_name", 1)
    i_lat = idx("lat", 2)
    i_lon = idx("lon", 3)
    i_rad = idx("radius_m", 4)

    for r in data:
        if len(r) <= max(i_id, i_name, i_lat, i_lon, i_rad):
            continue
        if str(r[i_id]).strip() == str(field_id).strip():
            try:
                return {
                    "field_id": str(r[i_id]).strip(),
                    "field_name": str(r[i_name]).strip(),
                    "lat": float(str(r[i_lat]).replace(",", ".")),
                    "lon": float(str(r[i_lon]).replace(",", ".")),
                    "radius_m": float(str(r[i_rad]).replace(",", ".")),
                }
            except:
                return None
    return None


def _is_inside_field(user_lat: float, user_lon: float, field: dict):
    d = _haversine_m(user_lat, user_lon, field["lat"], field["lon"])
    return d <= field["radius_m"], int(d)


# ============ Shift queries (Shifts A:N) ============
def _find_open_shift_for_lead_today(lead_telegram_id: int):
    rows = _get_values(f"{TAB_SHIFTS}!A:N")
    if not rows or len(rows) < 2:
        return None

    headers = rows[0]
    data = rows[1:]

    def idx(col, default):
        return headers.index(col) if col in headers else default

    idx_shift_id = idx("shift_id", 0)        # A
    idx_date = idx("date", 1)                # B
    idx_lead = idx("lead_telegram_id", 5)    # F
    idx_status = idx("status", 9)            # J

    for sheet_row, r in enumerate(data, start=2):
        lead = r[idx_lead] if len(r) > idx_lead else ""
        status = (r[idx_status] if len(r) > idx_status else "").strip().upper()
        date_str = r[idx_date] if len(r) > idx_date else ""
        if str(lead).strip() == str(lead_telegram_id) and status == "OPEN" and date_str == _today_str():
            shift_id = r[idx_shift_id] if len(r) > idx_shift_id else ""
            return {"sheet_row": sheet_row, "shift_id": shift_id, "row": r, "headers": headers}

    return None


def _list_shifts_today():
    rows = _get_values(f"{TAB_SHIFTS}!A:N")
    if not rows or len(rows) < 2:
        return []

    headers = rows[0]
    data = rows[1:]

    def idx(col, default):
        return headers.index(col) if col in headers else default

    idx_date = idx("date", 1)          # B
    idx_team = idx("team", 2)          # C
    idx_field = idx("field", 3)        # D
    idx_start = idx("start_time", 6)   # G
    idx_end = idx("end_time", 7)       # H
    idx_workers = idx("workers_start", 8)  # I
    idx_status = idx("status", 9)      # J
    idx_hh = idx("hh_total", 10)       # K

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
    rows = _get_values(f"{TAB_FIELDS}!A:E")
    if not rows or len(rows) < 2:
        return InlineKeyboardMarkup([[InlineKeyboardButton("⚠️ Sem campos em Fields", callback_data="NOFIELDS")]])

    headers = rows[0]
    data = rows[1:]

    def idx(col, default):
        return headers.index(col) if col in headers else default

    i_id = idx("field_id", 0)
    i_name = idx("field_name", 1)

    buttons = []
    for r in data:
        if len(r) <= max(i_id, i_name):
            continue
        field_id = str(r[i_id]).strip()
        field_name = str(r[i_name]).strip()
        if field_id and field_name:
            buttons.append([InlineKeyboardButton(field_name, callback_data=f"FIELDID::{field_id}")])

    if not buttons:
        buttons = [[InlineKeyboardButton("⚠️ Sem campos válidos", callback_data="NOFIELDS")]]

    return InlineKeyboardMarkup(buttons)


def _main_keyboard_for_role(role: str):
    if role == "admin":
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("🟢 ON (Abrir turno GPS)", callback_data="ON")],
            [InlineKeyboardButton("🔴 OFF (Fechar turno GPS)", callback_data="OFF")],
            [InlineKeyboardButton("⚠️ ON (Admin Override)", callback_data="ON_ADMIN")],
            [InlineKeyboardButton("⚠️ OFF (Admin Override)", callback_data="OFF_ADMIN")],
            [InlineKeyboardButton("📅 Hoje (Resumo)", callback_data="TODAY")],
            [InlineKeyboardButton("📋 Estado", callback_data="STATUS")],
        ])

    if role == "viewer":
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("📅 Hoje (Resumo)", callback_data="TODAY")],
            [InlineKeyboardButton("📋 Estado", callback_data="STATUS")],
        ])

    if role == "lead":
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("🟢 ON (Abrir turno GPS)", callback_data="ON")],
            [InlineKeyboardButton("🔴 OFF (Fechar turno GPS)", callback_data="OFF")],
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
        f"🧑‍🌾 ANF Labour Bot ativo!\nOlá {name or update.effective_user.first_name}.\nEscolhe uma opção:",
        reply_markup=_main_keyboard_for_role(role)
    )


async def myid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"🆔 O teu telegram_id é: {update.effective_user.id}")


async def today_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    role, _ = _get_user_role_and_name(query.from_user.id)
    if role not in ("admin", "viewer"):
        await query.edit_message_text("⛔ Sem permissão.", reply_markup=_main_keyboard_for_role(role or ""))
        return

    shifts = _list_shifts_today()
    if not shifts:
        await query.edit_message_text("📅 Hoje: sem turnos registados.", reply_markup=_main_keyboard_for_role(role))
        return

    lines = [f"📅 Hoje ({_today_str()}):"]
    for s in shifts[:30]:
        st = (s["status"] or "").upper()
        line = f"• {s['team']} — {s['field']} — {st} — {s['start']}"
        if s["end"]:
            line += f"→{s['end']}"
        if s["workers"]:
            line += f" — 👥 {s['workers']}"
        if s["hh"]:
            line += f" — ⏱️ HH {s['hh']}"
        lines.append(line)

    await query.edit_message_text("\n".join(lines), reply_markup=_main_keyboard_for_role(role))


async def status_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    role, _ = _get_user_role_and_name(query.from_user.id)
    if not role:
        await query.edit_message_text("⛔ Sem autorização.")
        return

    if role in ("admin", "viewer"):
        shifts = _list_shifts_today()
        open_count = sum(1 for s in shifts if (s["status"] or "").upper() == "OPEN")
        closed_count = sum(1 for s in shifts if (s["status"] or "").upper() == "CLOSED")
        await query.edit_message_text(
            f"📋 Estado hoje ({_today_str()}):\n🟢 OPEN: {open_count}\n🔴 CLOSED: {closed_count}",
            reply_markup=_main_keyboard_for_role(role)
        )
        return

    # Lead
    open_shift = _find_open_shift_for_lead_today(query.from_user.id)
    if not open_shift:
        await query.edit_message_text("📋 Hoje: sem turno OPEN teu.", reply_markup=_main_keyboard_for_role(role))
        return
    await query.edit_message_text(
        f"📋 Turno OPEN\nShift: {open_shift['shift_id']}\nData: {_today_str()}",
        reply_markup=_main_keyboard_for_role(role)
    )


async def on_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id
    role, _ = _get_user_role_and_name(user_id)
    if not role or not _can_manage_shifts(role):
        await query.edit_message_text("⛔ Não tens permissão para abrir turnos.",
                                      reply_markup=_main_keyboard_for_role(role or ""))
        return

    open_shift = _find_open_shift_for_lead_today(user_id)
    if open_shift:
        await query.edit_message_text(f"⚠️ Já tens um turno OPEN hoje.\nShift: {open_shift['shift_id']}",
                                      reply_markup=_main_keyboard_for_role(role))
        return

    context.user_data.clear()
    context.user_data["flow_state"] = STATE_PICK_TEAM
    context.user_data["admin_override"] = False
    await query.edit_message_text("Escolhe a equipa:", reply_markup=_teams_keyboard())


async def on_admin_override(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id
    role, _ = _get_user_role_and_name(user_id)
    if role != "admin":
        await query.edit_message_text("⛔ Apenas admin.")
        return

    open_shift = _find_open_shift_for_lead_today(user_id)
    if open_shift:
        await query.edit_message_text(f"⚠️ Já tens um turno OPEN hoje.\nShift: {open_shift['shift_id']}",
                                      reply_markup=_main_keyboard_for_role(role))
        return

    context.user_data.clear()
    context.user_data["flow_state"] = STATE_PICK_TEAM
    context.user_data["admin_override"] = True
    await query.edit_message_text("⚠️ ADMIN OVERRIDE: Escolhe a equipa:", reply_markup=_teams_keyboard())


async def off_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id
    role, _ = _get_user_role_and_name(user_id)
    if not role or not _can_manage_shifts(role):
        await query.edit_message_text("⛔ Não tens permissão para fechar turnos.",
                                      reply_markup=_main_keyboard_for_role(role or ""))
        return

    open_shift = _find_open_shift_for_lead_today(user_id)
    if not open_shift:
        await query.edit_message_text("⚠️ Não tens turno OPEN hoje.", reply_markup=_main_keyboard_for_role(role))
        return

    context.user_data.clear()
    context.user_data["flow_state"] = STATE_WAIT_LOCATION_OFF
    await query.edit_message_text(
        "📍 Para fechar o turno, envia a tua localização.\n"
        "Telegram: 📎 → Localização → Enviar localização."
    )


async def off_admin_override(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id
    role, _ = _get_user_role_and_name(user_id)
    if role != "admin":
        await query.edit_message_text("⛔ Apenas admin.")
        return

    open_shift = _find_open_shift_for_lead_today(user_id)
    if not open_shift:
        await query.edit_message_text("⚠️ Não tens turno OPEN hoje.", reply_markup=_main_keyboard_for_role(role))
        return

    headers = open_shift["headers"]
    row = open_shift["row"]

    def idx(col, default):
        return headers.index(col) if col in headers else default

    idx_start = idx("start_time", 6)      # G
    idx_workers = idx("workers_start", 8) # I

    start_time = row[idx_start] if len(row) > idx_start else ""
    workers_raw = row[idx_workers] if len(row) > idx_workers else "0"
    try:
        workers = int(str(workers_raw).strip())
    except:
        workers = 0

    end_time = _time_str()
    hh_total = _calc_hh_total(start_time, end_time, workers) if workers > 0 else ""

    sheet_row = open_shift["sheet_row"]
    _update_values(f"{TAB_SHIFTS}!H{sheet_row}:H{sheet_row}", [[end_time]])       # end_time H
    _update_values(f"{TAB_SHIFTS}!J{sheet_row}:J{sheet_row}", [["CLOSED"]])      # status J
    if hh_total != "":
        _update_values(f"{TAB_SHIFTS}!K{sheet_row}:K{sheet_row}", [[hh_total]])  # hh_total K
    _update_values(f"{TAB_SHIFTS}!M{sheet_row}:N{sheet_row}", [[_datetime_str(), str(user_id)]])

    await query.edit_message_text(
        f"⚠️ ADMIN OVERRIDE: Turno fechado.\n🕒 Saída: {end_time}\n⏱️ HH total: {hh_total}",
        reply_markup=_main_keyboard_for_role(role)
    )


async def pick_team_or_field(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    state = context.user_data.get("flow_state")
    data = query.data

    if data.startswith("TEAM::") and state == STATE_PICK_TEAM:
        team = data.split("TEAM::", 1)[1]
        context.user_data["team"] = team
        context.user_data["flow_state"] = STATE_PICK_FIELD
        await query.edit_message_text("Escolhe o campo:", reply_markup=_fields_keyboard())
        return

    if data.startswith("FIELDID::") and state == STATE_PICK_FIELD:
        field_id = data.split("FIELDID::", 1)[1]
        field = _get_field_by_id(field_id)
        if not field:
            await query.edit_message_text("⚠️ Campo inválido em Fields (confirma lat/lon/radius_m).")
            return

        context.user_data["field_id"] = field_id
        context.user_data["field_name"] = field["field_name"]
        context.user_data["flow_state"] = STATE_WAIT_WORKERS
        await query.edit_message_text("Quantos trabalhadores iniciam o turno? (envia só o número, ex: 12)")
        return

    await query.edit_message_text("⚠️ Ação inválida. Recomeça com /start.")


async def workers_count_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get("flow_state") != STATE_WAIT_WORKERS:
        return

    user_id = update.effective_user.id
    role, _ = _get_user_role_and_name(user_id)
    if not role or not _can_manage_shifts(role):
        await update.message.reply_text("⛔ Sem permissão.")
        context.user_data.clear()
        return

    text = (update.message.text or "").strip()
    if not text.isdigit():
        await update.message.reply_text("⚠️ Envia só um número (ex: 12).")
        return

    workers = int(text)
    team = context.user_data.get("team")
    field_id = context.user_data.get("field_id")
    field_name = context.user_data.get("field_name")
    admin_override = context.user_data.get("admin_override") is True

    # Admin override abre já sem GPS
    if admin_override:
        date_str = _today_str()
        start_time = _time_str()
        shift_id = _make_shift_id(date_str, team, field_id)

        new_row = [
            shift_id, date_str, team, field_name, field_id,
            str(user_id), start_time, "", str(workers),
            "OPEN", "", str(user_id), "", ""
        ]
        _append_values(f"{TAB_SHIFTS}!A:N", [new_row])

        context.user_data.clear()
        await update.message.reply_text(
            f"⚠️ ADMIN OVERRIDE: Turno aberto.\nShift: {shift_id}\n👥 {workers}\n🕒 Entrada: {start_time}",
            reply_markup=_main_keyboard_for_role(role)
        )
        return

    # Fluxo normal: pedir GPS para abrir
    context.user_data["workers"] = workers
    context.user_data["flow_state"] = STATE_WAIT_LOCATION_ON
    await update.message.reply_text(
        "📍 Agora envia a tua localização para confirmar que estás no campo.\n"
        "Telegram: 📎 → Localização → Enviar localização."
    )


async def location_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.location:
        return

    user_id = update.effective_user.id
    role, _ = _get_user_role_and_name(user_id)
    state = context.user_data.get("flow_state")

    # ========== GPS para INICIAR ==========
    if state == STATE_WAIT_LOCATION_ON:
        if not role or not _can_manage_shifts(role):
            await update.message.reply_text("⛔ Sem permissão.")
            context.user_data.clear()
            return

        team = context.user_data.get("team")
        field_id = context.user_data.get("field_id")
        field_name = context.user_data.get("field_name")
        workers = int(context.user_data.get("workers") or 0)

        field = _get_field_by_id(field_id)
        if not field:
            await update.message.reply_text("⚠️ Campo não encontrado em Fields.")
            return

        ok, dist = _is_inside_field(
            update.message.location.latitude,
            update.message.location.longitude,
            field
        )

        if not ok:
            await update.message.reply_text(
                f"🚫 Fora do perímetro.\nDistância: {dist} m | Raio: {int(field['radius_m'])} m\n"
                "Aproxima-te e envia novamente a localização."
            )
            return

        date_str = _today_str()
        start_time = _time_str()
        shift_id = _make_shift_id(date_str, team, field_id)

        new_row = [
            shift_id, date_str, team, field_name, field_id,
            str(user_id), start_time, "", str(workers),
            "OPEN", "", str(user_id), "", ""
        ]
        _append_values(f"{TAB_SHIFTS}!A:N", [new_row])

        context.user_data.clear()
        await update.message.reply_text(
            f"✅ Turno aberto (GPS OK: {dist} m).\nShift: {shift_id}\n👥 {workers}\n🕒 Entrada: {start_time}",
            reply_markup=_main_keyboard_for_role(role)
        )
        return

    # ========== GPS para TERMINAR ==========
    if state == STATE_WAIT_LOCATION_OFF:
        if not role or not _can_manage_shifts(role):
            await update.message.reply_text("⛔ Sem permissão.")
            context.user_data.clear()
            return

        open_shift = _find_open_shift_for_lead_today(user_id)
        if not open_shift:
            await update.message.reply_text("⚠️ Não tens turno OPEN hoje.")
            context.user_data.clear()
            return

        headers = open_shift["headers"]
        row = open_shift["row"]

        def idx(col, default):
            return headers.index(col) if col in headers else default

        idx_field_id = idx("field_id", 4)      # E
        idx_start = idx("start_time", 6)       # G
        idx_workers = idx("workers_start", 8)  # I

        field_id = row[idx_field_id] if len(row) > idx_field_id else ""
        field = _get_field_by_id(field_id)
        if not field:
            await update.message.reply_text("⚠️ Campo deste turno não existe em Fields.")
            context.user_data.clear()
            return

        ok, dist = _is_inside_field(
            update.message.location.latitude,
            update.message.location.longitude,
            field
        )
        if not ok:
            await update.message.reply_text(
                f"🚫 Fora do perímetro.\nDistância: {dist} m | Raio: {int(field['radius_m'])} m\n"
                "Aproxima-te e envia novamente a localização."
            )
            return

        start_time = row[idx_start] if len(row) > idx_start else ""
        workers_raw = row[idx_workers] if len(row) > idx_workers else "0"
        try:
            workers = int(str(workers_raw).strip())
        except:
            workers = 0

        end_time = _time_str()
        hh_total = _calc_hh_total(start_time, end_time, workers) if workers > 0 else ""

        sheet_row = open_shift["sheet_row"]
        _update_values(f"{TAB_SHIFTS}!H{sheet_row}:H{sheet_row}", [[end_time]])      # H end_time
        _update_values(f"{TAB_SHIFTS}!J{sheet_row}:J{sheet_row}", [["CLOSED"]])     # J status
        if hh_total != "":
            _update_values(f"{TAB_SHIFTS}!K{sheet_row}:K{sheet_row}", [[hh_total]]) # K hh_total
        _update_values(f"{TAB_SHIFTS}!M{sheet_row}:N{sheet_row}", [[_datetime_str(), str(user_id)]])

        context.user_data.clear()
        await update.message.reply_text(
            f"✅ Turno fechado (GPS OK: {dist} m).\n🕒 Saída: {end_time}\n⏱️ HH total: {hh_total}",
            reply_markup=_main_keyboard_for_role(role)
        )
        return


def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN não definido")
    if not SHEET_ID:
        raise RuntimeError("SHEET_ID/sheet_id não definido")
    if not GOOGLE_SA_JSON:
        raise RuntimeError("GOOGLE_SA_JSON/google_sa_json não definido")

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("id", myid))

    app.add_handler(CallbackQueryHandler(today_button, pattern="^TODAY$"))
    app.add_handler(CallbackQueryHandler(status_button, pattern="^STATUS$"))

    app.add_handler(CallbackQueryHandler(on_button, pattern="^ON$"))
    app.add_handler(CallbackQueryHandler(off_button, pattern="^OFF$"))

    app.add_handler(CallbackQueryHandler(on_admin_override, pattern="^ON_ADMIN$"))
    app.add_handler(CallbackQueryHandler(off_admin_override, pattern="^OFF_ADMIN$"))

    app.add_handler(CallbackQueryHandler(pick_team_or_field, pattern="^(TEAM::|FIELDID::)"))

    app.add_handler(MessageHandler(filters.LOCATION, location_message))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, workers_count_message))

    print("🤖 Bot iniciado com polling (GPS obrigatório + admin override)...")
    app.run_polling()


if __name__ == "__main__":
    main()
