"""
Telegram-бот "Реєстрація чеків ТЦ LAKE PLAZA" — версія 2 (новий розіграш)
=========================================================================
Стек: python-telegram-bot 21.x, gspread 6.x, Google Drive API (OAuth особистого акаунта).

При першому запуску бот САМ створює на вашому Google Диску:
  - папку для фото чеків;
  - Google-таблицю з аркушами Settings, Participants, Log.
При наступних запусках (в т.ч. після redeploy на Railway) знаходить їх за назвою.
Старі змінні SHEET_ID та DRIVE_FOLDER_ID ІГНОРУЮТЬСЯ — перший розіграш не чіпається.

Обов'язкові змінні середовища:
  TELEGRAM_TOKEN, MODERATOR_CHAT_ID, ADMIN_IDS,
  GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN, PORT
Необов'язкові:
  CAMPAIGN_ID        (за замовчуванням "raffle-2"; змініть для наступного розіграшу)
  CAMPAIGN_TITLE     (назва для файлів на Диску)
  MODERATOR_IDS      (список Telegram ID через кому — хто може натискати кнопки)
  MODERATOR_EMAILS   (Google-пошти через кому — бот надасть доступ на перегляд)
  CONVERSATION_TIMEOUT (секунди, за замовчуванням 300)
"""

import asyncio
import io
import logging
import os
import re
import threading
import time
import uuid
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from zoneinfo import ZoneInfo

import gspread
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from telegram import (
    BotCommand,
    BotCommandScopeChat,
    BotCommandScopeDefault,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
    Update,
)
from telegram.ext import (
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    TypeHandler,
    filters,
)

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("checks-bot")

# ----------------------------------------------------------------------------
# Конфігурація
# ----------------------------------------------------------------------------
TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
MODERATOR_CHAT_ID = int(os.environ["MODERATOR_CHAT_ID"])


def _ids(name: str) -> set:
    return {int(x) for x in os.environ.get(name, "").split(",") if x.strip()}


ADMIN_IDS = _ids("ADMIN_IDS")
MODERATOR_IDS = _ids("MODERATOR_IDS")
MODERATOR_EMAILS = [x.strip() for x in os.environ.get("MODERATOR_EMAILS", "").split(",") if x.strip()]
CAMPAIGN_ID = os.environ.get("CAMPAIGN_ID", "raffle-2").strip()
CAMPAIGN_TITLE = os.environ.get("CAMPAIGN_TITLE", f"LAKE PLAZA — {CAMPAIGN_ID}").strip()
CONVERSATION_TIMEOUT = int(os.environ.get("CONVERSATION_TIMEOUT", "300"))
TZ = ZoneInfo("Europe/Kyiv")

SPREADSHEET_NAME = f"{CAMPAIGN_TITLE} — Participants"
FOLDER_NAME = f"{CAMPAIGN_TITLE} — Фото чеків"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]

DEFAULT_TERMS = "Умови акції уточнюйте у представників ТЦ."

COLS = [
    "timestamp", "application_id", "campaign_id", "chat_id", "username",
    "surname", "name", "phone", "photo_url", "status", "participant_number",
    "moderator", "moderation_date", "chance_type", "chance_index",
    "chance_total", "base_application_id", "source_key", "telegram_file_id",
    "delivery_status", "mod_message_id",
]
C = {name: i for i, name in enumerate(COLS)}  # 0-based

ST_PENDING = "На модерації"
ST_PROCESSING = "Обробляється"
ST_APPROVED = "Підтверджено"
ST_REJECTED = "Відхилено"


def now_str() -> str:
    return datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")


def today_kyiv():
    return datetime.now(TZ).date()


# ----------------------------------------------------------------------------
# Google: авторизація та автоматичне створення таблиці й папки
# ----------------------------------------------------------------------------
def build_google_creds() -> Credentials:
    return Credentials(
        token=None,
        refresh_token=os.environ["GOOGLE_REFRESH_TOKEN"],
        client_id=os.environ["GOOGLE_CLIENT_ID"],
        client_secret=os.environ["GOOGLE_CLIENT_SECRET"],
        token_uri="https://oauth2.googleapis.com/token",
        scopes=SCOPES,
    )


_google_creds = build_google_creds()
gc = gspread.authorize(_google_creds, http_client=gspread.BackOffHTTPClient)  # автоповтор при 429/5xx
drive_service = build("drive", "v3", credentials=_google_creds, cache_discovery=False)

SPREADSHEET = None
WS_SETTINGS = None
WS_PART = None
WS_LOG = None
FOLDER_ID = ""
INFRA_CREATED = False
_date_folders: dict = {}

# Усі звернення до Google виконуються послідовно (httplib2/gspread не потокобезпечні).
_sheet_lock = asyncio.Lock()


async def gcall(fn, *args):
    async with _sheet_lock:
        return await asyncio.to_thread(fn, *args)


def _q(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _drive_find(name: str, mime: str, parent: str = ""):
    query = f"name='{_q(name)}' and mimeType='{mime}' and trashed=false"
    if parent:
        query += f" and '{parent}' in parents"
    res = drive_service.files().list(q=query, fields="files(id,name)", pageSize=10).execute()
    files = res.get("files", [])
    return files[0]["id"] if files else ""


def _drive_folder(name: str, parent: str = "") -> str:
    mime = "application/vnd.google-apps.folder"
    found = _drive_find(name, mime, parent)
    if found:
        return found
    body = {"name": name, "mimeType": mime}
    if parent:
        body["parents"] = [parent]
    return drive_service.files().create(body=body, fields="id").execute()["id"]


def _ensure_headers(ws, headers):
    first = ws.row_values(1)
    if first != headers:
        ws.update(values=[headers], range_name="A1", value_input_option="RAW")


def init_infrastructure() -> None:
    global SPREADSHEET, WS_SETTINGS, WS_PART, WS_LOG, FOLDER_ID, INFRA_CREATED

    FOLDER_ID = _drive_folder(FOLDER_NAME)

    sheet_id = _drive_find(SPREADSHEET_NAME, "application/vnd.google-apps.spreadsheet")
    if sheet_id:
        SPREADSHEET = gc.open_by_key(sheet_id)
        logger.info("Знайдено існуючу таблицю: %s", SPREADSHEET.url)
    else:
        SPREADSHEET = gc.create(SPREADSHEET_NAME)
        INFRA_CREATED = True
        logger.info("Створено нову таблицю: %s", SPREADSHEET.url)

    existing = {w.title: w for w in SPREADSHEET.worksheets()}

    if "Participants" in existing:
        WS_PART = existing["Participants"]
    elif "Sheet1" in existing or "Аркуш1" in existing:
        WS_PART = existing.get("Sheet1") or existing.get("Аркуш1")
        WS_PART.update_title("Participants")
    else:
        WS_PART = SPREADSHEET.add_worksheet("Participants", rows=2000, cols=len(COLS))
    WS_SETTINGS = existing.get("Settings") or SPREADSHEET.add_worksheet("Settings", rows=50, cols=2)
    WS_LOG = existing.get("Log") or SPREADSHEET.add_worksheet("Log", rows=2000, cols=4)

    _ensure_headers(WS_PART, COLS)
    _ensure_headers(WS_LOG, ["timestamp", "event", "application_id", "details"])
    _ensure_headers(WS_SETTINGS, ["key", "value"])

    keys = WS_SETTINGS.col_values(1)
    defaults = {
        "is_active": "FALSE",
        "start_date": "",
        "end_date": "",
        "terms_text": DEFAULT_TERMS,
        "last_number": "0",
        "campaign_id": CAMPAIGN_ID,
        "folder_id": FOLDER_ID,
        "spreadsheet_id": SPREADSHEET.id,
        "shared_with": "",
    }
    missing = [[k, v] for k, v in defaults.items() if k not in keys]
    if missing:
        WS_SETTINGS.append_rows(missing, value_input_option="RAW")

    _share_with_moderators()


def _share_with_moderators() -> None:
    if not MODERATOR_EMAILS:
        return
    already = {x.strip() for x in _raw_setting("shared_with").split(",") if x.strip()}
    for email in MODERATOR_EMAILS:
        if email in already:
            continue
        for file_id in (SPREADSHEET.id, FOLDER_ID):
            try:
                drive_service.permissions().create(
                    fileId=file_id,
                    body={"type": "user", "role": "reader", "emailAddress": email},
                    sendNotificationEmail=False,
                ).execute()
            except Exception:
                logger.exception("Не вдалося надати доступ %s до %s", email, file_id)
                break
        else:
            already.add(email)
    _set_setting_sync("shared_with", ",".join(sorted(already)))


def sheet_url() -> str:
    return SPREADSHEET.url if SPREADSHEET else ""


def folder_url() -> str:
    return f"https://drive.google.com/drive/folders/{FOLDER_ID}" if FOLDER_ID else ""


# ----------------------------------------------------------------------------
# Settings
# ----------------------------------------------------------------------------
_settings_cache = {"data": None, "ts": 0.0}
SETTINGS_CACHE_TTL = 10


def _raw_setting(key: str) -> str:
    keys = WS_SETTINGS.col_values(1)
    if key in keys:
        vals = WS_SETTINGS.col_values(2)
        idx = keys.index(key)
        return vals[idx] if idx < len(vals) else ""
    return ""


def _set_setting_sync(key: str, value) -> None:
    keys = WS_SETTINGS.col_values(1)
    if key in keys:
        WS_SETTINGS.update(values=[[str(value)]], range_name=f"B{keys.index(key) + 1}", value_input_option="RAW")
    else:
        WS_SETTINGS.append_row([key, str(value)], value_input_option="RAW")
    _settings_cache["ts"] = 0.0


def _read_settings_sync() -> dict:
    rows = WS_SETTINGS.get_all_values()[1:]
    raw = {r[0]: (r[1] if len(r) > 1 else "") for r in rows if r and r[0]}
    return {
        "is_active": raw.get("is_active", "FALSE").strip().upper() == "TRUE",
        "start_date": raw.get("start_date", "").strip(),
        "end_date": raw.get("end_date", "").strip(),
        "terms_text": raw.get("terms_text", DEFAULT_TERMS) or DEFAULT_TERMS,
        "last_number": int(raw.get("last_number", "0") or 0),
    }


async def get_settings(force: bool = False) -> dict:
    if not force and _settings_cache["data"] is not None and time.monotonic() - _settings_cache["ts"] < SETTINGS_CACHE_TTL:
        return _settings_cache["data"]
    data = await gcall(_read_settings_sync)
    _settings_cache["data"] = data
    _settings_cache["ts"] = time.monotonic()
    return data


async def set_setting(key: str, value) -> None:
    await gcall(_set_setting_sync, key, value)


def _parse_date(s: str):
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def promo_state(settings: dict) -> tuple:
    """Повертає (відкрито?, причина_закриття)."""
    if not settings["is_active"]:
        return False, "paused"
    today = today_kyiv()
    start = _parse_date(settings["start_date"])
    end = _parse_date(settings["end_date"])
    if start and today < start:
        return False, f"not_started:{settings['start_date']}"
    if end and today > end:
        return False, f"ended:{settings['end_date']}"
    return True, ""


def closed_message(reason: str) -> str:
    if reason.startswith("not_started:"):
        return f"⏳ Реєстрація ще не розпочалась. Старт: {reason.split(':', 1)[1]}. Слідкуйте за оновленнями!"
    if reason.startswith("ended:"):
        return f"😔 Акція завершилась {reason.split(':', 1)[1]}. Дякуємо за участь!"
    return "😔 Наразі реєстрація чеків тимчасово призупинена організаторами. Слідкуйте за оновленнями!"


# ----------------------------------------------------------------------------
# Participants: операції (усі синхронні, викликаються через gcall)
# ----------------------------------------------------------------------------
def _log_sync(event: str, app_id: str = "", details: str = "") -> None:
    try:
        WS_LOG.append_row([now_str(), event, app_id, details], value_input_option="RAW")
    except Exception:
        logger.exception("Не вдалося записати Log")


def _row_to_dict(values: list, row: int) -> dict:
    values = list(values) + [""] * (len(COLS) - len(values))
    d = dict(zip(COLS, values[: len(COLS)]))
    d["row"] = row
    return d


def _find_row_sync(app_id: str):
    ids = WS_PART.col_values(C["application_id"] + 1)
    if app_id not in ids:
        return None
    row = ids.index(app_id) + 1
    return _row_to_dict(WS_PART.row_values(row), row)


def _update_row_sync(row: int, changes: dict) -> None:
    data = []
    for name, value in changes.items():
        a1 = gspread.utils.rowcol_to_a1(row, C[name] + 1)
        data.append({"range": a1, "values": [[str(value)]]})
    WS_PART.batch_update(data, value_input_option="RAW")


def create_application_sync(profile: dict, source_key: str, file_id: str, photo_url: str):
    keys = WS_PART.col_values(C["source_key"] + 1)
    if source_key in keys:
        return None
    app_id = uuid.uuid4().hex[:8]
    row = [""] * len(COLS)
    row[C["timestamp"]] = now_str()
    row[C["application_id"]] = app_id
    row[C["campaign_id"]] = CAMPAIGN_ID
    row[C["chat_id"]] = str(profile["chat_id"])
    row[C["username"]] = profile.get("username", "")
    row[C["surname"]] = profile["surname"]
    row[C["name"]] = profile["name"]
    row[C["phone"]] = profile["phone"]
    row[C["photo_url"]] = photo_url
    row[C["status"]] = ST_PENDING
    row[C["chance_type"]] = "pending"
    row[C["base_application_id"]] = app_id
    row[C["source_key"]] = source_key
    row[C["telegram_file_id"]] = file_id
    row[C["delivery_status"]] = "pending"
    resp = WS_PART.append_row(row, value_input_option="RAW")
    m = re.search(r"![A-Z]+(\d+)", ((resp or {}).get("updates") or {}).get("updatedRange", ""))
    if m:
        return _row_to_dict(row, int(m.group(1)))
    return _find_row_sync(app_id)


def begin_approval_sync(app_id: str, chances: int, moderator: str) -> dict:
    app = _find_row_sync(app_id)
    if not app:
        return {"error": "Заявку не знайдено."}
    if app["campaign_id"] != CAMPAIGN_ID:
        return {"error": "Ця заявка належить іншому розіграшу."}
    if app["status"] == ST_PROCESSING:
        return {"error": "Заявка вже обробляється. Зачекайте або перевірте /status."}
    if app["status"] != ST_PENDING:
        return {"error": "Цю заявку вже опрацьовано."}
    last = int(_raw_setting("last_number") or 0)
    first = last + 1
    _set_setting_sync("last_number", last + chances)
    _update_row_sync(app["row"], {
        "status": ST_PROCESSING,
        "participant_number": first,
        "moderator": moderator,
        "moderation_date": now_str(),
        "chance_type": "original",
        "chance_index": 1,
        "chance_total": chances,
    })
    _log_sync("approve_begin", app_id, f"{first}..{first + chances - 1} by {moderator}")
    return {"app": app, "first": first, "chances": chances}


def finish_approval_sync(app_id: str) -> dict:
    """Ідемпотентно дописує додаткові шанси та завершує підтвердження."""
    app = _find_row_sync(app_id)
    if not app or app["status"] not in (ST_PROCESSING, ST_APPROVED):
        return {"error": "Некоректний стан заявки."}
    first = int(app["participant_number"])
    total = int(app["chance_total"])
    values = WS_PART.get_all_values()[1:]
    have = {
        int(r[C["chance_index"]])
        for r in values
        if len(r) > C["chance_index"]
        and r[C["base_application_id"]] == app_id
        and r[C["chance_type"]] == "duplicate"
        and r[C["chance_index"]].isdigit()
    }
    new_rows = []
    for idx in range(2, total + 1):
        if idx in have:
            continue
        row = [""] * len(COLS)
        row[C["timestamp"]] = now_str()
        row[C["application_id"]] = f"{app_id}-{idx}"
        row[C["campaign_id"]] = CAMPAIGN_ID
        row[C["chat_id"]] = app["chat_id"]
        row[C["username"]] = app["username"]
        row[C["surname"]] = app["surname"]
        row[C["name"]] = app["name"]
        row[C["phone"]] = app["phone"]
        row[C["photo_url"]] = app["photo_url"]
        row[C["status"]] = ST_APPROVED
        row[C["participant_number"]] = str(first + idx - 1)
        row[C["moderator"]] = app["moderator"]
        row[C["moderation_date"]] = app["moderation_date"]
        row[C["chance_type"]] = "duplicate"
        row[C["chance_index"]] = str(idx)
        row[C["chance_total"]] = str(total)
        row[C["base_application_id"]] = app_id
        row[C["delivery_status"]] = "n/a"
        new_rows.append(row)
    if new_rows:
        WS_PART.append_rows(new_rows, value_input_option="RAW")
    _update_row_sync(app["row"], {"status": ST_APPROVED})
    _log_sync("approve_done", app_id, f"{total} chances")
    return {"app": app, "numbers": list(range(first, first + total))}


def reject_sync(app_id: str, moderator: str) -> dict:
    app = _find_row_sync(app_id)
    if not app:
        return {"error": "Заявку не знайдено."}
    if app["campaign_id"] != CAMPAIGN_ID:
        return {"error": "Ця заявка належить іншому розіграшу."}
    if app["status"] != ST_PENDING:
        return {"error": "Цю заявку вже опрацьовано."}
    _update_row_sync(app["row"], {
        "status": ST_REJECTED,
        "moderator": moderator,
        "moderation_date": now_str(),
        "chance_type": "rejected",
    })
    _log_sync("reject", app_id, moderator)
    return {"app": app}


def recover_incomplete_sync() -> list:
    values = WS_PART.get_all_values()[1:]
    stuck = [r[C["application_id"]] for r in values if len(r) > C["status"] and r[C["status"]] == ST_PROCESSING]
    done = []
    for app_id in stuck:
        res = finish_approval_sync(app_id)
        if "error" not in res:
            done.append((app_id, res["numbers"], res["app"]["chat_id"]))
    return done


def latest_profile_sync(chat_id: int):
    values = WS_PART.get_all_values()[1:]
    for r in reversed(values):
        r = r + [""] * (len(COLS) - len(r))
        if r[C["chat_id"]] == str(chat_id) and r[C["chance_type"]] != "duplicate":
            return {
                "chat_id": chat_id,
                "username": r[C["username"]],
                "surname": r[C["surname"]],
                "name": r[C["name"]],
                "phone": r[C["phone"]],
                "status": r[C["status"]],
            }
    return None


def stats_sync() -> dict:
    values = WS_PART.get_all_values()[1:]
    s = {ST_PENDING: 0, ST_PROCESSING: 0, ST_APPROVED: 0, ST_REJECTED: 0, "undelivered": 0, "issued": 0}
    for r in values:
        r = r + [""] * (len(COLS) - len(r))
        if r[C["chance_type"]] == "duplicate":
            s[ST_APPROVED] += 1
            s["issued"] += 1
            continue
        st = r[C["status"]]
        if st in s:
            s[st] += 1
        if st in (ST_APPROVED, ST_PROCESSING):
            s["issued"] += 1
        if st == ST_PENDING and r[C["delivery_status"]] != "ok":
            s["undelivered"] += 1
    return s


def undelivered_sync() -> list:
    values = WS_PART.get_all_values()[1:]
    out = []
    for i, r in enumerate(values, start=2):
        r = r + [""] * (len(COLS) - len(r))
        if r[C["status"]] == ST_PENDING and r[C["delivery_status"]] != "ok" and r[C["telegram_file_id"]]:
            out.append(_row_to_dict(r, i))
    return out


def _reset_drive_service() -> None:
    """Перестворює клієнт Drive: старе HTTP-з'єднання могло «протухнути»."""
    global drive_service
    drive_service = build("drive", "v3", credentials=_google_creds, cache_discovery=False)


def upload_photo_sync(data: bytes, name: str, date_str: str = "") -> str:
    date_str = date_str or datetime.now(TZ).strftime("%Y-%m-%d")
    last_exc = None
    for attempt in range(1, 4):
        try:
            folder = _date_folders.get(date_str)
            if not folder:
                folder = _drive_folder(date_str, FOLDER_ID)
                _date_folders[date_str] = folder
            media = MediaIoBaseUpload(io.BytesIO(data), mimetype="image/jpeg", resumable=False)
            created = drive_service.files().create(
                body={"name": f"{name}.jpg", "parents": [folder]},
                media_body=media,
                fields="id, webViewLink",
            ).execute(num_retries=2)
            link = created.get("webViewLink", "")
            if link:
                return link
            raise RuntimeError("Drive не повернув webViewLink")
        except Exception as e:
            last_exc = e
            logger.warning("Drive upload %s: спроба %d невдала: %r", name, attempt, e)
            _reset_drive_service()
            time.sleep(1.5 * attempt)
    raise last_exc


def missing_photos_sync() -> list:
    """Основні заявки (будь-який статус) без photo_url, але з telegram_file_id."""
    values = WS_PART.get_all_values()[1:]
    out = []
    for r in values:
        r = r + [""] * (len(COLS) - len(r))
        if r[C["chance_type"]] == "duplicate":
            continue
        if not r[C["photo_url"]].strip() and r[C["telegram_file_id"]].strip():
            out.append({
                "application_id": r[C["application_id"]],
                "file_id": r[C["telegram_file_id"]],
                "source_key": r[C["source_key"]],
                "surname": r[C["surname"]],
                "date": (r[C["timestamp"]] or "")[:10],
            })
    return out


def set_photo_url_sync(app_id: str, url: str) -> int:
    """Записує посилання в заявку та всі її дублікати. Нічого іншого не змінює."""
    values = WS_PART.get_all_values()
    data = []
    for i, r in enumerate(values[1:], start=2):
        r = r + [""] * (len(COLS) - len(r))
        if (r[C["application_id"]] == app_id or r[C["base_application_id"]] == app_id) and not r[C["photo_url"]].strip():
            a1 = gspread.utils.rowcol_to_a1(i, C["photo_url"] + 1)
            data.append({"range": a1, "values": [[url]]})
    if data:
        WS_PART.batch_update(data, value_input_option="RAW")
        _log_sync("photo_backfill", app_id, f"{len(data)} rows")
    return len(data)


# ----------------------------------------------------------------------------
# Інтерфейс
# ----------------------------------------------------------------------------
CONSENT, CONTACT, NAME, PHOTO = range(4)

MAIN_BUTTON_TEXT = "🧾 Зареєструвати чек"
CANCEL_BUTTON_TEXT = "❌ Скасувати"
CONTACT_BUTTON_TEXT = "📱 Надіслати номер телефону"


def main_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup([[KeyboardButton(MAIN_BUTTON_TEXT)]], resize_keyboard=True)


def contact_request_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [[KeyboardButton(CONTACT_BUTTON_TEXT, request_contact=True)], [KeyboardButton(CANCEL_BUTTON_TEXT)]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def cancel_only_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup([[KeyboardButton(CANCEL_BUTTON_TEXT)]], resize_keyboard=True)


def build_moderator_keyboard(app_id: str) -> InlineKeyboardMarkup:
    def b(n):
        return InlineKeyboardButton(str(n), callback_data=f"r:{app_id}" if n == 0 else f"a:{app_id}:{n}")

    return InlineKeyboardMarkup([[b(n) for n in range(0, 6)], [b(n) for n in range(6, 11)]])


async def notify_admins(bot, text: str) -> None:
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(chat_id=admin_id, text=text, disable_web_page_preview=True)
        except Exception:
            logger.exception("Не вдалося сповістити адміна %s", admin_id)


async def deliver_to_moderators(bot, app: dict) -> bool:
    caption = (
        f"🧾 Нова заявка #{app['application_id']}\n"
        f"Прізвище Ім'я: {app['surname']} {app['name']}\n"
        f"Телефон: {app['phone']}\n"
        f"Telegram: @{app.get('username') or '—'}"
    )
    caption += f"\nDrive: {app['photo_url']}" if app.get("photo_url") else "\n⚠️ Фото не збереглося у Google Drive — перевірте тут."
    try:
        msg = await bot.send_photo(
            chat_id=MODERATOR_CHAT_ID,
            photo=app["telegram_file_id"],
            caption=caption,
            reply_markup=build_moderator_keyboard(app["application_id"]),
        )
    except Exception:
        logger.exception("Не вдалося доставити заявку %s модератору", app["application_id"])
        return False
    try:
        await gcall(_update_row_sync, app["row"], {"delivery_status": "ok", "mod_message_id": msg.message_id})
    except Exception:
        logger.exception("Не вдалося оновити delivery_status для %s", app["application_id"])
    return True


# ----------------------------------------------------------------------------
# Дозбереження фото, які не вдалося завантажити в Drive одразу
# ----------------------------------------------------------------------------
_backfill_lock = asyncio.Lock()


async def backfill_photos(bot) -> tuple:
    if _backfill_lock.locked():
        return (0, 0)
    async with _backfill_lock:
        items = await gcall(missing_photos_sync)
        ok = 0
        for it in items:
            try:
                tg_file = await bot.get_file(it["file_id"])
                data = bytes(await tg_file.download_as_bytearray())
                safe = re.sub(r"[^\w\-]+", "_", it["surname"])[:30]
                base = (it["source_key"] or it["application_id"]).replace(":", "_")
                url = await gcall(upload_photo_sync, data, f"{base}_{safe}", it["date"])
                await gcall(set_photo_url_sync, it["application_id"], url)
                ok += 1
            except Exception:
                logger.exception("Backfill фото для %s не вдався", it["application_id"])
        if items:
            logger.info("Backfill фото: %d з %d", ok, len(items))
        return (ok, len(items))


async def _backfill_loop(bot) -> None:
    await asyncio.sleep(30)
    while True:
        try:
            await backfill_photos(bot)
        except Exception:
            logger.exception("Помилка циклу backfill")
        await asyncio.sleep(600)  # кожні 10 хвилин


# ----------------------------------------------------------------------------
# Єдиний шлях приймання чека
# ----------------------------------------------------------------------------
async def submit_receipt(update: Update, context: ContextTypes.DEFAULT_TYPE, profile: dict) -> None:
    settings = await get_settings()
    is_open, reason = promo_state(settings)
    if not is_open:
        await update.message.reply_text(closed_message(reason), reply_markup=main_menu_keyboard())
        return

    chat_id = update.effective_chat.id
    message_id = update.message.message_id
    source_key = f"{chat_id}:{message_id}"
    photo = update.message.photo[-1]

    tg_file = await context.bot.get_file(photo.file_id)
    data = bytes(await tg_file.download_as_bytearray())

    photo_url = ""
    try:
        safe = re.sub(r"[^\w\-]+", "_", profile["surname"])[:30]
        photo_url = await gcall(upload_photo_sync, data, f"{source_key.replace(':', '_')}_{safe}")
    except Exception:
        logger.exception("Не вдалося зберегти фото в Drive (%s)", source_key)

    full_profile = {**profile, "chat_id": chat_id}
    app = await gcall(create_application_sync, full_profile, source_key, photo.file_id, photo_url)
    if app is None:
        logger.info("Дублікат повідомлення %s — ігноруємо", source_key)
        return
    app_id = app["application_id"]

    # Заявку вже збережено: далі жодна помилка не повинна змушувати користувача надсилати фото вдруге.
    try:
        await update.message.reply_text(
            "Дякуємо! Ваш чек прийнято та відправлено на модерацію ✅.\n"
            "Очікуйте, будь ласка — ми повідомимо, коли модератор опрацює заявку.",
            reply_markup=main_menu_keyboard(),
        )
    except Exception:
        logger.exception("Не вдалося підтвердити прийом заявки %s користувачу", app_id)

    if not await deliver_to_moderators(context.bot, app):
        try:
            await gcall(_update_row_sync, app["row"], {"delivery_status": "error"})
        except Exception:
            logger.exception("Не вдалося позначити delivery_status=error для %s", app_id)
        await notify_admins(
            context.bot,
            f"⚠️ Заявку {app_id} збережено, але не доставлено в чат модерації. Виконайте /resend.",
        )


# ----------------------------------------------------------------------------
# Реєстрація користувача (розмова)
# ----------------------------------------------------------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    settings = await get_settings()
    is_open, reason = promo_state(settings)
    context.user_data.clear()
    if not is_open:
        await update.effective_message.reply_text(closed_message(reason), reply_markup=main_menu_keyboard())
        return ConversationHandler.END

    period = f"\n\nАкція триває до {settings['end_date']}." if settings["end_date"] else ""
    await update.effective_message.reply_text(
        "Вітаємо! 🎉\n"
        "Це бот реєстрації чеків для участі в розіграші від ТЦ LAKE PLAZA.\n\n"
        f"Умови акції:\n{settings['terms_text']}{period}\n\n"
        "Для участі потрібно погодитись на обробку персональних даних "
        "(ім'я, телефон, Telegram) та надіслати фото чека.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✅ Погоджуюсь з умовами", callback_data="consent_agree")]]),
    )
    return CONSENT


async def consent_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("Дякуємо за згоду!")
    await query.message.reply_text(
        "Будь ласка, поділіться номером телефону для реєстрації участі:",
        reply_markup=contact_request_keyboard(),
    )
    return CONTACT


async def contact_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if (update.message.text or "").strip() == CANCEL_BUTTON_TEXT:
        return await cancel(update, context)
    contact = update.message.contact
    if not contact:
        await update.message.reply_text(
            "Будь ласка, натисніть кнопку нижче, щоб надіслати номер телефону.",
            reply_markup=contact_request_keyboard(),
        )
        return CONTACT
    if contact.user_id != update.effective_user.id:
        await update.message.reply_text(
            "Будь ласка, поділіться саме СВОЇМ номером телефону за допомогою кнопки нижче.",
            reply_markup=contact_request_keyboard(),
        )
        return CONTACT

    context.user_data["phone"] = contact.phone_number
    context.user_data["username"] = update.effective_user.username or ""
    await update.message.reply_text(
        "Дякуємо! Тепер напишіть, будь ласка, Прізвище та Ім'я одним повідомленням (наприклад: Петренко Іван).",
        reply_markup=cancel_only_keyboard(),
    )
    return NAME


async def name_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    if text == CANCEL_BUTTON_TEXT:
        return await cancel(update, context)
    parts = text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await update.message.reply_text(
            "Будь ласка, напишіть і Прізвище, і Ім'я одним повідомленням (наприклад: Петренко Іван).",
            reply_markup=cancel_only_keyboard(),
        )
        return NAME
    context.user_data["surname"] = parts[0]
    context.user_data["name"] = parts[1].strip()
    await update.message.reply_text(
        "Дякуємо! Тепер надішліть, будь ласка, чітке фото чека 📷.",
        reply_markup=cancel_only_keyboard(),
    )
    return PHOTO


async def photo_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if (update.message.text or "").strip() == CANCEL_BUTTON_TEXT:
        return await cancel(update, context)
    if not update.message.photo:
        await update.message.reply_text(
            "Будь ласка, надішліть саме фото чека (не файл і не текст).",
            reply_markup=cancel_only_keyboard(),
        )
        return PHOTO

    d = context.user_data
    profile = {
        "username": d.get("username", ""),
        "surname": d["surname"],
        "name": d["name"],
        "phone": d["phone"],
    }
    try:
        await submit_receipt(update, context, profile)
    except Exception:
        logger.exception("Помилка під час приймання чека")
        await update.message.reply_text(
            "⚠️ Сталася технічна помилка. Будь ласка, надішліть фото ще раз через хвилину.",
            reply_markup=cancel_only_keyboard(),
        )
        return PHOTO
    context.user_data.clear()
    return ConversationHandler.END


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.effective_message.reply_text("Реєстрацію скасовано.", reply_markup=main_menu_keyboard())
    return ConversationHandler.END


async def conversation_timeout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    if update and update.effective_message:
        try:
            await update.effective_message.reply_text(
                "⌛ Час очікування вичерпано. Натисніть кнопку нижче, щоб почати реєстрацію знову.",
                reply_markup=main_menu_keyboard(),
            )
        except Exception:
            logger.exception("Не вдалося надіслати повідомлення про таймаут")
    return ConversationHandler.END


async def quick_photo_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Фото поза розмовою: приймаємо лише від користувача, чию останню заявку відхилено."""
    if update.message is None or not update.message.photo or update.effective_chat.type != "private":
        return
    settings = await get_settings()
    is_open, reason = promo_state(settings)
    if not is_open:
        await update.message.reply_text(closed_message(reason), reply_markup=main_menu_keyboard())
        return
    profile = await gcall(latest_profile_sync, update.effective_chat.id)
    if profile and profile["status"] == ST_REJECTED:
        try:
            await submit_receipt(update, context, profile)
        except Exception:
            logger.exception("Помилка повторного подання")
            await update.message.reply_text("⚠️ Технічна помилка. Спробуйте ще раз через хвилину.")
        return
    await update.message.reply_text(
        "Щоб зареєструвати чек, натисніть кнопку «🧾 Зареєструвати чек» і пройдіть кроки реєстрації.",
        reply_markup=main_menu_keyboard(),
    )


async def stale_consent_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Кнопка згоди, натиснута після перезапуску бота або таймауту."""
    query = update.callback_query
    await query.answer()
    await query.message.reply_text(
        "Сесія реєстрації завершилась. Натисніть «🧾 Зареєструвати чек», щоб почати знову.",
        reply_markup=main_menu_keyboard(),
    )


async def fallback_private_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Будь-яке повідомлення поза розмовою (напр. після перезапуску бота)."""
    await update.message.reply_text(
        "Щоб зареєструвати чек, натисніть кнопку «🧾 Зареєструвати чек».",
        reply_markup=main_menu_keyboard(),
    )


# ----------------------------------------------------------------------------
# Модерація
# ----------------------------------------------------------------------------
def moderator_allowed(user_id: int) -> bool:
    if MODERATOR_IDS:
        return user_id in MODERATOR_IDS or user_id in ADMIN_IDS
    return True


async def legacy_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer(
        "Ця кнопка з попереднього розіграшу і більше не працює.", show_alert=True
    )


async def moderation_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query.message.chat_id != MODERATOR_CHAT_ID or not moderator_allowed(query.from_user.id):
        await query.answer("Немає доступу.", show_alert=True)
        return
    await query.answer()

    moderator = query.from_user.full_name
    m = re.fullmatch(r"([ar]):([0-9a-f]{8})(?::(\d+))?", query.data or "")
    if not m:
        return
    action, app_id, n = m.group(1), m.group(2), m.group(3)

    if action == "r":
        res = await gcall(reject_sync, app_id, moderator)
        if "error" in res:
            await query.message.reply_text(res["error"])
            return
        app = res["app"]
        try:
            await context.bot.send_message(
                chat_id=int(app["chat_id"]),
                text="На жаль, ваш чек відхилено модератором.\n"
                     "Просто надішліть нове фото чека прямо в цей чат — реєструватись знову не потрібно.",
                reply_markup=main_menu_keyboard(),
            )
        except Exception:
            logger.exception("Не вдалося сповістити користувача %s", app["chat_id"])
        try:
            await query.edit_message_caption(
                caption=f"{query.message.caption}\n\n❌ ВІДХИЛЕНО\nМодератор: {moderator}", reply_markup=None
            )
        except Exception:
            logger.exception("Не вдалося оновити підпис %s", app_id)
        await query.message.reply_text(f"Заявку #{app_id} відхилено ❌")
        return

    chances = int(n or 0)
    if not 1 <= chances <= 10:
        return
    begin = await gcall(begin_approval_sync, app_id, chances, moderator)
    if "error" in begin:
        await query.message.reply_text(begin["error"])
        return
    done = await gcall(finish_approval_sync, app_id)
    if "error" in done:
        await query.message.reply_text(f"⚠️ Помилка завершення заявки #{app_id}. Виконайте /recover.")
        return

    numbers = ", ".join(str(x) for x in done["numbers"])
    app = done["app"]
    try:
        await context.bot.send_message(
            chat_id=int(app["chat_id"]),
            text=f"🎉 Вітаємо! Ваш чек підтверджено.\nКількість шансів у розіграші: {chances}\n"
                 f"Ваші номери учасника: {numbers}\n\nБажаємо удачі!",
        )
    except Exception:
        logger.exception("Не вдалося сповістити користувача %s", app["chat_id"])
    try:
        await query.edit_message_caption(
            caption=f"{query.message.caption}\n\n✅ ПІДТВЕРДЖЕНО\nШансів: {chances}\nНомери: {numbers}\nМодератор: {moderator}",
            reply_markup=None,
        )
    except Exception:
        logger.exception("Не вдалося оновити підпис %s", app_id)
    await query.message.reply_text(f"Заявку #{app_id} підтверджено ✅\nШансів: {chances}\nНомери: {numbers}")


# ----------------------------------------------------------------------------
# Адмін-команди
# ----------------------------------------------------------------------------
def admin_only(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id not in ADMIN_IDS:
            return
        return await func(update, context)

    return wrapper


@admin_only
async def cmd_admin(update, context):
    await update.message.reply_text(
        "🛠 Адмін-команди\n"
        "/status — стан розіграшу\n"
        "/links — посилання на таблицю та папку\n"
        "/setterms <текст> — умови акції\n"
        "/setstartdate 2026-11-01 — дата старту\n"
        "/setdate 2026-11-30 — остання дата реєстрації (включно)\n"
        "/setnumber 1 — перший номер (лише поки номери не видані)\n"
        "/resetcounter — лічильник на 0 (лише поки номери не видані)\n"
        "/activate — відкрити реєстрацію\n"
        "/deactivate — закрити реєстрацію\n"
        "/resend — повторно надіслати недоставлені заявки модератору\n"
        "/recover — дозавершити перервані підтвердження\n"
        "/fixphotos — дозберегти в Drive фото без посилань\n"
        "/chatid — ID чату"
    )


async def cmd_chatid(update, context):
    await update.message.reply_text(f"Chat ID цього чату: {update.effective_chat.id}")


@admin_only
async def cmd_status(update, context):
    s = await get_settings(force=True)
    st = await gcall(stats_sync)
    is_open, reason = promo_state(s)
    state = "🟢 ВІДКРИТА" if is_open else f"🔴 ЗАКРИТА ({reason or 'вимкнена'})"
    await update.message.reply_text(
        f"Розіграш: {CAMPAIGN_ID}\n"
        f"Реєстрація: {state}\n"
        f"is_active: {s['is_active']}\n"
        f"Старт: {s['start_date'] or 'не встановлено'}\n"
        f"Завершення: {s['end_date'] or 'не встановлено'}\n"
        f"Останній виданий номер: {s['last_number']}\n\n"
        f"На модерації: {st[ST_PENDING]} (недоставлених: {st['undelivered']})\n"
        f"Обробляється: {st[ST_PROCESSING]}\n"
        f"Підтверджено (номерів): {st[ST_APPROVED]}\n"
        f"Відхилено: {st[ST_REJECTED]}\n\n"
        f"Умови:\n{s['terms_text']}"
    )


@admin_only
async def cmd_links(update, context):
    await update.message.reply_text(
        f"📊 Таблиця: {sheet_url()}\n📁 Папка з фото: {folder_url()}", disable_web_page_preview=True
    )


@admin_only
async def cmd_activate(update, context):
    s = await get_settings(force=True)
    if not s["end_date"]:
        await update.message.reply_text("Спочатку встановіть дату завершення: /setdate 2026-11-30")
        return
    start_d, end_d = _parse_date(s["start_date"]), _parse_date(s["end_date"])
    if start_d and end_d and start_d > end_d:
        await update.message.reply_text("⛔ Дата старту пізніша за дату завершення. Перевірте /setstartdate і /setdate.")
        return
    if s["terms_text"] == DEFAULT_TERMS:
        await update.message.reply_text("Спочатку встановіть умови акції: /setterms <текст>")
        return
    await set_setting("is_active", "TRUE")
    s = await get_settings(force=True)
    is_open, reason = promo_state(s)
    note = "" if is_open else f"\n⚠️ Але зараз реєстрація закрита за датами ({reason}). Перевірте /status."
    await update.message.reply_text(f"🟢 Реєстрацію увімкнено.{note}")


@admin_only
async def cmd_deactivate(update, context):
    await set_setting("is_active", "FALSE")
    await update.message.reply_text("🔴 Реєстрацію вимкнено. Нові чеки не приймаються.")


@admin_only
async def cmd_setterms(update, context):
    text = re.sub(r"^/setterms(@\w+)?\s*", "", update.message.text or "", flags=re.IGNORECASE).strip()
    if not text:
        await update.message.reply_text("Використання: /setterms <новий текст умов>")
        return
    await set_setting("terms_text", text)
    await update.message.reply_text("✅ Умови акції оновлено.")


async def _set_date(update, context, key: str, label: str):
    if not context.args or not _parse_date(context.args[0]):
        await update.message.reply_text("Формат: YYYY-MM-DD, наприклад 2026-11-30")
        return
    await set_setting(key, context.args[0])
    await update.message.reply_text(f"✅ {label}: {context.args[0]}")


@admin_only
async def cmd_setdate(update, context):
    await _set_date(update, context, "end_date", "Дату завершення встановлено")


@admin_only
async def cmd_setstartdate(update, context):
    await _set_date(update, context, "start_date", "Дату старту встановлено")


async def _set_counter(update, value: int):
    st = await gcall(stats_sync)
    if st["issued"] > 0:
        await update.message.reply_text(
            f"⛔ У цьому розіграші вже видано номери ({st['issued']}). Зміну лічильника заблоковано, "
            "щоб не з'явилися повторні номери. Для нового розіграшу змініть CAMPAIGN_ID."
        )
        return
    await set_setting("last_number", value)
    await gcall(_log_sync, "set_counter", "", str(value))
    await update.message.reply_text(f"✅ Наступний виданий номер буде {value + 1}.")


@admin_only
async def cmd_resetcounter(update, context):
    await _set_counter(update, 0)


@admin_only
async def cmd_setnumber(update, context):
    if not context.args or not context.args[0].isdigit() or int(context.args[0]) < 1:
        await update.message.reply_text("Використання: /setnumber 1 (перший номер учасника)")
        return
    await _set_counter(update, int(context.args[0]) - 1)


@admin_only
async def cmd_resend(update, context):
    apps = await gcall(undelivered_sync)
    ok = 0
    for app in apps:
        if await deliver_to_moderators(context.bot, app):
            ok += 1
    await update.message.reply_text(f"Повторно надіслано: {ok} з {len(apps)}.")


@admin_only
async def cmd_fixphotos(update, context):
    await update.message.reply_text("⏳ Дозберігаю фото без посилань…")
    ok, total = await backfill_photos(context.bot)
    await update.message.reply_text(f"📷 Збережено фото: {ok} з {total}.")


@admin_only
async def cmd_recover(update, context):
    done = await gcall(recover_incomplete_sync)
    await _notify_recovered(context.bot, done)
    await update.message.reply_text(f"Дозавершено заявок: {len(done)}.")


async def _notify_recovered(bot, done: list) -> None:
    for app_id, numbers, chat_id in done:
        try:
            await bot.send_message(
                chat_id=int(chat_id),
                text=f"🎉 Вітаємо! Ваш чек підтверджено.\nВаші номери учасника: {', '.join(map(str, numbers))}\n\nБажаємо удачі!",
            )
        except Exception:
            logger.exception("Не вдалося сповістити %s про відновлену заявку %s", chat_id, app_id)


# ----------------------------------------------------------------------------
# Запуск
# ----------------------------------------------------------------------------
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Помилка обробки update %s: %s", update, context.error, exc_info=context.error)


def start_healthcheck_server():
    port = int(os.environ.get("PORT", 8080))

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"OK")

        def log_message(self, fmt, *args):
            return

    HTTPServer(("0.0.0.0", port), Handler).serve_forever()


async def post_init(app):
    user_cmds = [BotCommand("start", "Зареєструвати чек"), BotCommand("cancel", "Скасувати реєстрацію")]
    await app.bot.set_my_commands(user_cmds, scope=BotCommandScopeDefault())
    admin_cmds = user_cmds + [
        BotCommand("status", "Стан розіграшу"),
        BotCommand("links", "Посилання на таблицю й папку"),
        BotCommand("setterms", "Умови акції"),
        BotCommand("setstartdate", "Дата старту"),
        BotCommand("setdate", "Дата завершення"),
        BotCommand("setnumber", "Перший номер"),
        BotCommand("resetcounter", "Лічильник на 0"),
        BotCommand("activate", "Відкрити реєстрацію"),
        BotCommand("deactivate", "Закрити реєстрацію"),
        BotCommand("resend", "Повторити недоставлені"),
        BotCommand("recover", "Дозавершити підтвердження"),
        BotCommand("fixphotos", "Дозберегти фото в Drive"),
        BotCommand("admin", "Адмін-команди"),
    ]
    for admin_id in ADMIN_IDS:
        try:
            await app.bot.set_my_commands(admin_cmds, scope=BotCommandScopeChat(chat_id=admin_id))
        except Exception:
            logger.exception("Не вдалося встановити команди для %s", admin_id)

    try:
        done = await gcall(recover_incomplete_sync)
        if done:
            await _notify_recovered(app.bot, done)
            await notify_admins(app.bot, f"♻️ Після перезапуску дозавершено підтвердження заявок: {len(done)}.")
    except Exception:
        logger.exception("Помилка відновлення після перезапуску")

    app.create_task(_backfill_loop(app.bot))

    if INFRA_CREATED:
        await notify_admins(
            app.bot,
            "✅ Створено нову інфраструктуру розіграшу.\n"
            f"📊 Таблиця: {sheet_url()}\n📁 Фото чеків: {folder_url()}\n\n"
            "Реєстрація ВИМКНЕНА. Налаштуйте: /setterms, /setstartdate, /setdate, /setnumber, потім /activate.",
        )


def main():
    init_infrastructure()

    app = ApplicationBuilder().token(TELEGRAM_TOKEN).post_init(post_init).build()

    conv = ConversationHandler(
        entry_points=[
            CommandHandler("start", start),
            MessageHandler(filters.Regex(f"^{re.escape(MAIN_BUTTON_TEXT)}$"), start),
        ],
        states={
            CONSENT: [CallbackQueryHandler(consent_callback, pattern=r"^consent_agree$")],
            CONTACT: [MessageHandler(filters.ALL & ~filters.COMMAND, contact_handler)],
            NAME: [MessageHandler(filters.ALL & ~filters.COMMAND, name_handler)],
            PHOTO: [MessageHandler(filters.ALL & ~filters.COMMAND, photo_handler)],
            ConversationHandler.TIMEOUT: [TypeHandler(Update, conversation_timeout)],
        },
        fallbacks=[
            CommandHandler("cancel", cancel),
            MessageHandler(filters.Regex(f"^{re.escape(CANCEL_BUTTON_TEXT)}$"), cancel),
        ],
        conversation_timeout=CONVERSATION_TIMEOUT,
        allow_reentry=True,
    )
    app.add_handler(conv)
    app.add_handler(CallbackQueryHandler(moderation_callback, pattern=r"^[ar]:[0-9a-f]{8}(:\d+)?$"))
    app.add_handler(CallbackQueryHandler(legacy_callback, pattern=r"^(reject_\d+|approve_\d+_\d+)$"))

    for name, fn in [
        ("chatid", cmd_chatid), ("admin", cmd_admin), ("status", cmd_status), ("links", cmd_links),
        ("activate", cmd_activate), ("deactivate", cmd_deactivate), ("setterms", cmd_setterms),
        ("setdate", cmd_setdate), ("setstartdate", cmd_setstartdate), ("resetcounter", cmd_resetcounter),
        ("setnumber", cmd_setnumber), ("resend", cmd_resend), ("recover", cmd_recover),
        ("fixphotos", cmd_fixphotos),
    ]:
        app.add_handler(CommandHandler(name, fn))

    # Фото поза розмовою — той самий group 0, після conv: якщо розмова обробила повідомлення, цей хендлер не спрацює.
    app.add_handler(MessageHandler(filters.PHOTO & filters.ChatType.PRIVATE, quick_photo_handler))
    app.add_handler(CallbackQueryHandler(stale_consent_callback, pattern=r"^consent_agree$"))
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND & ~filters.PHOTO, fallback_private_handler))
    app.add_error_handler(error_handler)

    threading.Thread(target=start_healthcheck_server, daemon=True).start()
    logger.info("Бот запускається (polling), розіграш %s", CAMPAIGN_ID)
    app.run_polling(drop_pending_updates=False)


if __name__ == "__main__":
    main()
