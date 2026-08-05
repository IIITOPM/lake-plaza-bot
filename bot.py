"""
Telegram-бот "Реєстрація чеків ТЦ" — Python-версія (MVP)
========================================================

Стек: python-telegram-bot (async), gspread + Google Drive API
через ОСОБИСТИЙ Google-акаунт (OAuth, НЕ service account).

Обов'язкові змінні середовища:
TELEGRAM_TOKEN
MODERATOR_CHAT_ID
ADMIN_IDS
SHEET_ID
DRIVE_FOLDER_ID
GOOGLE_CLIENT_ID
GOOGLE_CLIENT_SECRET
GOOGLE_REFRESH_TOKEN
PORT

Необов'язкові:
CONVERSATION_TIMEOUT
"""

import asyncio
import io
import logging
import os
import re
import threading
import time
from datetime import date, datetime
from http.server import BaseHTTPRequestHandler, HTTPServer

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
    filters,
)

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
logger = logging.getLogger("checks-bot")

TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
MODERATOR_CHAT_ID = int(os.environ["MODERATOR_CHAT_ID"])
ADMIN_IDS = {int(x) for x in os.environ.get("ADMIN_IDS", "").split(",") if x.strip()}
SHEET_ID = os.environ["SHEET_ID"]
DRIVE_FOLDER_ID = os.environ["DRIVE_FOLDER_ID"]
CONVERSATION_TIMEOUT = int(os.environ.get("CONVERSATION_TIMEOUT", "300"))

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]


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
gc = gspread.authorize(_google_creds)
drive_service = build("drive", "v3", credentials=_google_creds)


def get_spreadsheet():
    return gc.open_by_key(SHEET_ID)


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


_settings_cache = {"data": None, "ts": 0.0}
SETTINGS_CACHE_TTL = 20


def get_settings(force_refresh: bool = False) -> dict:
    now = time.monotonic()
    if (
        not force_refresh
        and _settings_cache["data"] is not None
        and (now - _settings_cache["ts"] < SETTINGS_CACHE_TTL)
    ):
        return _settings_cache["data"]

    sheet = get_spreadsheet().worksheet("Settings")
    rows = sheet.get_all_values()[1:]
    raw = {r[0]: (r[1] if len(r) > 1 else "") for r in rows if r and r[0]}

    result = {
        "is_active": raw.get("is_active", "TRUE").strip().upper() == "TRUE",
        "end_date": raw.get("end_date", "").strip(),
        "terms_text": raw.get(
            "terms_text",
            "Умови акції уточнюйте у представників ТЦ.",
        ),
    }
    _settings_cache["data"] = result
    _settings_cache["ts"] = now
    return result


def _find_settings_cell(sheet, key):
    try:
        return sheet.find(key)
    except gspread.exceptions.CellNotFound:
        return None


def set_setting(key: str, value) -> None:
    sheet = get_spreadsheet().worksheet("Settings")
    cell = _find_settings_cell(sheet, key)
    if cell:
        sheet.update_cell(cell.row, 2, value)
    else:
        sheet.append_row([key, value])
    _settings_cache["ts"] = 0.0


def is_promo_active(settings: dict) -> bool:
    if not settings["is_active"]:
        return False
    if settings["end_date"]:
        try:
            end = datetime.strptime(settings["end_date"], "%Y-%m-%d").date()
            if date.today() > end:
                return False
        except ValueError:
            pass
    return True


_counter_lock = asyncio.Lock()


async def get_next_number() -> int:
    async with _counter_lock:
        sheet = get_spreadsheet().worksheet("Settings")
        cell = _find_settings_cell(sheet, "last_number")
        current = int((sheet.cell(cell.row, 2).value or "0")) if cell else 0
        nxt = current + 1
        if cell:
            sheet.update_cell(cell.row, 2, nxt)
        else:
            sheet.append_row(["last_number", nxt])
        return nxt


PARTICIPANTS_KEYS = [
    "timestamp",
    "chat_id",
    "username",
    "surname",
    "name",
    "phone",
    "photo_url",
    "status",
    "participant_number",
    "moderator",
    "moderation_date",
    "chance_type",
    "chance_index",
    "chance_total",
    "base_row",
]


def _extract_row_from_append_response(resp: dict) -> int:
    updated_range = resp["updates"]["updatedRange"]
    first_cell = updated_range.split("!")[1].split(":")[0]
    return int("".join(ch for ch in first_cell if ch.isdigit()))


def append_participant(
    chat_id,
    username,
    surname,
    name,
    phone,
    *,
    status="На модерації",
    participant_number="",
    moderator="",
    moderation_date="",
    chance_type="pending",
    chance_index="",
    chance_total="",
    base_row="",
    photo_url="",
) -> int:
    sheet = get_spreadsheet().worksheet("Participants")
    resp = sheet.append_row(
        [
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            str(chat_id),
            username,
            surname,
            name,
            phone,
            photo_url,
            status,
            str(participant_number) if participant_number != "" else "",
            moderator,
            moderation_date,
            chance_type,
            str(chance_index) if chance_index != "" else "",
            str(chance_total) if chance_total != "" else "",
            str(base_row) if base_row != "" else "",
        ]
    )
    return _extract_row_from_append_response(resp)


def append_duplicate_chance(
    base_participant: dict,
    participant_number: int,
    chance_index: int,
    chance_total: int,
    moderator_name: str,
    base_row: int,
) -> int:
    sheet = get_spreadsheet().worksheet("Participants")
    resp = sheet.append_row(
        [
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            str(base_participant["chat_id"]),
            base_participant.get("username", ""),
            base_participant["surname"],
            base_participant["name"],
            base_participant["phone"],
            base_participant.get("photo_url", ""),
            "Підтверджено",
            str(participant_number),
            moderator_name,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "duplicate",
            str(chance_index),
            str(chance_total),
            str(base_row),
        ]
    )
    return _extract_row_from_append_response(resp)


def set_participant_photo_url(row: int, url: str) -> None:
    get_spreadsheet().worksheet("Participants").update_cell(row, 7, url)


def get_participant_row(row: int) -> dict | None:
    sheet = get_spreadsheet().worksheet("Participants")
    values = sheet.row_values(row)
    if not values or len(values) < 8:
        return None

    values += [""] * (len(PARTICIPANTS_KEYS) - len(values))
    data = dict(zip(PARTICIPANTS_KEYS, values[: len(PARTICIPANTS_KEYS)]))
    data["row"] = row
    return data


def mark_participant_rejected(row: int, moderator_name: str) -> None:
    sheet = get_spreadsheet().worksheet("Participants")
    sheet.update(
        f"H{row}:O{row}",
        [[
            "Відхилено",
            "",
            moderator_name,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "rejected",
            "",
            "",
            "",
        ]],
    )


def mark_participant_approved_original(
    row: int,
    participant_number: int,
    moderator_name: str,
    chance_total: int,
) -> None:
    sheet = get_spreadsheet().worksheet("Participants")
    sheet.update(
        f"H{row}:O{row}",
        [[
            "Підтверджено",
            str(participant_number),
            moderator_name,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "original",
            "1",
            str(chance_total),
            str(row),
        ]],
    )


def get_or_create_date_folder() -> str:
    date_str = datetime.now().strftime("%Y-%m-%d")
    query = (
        f"'{DRIVE_FOLDER_ID}' in parents and name='{date_str}' "
        "and mimeType='application/vnd.google-apps.folder' and trashed=false"
    )
    res = drive_service.files().list(q=query, fields="files(id,name)").execute()
    files = res.get("files", [])
    if files:
        return files[0]["id"]

    folder = drive_service.files().create(
        body={
            "name": date_str,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [DRIVE_FOLDER_ID],
        },
        fields="id",
    ).execute()
    return folder["id"]


async def save_photo_to_drive(tg_file, name_prefix: str) -> str:
    file_bytes = await tg_file.download_as_bytearray()
    folder_id = get_or_create_date_folder()
    media = MediaIoBaseUpload(
        io.BytesIO(bytes(file_bytes)),
        mimetype="image/jpeg",
        resumable=False,
    )
    created = drive_service.files().create(
        body={"name": f"{name_prefix}.jpg", "parents": [folder_id]},
        media_body=media,
        fields="id, webViewLink",
    ).execute()
    return created.get("webViewLink", "")


CONSENT, CONTACT, NAME, PHOTO = range(4)
pending_resubmit: dict[int, dict] = {}

MAIN_BUTTON_TEXT = "🧾 Зареєструвати чек"
CANCEL_BUTTON_TEXT = "❌ Скасувати"
CONTACT_BUTTON_TEXT = "📱 Надіслати номер телефону"


def main_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup([[KeyboardButton(MAIN_BUTTON_TEXT)]], resize_keyboard=True)


def contact_request_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton(CONTACT_BUTTON_TEXT, request_contact=True)],
            [KeyboardButton(CANCEL_BUTTON_TEXT)],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def cancel_only_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup([[KeyboardButton(CANCEL_BUTTON_TEXT)]], resize_keyboard=True)

def build_moderator_keyboard(row: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("0", callback_data=f"reject_{row}"),
            InlineKeyboardButton("1", callback_data=f"approve_{row}_1"),
            InlineKeyboardButton("2", callback_data=f"approve_{row}_2"),
            InlineKeyboardButton("3", callback_data=f"approve_{row}_3"),
            InlineKeyboardButton("4", callback_data=f"approve_{row}_4"),
            InlineKeyboardButton("5", callback_data=f"approve_{row}_5"),
        ],
        [
            InlineKeyboardButton("6", callback_data=f"approve_{row}_6"),
            InlineKeyboardButton("7", callback_data=f"approve_{row}_7"),
            InlineKeyboardButton("8", callback_data=f"approve_{row}_8"),
            InlineKeyboardButton("9", callback_data=f"approve_{row}_9"),
            InlineKeyboardButton("10", callback_data=f"approve_{row}_10"),
        ],
    ])



async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    settings = get_settings()
    if not is_promo_active(settings):
        if not settings["is_active"]:
            msg = "😔 Наразі реєстрація чеків тимчасово призупинена організаторами. Слідкуйте за оновленнями!"
        else:
            msg = f"😔 Акція завершилась {settings['end_date']}. Дякуємо за участь!"
        await update.effective_message.reply_text(msg, reply_markup=main_menu_keyboard())
        context.user_data.clear()
        return ConversationHandler.END

    context.user_data.clear()
    context.user_data["in_progress"] = True
    period = f"\n\nАкція триває до {settings['end_date']}." if settings["end_date"] else ""
    await update.effective_message.reply_text(
        "Вітаємо! 🎉\n"
        "Це бот реєстрації чеків для участі в розіграші від ТЦ LAKE PLAZA.\n\n"
        f"Умови акції:\n{settings['terms_text']}{period}\n\n"
        "Для участі потрібно погодитись на обробку персональних даних "
        "(ім'я, телефон, Telegram) та надіслати фото чека.",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("✅ Погоджуюсь з умовами", callback_data="consent_agree")]]
        ),
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
        "Дякуємо! Тепер напишіть, будь ласка, Прізвище та Ім'я одним повідомленням "
        "(наприклад: Петренко Іван).",
        reply_markup=cancel_only_keyboard(),
    )
    return NAME


async def name_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    if text == CANCEL_BUTTON_TEXT:
        return await cancel(update, context)
    if not text:
        await update.message.reply_text(
            "Напишіть, будь ласка, текстом Прізвище та Ім'я.",
            reply_markup=cancel_only_keyboard(),
        )
        return NAME

    parts = text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await update.message.reply_text(
            "Будь ласка, напишіть і Прізвище, і Ім'я одним повідомленням "
            "(наприклад: Петренко Іван).",
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
    chat_id = update.effective_chat.id
    row = append_participant(chat_id, d.get("username", ""), d["surname"], d["name"], d["phone"])

    photo = update.message.photo[-1]
    tg_file = await context.bot.get_file(photo.file_id)
    drive_url = ""
    try:
        drive_url = await save_photo_to_drive(tg_file, f"{row}_{d['surname']}_{d['phone']}")
        set_participant_photo_url(row, drive_url)
    except Exception:
        logger.exception("Не вдалося зберегти фото в Drive (заявка #%s)", row)

    await update.message.reply_text(
        "Дякуємо! Ваш чек прийнято та відправлено на модерацію ✅.\n"
        "Очікуйте, будь ласка — ми повідомимо, коли модератор опрацює заявку.",
        reply_markup=main_menu_keyboard(),
    )

    caption = (
        f"🧾 Нова заявка #{row}\n"
        f"Прізвище Ім'я: {d['surname']} {d['name']}\n"
        f"Телефон: {d['phone']}\n"
        f"Telegram: @{d.get('username') or '—'}"
    )
    if drive_url:
        caption += f"\nDrive: {drive_url}"
    else:
        caption += "\n⚠️ Фото НЕ збереглося у Google Drive — перевірте вручну (є лише тут, у Telegram)."

    await context.bot.send_photo(
        chat_id=MODERATOR_CHAT_ID,
        photo=photo.file_id,
        caption=caption,
        reply_markup=build_moderator_keyboard(row),
    )

    context.user_data.clear()
    return ConversationHandler.END


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    pending_resubmit.pop(update.effective_chat.id, None)
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
            logger.exception("Не вдалося надіслати повідомлення про таймаут розмови")
    return ConversationHandler.END


async def moderation_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query.message.chat_id != MODERATOR_CHAT_ID:
        await query.answer()
        return

    await query.answer()
    data = query.data or ""
    moderator_name = query.from_user.full_name
    reject_match = re.fullmatch(r"reject_(\d+)", data)
    approve_match = re.fullmatch(r"approve_(\d+)_(\d+)", data)
    if not reject_match and not approve_match:
        return

    row = int(reject_match.group(1) if reject_match else approve_match.group(1))
    chance_total = 0 if reject_match else int(approve_match.group(2))
    participant = get_participant_row(row)
    if participant is None:
        await query.message.reply_text("Заявку не знайдено.")
        return
    if participant["status"] != "На модерації":
        await query.message.reply_text("Цю заявку вже опрацьовано.")
        return

    if chance_total == 0:
        mark_participant_rejected(row, moderator_name)
        pending_resubmit[int(participant["chat_id"])] = {
            "surname": participant["surname"],
            "name": participant["name"],
            "phone": participant["phone"],
            "username": participant.get("username", ""),
        }
        try:
            await context.bot.send_message(
                chat_id=int(participant["chat_id"]),
                text=(
                    "На жаль, ваш чек відхилено модератором.\n"
                    "Просто надішліть нове фото чека прямо в цей чат — реєструватись знову не потрібно."
                ),
                reply_markup=main_menu_keyboard(),
            )
        except Exception:
            logger.exception("Не вдалося сповістити користувача %s", participant["chat_id"])

        try:
            await query.edit_message_caption(
                caption=f"{query.message.caption}\n\n❌ ВІДХИЛЕНО\nМодератор: {moderator_name}",
                reply_markup=None,
            )
        except Exception:
            logger.exception("Не вдалося оновити caption для заявки #%s", row)

        await query.message.reply_text(f"Заявку #{row} відхилено ❌")
        return

    assigned_numbers = []
    first_number = await get_next_number()
    mark_participant_approved_original(row, first_number, moderator_name, chance_total)
    assigned_numbers.append(first_number)

    for idx in range(2, chance_total + 1):
        num = await get_next_number()
        append_duplicate_chance(
            base_participant=participant,
            participant_number=num,
            chance_index=idx,
            chance_total=chance_total,
            moderator_name=moderator_name,
            base_row=row,
        )
        assigned_numbers.append(num)

    try:
        await context.bot.send_message(
            chat_id=int(participant["chat_id"]),
            text=(
                "🎉 Вітаємо! Ваш чек підтверджено.\n"
                f"Кількість шансів у розіграші: {chance_total}\n"
                f"Ваші номери учасника: {', '.join(str(x) for x in assigned_numbers)}\n\n"
                "Бажаємо удачі!"
            ),
        )
    except Exception:
        logger.exception("Не вдалося сповістити користувача %s", participant["chat_id"])

    try:
        await query.edit_message_caption(
            caption=(
                f"{query.message.caption}\n\n"
                f"✅ ПІДТВЕРДЖЕНО\n"
                f"Шансів: {chance_total}\n"
                f"Номери: {', '.join(str(x) for x in assigned_numbers)}\n"
                f"Модератор: {moderator_name}"
            ),
            reply_markup=None,
        )
    except Exception:
        logger.exception("Не вдалося оновити caption для заявки #%s", row)

    await query.message.reply_text(
        f"Заявку #{row} підтверджено ✅\nШансів: {chance_total}\nНомери: {', '.join(str(x) for x in assigned_numbers)}"
    )


async def cmd_chatid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"Chat ID цього чату: {update.effective_chat.id}")


async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    await update.message.reply_text(
        "🛠 Адмін-команди\n"
        "/status — поточний стан бота\n"
        "/activate — увімкнути реєстрацію\n"
        "/deactivate — вимкнути реєстрацію\n"
        "/resetcounter — обнулити лічильник номерів учасників\n"
        "/setdate 2026-08-31 — дата завершення акції\n"
        "/setterms <текст> — новий текст умов акції\n"
        "/chatid — дізнатись ID поточного чату"
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    s = get_settings(force_refresh=True)
    await update.message.reply_text(
        f"Статус бота: {'🟢 Активний' if s['is_active'] else '🔴 Деактивовано'}\n"
        f"Дата завершення акції: {s['end_date'] or 'не встановлена'}\n\n"
        f"Поточні умови акції:\n{s['terms_text']}"
    )


async def cmd_deactivate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    set_setting("is_active", "FALSE")
    await update.message.reply_text("🔴 Бот деактивовано. Нові реєстрації тимчасово недоступні для відвідувачів.")


async def cmd_activate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    set_setting("is_active", "TRUE")
    await update.message.reply_text("🟢 Бот активовано. Реєстрація знову доступна.")


async def cmd_resetcounter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    set_setting("last_number", "0")
    await update.message.reply_text("🔄 Лічильник номерів учасників скинуто.\nНаступний виданий номер буде 1.")


async def cmd_setdate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("Використання: /setdate 2026-08-31")
        return

    date_str = context.args[0]
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        await update.message.reply_text("Невірний формат дати. Використовуйте YYYY-MM-DD, напр. 2026-08-31")
        return

    set_setting("end_date", date_str)
    await update.message.reply_text(f"✅ Дату завершення акції встановлено: {date_str}")


async def cmd_setterms(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    full_text = update.message.text or ""
    new_terms = re.sub(r"^/setterms(@\w+)?\s*", "", full_text, flags=re.IGNORECASE).strip()
    if not new_terms:
        await update.message.reply_text(
            "Використання:\n/setterms <новий текст умов акції>\n\nМожна писати кілька рядків одним повідомленням."
        )
        return

    set_setting("terms_text", new_terms)
    await update.message.reply_text("✅ Умови акції оновлено.")


async def resubmit_photo_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message is None or not update.message.photo:
        return

    chat_id = update.effective_chat.id
    if chat_id not in pending_resubmit:
        return

    d = pending_resubmit[chat_id]

    row = append_participant(
        chat_id,
        d["username"],
        d["surname"],
        d["name"],
        d["phone"],
    )

    photo = update.message.photo[-1]
    tg_file = await context.bot.get_file(photo.file_id)

    drive_url = ""
    try:
        drive_url = await save_photo_to_drive(
            tg_file,
            f"{row}_{d['surname']}_{d['phone']}"
        )
        set_participant_photo_url(row, drive_url)
    except Exception:
        logger.exception("Не вдалося зберегти повторне фото в Drive (заявка #%s)", row)

    caption = (
        f"🧾 Повторна заявка #{row}\n"
        f"Прізвище Ім'я: {d['surname']} {d['name']}\n"
        f"Телефон: {d['phone']}\n"
        f"Telegram: @{d.get('username') or '—'}\n"
        f"Тип: повторне подання після відхилення"
    )

    if drive_url:
        caption += f"\nDrive: {drive_url}"
    else:
        caption += "\n⚠️ Фото НЕ збереглося у Google Drive — перевірте вручну."

    try:
        await context.bot.send_photo(
            chat_id=MODERATOR_CHAT_ID,
            photo=photo.file_id,
            caption=caption,
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("0", callback_data=f"reject_{row}"),
                    InlineKeyboardButton("1", callback_data=f"approve_{row}_1"),
                    InlineKeyboardButton("2", callback_data=f"approve_{row}_2"),
                    InlineKeyboardButton("3", callback_data=f"approve_{row}_3"),
                    InlineKeyboardButton("4", callback_data=f"approve_{row}_4"),
                    InlineKeyboardButton("5", callback_data=f"approve_{row}_5"),
                ],
                [
                    InlineKeyboardButton("6", callback_data=f"approve_{row}_6"),
                    InlineKeyboardButton("7", callback_data=f"approve_{row}_7"),
                    InlineKeyboardButton("8", callback_data=f"approve_{row}_8"),
                    InlineKeyboardButton("9", callback_data=f"approve_{row}_9"),
                    InlineKeyboardButton("10", callback_data=f"approve_{row}_10"),
                ],
            ]),
        )
    except Exception:
        logger.exception(
            "Не вдалося відправити повторне фото модератору (заявка #%s, chat_id=%s)",
            row,
            chat_id,
        )
        await update.message.reply_text(
            "⚠️ Сталася помилка під час повторної відправки на модерацію.\n"
            "Будь ласка, надішліть фото ще раз через кілька секунд.",
            reply_markup=main_menu_keyboard(),
        )
        return

    pending_resubmit.pop(chat_id, None)

    await update.message.reply_text(
        "Дякуємо! Нове фото чека прийнято та відправлено на модерацію ✅.\n"
        "Очікуйте, будь ласка — ми повідомимо, коли модератор опрацює заявку.",
        reply_markup=main_menu_keyboard(),
    )





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
    await app.bot.set_my_commands(
        [BotCommand("start", "Зареєструвати чек"), BotCommand("cancel", "Скасувати реєстрацію")],
        scope=BotCommandScopeDefault(),
    )
    for admin_id in ADMIN_IDS:
        try:
            await app.bot.set_my_commands(
                [
                    BotCommand("start", "Зареєструвати чек"),
                    BotCommand("cancel", "Скасувати реєстрацію"),
                    BotCommand("status", "Стан бота"),
                    BotCommand("activate", "Увімкнути реєстрацію"),
                    BotCommand("resetcounter", "Обнулити лічильник номерів"),
                    BotCommand("deactivate", "Призупинити реєстрацію"),
                    BotCommand("setdate", "Дата завершення акції"),
                    BotCommand("setterms", "Оновити умови акції"),
                    BotCommand("chatid", "ID поточного чату"),
                    BotCommand("admin", "Показати адмін-команди"),
                ],
                scope=BotCommandScopeChat(chat_id=admin_id),
            )
        except Exception:
            logger.exception("Не вдалося встановити команди для адміна %s", admin_id)


def main():
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).post_init(post_init).build()

    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("start", start),
            MessageHandler(filters.Regex(f"^{re.escape(MAIN_BUTTON_TEXT)}$"), start),
        ],
        states={
            CONSENT: [
                CallbackQueryHandler(consent_callback, pattern=r"^consent_agree$")
            ],
            CONTACT: [
                MessageHandler(filters.ALL & ~filters.COMMAND, contact_handler)
            ],
            NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, name_handler)
            ],
            PHOTO: [
                MessageHandler(filters.ALL & ~filters.COMMAND, photo_handler)
            ],
            ConversationHandler.TIMEOUT: [
                MessageHandler(filters.ALL, conversation_timeout)
            ],
        },
        fallbacks=[
            CommandHandler("cancel", cancel),
            MessageHandler(filters.Regex(f"^{re.escape(CANCEL_BUTTON_TEXT)}$"), cancel),
        ],
        conversation_timeout=CONVERSATION_TIMEOUT,
        allow_reentry=True,
    )

    app.add_handler(conv_handler)

    app.add_handler(
        CallbackQueryHandler(
            moderation_callback,
            pattern=r"^(reject_\d+|approve_\d+_\d+)$",
        )
    )

    app.add_handler(CommandHandler("chatid", cmd_chatid))
    app.add_handler(CommandHandler("admin", cmd_admin))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("activate", cmd_activate))
    app.add_handler(CommandHandler("resetcounter", cmd_resetcounter))
    app.add_handler(CommandHandler("deactivate", cmd_deactivate))
    app.add_handler(CommandHandler("setdate", cmd_setdate))
    app.add_handler(CommandHandler("setterms", cmd_setterms))

    app.add_handler(MessageHandler(filters.PHOTO, resubmit_photo_handler), group=1)

    app.add_error_handler(error_handler)

    threading.Thread(target=start_healthcheck_server, daemon=True).start()

    logger.info("Бот запускається (polling)...")
    app.run_polling(drop_pending_updates=True)



if __name__ == "__main__":
    main()
