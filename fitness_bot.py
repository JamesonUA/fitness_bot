"""
Телеграм-бот: Онлайн-тренування (фітнес-канал)
================================================

Концепція:
  • Клієнт пише боту → отримує реквізити для разової оплати
  • Надсилає скріншот квитанції
  • Бот пересилає скріншот адміну з кнопками Підтвердити / Відхилити
  • Після підтвердження клієнт отримує посилання на Microsoft Teams
  • Адмін додає тренування через /add_workout або меню ⚙️
  • Бот автоматично публікує анонс у Telegram-канал
  • Нагадування з посиланням надсилаються за 1 год та при старті
    тільки оплаченим клієнтам

Зберігання даних — як в оригінальному боті:
  • dict у пам'яті
  • при кожній зміні → локальний JSON + фоновий thread → Telegram (pinned message)
  • при перезапуску → local file_id → pinned message → локальний JSON (резерв)
  Це дозволяє боту коректно працювати на хостингу з ephemeral-файловою системою
  (Koyeb, Railway, Heroku і т.д.) — дані не втрачаються при рестартах.

Встановлення:
  pip install python-telegram-bot requests

Запуск локально (polling):
  python fitness_bot.py

Запуск на хостингу (webhook):
  Задайте змінну WEBHOOK_URL=https://your-domain.com/webhook
"""

import os
import io
import json
import logging
import asyncio
import threading
import requests as req
from datetime import datetime
from zoneinfo import ZoneInfo

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes
)

# ═══════════════════════════════════════════════════════════════
#  НАЛАШТУВАННЯ — змініть під себе
# ═══════════════════════════════════════════════════════════════

BOT_TOKEN      = os.environ.get("BOT_TOKEN",      "8623568533:AAEC0nfGONryCrcLQ86njVwGQMt0f1erZD4")
ADMIN_ID       = int(os.environ.get("ADMIN_ID",   "447671579"))
CHANNEL_ID     = os.environ.get("CHANNEL_ID",     "@Fitness_start_pro")
BACKUP_CHAT_ID = int(os.environ.get("BACKUP_CHAT_ID", "447671579"))

CARD_NUMBER    = os.environ.get("CARD_NUMBER",    "0000 0000 0000 0000")  # вкажіть номер картки
CARD_OWNER     = os.environ.get("CARD_OWNER",     "Іванenko І.І.")        # вкажіть ПІБ отримувача
WORKOUT_PRICE  = int(os.environ.get("WORKOUT_PRICE", "150"))

TIMEZONE = ZoneInfo("Europe/Kyiv")

LOCAL_PAYMENTS_FILE = "fit_payments.json"
LOCAL_WORKOUTS_FILE = "fit_workouts.json"
PAYMENTS_FID_FILE   = ".fit_pay_fid"
WORKOUTS_FID_FILE   = ".fit_wrk_fid"

# ═══════════════════════════════════════════════════════════════
#  ЛОГУВАННЯ
# ═══════════════════════════════════════════════════════════════

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  БАЗОВИЙ МЕНЕДЖЕР (спільна логіка зберігання через Telegram)
# ═══════════════════════════════════════════════════════════════

class TelegramStorageManager:
    """
    Зберігає JSON-дані через Telegram pinned message як хмарний бекап.

    Схема (точно як у оригінальному боті):
      save()  → локальний JSON (миттєво) + Telegram document (фоновий thread)
      load()  → local file_id → pinned message → локальний JSON (резерв)
    """

    def __init__(self, local_file: str, fid_file: str, caption_marker: str):
        self.local_file     = local_file
        self.fid_file       = fid_file
        self.caption_marker = caption_marker  # унікальний маркер у caption

    # ── низькорівневі HTTP-методи (синхронні, для thread) ──

    def _tg_post(self, method: str, **kwargs) -> dict | None:
        try:
            resp = req.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/{method}",
                timeout=30, **kwargs
            )
            r = resp.json()
            if r.get("ok"):
                return r["result"]
            logger.error(f"TG {method}: {r.get('description')}")
        except Exception as e:
            logger.error(f"TG {method} exception: {e}")
        return None

    def _tg_get(self, method: str, **kwargs) -> dict | None:
        try:
            resp = req.get(
                f"https://api.telegram.org/bot{BOT_TOKEN}/{method}",
                timeout=30, **kwargs
            )
            r = resp.json()
            if r.get("ok"):
                return r["result"]
            logger.error(f"TG {method}: {r.get('description')}")
        except Exception as e:
            logger.error(f"TG {method} exception: {e}")
        return None

    # ── робота з локальним file_id ──

    def _save_fid(self, file_id: str):
        try:
            with open(self.fid_file, "w") as f:
                f.write(file_id)
        except Exception:
            pass

    def _load_fid(self) -> str | None:
        try:
            if os.path.exists(self.fid_file):
                v = open(self.fid_file).read().strip()
                return v or None
        except Exception:
            pass
        return None

    # ── pinned message ──

    def _get_pinned_file_id(self) -> str | None:
        result = self._tg_get("getChat", params={"chat_id": BACKUP_CHAT_ID})
        if not result:
            return None
        pinned = result.get("pinned_message")
        if not pinned:
            return None
        doc = pinned.get("document")
        if not doc:
            return None
        if self.caption_marker not in pinned.get("caption", ""):
            return None
        logger.info(f"[{self.caption_marker}] pinned file_id знайдено")
        return doc["file_id"]

    def _upload_and_pin(self, data: dict):
        """Синхронно: надсилає JSON адміну і пінує повідомлення."""
        buf = io.BytesIO(json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8"))
        fname = os.path.basename(self.local_file)
        result = self._tg_post(
            "sendDocument",
            data={"chat_id": BACKUP_CHAT_ID, "caption": self.caption_marker},
            files={"document": (fname, buf, "application/json")},
        )
        if not result:
            return
        file_id = result["document"]["file_id"]
        msg_id  = result["message_id"]
        self._save_fid(file_id)
        logger.info(f"[{self.caption_marker}] → Telegram OK (msg_id={msg_id})")

        pin = self._tg_post(
            "pinChatMessage",
            data={"chat_id": BACKUP_CHAT_ID, "message_id": msg_id, "disable_notification": True},
        )
        if pin is None:
            logger.warning(f"[{self.caption_marker}] Pin не вдався — file_id збережено локально")

    def _download_by_fid(self, file_id: str) -> dict | None:
        r1 = self._tg_get("getFile", params={"file_id": file_id})
        if not r1:
            return None
        try:
            r2 = req.get(
                f"https://api.telegram.org/file/bot{BOT_TOKEN}/{r1['file_path']}",
                timeout=30
            )
            if r2.ok:
                return r2.json()
        except Exception as e:
            logger.error(f"Download [{self.caption_marker}]: {e}")
        return None

    # ── публічні методи ──

    def load_raw(self) -> dict | None:
        """Завантажує дані при старті. Повертає dict або None."""
        data = None

        # 1. Локальний file_id (найшвидше після рестарту)
        fid = self._load_fid()
        if fid:
            data = self._download_by_fid(fid)

        # 2. Pinned message (хмарний fallback)
        if data is None:
            fid = self._get_pinned_file_id()
            if fid:
                data = self._download_by_fid(fid)
                if data:
                    self._save_fid(fid)

        # 3. Локальний JSON (останній резерв)
        if data is None and os.path.exists(self.local_file):
            try:
                with open(self.local_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                logger.info(f"[{self.caption_marker}] ← локальний файл (резерв)")
            except Exception as e:
                logger.error(f"Локальний файл [{self.caption_marker}]: {e}")

        return data

    def save_raw(self, data: dict):
        """
        Зберігає дані.
        Локально — миттєво (синхронно).
        Telegram — у фоновому thread (не блокує event loop).
        """
        # 1. Локально — миттєво
        try:
            with open(self.local_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Локальне збереження [{self.caption_marker}]: {e}")

        # 2. Telegram — у фоні
        data_copy = dict(data)
        threading.Thread(
            target=self._upload_and_pin,
            args=(data_copy,),
            daemon=True,
        ).start()


# ═══════════════════════════════════════════════════════════════
#  МЕНЕДЖЕР ПЛАТЕЖІВ
# ═══════════════════════════════════════════════════════════════

class PaymentManager:
    """
    Структура payments (dict у пам'яті):
    {
      "user_id_str": {
        "username":   "@...",
        "full_name":  "...",
        "pending": {                     # поточний очікуючий платіж або null
          "payment_id": 1,
          "workout_id": 3
        } | null,
        "paid_workouts": [3, 7, 12],    # id тренувань, за які підтверджено оплату
        "created_at": "2025-01-01T..."
      }
    }
    """

    def __init__(self):
        self.payments: dict = {}
        self._next_payment_id: int = 1
        self._storage = TelegramStorageManager(
            local_file=LOCAL_PAYMENTS_FILE,
            fid_file=PAYMENTS_FID_FILE,
            caption_marker="fit_payments_backup"
        )

    def load(self):
        raw = self._storage.load_raw()
        if raw:
            self.payments         = raw.get("payments", {})
            self._next_payment_id = raw.get("next_payment_id", 1)
        logger.info(f"Клієнтів завантажено: {len(self.payments)}")

    def save(self):
        self._storage.save_raw({
            "payments":         self.payments,
            "next_payment_id":  self._next_payment_id,
        })

    def _key(self, user_id: int) -> str:
        return str(user_id)

    def upsert_client(self, user_id: int, username: str, full_name: str):
        key = self._key(user_id)
        if key not in self.payments:
            self.payments[key] = {
                "username":    username or "",
                "full_name":   full_name or "",
                "pending":     None,
                "paid_workouts": [],
                "created_at":  datetime.now(TIMEZONE).isoformat(),
            }
        else:
            self.payments[key]["username"]  = username or ""
            self.payments[key]["full_name"] = full_name or ""
        self.save()

    def set_pending(self, user_id: int, workout_id: int) -> int:
        """Створює pending-платіж, повертає payment_id."""
        key = self._key(user_id)
        pid = self._next_payment_id
        self._next_payment_id += 1
        if key not in self.payments:
            self.payments[key] = {
                "username": "", "full_name": "",
                "paid_workouts": [], "created_at": datetime.now(TIMEZONE).isoformat()
            }
        self.payments[key]["pending"] = {"payment_id": pid, "workout_id": workout_id}
        self.save()
        return pid

    def get_pending(self, user_id: int) -> dict | None:
        return self.payments.get(self._key(user_id), {}).get("pending")

    def clear_pending(self, user_id: int):
        key = self._key(user_id)
        if key in self.payments:
            self.payments[key]["pending"] = None
            self.save()

    def approve(self, user_id: int):
        """Підтверджує pending-платіж: переносить workout_id у paid_workouts."""
        key = self._key(user_id)
        rec = self.payments.get(key, {})
        pending = rec.get("pending")
        if pending:
            wid = pending.get("workout_id")
            if wid and wid not in rec.setdefault("paid_workouts", []):
                rec["paid_workouts"].append(wid)
            rec["pending"] = None
            self.payments[key] = rec
            self.save()

    def has_paid(self, user_id: int, workout_id: int) -> bool:
        return workout_id in self.payments.get(self._key(user_id), {}).get("paid_workouts", [])

    def get_paid_workout_ids(self, workout_id: int) -> list[int]:
        """Повертає список user_id, які оплатили це тренування."""
        result = []
        for uid_str, rec in self.payments.items():
            if workout_id in rec.get("paid_workouts", []):
                result.append(int(uid_str))
        return result

    def all_client_ids(self) -> list[int]:
        return [int(k) for k in self.payments.keys()]

    def get_client_info(self, user_id: int) -> dict:
        return self.payments.get(self._key(user_id), {})


# ═══════════════════════════════════════════════════════════════
#  МЕНЕДЖЕР ТРЕНУВАНЬ
# ═══════════════════════════════════════════════════════════════

class WorkoutManager:
    """
    Структура workouts (dict у пам'яті):
    {
      "workouts": [
        {
          "id": 1,
          "title": "Кардіо",
          "datetime": "2025-06-15T18:00:00+03:00",
          "teams_link": "https://teams.microsoft.com/...",
          "channel_msg_id": 123,
          "notified_1h": false,
          "notified_start": false
        }
      ],
      "next_id": 2
    }
    """

    def __init__(self):
        self.workouts: list[dict] = []
        self._next_id: int = 1
        self._storage = TelegramStorageManager(
            local_file=LOCAL_WORKOUTS_FILE,
            fid_file=WORKOUTS_FID_FILE,
            caption_marker="fit_workouts_backup"
        )

    def load(self):
        raw = self._storage.load_raw()
        if raw:
            self.workouts  = raw.get("workouts", [])
            self._next_id  = raw.get("next_id", 1)
        logger.info(f"Тренувань завантажено: {len(self.workouts)}")

    def save(self):
        self._storage.save_raw({
            "workouts": self.workouts,
            "next_id":  self._next_id,
        })

    def add(self, title: str, dt: datetime, teams_link: str) -> dict:
        w = {
            "id":              self._next_id,
            "title":           title,
            "datetime":        dt.isoformat(),
            "teams_link":      teams_link,
            "channel_msg_id":  None,
            "notified_1h":     False,
            "notified_start":  False,
        }
        self.workouts.append(w)
        self._next_id += 1
        self.save()
        return w

    def set_channel_msg(self, workout_id: int, msg_id: int):
        for w in self.workouts:
            if w["id"] == workout_id:
                w["channel_msg_id"] = msg_id
                break
        self.save()

    def delete(self, workout_id: int):
        self.workouts = [w for w in self.workouts if w["id"] != workout_id]
        self.save()

    def get(self, workout_id: int) -> dict | None:
        return next((w for w in self.workouts if w["id"] == workout_id), None)

    def upcoming(self) -> list[dict]:
        now = datetime.now(TIMEZONE).isoformat()
        return sorted(
            [w for w in self.workouts if w["datetime"] > now],
            key=lambda x: x["datetime"]
        )

    def get_pending_notifications(self) -> list[tuple[dict, str]]:
        now     = datetime.now(TIMEZONE)
        result  = []
        changed = False
        for w in self.workouts:
            dt = datetime.fromisoformat(w["datetime"])
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=TIMEZONE)
            diff = (dt - now).total_seconds()
            if not w["notified_1h"] and 3300 <= diff <= 3900:
                result.append((w, "1h"))
                w["notified_1h"] = True
                changed = True
            if not w["notified_start"] and -300 <= diff <= 300:
                result.append((w, "start"))
                w["notified_start"] = True
                changed = True
        if changed:
            self.save()
        return result

    def count_paid(self, workout_id: int) -> int:
        return len(pay.get_paid_workout_ids(workout_id))


# ═══════════════════════════════════════════════════════════════
#  ГЛОБАЛЬНІ ЕКЗЕМПЛЯРИ
# ═══════════════════════════════════════════════════════════════

pay = PaymentManager()
wm  = WorkoutManager()


# ═══════════════════════════════════════════════════════════════
#  ДОПОМІЖНІ ФУНКЦІЇ
# ═══════════════════════════════════════════════════════════════

def _is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID


def _fmt_dt(w: dict) -> str:
    dt = datetime.fromisoformat(w["datetime"])
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TIMEZONE)
    return dt.strftime("%d.%m.%Y о %H:%M")


# ═══════════════════════════════════════════════════════════════
#  /start — ГОЛОВНЕ МЕНЮ
# ═══════════════════════════════════════════════════════════════

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    pay.upsert_client(user.id, user.username or "", user.full_name or "")
    context.user_data.clear()

    kb = [
        [InlineKeyboardButton("🗓 Розклад тренувань",   callback_data="schedule")],
        [InlineKeyboardButton("💳 Оплатити тренування", callback_data="pay_start")],
        [InlineKeyboardButton("👤 Мій статус",           callback_data="my_status")],
    ]
    if _is_admin(user.id):
        kb.append([InlineKeyboardButton("⚙️ Адмін-панель", callback_data="admin_panel")])

    await update.message.reply_text(
        f"👋 Привіт, <b>{user.first_name}</b>!\n\n"
        "Це бот онлайн-тренувань. Тут можна:\n"
        "• переглянути розклад\n"
        "• оплатити тренування та отримати посилання на Microsoft Teams\n\n"
        "Обери дію:",
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode="HTML"
    )


async def _edit_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user  = update.effective_user
    kb = [
        [InlineKeyboardButton("🗓 Розклад тренувань",   callback_data="schedule")],
        [InlineKeyboardButton("💳 Оплатити тренування", callback_data="pay_start")],
        [InlineKeyboardButton("👤 Мій статус",           callback_data="my_status")],
    ]
    if _is_admin(user.id):
        kb.append([InlineKeyboardButton("⚙️ Адмін-панель", callback_data="admin_panel")])
    await query.edit_message_text(
        "🏠 <b>Головне меню</b>\n\nОбери дію:",
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode="HTML"
    )


# ═══════════════════════════════════════════════════════════════
#  РОЗКЛАД
# ═══════════════════════════════════════════════════════════════

async def show_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query    = update.callback_query
    upcoming = wm.upcoming()

    if not upcoming:
        await query.edit_message_text(
            "🗓 <b>Розклад тренувань</b>\n\nНаразі тренувань немає. Стежте за каналом! 💪",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🏠 Головне меню", callback_data="main_menu")
            ]]),
            parse_mode="HTML"
        )
        return

    lines = ["🗓 <b>Найближчі тренування:</b>\n"]
    for w in upcoming[:5]:
        count = wm.count_paid(w["id"])
        lines.append(
            f"• <b>{w['title']}</b>\n"
            f"  📅 {_fmt_dt(w)}  👥 {count} оплачено"
        )
    lines.append("\n<i>Для отримання посилання на Teams — оплатіть тренування.</i>")

    await query.edit_message_text(
        "\n\n".join(lines),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("💳 Оплатити тренування", callback_data="pay_start")],
            [InlineKeyboardButton("🏠 Головне меню",        callback_data="main_menu")],
        ]),
        parse_mode="HTML"
    )


# ═══════════════════════════════════════════════════════════════
#  ОПЛАТА
# ═══════════════════════════════════════════════════════════════

async def pay_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query    = update.callback_query
    user     = update.effective_user
    upcoming = wm.upcoming()
    available = [w for w in upcoming if not pay.has_paid(user.id, w["id"])]

    if not upcoming:
        msg = "😔 Наразі немає запланованих тренувань. Слідкуйте за каналом!"
    elif not available:
        msg = "✅ Ви вже оплатили всі заплановані тренування!\n\nПосилання надійдуть перед початком."

    if not upcoming or not available:
        await query.edit_message_text(
            msg,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🏠 Головне меню", callback_data="main_menu")
            ]])
        )
        return

    kb = []
    for w in available:
        kb.append([InlineKeyboardButton(
            f"{w['title']} — {_fmt_dt(w)}",
            callback_data=f"pay_select_{w['id']}"
        )])
    kb.append([InlineKeyboardButton("🏠 Головне меню", callback_data="main_menu")])

    await query.edit_message_text(
        f"💳 <b>Оплата тренування</b>\n\nВартість: <b>{WORKOUT_PRICE} грн</b>\n\nОберіть тренування:",
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode="HTML"
    )


async def pay_show_details(update: Update, context: ContextTypes.DEFAULT_TYPE, workout_id: int):
    query   = update.callback_query
    user    = update.effective_user
    workout = wm.get(workout_id)

    if not workout:
        await query.answer("Тренування не знайдено", show_alert=True)
        return

    payment_id = pay.set_pending(user.id, workout_id)
    context.user_data["waiting_screenshot"] = True
    context.user_data["payment_id"]         = payment_id
    context.user_data["workout_id"]         = workout_id

    await query.edit_message_text(
        f"💳 <b>Оплата тренування</b>\n\n"
        f"🏋️ {workout['title']}\n"
        f"📅 {_fmt_dt(workout)}\n\n"
        f"Сума: <b>{WORKOUT_PRICE} грн</b>\n\n"
        f"Переказати на картку:\n"
        f"<code>{CARD_NUMBER}</code>\n"
        f"Отримувач: <b>{CARD_OWNER}</b>\n\n"
        f"Після переказу надішліть сюди 📸 <b>скріншот підтвердження</b>.\n\n"
        f"<i>⏱ Підтвердження займає до 1–3 годин.</i>",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("◀️ Інше тренування", callback_data="pay_start"),
             InlineKeyboardButton("🏠 Головне меню",    callback_data="main_menu")],
        ]),
        parse_mode="HTML"
    )


# ═══════════════════════════════════════════════════════════════
#  МІЙ СТАТУС
# ═══════════════════════════════════════════════════════════════

async def show_my_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query    = update.callback_query
    user     = update.effective_user
    upcoming = wm.upcoming()

    lines = [f"👤 <b>Статус: {user.full_name}</b>\n"]
    if not upcoming:
        lines.append("Запланованих тренувань немає.")
    else:
        lines.append("<b>Тренування:</b>")
        for w in upcoming[:5]:
            icon = "✅" if pay.has_paid(user.id, w["id"]) else "❌"
            lines.append(f"{icon} {w['title']} ({_fmt_dt(w)})")

    await query.edit_message_text(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("💳 Оплатити тренування", callback_data="pay_start")],
            [InlineKeyboardButton("🏠 Головне меню",        callback_data="main_menu")],
        ]),
        parse_mode="HTML"
    )


# ═══════════════════════════════════════════════════════════════
#  ОБРОБНИК ФОТО (скріншот оплати)
# ═══════════════════════════════════════════════════════════════

async def photo_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user

    if not context.user_data.get("waiting_screenshot"):
        return

    payment_id = context.user_data.get("payment_id")
    workout_id = context.user_data.get("workout_id")

    if not payment_id:
        await update.message.reply_text("⚠️ Не знайдено активний платіж. Спробуйте /start")
        return

    workout = wm.get(workout_id) if workout_id else None
    photo   = update.message.photo[-1]
    context.user_data["waiting_screenshot"] = False

    # Підтвердження клієнту
    await update.message.reply_text(
        "✅ <b>Скріншот отримано!</b>\n\n"
        "Платіж передано адміністратору на перевірку.\n"
        "Зазвичай це займає до 1–3 годин.\n\n"
        "Як тільки оплату підтвердять — ви отримаєте посилання на тренування 🙏",
        parse_mode="HTML"
    )

    # Пересилаємо адміну
    workout_name = workout["title"] if workout else "—"
    dt_str       = _fmt_dt(workout) if workout else "—"
    caption = (
        f"💳 <b>Новий платіж!</b>\n\n"
        f"👤 {user.full_name}"
        f"{' (@' + user.username + ')' if user.username else ''}\n"
        f"🆔 <code>{user.id}</code>\n"
        f"🏋️ {workout_name}\n"
        f"📅 {dt_str}\n"
        f"💰 {WORKOUT_PRICE} грн\n"
        f"🕐 {datetime.now(TIMEZONE).strftime('%d.%m.%Y %H:%M')}\n"
        f"#pay{payment_id}"
    )
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton(
            "✅ Підтвердити",
            callback_data=f"adm_ok_{payment_id}_{user.id}_{workout_id or 0}"
        ),
        InlineKeyboardButton(
            "❌ Відхилити",
            callback_data=f"adm_no_{payment_id}_{user.id}"
        ),
    ]])
    await context.bot.send_photo(
        chat_id=ADMIN_ID,
        photo=photo.file_id,
        caption=caption,
        reply_markup=kb,
        parse_mode="HTML"
    )


# ═══════════════════════════════════════════════════════════════
#  АДМІН-ПАНЕЛЬ
# ═══════════════════════════════════════════════════════════════

async def show_admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user  = update.effective_user
    if not _is_admin(user.id):
        await query.answer("⛔ Немає доступу", show_alert=True)
        return

    upcoming = wm.upcoming()
    total    = len(pay.all_client_ids())

    await query.edit_message_text(
        f"⚙️ <b>Адмін-панель</b>\n\n"
        f"👥 Клієнтів у боті: {total}\n"
        f"🗓 Тренувань заплановано: {len(upcoming)}\n\n"
        "Оберіть дію:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Додати тренування", callback_data="adm_add")],
            [InlineKeyboardButton("📋 Список тренувань",  callback_data="adm_list")],
            [InlineKeyboardButton("📢 Розіслати всім",    callback_data="adm_bcast")],
            [InlineKeyboardButton("🏠 Головне меню",      callback_data="main_menu")],
        ]),
        parse_mode="HTML"
    )


async def adm_add_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not _is_admin(update.effective_user.id):
        await query.answer("⛔", show_alert=True)
        return
    context.user_data["adm_state"] = "title"
    await query.edit_message_text(
        "➕ <b>Нове тренування</b> — крок 1/3\n\n"
        "Введіть <b>назву тренування</b>:\n"
        "<i>Наприклад: Кардіо для початківців</i>",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("❌ Скасувати", callback_data="admin_panel")
        ]]),
        parse_mode="HTML"
    )


async def adm_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not _is_admin(update.effective_user.id):
        await query.answer("⛔", show_alert=True)
        return

    upcoming = wm.upcoming()
    if not upcoming:
        await query.edit_message_text(
            "📋 Тренувань немає.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ Додати",  callback_data="adm_add")],
                [InlineKeyboardButton("◀️ Назад",   callback_data="admin_panel")],
            ])
        )
        return

    lines = ["📋 <b>Заплановані тренування:</b>\n"]
    kb    = []
    for w in upcoming:
        cnt = wm.count_paid(w["id"])
        lines.append(f"#{w['id']} <b>{w['title']}</b> — {_fmt_dt(w)} | 👥{cnt}")
        kb.append([InlineKeyboardButton(
            f"🗑 Видалити #{w['id']} {w['title'][:20]}",
            callback_data=f"adm_del_{w['id']}"
        )])
    kb.append([InlineKeyboardButton("◀️ Назад", callback_data="admin_panel")])

    await query.edit_message_text(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode="HTML"
    )


async def adm_bcast_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not _is_admin(update.effective_user.id):
        await query.answer("⛔", show_alert=True)
        return
    context.user_data["adm_state"] = "bcast"
    await query.edit_message_text(
        "📢 <b>Розсилка</b>\n\n"
        "Введіть текст — він піде <b>всім клієнтам</b> у боті.\n"
        "<i>HTML підтримується: &lt;b&gt;, &lt;i&gt;</i>",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("❌ Скасувати", callback_data="admin_panel")
        ]]),
        parse_mode="HTML"
    )


# ═══════════════════════════════════════════════════════════════
#  ОБРОБНИК ТЕКСТУ (покрокові діалоги для адміна)
# ═══════════════════════════════════════════════════════════════

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user  = update.effective_user
    text  = update.message.text.strip()
    state = context.user_data.get("adm_state")

    if context.user_data.get("waiting_screenshot"):
        await update.message.reply_text(
            "📸 Надішліть <b>фото</b> (скріншот) підтвердження, а не текст.",
            parse_mode="HTML"
        )
        return

    if not _is_admin(user.id) or not state:
        return

    if state == "title":
        context.user_data["new_title"] = text
        context.user_data["adm_state"] = "dt"
        await update.message.reply_text(
            f"✅ Назва: <b>{text}</b>\n\n"
            "Крок 2/3 — введіть <b>дату та час</b>:\n"
            "<code>ДД.ММ.РРРР ГГ:ХХ</code>  Наприклад: <code>20.06.2025 18:00</code>",
            parse_mode="HTML"
        )
        return

    if state == "dt":
        try:
            dt = datetime.strptime(text, "%d.%m.%Y %H:%M").replace(tzinfo=TIMEZONE)
        except ValueError:
            await update.message.reply_text(
                "⚠️ Невірний формат. Приклад: <code>20.06.2025 18:00</code>",
                parse_mode="HTML"
            )
            return
        context.user_data["new_dt"]    = dt.isoformat()
        context.user_data["adm_state"] = "link"
        await update.message.reply_text(
            f"✅ Дата: <b>{dt.strftime('%d.%m.%Y о %H:%M')}</b>\n\n"
            "Крок 3/3 — введіть <b>посилання Microsoft Teams</b>:",
            parse_mode="HTML"
        )
        return

    if state == "link":
        if not text.startswith("http"):
            await update.message.reply_text("⚠️ Посилання має починатися з https://")
            return

        title   = context.user_data.pop("new_title", "Тренування")
        dt      = datetime.fromisoformat(context.user_data.pop("new_dt"))
        workout = wm.add(title, dt, text)
        context.user_data["adm_state"] = None

        # Публікуємо анонс у канал
        bot_me = await context.bot.get_me()
        channel_text = (
            f"🏋️ <b>Онлайн-тренування!</b>\n\n"
            f"📌 {title}\n"
            f"📅 {dt.strftime('%d.%m.%Y о %H:%M')}\n\n"
            f"💳 Вартість: <b>{WORKOUT_PRICE} грн</b>\n\n"
            f"👉 Пишіть боту @{bot_me.username} — оплата та посилання на Microsoft Teams"
        )
        channel_note = ""
        try:
            msg = await context.bot.send_message(
                chat_id=CHANNEL_ID, text=channel_text, parse_mode="HTML"
            )
            wm.set_channel_msg(workout["id"], msg.message_id)
            channel_note = f"✅ Анонс опубліковано у {CHANNEL_ID}"
        except Exception as e:
            logger.error(f"Канал: {e}")
            channel_note = f"⚠️ Не вдалося опублікувати у канал:\n{e}"

        await update.message.reply_text(
            f"✅ <b>Тренування додано!</b>\n\n"
            f"🏋️ {title}\n"
            f"📅 {dt.strftime('%d.%m.%Y о %H:%M')}\n\n"
            f"{channel_note}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📋 Список тренувань", callback_data="adm_list")],
                [InlineKeyboardButton("⚙️ Адмін-панель",    callback_data="admin_panel")],
            ]),
            parse_mode="HTML"
        )
        return

    if state == "bcast":
        context.user_data["adm_state"] = None
        all_ids = pay.all_client_ids()
        sent = failed = 0
        for uid in all_ids:
            try:
                await context.bot.send_message(
                    chat_id=uid,
                    text=f"📢 <b>Від тренера:</b>\n\n{text}",
                    parse_mode="HTML"
                )
                sent += 1
                await asyncio.sleep(0.05)
            except Exception:
                failed += 1
        await update.message.reply_text(
            f"📢 <b>Розсилку завершено</b>\n✅ {sent} надіслано  ❌ {failed} помилок",
            parse_mode="HTML"
        )


# ═══════════════════════════════════════════════════════════════
#  КОМАНДА /add_workout
# ═══════════════════════════════════════════════════════════════

async def cmd_add_workout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update.effective_user.id):
        return
    context.user_data["adm_state"] = "title"
    await update.message.reply_text(
        "➕ <b>Нове тренування</b> — крок 1/3\n\nВведіть <b>назву тренування</b>:",
        parse_mode="HTML"
    )


# ═══════════════════════════════════════════════════════════════
#  ГОЛОВНИЙ ОБРОБНИК КНОПОК
# ═══════════════════════════════════════════════════════════════

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user = update.effective_user

    # ── Навігація ──
    if data == "main_menu":
        await _edit_main_menu(update, context)
        return
    if data == "schedule":
        await show_schedule(update, context)
        return
    if data == "pay_start":
        await pay_start(update, context)
        return
    if data == "my_status":
        await show_my_status(update, context)
        return
    if data.startswith("pay_select_"):
        await pay_show_details(update, context, int(data[11:]))
        return

    # ── Адмін: підтвердити оплату ──
    if data.startswith("adm_ok_"):
        if not _is_admin(user.id):
            await query.answer("⛔", show_alert=True)
            return
        _, _, pid, uid, wid = data.split("_", 4)
        payment_id = int(pid)
        client_uid = int(uid)
        workout_id = int(wid) or None

        pay.approve(client_uid)

        try:
            await query.edit_message_caption(
                caption=(query.message.caption or "") + "\n\n✅ <b>ПІДТВЕРДЖЕНО</b>",
                parse_mode="HTML"
            )
        except Exception:
            pass

        workout = wm.get(workout_id) if workout_id else None
        if workout:
            client_text = (
                f"🎉 <b>Оплату підтверджено!</b>\n\n"
                f"🏋️ <b>{workout['title']}</b>\n"
                f"📅 {_fmt_dt(workout)}\n\n"
                f"🔗 <b>Посилання Microsoft Teams:</b>\n"
                f"{workout['teams_link']}\n\n"
                f"<i>До зустрічі на тренуванні! 💪</i>"
            )
        else:
            client_text = "🎉 <b>Оплату підтверджено!</b>\n\nПосилання надійде найближчим часом."

        await context.bot.send_message(
            chat_id=client_uid,
            text=client_text,
            parse_mode="HTML",
            disable_web_page_preview=False
        )
        return

    # ── Адмін: відхилити оплату ──
    if data.startswith("adm_no_"):
        if not _is_admin(user.id):
            await query.answer("⛔", show_alert=True)
            return
        _, _, pid, uid = data.split("_", 3)
        payment_id = int(pid)
        client_uid = int(uid)

        pay.clear_pending(client_uid)

        try:
            await query.edit_message_caption(
                caption=(query.message.caption or "") + "\n\n❌ <b>ВІДХИЛЕНО</b>",
                parse_mode="HTML"
            )
        except Exception:
            pass

        await context.bot.send_message(
            chat_id=client_uid,
            text=(
                "❌ <b>Платіж не підтверджено.</b>\n\n"
                "Можливі причини: скріншот нечіткий, сума не збігається "
                "або переказ ще не надійшов.\n\n"
                "Спробуйте ще раз або зверніться до тренера."
            ),
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("💳 Спробувати ще раз", callback_data="pay_start")
            ]]),
            parse_mode="HTML"
        )
        return

    # ── Адмін-панель ──
    if data == "admin_panel":
        if _is_admin(user.id):
            await show_admin_panel(update, context)
        return
    if data == "adm_add":
        await adm_add_start(update, context)
        return
    if data == "adm_list":
        await adm_list(update, context)
        return
    if data == "adm_bcast":
        await adm_bcast_start(update, context)
        return
    if data.startswith("adm_del_"):
        if not _is_admin(user.id):
            await query.answer("⛔", show_alert=True)
            return
        wm.delete(int(data[8:]))
        await query.answer("✅ Видалено")
        await adm_list(update, context)
        return


# ═══════════════════════════════════════════════════════════════
#  ФОНОВИЙ ЦИКЛ НАГАДУВАНЬ
# ═══════════════════════════════════════════════════════════════

async def notification_loop(app: Application):
    """
    Щохвилини перевіряє тренування і надсилає нагадування
    оплаченим клієнтам: за 1 год та при старті.
    """
    while True:
        try:
            for workout, ntype in wm.get_pending_notifications():
                client_ids = pay.get_paid_workout_ids(workout["id"])
                if not client_ids:
                    continue

                dt = datetime.fromisoformat(workout["datetime"])
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=TIMEZONE)

                if ntype == "1h":
                    text = (
                        f"⏰ <b>Тренування через 1 годину!</b>\n\n"
                        f"🏋️ <b>{workout['title']}</b>\n"
                        f"📅 {dt.strftime('%d.%m.%Y о %H:%M')}\n\n"
                        f"🔗 <a href=\"{workout['teams_link']}\">Посилання Microsoft Teams</a>\n\n"
                        f"Готуйтеся! 💪"
                    )
                else:
                    text = (
                        f"🚀 <b>Тренування починається!</b>\n\n"
                        f"🏋️ <b>{workout['title']}</b>\n\n"
                        f"🔗 <a href=\"{workout['teams_link']}\">Підключитись зараз!</a>"
                    )

                sent = 0
                for uid in client_ids:
                    try:
                        await app.bot.send_message(
                            chat_id=uid,
                            text=text,
                            parse_mode="HTML",
                            disable_web_page_preview=False
                        )
                        sent += 1
                        await asyncio.sleep(0.05)
                    except Exception as e:
                        logger.warning(f"notify → {uid}: {e}")

                logger.info(f"Нагадування '{ntype}' | '{workout['title']}' → {sent} клієнтів")

        except Exception as e:
            logger.error(f"notification_loop: {e}")

        await asyncio.sleep(60)


# ═══════════════════════════════════════════════════════════════
#  ЗАПУСК
# ═══════════════════════════════════════════════════════════════

def main():
    pay.load()
    wm.load()

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start",       start))
    app.add_handler(CommandHandler("add_workout", cmd_add_workout))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.PHOTO,                   photo_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))

    async def post_init(application: Application):
        asyncio.create_task(notification_loop(application))

    app.post_init = post_init

    WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "")
    PORT        = int(os.environ.get("PORT", 8000))

    if WEBHOOK_URL:
        logger.info(f"Webhook: {WEBHOOK_URL}")
        app.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            webhook_url=WEBHOOK_URL,
            allowed_updates=["message", "callback_query"],
            drop_pending_updates=True,
        )
    else:
        logger.info("Polling (локальний режим)...")
        app.run_polling(
            allowed_updates=["message", "callback_query"],
            drop_pending_updates=True,
        )


if __name__ == "__main__":
    main()
