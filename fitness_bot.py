"""
Телеграм-бот: Онлайн-тренування (фітнес-канал)
================================================

Концепція:
  • Головне меню: Групові тренування | Персональне тренування | Мій статус
  • Групові: розклад + оплата (150 грн)
  • Персональні: календар з доступними слотами (500 грн), 1-на-1
  • Оплата: скріншот → адмін підтверджує → Teams-лінк
  • Нагадування за 1 год та при старті (оплаченим клієнтам)

Зберігання даних — dict у пам'яті + JSON + Telegram pinned message (бекап).
"""

import os
import io
import json
import logging
import asyncio
import threading
import calendar
import requests as req
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from http.server import HTTPServer, BaseHTTPRequestHandler

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes
)

# ═══════════════════════════════════════════════════════════════
#  НАЛАШТУВАННЯ
# ═══════════════════════════════════════════════════════════════

BOT_TOKEN      = os.environ.get("BOT_TOKEN",      "8623568533:AAEC0nfGONryCrcLQ86njVwGQMt0f1erZD4")
ADMIN_ID       = int(os.environ.get("ADMIN_ID",   "447671579"))
CHANNEL_ID     = os.environ.get("CHANNEL_ID",     "@Fitness_start_pro")
BACKUP_CHAT_ID = int(os.environ.get("BACKUP_CHAT_ID", "447671579"))

CARD_NUMBER    = os.environ.get("CARD_NUMBER",    "0000 0000 0000 0000")
CARD_OWNER     = os.environ.get("CARD_OWNER",     "Іванenko І.І.")
GROUP_PRICE    = int(os.environ.get("GROUP_PRICE",    "150"))
PERSONAL_PRICE = int(os.environ.get("PERSONAL_PRICE", "500"))

TIMEZONE = ZoneInfo("Europe/Kyiv")

LOCAL_PAYMENTS_FILE = "fit_payments.json"
LOCAL_WORKOUTS_FILE = "fit_workouts.json"
LOCAL_PERSONAL_FILE = "fit_personal.json"
PAYMENTS_FID_FILE   = ".fit_pay_fid"
WORKOUTS_FID_FILE   = ".fit_wrk_fid"
PERSONAL_FID_FILE   = ".fit_per_fid"

# ═══════════════════════════════════════════════════════════════
#  ЛОГУВАННЯ
# ═══════════════════════════════════════════════════════════════

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  БАЗОВИЙ МЕНЕДЖЕР (Telegram-storage)
# ═══════════════════════════════════════════════════════════════

class TelegramStorageManager:
    def __init__(self, local_file: str, fid_file: str, caption_marker: str):
        self.local_file     = local_file
        self.fid_file       = fid_file
        self.caption_marker = caption_marker

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
        self._tg_post(
            "pinChatMessage",
            data={"chat_id": BACKUP_CHAT_ID, "message_id": msg_id, "disable_notification": True},
        )

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

    def load_raw(self) -> dict | None:
        data = None
        fid = self._load_fid()
        if fid:
            data = self._download_by_fid(fid)
        if data is None:
            fid = self._get_pinned_file_id()
            if fid:
                data = self._download_by_fid(fid)
                if data:
                    self._save_fid(fid)
        if data is None and os.path.exists(self.local_file):
            try:
                with open(self.local_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                logger.info(f"[{self.caption_marker}] ← локальний файл (резерв)")
            except Exception as e:
                logger.error(f"Локальний файл [{self.caption_marker}]: {e}")
        return data

    def save_raw(self, data: dict):
        try:
            with open(self.local_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Локальне збереження [{self.caption_marker}]: {e}")
        data_copy = dict(data)
        threading.Thread(target=self._upload_and_pin, args=(data_copy,), daemon=True).start()


# ═══════════════════════════════════════════════════════════════
#  МЕНЕДЖЕР ПЛАТЕЖІВ
# ═══════════════════════════════════════════════════════════════

class PaymentManager:
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
                "username": username or "", "full_name": full_name or "",
                "pending": None,
                "paid_workouts": [],        # групові
                "paid_personal": [],        # персональні (slot_id)
                "created_at": datetime.now(TIMEZONE).isoformat(),
            }
        else:
            self.payments[key]["username"]  = username or ""
            self.payments[key]["full_name"] = full_name or ""
            # міграція: додаємо paid_personal якщо відсутній
            self.payments[key].setdefault("paid_personal", [])
        self.save()

    def set_pending(self, user_id: int, workout_id: int = None, slot_id: str = None, pay_type: str = "group") -> int:
        key = self._key(user_id)
        pid = self._next_payment_id
        self._next_payment_id += 1
        if key not in self.payments:
            self.payments[key] = {
                "username": "", "full_name": "",
                "paid_workouts": [], "paid_personal": [],
                "created_at": datetime.now(TIMEZONE).isoformat()
            }
        self.payments[key]["pending"] = {
            "payment_id": pid,
            "type": pay_type,           # "group" або "personal"
            "workout_id": workout_id,   # для групових
            "slot_id": slot_id,         # для персональних
        }
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
        key = self._key(user_id)
        rec = self.payments.get(key, {})
        pending = rec.get("pending")
        if not pending:
            return
        ptype = pending.get("type", "group")
        if ptype == "group":
            wid = pending.get("workout_id")
            if wid and wid not in rec.setdefault("paid_workouts", []):
                rec["paid_workouts"].append(wid)
        elif ptype == "personal":
            sid = pending.get("slot_id")
            if sid and sid not in rec.setdefault("paid_personal", []):
                rec["paid_personal"].append(sid)
        rec["pending"] = None
        self.payments[key] = rec
        self.save()

    def has_paid_group(self, user_id: int, workout_id: int) -> bool:
        return workout_id in self.payments.get(self._key(user_id), {}).get("paid_workouts", [])

    def has_paid_personal(self, user_id: int, slot_id: str) -> bool:
        return slot_id in self.payments.get(self._key(user_id), {}).get("paid_personal", [])

    def get_paid_workout_ids(self, workout_id: int) -> list[int]:
        result = []
        for uid_str, rec in self.payments.items():
            if workout_id in rec.get("paid_workouts", []):
                result.append(int(uid_str))
        return result

    def get_paid_personal_users(self, slot_id: str) -> list[int]:
        result = []
        for uid_str, rec in self.payments.items():
            if slot_id in rec.get("paid_personal", []):
                result.append(int(uid_str))
        return result

    def all_client_ids(self) -> list[int]:
        return [int(k) for k in self.payments.keys()]

    def get_client_info(self, user_id: int) -> dict:
        return self.payments.get(self._key(user_id), {})


# ═══════════════════════════════════════════════════════════════
#  МЕНЕДЖЕР ГРУПОВИХ ТРЕНУВАНЬ
# ═══════════════════════════════════════════════════════════════

class WorkoutManager:
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
        logger.info(f"Групових тренувань завантажено: {len(self.workouts)}")

    def save(self):
        self._storage.save_raw({"workouts": self.workouts, "next_id": self._next_id})

    def add(self, title: str, dt: datetime, teams_link: str) -> dict:
        w = {
            "id": self._next_id, "title": title,
            "datetime": dt.isoformat(), "teams_link": teams_link,
            "channel_msg_id": None, "notified_1h": False, "notified_start": False,
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
#  МЕНЕДЖЕР ПЕРСОНАЛЬНИХ ТРЕНУВАНЬ
# ═══════════════════════════════════════════════════════════════

class PersonalManager:
    """
    Слоти персональних тренувань.
    Кожен слот: {"id": "p_3", "date": "2025-06-20", "time": "10:00",
                 "teams_link": "https://...", "booked_by": null | user_id,
                 "notified_1h": false, "notified_start": false}
    """

    def __init__(self):
        self.slots: list[dict] = []
        self._next_id: int = 1
        self._storage = TelegramStorageManager(
            local_file=LOCAL_PERSONAL_FILE,
            fid_file=PERSONAL_FID_FILE,
            caption_marker="fit_personal_backup"
        )

    def load(self):
        raw = self._storage.load_raw()
        if raw:
            self.slots     = raw.get("slots", [])
            self._next_id  = raw.get("next_id", 1)
        logger.info(f"Персональних слотів завантажено: {len(self.slots)}")

    def save(self):
        self._storage.save_raw({"slots": self.slots, "next_id": self._next_id})

    def add_slot(self, date_str: str, time_str: str, teams_link: str) -> dict:
        slot = {
            "id": f"p_{self._next_id}",
            "date": date_str,       # "2025-06-20"
            "time": time_str,       # "10:00"
            "teams_link": teams_link,
            "booked_by": None,
            "notified_1h": False,
            "notified_start": False,
        }
        self.slots.append(slot)
        self._next_id += 1
        self.save()
        return slot

    def delete_slot(self, slot_id: str):
        self.slots = [s for s in self.slots if s["id"] != slot_id]
        self.save()

    def get(self, slot_id: str) -> dict | None:
        return next((s for s in self.slots if s["id"] == slot_id), None)

    def book(self, slot_id: str, user_id: int):
        for s in self.slots:
            if s["id"] == slot_id:
                s["booked_by"] = user_id
                break
        self.save()

    def unbook(self, slot_id: str):
        for s in self.slots:
            if s["id"] == slot_id:
                s["booked_by"] = None
                break
        self.save()

    def is_booked(self, slot_id: str) -> bool:
        s = self.get(slot_id)
        return s is not None and s["booked_by"] is not None

    def available_dates_in_month(self, year: int, month: int) -> set[int]:
        """Повертає set днів місяця, де є вільні слоти."""
        days = set()
        for s in self.slots:
            if s["booked_by"] is not None:
                continue
            try:
                d = datetime.strptime(s["date"], "%Y-%m-%d")
                if d.year == year and d.month == month:
                    dt_full = datetime.strptime(f"{s['date']} {s['time']}", "%Y-%m-%d %H:%M").replace(tzinfo=TIMEZONE)
                    if dt_full > datetime.now(TIMEZONE):
                        days.add(d.day)
            except Exception:
                pass
        return days

    def slots_for_date(self, date_str: str) -> list[dict]:
        """Повертає вільні слоти на конкретну дату."""
        now = datetime.now(TIMEZONE)
        result = []
        for s in self.slots:
            if s["date"] != date_str or s["booked_by"] is not None:
                continue
            try:
                dt_full = datetime.strptime(f"{s['date']} {s['time']}", "%Y-%m-%d %H:%M").replace(tzinfo=TIMEZONE)
                if dt_full > now:
                    result.append(s)
            except Exception:
                pass
        return sorted(result, key=lambda x: x["time"])

    def all_upcoming(self) -> list[dict]:
        now = datetime.now(TIMEZONE)
        result = []
        for s in self.slots:
            try:
                dt = datetime.strptime(f"{s['date']} {s['time']}", "%Y-%m-%d %H:%M").replace(tzinfo=TIMEZONE)
                if dt > now:
                    result.append(s)
            except Exception:
                pass
        return sorted(result, key=lambda x: f"{x['date']} {x['time']}")

    def get_pending_notifications(self) -> list[tuple[dict, str]]:
        now     = datetime.now(TIMEZONE)
        result  = []
        changed = False
        for s in self.slots:
            if s["booked_by"] is None:
                continue
            # Перевіряємо чи оплачено
            if not pay.has_paid_personal(s["booked_by"], s["id"]):
                continue
            try:
                dt = datetime.strptime(f"{s['date']} {s['time']}", "%Y-%m-%d %H:%M").replace(tzinfo=TIMEZONE)
            except Exception:
                continue
            diff = (dt - now).total_seconds()
            if not s.get("notified_1h", False) and 3300 <= diff <= 3900:
                result.append((s, "1h"))
                s["notified_1h"] = True
                changed = True
            if not s.get("notified_start", False) and -300 <= diff <= 300:
                result.append((s, "start"))
                s["notified_start"] = True
                changed = True
        if changed:
            self.save()
        return result


# ═══════════════════════════════════════════════════════════════
#  ГЛОБАЛЬНІ ЕКЗЕМПЛЯРИ
# ═══════════════════════════════════════════════════════════════

pay = PaymentManager()
wm  = WorkoutManager()
pm  = PersonalManager()


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

def _fmt_slot(s: dict) -> str:
    try:
        d = datetime.strptime(s["date"], "%Y-%m-%d")
        return f"{d.strftime('%d.%m.%Y')} о {s['time']}"
    except Exception:
        return f"{s['date']} {s['time']}"

MONTH_NAMES_UK = {
    1: "Січень", 2: "Лютий", 3: "Березень", 4: "Квітень",
    5: "Травень", 6: "Червень", 7: "Липень", 8: "Серпень",
    9: "Вересень", 10: "Жовтень", 11: "Листопад", 12: "Грудень"
}

DAY_HEADERS = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Нд"]


def build_calendar_kb(year: int, month: int, available_days: set[int]) -> list[list[InlineKeyboardButton]]:
    """Будує інлайн-клавіатуру календаря на місяць."""
    kb = []
    # Заголовок місяця
    kb.append([InlineKeyboardButton(
        f"📅 {MONTH_NAMES_UK[month]} {year}",
        callback_data="cal_ignore"
    )])
    # Дні тижня
    kb.append([InlineKeyboardButton(d, callback_data="cal_ignore") for d in DAY_HEADERS])

    # Дні місяця
    cal = calendar.monthcalendar(year, month)
    for week in cal:
        row = []
        for day in week:
            if day == 0:
                row.append(InlineKeyboardButton(" ", callback_data="cal_ignore"))
            elif day in available_days:
                # Доступний день — виділяємо
                row.append(InlineKeyboardButton(
                    f"✅{day}",
                    callback_data=f"cal_day_{year}_{month}_{day}"
                ))
            else:
                row.append(InlineKeyboardButton(
                    str(day),
                    callback_data="cal_ignore"
                ))
        kb.append(row)

    return kb


# ═══════════════════════════════════════════════════════════════
#  /start — ГОЛОВНЕ МЕНЮ
# ═══════════════════════════════════════════════════════════════

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    pay.upsert_client(user.id, user.username or "", user.full_name or "")
    context.user_data.clear()

    kb = [
        [InlineKeyboardButton("👥 Групові тренування",     callback_data="group_menu")],
        [InlineKeyboardButton("🧑‍🏫 Персональне тренування", callback_data="personal_menu")],
        [InlineKeyboardButton("👤 Мій статус",              callback_data="my_status")],
    ]
    if _is_admin(user.id):
        kb.append([InlineKeyboardButton("⚙️ Адмін-панель", callback_data="admin_panel")])

    await update.message.reply_text(
        f"👋 Привіт, <b>{user.first_name}</b>!\n\n"
        "Це бот онлайн-тренувань. Обери тип тренування:",
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode="HTML"
    )


async def _edit_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user  = update.effective_user
    context.user_data.pop("adm_state", None)
    context.user_data.pop("waiting_screenshot", None)

    kb = [
        [InlineKeyboardButton("👥 Групові тренування",     callback_data="group_menu")],
        [InlineKeyboardButton("🧑‍🏫 Персональне тренування", callback_data="personal_menu")],
        [InlineKeyboardButton("👤 Мій статус",              callback_data="my_status")],
    ]
    if _is_admin(user.id):
        kb.append([InlineKeyboardButton("⚙️ Адмін-панель", callback_data="admin_panel")])
    await query.edit_message_text(
        "🏠 <b>Головне меню</b>\n\nОбери дію:",
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode="HTML"
    )


# ═══════════════════════════════════════════════════════════════
#  ПІДМЕНЮ: ГРУПОВІ ТРЕНУВАННЯ
# ═══════════════════════════════════════════════════════════════

async def show_group_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.edit_message_text(
        "👥 <b>Групові тренування</b>\n\n"
        f"Вартість: <b>{GROUP_PRICE} грн</b>\n\nОберіть дію:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🗓 Заплановані групові тренування", callback_data="group_schedule")],
            [InlineKeyboardButton("💳 Оплатити тренування",            callback_data="group_pay_start")],
            [InlineKeyboardButton("◀️ Назад",  callback_data="main_menu")],
            [InlineKeyboardButton("🏠 Головне меню", callback_data="main_menu")],
        ]),
        parse_mode="HTML"
    )


async def show_group_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query    = update.callback_query
    upcoming = wm.upcoming()

    if not upcoming:
        await query.edit_message_text(
            "🗓 <b>Групові тренування</b>\n\nНаразі тренувань немає. Стежте за каналом! 💪",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("◀️ Назад", callback_data="group_menu")],
                [InlineKeyboardButton("🏠 Головне меню", callback_data="main_menu")],
            ]),
            parse_mode="HTML"
        )
        return

    lines = ["🗓 <b>Заплановані групові тренування:</b>\n"]
    for w in upcoming[:5]:
        count = wm.count_paid(w["id"])
        lines.append(
            f"• <b>{w['title']}</b>\n"
            f"  📅 {_fmt_dt(w)}  👥 {count} оплачено"
        )

    await query.edit_message_text(
        "\n\n".join(lines),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("💳 Оплатити тренування", callback_data="group_pay_start")],
            [InlineKeyboardButton("◀️ Назад", callback_data="group_menu")],
            [InlineKeyboardButton("🏠 Головне меню", callback_data="main_menu")],
        ]),
        parse_mode="HTML"
    )


async def group_pay_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query    = update.callback_query
    user     = update.effective_user
    upcoming = wm.upcoming()
    available = [w for w in upcoming if not pay.has_paid_group(user.id, w["id"])]

    if not upcoming:
        msg = "😔 Наразі немає запланованих групових тренувань."
    elif not available:
        msg = "✅ Ви вже оплатили всі заплановані групові тренування!"
    else:
        msg = None

    if msg:
        await query.edit_message_text(
            msg,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("◀️ Назад", callback_data="group_menu")],
                [InlineKeyboardButton("🏠 Головне меню", callback_data="main_menu")],
            ])
        )
        return

    kb = []
    for w in available:
        kb.append([InlineKeyboardButton(
            f"{w['title']} — {_fmt_dt(w)}",
            callback_data=f"gpay_select_{w['id']}"
        )])
    kb.append([InlineKeyboardButton("◀️ Назад", callback_data="group_menu")])
    kb.append([InlineKeyboardButton("🏠 Головне меню", callback_data="main_menu")])

    await query.edit_message_text(
        f"💳 <b>Оплата групового тренування</b>\n\nВартість: <b>{GROUP_PRICE} грн</b>\n\nОберіть тренування:",
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode="HTML"
    )


async def group_pay_details(update: Update, context: ContextTypes.DEFAULT_TYPE, workout_id: int):
    query   = update.callback_query
    user    = update.effective_user
    workout = wm.get(workout_id)

    if not workout:
        await query.answer("Тренування не знайдено", show_alert=True)
        return

    payment_id = pay.set_pending(user.id, workout_id=workout_id, pay_type="group")
    context.user_data["waiting_screenshot"] = True
    context.user_data["payment_id"]         = payment_id
    context.user_data["pay_type"]           = "group"
    context.user_data["workout_id"]         = workout_id

    await query.edit_message_text(
        f"💳 <b>Оплата групового тренування</b>\n\n"
        f"🏋️ {workout['title']}\n"
        f"📅 {_fmt_dt(workout)}\n\n"
        f"Сума: <b>{GROUP_PRICE} грн</b>\n\n"
        f"Переказати на картку:\n"
        f"<code>{CARD_NUMBER}</code>\n"
        f"Отримувач: <b>{CARD_OWNER}</b>\n\n"
        f"Після переказу надішліть сюди 📸 <b>скріншот підтвердження</b>.\n\n"
        f"<i>⏱ Підтвердження займає до 1–3 годин.</i>",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("◀️ Назад", callback_data="group_pay_start"),
             InlineKeyboardButton("🏠 Головне меню", callback_data="main_menu")],
        ]),
        parse_mode="HTML"
    )


# ═══════════════════════════════════════════════════════════════
#  ПІДМЕНЮ: ПЕРСОНАЛЬНЕ ТРЕНУВАННЯ
# ═══════════════════════════════════════════════════════════════

async def show_personal_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.edit_message_text(
        "🧑‍🏫 <b>Персональне тренування</b>\n\n"
        f"Вартість: <b>{PERSONAL_PRICE} грн</b>\n"
        "Формат: 1-на-1 з тренером через Microsoft Teams\n\nОберіть дію:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📅 Запис на тренування",  callback_data="personal_calendar")],
            [InlineKeyboardButton("💳 Оплатити тренування",  callback_data="personal_pay_start")],
            [InlineKeyboardButton("◀️ Назад",  callback_data="main_menu")],
            [InlineKeyboardButton("🏠 Головне меню", callback_data="main_menu")],
        ]),
        parse_mode="HTML"
    )


async def show_personal_calendar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    now   = datetime.now(TIMEZONE)
    year, month = now.year, now.month

    available_days = pm.available_dates_in_month(year, month)

    cal_kb = build_calendar_kb(year, month, available_days)

    # Додаємо кнопки навігації під календарем
    cal_kb.append([InlineKeyboardButton("◀️ Назад", callback_data="personal_menu")])
    cal_kb.append([InlineKeyboardButton("🏠 Головне меню", callback_data="main_menu")])

    hint = "Дні з ✅ — доступні для запису. Натисніть на день, щоб побачити час." if available_days else "На цей місяць вільних слотів немає."

    await query.edit_message_text(
        f"📅 <b>Запис на персональне тренування</b>\n\n{hint}",
        reply_markup=InlineKeyboardMarkup(cal_kb),
        parse_mode="HTML"
    )


async def show_day_slots(update: Update, context: ContextTypes.DEFAULT_TYPE, year: int, month: int, day: int):
    query    = update.callback_query
    date_str = f"{year}-{month:02d}-{day:02d}"
    slots    = pm.slots_for_date(date_str)

    if not slots:
        await query.answer("На цей день вільних слотів немає", show_alert=True)
        return

    d_fmt = f"{day:02d}.{month:02d}.{year}"
    kb = []
    for s in slots:
        kb.append([InlineKeyboardButton(
            f"🕐 {s['time']}",
            callback_data=f"pslot_book_{s['id']}"
        )])
    kb.append([InlineKeyboardButton("◀️ До календаря", callback_data="personal_calendar")])
    kb.append([InlineKeyboardButton("🏠 Головне меню", callback_data="main_menu")])

    await query.edit_message_text(
        f"📅 <b>{d_fmt}</b> — доступні слоти:\n\n"
        f"Оберіть час для запису (вартість: <b>{PERSONAL_PRICE} грн</b>):",
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode="HTML"
    )


async def book_personal_slot(update: Update, context: ContextTypes.DEFAULT_TYPE, slot_id: str):
    query = update.callback_query
    user  = update.effective_user
    slot  = pm.get(slot_id)

    if not slot:
        await query.answer("Слот не знайдено", show_alert=True)
        return

    if pm.is_booked(slot_id):
        await query.answer("Цей слот вже заброньовано", show_alert=True)
        return

    # Бронюємо слот
    pm.book(slot_id, user.id)

    await query.edit_message_text(
        f"✅ <b>Вас записано на персональне тренування!</b>\n\n"
        f"📅 {_fmt_slot(slot)}\n"
        f"💰 Вартість: <b>{PERSONAL_PRICE} грн</b>\n\n"
        f"Для отримання посилання на Microsoft Teams, будь ласка, оплатіть тренування.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("💳 Оплатити зараз", callback_data=f"ppay_slot_{slot_id}")],
            [InlineKeyboardButton("◀️ Назад", callback_data="personal_menu")],
            [InlineKeyboardButton("🏠 Головне меню", callback_data="main_menu")],
        ]),
        parse_mode="HTML"
    )


async def personal_pay_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user  = update.effective_user

    # Знаходимо заброньовані але неоплачені слоти
    booked_unpaid = []
    for s in pm.all_upcoming():
        if s["booked_by"] == user.id and not pay.has_paid_personal(user.id, s["id"]):
            booked_unpaid.append(s)

    if not booked_unpaid:
        await query.edit_message_text(
            "😔 У вас немає заброньованих неоплачених тренувань.\n\n"
            "Спочатку запишіться на тренування через календар.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📅 Запис на тренування", callback_data="personal_calendar")],
                [InlineKeyboardButton("◀️ Назад", callback_data="personal_menu")],
                [InlineKeyboardButton("🏠 Головне меню", callback_data="main_menu")],
            ])
        )
        return

    kb = []
    for s in booked_unpaid:
        kb.append([InlineKeyboardButton(
            f"🕐 {_fmt_slot(s)}",
            callback_data=f"ppay_slot_{s['id']}"
        )])
    kb.append([InlineKeyboardButton("◀️ Назад", callback_data="personal_menu")])
    kb.append([InlineKeyboardButton("🏠 Головне меню", callback_data="main_menu")])

    await query.edit_message_text(
        f"💳 <b>Оплата персонального тренування</b>\n\nВартість: <b>{PERSONAL_PRICE} грн</b>\n\nОберіть тренування:",
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode="HTML"
    )


async def personal_pay_details(update: Update, context: ContextTypes.DEFAULT_TYPE, slot_id: str):
    query = update.callback_query
    user  = update.effective_user
    slot  = pm.get(slot_id)

    if not slot:
        await query.answer("Слот не знайдено", show_alert=True)
        return

    payment_id = pay.set_pending(user.id, slot_id=slot_id, pay_type="personal")
    context.user_data["waiting_screenshot"] = True
    context.user_data["payment_id"]         = payment_id
    context.user_data["pay_type"]           = "personal"
    context.user_data["slot_id"]            = slot_id

    await query.edit_message_text(
        f"💳 <b>Оплата персонального тренування</b>\n\n"
        f"📅 {_fmt_slot(slot)}\n\n"
        f"Сума: <b>{PERSONAL_PRICE} грн</b>\n\n"
        f"Переказати на картку:\n"
        f"<code>{CARD_NUMBER}</code>\n"
        f"Отримувач: <b>{CARD_OWNER}</b>\n\n"
        f"Після переказу надішліть сюди 📸 <b>скріншот підтвердження</b>.\n\n"
        f"<i>⏱ Підтвердження займає до 1–3 годин.</i>",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("◀️ Назад", callback_data="personal_pay_start"),
             InlineKeyboardButton("🏠 Головне меню", callback_data="main_menu")],
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

    # Групові
    lines.append("<b>👥 Групові тренування:</b>")
    if not upcoming:
        lines.append("  Запланованих немає.")
    else:
        for w in upcoming[:5]:
            icon = "✅" if pay.has_paid_group(user.id, w["id"]) else "❌"
            lines.append(f"  {icon} {w['title']} ({_fmt_dt(w)})")

    # Персональні
    lines.append("\n<b>🧑‍🏫 Персональні тренування:</b>")
    personal_upcoming = pm.all_upcoming()
    user_personal = [s for s in personal_upcoming if s["booked_by"] == user.id]
    if not user_personal:
        lines.append("  Записів немає.")
    else:
        for s in user_personal[:5]:
            if pay.has_paid_personal(user.id, s["id"]):
                icon = "✅"
            else:
                icon = "⏳"
            lines.append(f"  {icon} {_fmt_slot(s)}")

    await query.edit_message_text(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("👥 Групові",     callback_data="group_menu"),
             InlineKeyboardButton("🧑‍🏫 Персональні", callback_data="personal_menu")],
            [InlineKeyboardButton("🏠 Головне меню", callback_data="main_menu")],
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
    pay_type   = context.user_data.get("pay_type", "group")

    if not payment_id:
        await update.message.reply_text("⚠️ Не знайдено активний платіж. Спробуйте /start")
        return

    photo = update.message.photo[-1]
    context.user_data["waiting_screenshot"] = False

    await update.message.reply_text(
        "✅ <b>Скріншот отримано!</b>\n\n"
        "Платіж передано адміністратору на перевірку.\n"
        "Зазвичай це займає до 1–3 годин.\n\n"
        "Як тільки оплату підтвердять — ви отримаєте посилання на тренування 🙏",
        parse_mode="HTML"
    )

    if pay_type == "group":
        workout_id = context.user_data.get("workout_id")
        workout = wm.get(workout_id) if workout_id else None
        item_name = workout["title"] if workout else "—"
        dt_str    = _fmt_dt(workout) if workout else "—"
        price     = GROUP_PRICE
        cb_ok     = f"adm_ok_{payment_id}_{user.id}_{workout_id or 0}_group"
    else:
        slot_id = context.user_data.get("slot_id")
        slot    = pm.get(slot_id) if slot_id else None
        item_name = f"Персональне ({_fmt_slot(slot)})" if slot else "—"
        dt_str    = _fmt_slot(slot) if slot else "—"
        price     = PERSONAL_PRICE
        cb_ok     = f"adm_ok_{payment_id}_{user.id}_{slot_id or 0}_personal"

    caption = (
        f"💳 <b>Новий платіж!</b>\n\n"
        f"👤 {user.full_name}"
        f"{' (@' + user.username + ')' if user.username else ''}\n"
        f"🆔 <code>{user.id}</code>\n"
        f"🏋️ {item_name}\n"
        f"📅 {dt_str}\n"
        f"💰 {price} грн ({pay_type})\n"
        f"🕐 {datetime.now(TIMEZONE).strftime('%d.%m.%Y %H:%M')}\n"
        f"#pay{payment_id}"
    )
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Підтвердити", callback_data=cb_ok),
        InlineKeyboardButton("❌ Відхилити",   callback_data=f"adm_no_{payment_id}_{user.id}"),
    ]])
    await context.bot.send_photo(
        chat_id=ADMIN_ID, photo=photo.file_id,
        caption=caption, reply_markup=kb, parse_mode="HTML"
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

    context.user_data.pop("adm_state", None)

    upcoming_group    = wm.upcoming()
    upcoming_personal = pm.all_upcoming()
    total = len(pay.all_client_ids())

    await query.edit_message_text(
        f"⚙️ <b>Адмін-панель</b>\n\n"
        f"👥 Клієнтів у боті: {total}\n"
        f"🗓 Групових тренувань: {len(upcoming_group)}\n"
        f"🧑‍🏫 Персональних слотів: {len(upcoming_personal)}\n\n"
        "Оберіть дію:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Додати групове тренування",   callback_data="adm_add_group")],
            [InlineKeyboardButton("📋 Список групових тренувань",   callback_data="adm_list_group")],
            [InlineKeyboardButton("➕ Додати персональний слот",    callback_data="adm_add_personal")],
            [InlineKeyboardButton("📋 Список персональних слотів",  callback_data="adm_list_personal")],
            [InlineKeyboardButton("📢 Розіслати всім",              callback_data="adm_bcast")],
            [InlineKeyboardButton("🏠 Головне меню",                callback_data="main_menu")],
        ]),
        parse_mode="HTML"
    )


async def adm_add_group_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not _is_admin(update.effective_user.id):
        await query.answer("⛔", show_alert=True)
        return
    context.user_data["adm_state"] = "group_title"
    await query.edit_message_text(
        "➕ <b>Нове групове тренування</b> — крок 1/3\n\n"
        "Введіть <b>назву тренування</b>:\n"
        "<i>Наприклад: Кардіо для початківців</i>",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("❌ Скасувати", callback_data="admin_panel")
        ]]),
        parse_mode="HTML"
    )


async def adm_list_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not _is_admin(update.effective_user.id):
        await query.answer("⛔", show_alert=True)
        return

    upcoming = wm.upcoming()
    if not upcoming:
        await query.edit_message_text(
            "📋 Групових тренувань немає.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ Додати", callback_data="adm_add_group")],
                [InlineKeyboardButton("◀️ Назад", callback_data="admin_panel")],
            ])
        )
        return

    lines = ["📋 <b>Заплановані групові тренування:</b>\n"]
    kb    = []
    for w in upcoming:
        cnt = wm.count_paid(w["id"])
        lines.append(f"#{w['id']} <b>{w['title']}</b> — {_fmt_dt(w)} | 👥{cnt}")
        kb.append([InlineKeyboardButton(
            f"🗑 Видалити #{w['id']} {w['title'][:20]}",
            callback_data=f"adm_del_group_{w['id']}"
        )])
    kb.append([InlineKeyboardButton("◀️ Назад", callback_data="admin_panel")])

    await query.edit_message_text(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode="HTML"
    )


async def adm_add_personal_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not _is_admin(update.effective_user.id):
        await query.answer("⛔", show_alert=True)
        return
    context.user_data["adm_state"] = "personal_date"
    await query.edit_message_text(
        "➕ <b>Новий персональний слот</b> — крок 1/3\n\n"
        "Введіть <b>дату</b>:\n"
        "<code>ДД.ММ.РРРР</code>  Наприклад: <code>20.06.2025</code>",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("❌ Скасувати", callback_data="admin_panel")
        ]]),
        parse_mode="HTML"
    )


async def adm_list_personal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not _is_admin(update.effective_user.id):
        await query.answer("⛔", show_alert=True)
        return

    upcoming = pm.all_upcoming()
    if not upcoming:
        await query.edit_message_text(
            "📋 Персональних слотів немає.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ Додати", callback_data="adm_add_personal")],
                [InlineKeyboardButton("◀️ Назад", callback_data="admin_panel")],
            ])
        )
        return

    lines = ["📋 <b>Персональні слоти:</b>\n"]
    kb    = []
    for s in upcoming:
        status = "🔒 зайнято" if s["booked_by"] else "🟢 вільно"
        paid = ""
        if s["booked_by"] and pay.has_paid_personal(s["booked_by"], s["id"]):
            paid = " ✅оплач."
        lines.append(f"{s['id']}: <b>{_fmt_slot(s)}</b> — {status}{paid}")
        kb.append([InlineKeyboardButton(
            f"🗑 Видалити {s['id']} ({s['date']} {s['time']})",
            callback_data=f"adm_del_personal_{s['id']}"
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
#  ОБРОБНИК ТЕКСТУ (покрокові діалоги)
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

    # ── Групове тренування: назва ──
    if state == "group_title":
        context.user_data["new_title"] = text
        context.user_data["adm_state"] = "group_dt"
        await update.message.reply_text(
            f"✅ Назва: <b>{text}</b>\n\n"
            "Крок 2/3 — введіть <b>дату та час</b>:\n"
            "<code>ДД.ММ.РРРР ГГ:ХХ</code>  Наприклад: <code>20.06.2025 18:00</code>",
            parse_mode="HTML"
        )
        return

    if state == "group_dt":
        try:
            dt = datetime.strptime(text, "%d.%m.%Y %H:%M").replace(tzinfo=TIMEZONE)
        except ValueError:
            await update.message.reply_text(
                "⚠️ Невірний формат. Приклад: <code>20.06.2025 18:00</code>",
                parse_mode="HTML"
            )
            return
        context.user_data["new_dt"]    = dt.isoformat()
        context.user_data["adm_state"] = "group_link"
        await update.message.reply_text(
            f"✅ Дата: <b>{dt.strftime('%d.%m.%Y о %H:%M')}</b>\n\n"
            "Крок 3/3 — введіть <b>посилання Microsoft Teams</b>:",
            parse_mode="HTML"
        )
        return

    if state == "group_link":
        if not text.startswith("http"):
            await update.message.reply_text("⚠️ Посилання має починатися з https://")
            return

        title   = context.user_data.pop("new_title", "Тренування")
        dt      = datetime.fromisoformat(context.user_data.pop("new_dt"))
        workout = wm.add(title, dt, text)
        context.user_data["adm_state"] = None

        bot_me = await context.bot.get_me()
        channel_text = (
            f"🏋️ <b>Онлайн-тренування!</b>\n\n"
            f"📌 {title}\n"
            f"📅 {dt.strftime('%d.%m.%Y о %H:%M')}\n\n"
            f"💳 Вартість: <b>{GROUP_PRICE} грн</b>\n\n"
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
            f"✅ <b>Групове тренування додано!</b>\n\n"
            f"🏋️ {title}\n📅 {dt.strftime('%d.%m.%Y о %H:%M')}\n\n{channel_note}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📋 Список тренувань", callback_data="adm_list_group")],
                [InlineKeyboardButton("⚙️ Адмін-панель",    callback_data="admin_panel")],
            ]),
            parse_mode="HTML"
        )
        return

    # ── Персональний слот: дата ──
    if state == "personal_date":
        try:
            d = datetime.strptime(text, "%d.%m.%Y")
            date_str = d.strftime("%Y-%m-%d")
        except ValueError:
            await update.message.reply_text(
                "⚠️ Невірний формат. Приклад: <code>20.06.2025</code>",
                parse_mode="HTML"
            )
            return
        context.user_data["new_personal_date"] = date_str
        context.user_data["adm_state"] = "personal_time"
        await update.message.reply_text(
            f"✅ Дата: <b>{text}</b>\n\n"
            "Крок 2/3 — введіть <b>час початку</b>:\n"
            "<code>ГГ:ХХ</code>  Наприклад: <code>10:00</code>",
            parse_mode="HTML"
        )
        return

    if state == "personal_time":
        try:
            datetime.strptime(text, "%H:%M")
        except ValueError:
            await update.message.reply_text(
                "⚠️ Невірний формат. Приклад: <code>10:00</code>",
                parse_mode="HTML"
            )
            return
        context.user_data["new_personal_time"] = text
        context.user_data["adm_state"] = "personal_link"
        await update.message.reply_text(
            f"✅ Час: <b>{text}</b>\n\n"
            "Крок 3/3 — введіть <b>посилання Microsoft Teams</b>:",
            parse_mode="HTML"
        )
        return

    if state == "personal_link":
        if not text.startswith("http"):
            await update.message.reply_text("⚠️ Посилання має починатися з https://")
            return

        date_str  = context.user_data.pop("new_personal_date", "")
        time_str  = context.user_data.pop("new_personal_time", "")
        slot      = pm.add_slot(date_str, time_str, text)
        context.user_data["adm_state"] = None

        d_fmt = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d.%m.%Y")
        await update.message.reply_text(
            f"✅ <b>Персональний слот додано!</b>\n\n"
            f"📅 {d_fmt} о {time_str}\n"
            f"🆔 {slot['id']}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📋 Список слотів", callback_data="adm_list_personal")],
                [InlineKeyboardButton("➕ Додати ще",     callback_data="adm_add_personal")],
                [InlineKeyboardButton("⚙️ Адмін-панель", callback_data="admin_panel")],
            ]),
            parse_mode="HTML"
        )
        return

    # ── Розсилка ──
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
    context.user_data["adm_state"] = "group_title"
    await update.message.reply_text(
        "➕ <b>Нове групове тренування</b> — крок 1/3\n\nВведіть <b>назву тренування</b>:",
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
    if data == "cal_ignore":
        return

    # ── Групові ──
    if data == "group_menu":
        await show_group_menu(update, context)
        return
    if data == "group_schedule":
        await show_group_schedule(update, context)
        return
    if data == "group_pay_start":
        await group_pay_start(update, context)
        return
    if data.startswith("gpay_select_"):
        await group_pay_details(update, context, int(data[12:]))
        return

    # ── Персональні ──
    if data == "personal_menu":
        await show_personal_menu(update, context)
        return
    if data == "personal_calendar":
        await show_personal_calendar(update, context)
        return
    if data.startswith("cal_day_"):
        parts = data.split("_")  # cal_day_2025_6_15
        year, month, day = int(parts[2]), int(parts[3]), int(parts[4])
        await show_day_slots(update, context, year, month, day)
        return
    if data.startswith("pslot_book_"):
        await book_personal_slot(update, context, data[11:])
        return
    if data == "personal_pay_start":
        await personal_pay_start(update, context)
        return
    if data.startswith("ppay_slot_"):
        await personal_pay_details(update, context, data[10:])
        return

    # ── Мій статус ──
    if data == "my_status":
        await show_my_status(update, context)
        return

    # ── Адмін: підтвердити оплату ──
    if data.startswith("adm_ok_"):
        if not _is_admin(user.id):
            await query.answer("⛔", show_alert=True)
            return
        # adm_ok_{pid}_{uid}_{item_id}_{type}
        parts = data.split("_")
        payment_id = int(parts[2])
        client_uid = int(parts[3])
        item_id    = parts[4]
        pay_type   = parts[5] if len(parts) > 5 else "group"

        pay.approve(client_uid)

        try:
            await query.edit_message_caption(
                caption=(query.message.caption or "") + "\n\n✅ <b>ПІДТВЕРДЖЕНО</b>",
                parse_mode="HTML"
            )
        except Exception:
            pass

        if pay_type == "group":
            workout_id = int(item_id) if item_id != "0" else None
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
        else:
            slot = pm.get(item_id)
            if slot:
                client_text = (
                    f"🎉 <b>Оплату підтверджено!</b>\n\n"
                    f"🧑‍🏫 <b>Персональне тренування</b>\n"
                    f"📅 {_fmt_slot(slot)}\n\n"
                    f"🔗 <b>Посилання Microsoft Teams:</b>\n"
                    f"{slot['teams_link']}\n\n"
                    f"<i>До зустрічі на тренуванні! 💪</i>"
                )
            else:
                client_text = "🎉 <b>Оплату підтверджено!</b>\n\nПосилання надійде найближчим часом."

        await context.bot.send_message(
            chat_id=client_uid, text=client_text,
            parse_mode="HTML", disable_web_page_preview=False
        )
        return

    # ── Адмін: відхилити оплату ──
    if data.startswith("adm_no_"):
        if not _is_admin(user.id):
            await query.answer("⛔", show_alert=True)
            return
        parts = data.split("_")
        client_uid = int(parts[3])

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
                "або переказ ще не надійшов.\n\nСпробуйте ще раз або зверніться до тренера."
            ),
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🏠 Головне меню", callback_data="main_menu")
            ]]),
            parse_mode="HTML"
        )
        return

    # ── Адмін-панель ──
    if data == "admin_panel":
        if _is_admin(user.id):
            await show_admin_panel(update, context)
        return
    if data == "adm_add_group":
        await adm_add_group_start(update, context)
        return
    if data == "adm_list_group":
        await adm_list_group(update, context)
        return
    if data == "adm_add_personal":
        await adm_add_personal_start(update, context)
        return
    if data == "adm_list_personal":
        await adm_list_personal(update, context)
        return
    if data == "adm_bcast":
        await adm_bcast_start(update, context)
        return
    if data.startswith("adm_del_group_"):
        if not _is_admin(user.id):
            await query.answer("⛔", show_alert=True)
            return
        wm.delete(int(data[14:]))
        await query.answer("✅ Видалено")
        await adm_list_group(update, context)
        return
    if data.startswith("adm_del_personal_"):
        if not _is_admin(user.id):
            await query.answer("⛔", show_alert=True)
            return
        pm.delete_slot(data[17:])
        await query.answer("✅ Видалено")
        await adm_list_personal(update, context)
        return


# ═══════════════════════════════════════════════════════════════
#  ФОНОВИЙ ЦИКЛ НАГАДУВАНЬ
# ═══════════════════════════════════════════════════════════════

async def notification_loop(app: Application):
    while True:
        try:
            # Групові нагадування
            for workout, ntype in wm.get_pending_notifications():
                client_ids = pay.get_paid_workout_ids(workout["id"])
                if not client_ids:
                    continue
                dt = datetime.fromisoformat(workout["datetime"])
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=TIMEZONE)

                if ntype == "1h":
                    text = (
                        f"⏰ <b>Групове тренування через 1 годину!</b>\n\n"
                        f"🏋️ <b>{workout['title']}</b>\n"
                        f"📅 {dt.strftime('%d.%m.%Y о %H:%M')}\n\n"
                        f"🔗 <a href=\"{workout['teams_link']}\">Посилання Microsoft Teams</a>\n\n"
                        f"Готуйтеся! 💪"
                    )
                else:
                    text = (
                        f"🚀 <b>Групове тренування починається!</b>\n\n"
                        f"🏋️ <b>{workout['title']}</b>\n\n"
                        f"🔗 <a href=\"{workout['teams_link']}\">Підключитись зараз!</a>"
                    )

                for uid in client_ids:
                    try:
                        await app.bot.send_message(
                            chat_id=uid, text=text,
                            parse_mode="HTML", disable_web_page_preview=False
                        )
                        await asyncio.sleep(0.05)
                    except Exception as e:
                        logger.warning(f"notify group → {uid}: {e}")

            # Персональні нагадування
            for slot, ntype in pm.get_pending_notifications():
                uid = slot["booked_by"]
                if not uid:
                    continue
                dt = datetime.strptime(f"{slot['date']} {slot['time']}", "%Y-%m-%d %H:%M").replace(tzinfo=TIMEZONE)

                if ntype == "1h":
                    text = (
                        f"⏰ <b>Персональне тренування через 1 годину!</b>\n\n"
                        f"📅 {_fmt_slot(slot)}\n\n"
                        f"🔗 <a href=\"{slot['teams_link']}\">Посилання Microsoft Teams</a>\n\n"
                        f"Готуйтеся! 💪"
                    )
                else:
                    text = (
                        f"🚀 <b>Персональне тренування починається!</b>\n\n"
                        f"📅 {_fmt_slot(slot)}\n\n"
                        f"🔗 <a href=\"{slot['teams_link']}\">Підключитись зараз!</a>"
                    )

                try:
                    await app.bot.send_message(
                        chat_id=uid, text=text,
                        parse_mode="HTML", disable_web_page_preview=False
                    )
                except Exception as e:
                    logger.warning(f"notify personal → {uid}: {e}")

        except Exception as e:
            logger.error(f"notification_loop: {e}")

        await asyncio.sleep(60)


# ═══════════════════════════════════════════════════════════════
#  HEALTH-CHECK СЕРВЕР (для Koyeb)
# ═══════════════════════════════════════════════════════════════

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")
    def log_message(self, format, *args):
        pass

def run_health_server():
    port = int(os.environ.get("PORT", 8000))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    logger.info(f"Health-check server на порту {port}")
    server.serve_forever()


# ═══════════════════════════════════════════════════════════════
#  ЗАПУСК
# ═══════════════════════════════════════════════════════════════

def main():
    pay.load()
    wm.load()
    pm.load()

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
            listen="0.0.0.0", port=PORT,
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
    threading.Thread(target=run_health_server, daemon=True).start()
    main()
