"""
Телеграм-бот: Онлайн-тренування (фітнес-канал)
================================================

Головне меню: Групові тренування | Персональне тренування | Мій статус
Адмін: календар для вибору дати + сітка годин для вибору часу
"""

import os
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

BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_IDS = [int(x.strip()) for x in os.environ.get("ADMIN_IDS", os.environ.get("ADMIN_ID", "0")).split(",") if x.strip()]
CHANNEL_ID = os.environ["CHANNEL_ID"]
CHANNEL_TOPIC_ID = int(os.environ.get("CHANNEL_TOPIC_ID", "0"))
CARD_NUMBER = os.environ["CARD_NUMBER"]
CARD_OWNER = os.environ["CARD_OWNER"]

GROUP_PRICE = int(os.environ.get("GROUP_PRICE", "200"))
PERSONAL_PRICE = int(os.environ.get("PERSONAL_PRICE", "500"))

TIMEZONE = ZoneInfo("Europe/Kyiv")

# GitHub Gist — хмарне сховище для бекапів
GITHUB_TOKEN   = os.environ.get("GITHUB_TOKEN", "")       # Personal Access Token з правом "gist"
GIST_ID_FILE   = ".gist_id"                                # локально зберігає ID створеного Gist

LOCAL_PAYMENTS_FILE = "fit_payments.json"
LOCAL_WORKOUTS_FILE = "fit_workouts.json"
LOCAL_PERSONAL_FILE = "fit_personal.json"

# ═══════════════════════════════════════════════════════════════
#  ЛОГУВАННЯ
# ═══════════════════════════════════════════════════════════════

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  GITHUB GIST STORAGE MANAGER
# ═══════════════════════════════════════════════════════════════

import time as _time

BACKUP_THROTTLE_SEC = int(os.environ.get("BACKUP_THROTTLE_SEC", "60"))  # 1 хв


class GistStorageManager:
    """
    Зберігає JSON-дані у GitHub Gist (приватний) як хмарний бекап.

    Схема:
      save()  → локальний JSON (миттєво) + Gist (фоновий thread, з throttle)
      load()  → Gist → локальний JSON (резерв)
    """

    _gist_id: str | None = None
    _gist_lock = threading.Lock()

    def __init__(self, local_file: str, gist_filename: str):
        self.local_file    = local_file
        self.gist_filename = gist_filename
        self._last_upload  = 0.0
        self._pending_data = None
        self._lock         = threading.Lock()
        self._ever_uploaded = False  # перший аплоад завжди без throttle

    # ── GitHub API ──

    @classmethod
    def _gh_headers(cls) -> dict:
        return {
            "Authorization": f"Bearer {GITHUB_TOKEN}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    @classmethod
    def _load_gist_id(cls) -> str | None:
        gid = os.environ.get("GIST_ID", "")
        if gid:
            cls._gist_id = gid
            return gid
        try:
            if os.path.exists(GIST_ID_FILE):
                v = open(GIST_ID_FILE).read().strip()
                if v:
                    cls._gist_id = v
                    return v
        except Exception:
            pass
        return None

    @classmethod
    def _save_gist_id(cls, gist_id: str):
        cls._gist_id = gist_id
        try:
            with open(GIST_ID_FILE, "w") as f:
                f.write(gist_id)
        except Exception:
            pass

    @classmethod
    def _create_gist(cls) -> str | None:
        if not GITHUB_TOKEN:
            logger.warning("GITHUB_TOKEN не задано — хмарний бекап вимкнено")
            return None
        try:
            resp = req.post(
                "https://api.github.com/gists",
                headers=cls._gh_headers(),
                json={
                    "description": "FitSessionBot backup (auto-created)",
                    "public": False,
                    "files": {
                        "fit_payments.json": {"content": "{}"},
                        "fit_workouts.json": {"content": "{}"},
                        "fit_personal.json": {"content": "{}"},
                    }
                },
                timeout=30,
            )
            if resp.status_code == 201:
                gist_id = resp.json()["id"]
                cls._save_gist_id(gist_id)
                logger.info(f"Gist створено: {gist_id}")
                return gist_id
            logger.error(f"Gist create: {resp.status_code} {resp.text[:200]}")
        except Exception as e:
            logger.error(f"Gist create exception: {e}")
        return None

    @classmethod
    def _ensure_gist(cls) -> str | None:
        with cls._gist_lock:
            if cls._gist_id:
                return cls._gist_id
            gid = cls._load_gist_id()
            if gid:
                return gid
            return cls._create_gist()

    def _download_from_gist(self) -> dict | None:
        gist_id = self._ensure_gist()
        if not gist_id or not GITHUB_TOKEN:
            return None
        try:
            resp = req.get(
                f"https://api.github.com/gists/{gist_id}",
                headers=self._gh_headers(),
                timeout=30,
            )
            if resp.status_code != 200:
                logger.error(f"Gist get [{self.gist_filename}]: {resp.status_code}")
                return None
            files = resp.json().get("files", {})
            f = files.get(self.gist_filename)
            if not f:
                logger.warning(f"[{self.gist_filename}] відсутній у Gist")
                return None
            content = f.get("content", "{}")
            data = json.loads(content)
            # Порожній dict {} — це валідні дані (просто ще нічого немає)
            if isinstance(data, dict):
                logger.info(f"[{self.gist_filename}] ← Gist OK ({len(data)} ключів)")
                return data
            return None
        except Exception as e:
            logger.error(f"Gist download [{self.gist_filename}]: {e}")
        return None

    def _upload_to_gist(self, data: dict):
        gist_id = self._ensure_gist()
        if not gist_id or not GITHUB_TOKEN:
            return
        try:
            content = json.dumps(data, ensure_ascii=False, indent=2)
            resp = req.patch(
                f"https://api.github.com/gists/{gist_id}",
                headers=self._gh_headers(),
                json={"files": {self.gist_filename: {"content": content}}},
                timeout=30,
            )
            if resp.status_code == 200:
                logger.info(f"[{self.gist_filename}] → Gist OK")
            else:
                logger.error(f"Gist update [{self.gist_filename}]: {resp.status_code} {resp.text[:200]}")
        except Exception as e:
            logger.error(f"Gist upload [{self.gist_filename}]: {e}")

    def _throttled_upload(self, data: dict):
        with self._lock:
            now = _time.time()
            # Перший аплоад — завжди одразу (без throttle)
            if self._ever_uploaded and now - self._last_upload < BACKUP_THROTTLE_SEC:
                self._pending_data = data
                return
            self._last_upload = now
            self._pending_data = None
            self._ever_uploaded = True
        self._upload_to_gist(data)

    # ── Публічні методи ──

    def load_raw(self) -> dict | None:
        # 1. Gist (основне джерело)
        data = self._download_from_gist()

        # 2. Локальний файл (резерв, наприклад якщо GitHub недоступний)
        if data is None and os.path.exists(self.local_file):
            try:
                with open(self.local_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                logger.info(f"[{self.gist_filename}] ← локальний файл (резерв)")
            except Exception as e:
                logger.error(f"Локальний файл [{self.gist_filename}]: {e}")

        return data

    def save_raw(self, data: dict):
        # 1. Локально — завжди, миттєво
        try:
            with open(self.local_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Локальне збереження [{self.gist_filename}]: {e}")

        # 2. Gist — у фоні, з throttle (перший раз — одразу)
        data_copy = json.loads(json.dumps(data, ensure_ascii=False))  # deep copy
        threading.Thread(target=self._throttled_upload, args=(data_copy,), daemon=True).start()

    def flush_pending(self):
        with self._lock:
            data = self._pending_data
            if data is None:
                return
            self._pending_data = None
            self._last_upload = _time.time()
        self._upload_to_gist(data)


# ═══════════════════════════════════════════════════════════════
#  МЕНЕДЖЕР ПЛАТЕЖІВ
# ═══════════════════════════════════════════════════════════════

class PaymentManager:
    def __init__(self):
        self.payments: dict = {}
        self._next_payment_id: int = 1
        self._storage = GistStorageManager(
            local_file=LOCAL_PAYMENTS_FILE,
            gist_filename="fit_payments.json"
        )

    def load(self):
        raw = self._storage.load_raw()
        if raw:
            self.payments         = raw.get("payments", {})
            self._next_payment_id = raw.get("next_payment_id", 1)
        logger.info(f"Клієнтів завантажено: {len(self.payments)}")

    def save(self):
        self._storage.save_raw({
            "payments": self.payments,
            "next_payment_id": self._next_payment_id,
        })

    def _key(self, uid: int) -> str:
        return str(uid)

    def upsert_client(self, uid: int, username: str, full_name: str):
        key = self._key(uid)
        if key not in self.payments:
            self.payments[key] = {
                "username": username or "", "full_name": full_name or "",
                "pending": None, "paid_workouts": [], "paid_personal": [],
                "created_at": datetime.now(TIMEZONE).isoformat(),
            }
        else:
            self.payments[key]["username"]  = username or ""
            self.payments[key]["full_name"] = full_name or ""
            self.payments[key].setdefault("paid_personal", [])
        self.save()

    def set_pending(self, uid: int, workout_id: int = None, slot_id: str = None, pay_type: str = "group") -> int:
        key = self._key(uid)
        pid = self._next_payment_id
        self._next_payment_id += 1
        if key not in self.payments:
            self.payments[key] = {
                "username": "", "full_name": "",
                "paid_workouts": [], "paid_personal": [],
                "created_at": datetime.now(TIMEZONE).isoformat()
            }
        self.payments[key]["pending"] = {
            "payment_id": pid, "type": pay_type,
            "workout_id": workout_id, "slot_id": slot_id,
        }
        self.save()
        return pid

    def get_pending(self, uid: int) -> dict | None:
        return self.payments.get(self._key(uid), {}).get("pending")

    def clear_pending(self, uid: int):
        key = self._key(uid)
        if key in self.payments:
            self.payments[key]["pending"] = None
            self.save()

    def approve(self, uid: int):
        key = self._key(uid)
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

    def has_paid_group(self, uid: int, workout_id: int) -> bool:
        return workout_id in self.payments.get(self._key(uid), {}).get("paid_workouts", [])

    def has_paid_personal(self, uid: int, slot_id: str) -> bool:
        return slot_id in self.payments.get(self._key(uid), {}).get("paid_personal", [])

    def get_paid_workout_ids(self, workout_id: int) -> list[int]:
        return [int(k) for k, v in self.payments.items() if workout_id in v.get("paid_workouts", [])]

    def get_paid_personal_users(self, slot_id: str) -> list[int]:
        return [int(k) for k, v in self.payments.items() if slot_id in v.get("paid_personal", [])]

    def all_client_ids(self) -> list[int]:
        return [int(k) for k in self.payments.keys()]

    def get_paid_users_info(self, workout_id: int) -> list[dict]:
        """Повертає список {uid, username, full_name} тих, хто оплатив workout_id."""
        result = []
        for k, v in self.payments.items():
            if workout_id in v.get("paid_workouts", []):
                result.append({
                    "uid": int(k),
                    "username": v.get("username", ""),
                    "full_name": v.get("full_name", ""),
                })
        return result


# ═══════════════════════════════════════════════════════════════
#  МЕНЕДЖЕР ГРУПОВИХ ТРЕНУВАНЬ
# ═══════════════════════════════════════════════════════════════

class WorkoutManager:
    def __init__(self):
        self.workouts: list[dict] = []
        self._next_id: int = 1
        self._storage = GistStorageManager(
            local_file=LOCAL_WORKOUTS_FILE,
            gist_filename="fit_workouts.json"
        )

    def load(self):
        raw = self._storage.load_raw()
        if raw:
            self.workouts = raw.get("workouts", [])
            self._next_id = raw.get("next_id", 1)
        logger.info(f"Групових тренувань: {len(self.workouts)}")

    def save(self):
        self._storage.save_raw({"workouts": self.workouts, "next_id": self._next_id})

    def add(self, title: str, dt: datetime, teams_link: str) -> dict:
        w = {
            "id": self._next_id, "title": title,
            "datetime": dt.isoformat(), "teams_link": teams_link,
            "channel_msg_id": None, "notified_1h": False, "notified_start": False, "notified_end": False,
        }
        self.workouts.append(w)
        self._next_id += 1
        self.save()
        return w

    def set_channel_msg(self, wid: int, msg_id: int):
        for w in self.workouts:
            if w["id"] == wid:
                w["channel_msg_id"] = msg_id
                break
        self.save()

    def delete(self, wid: int):
        self.workouts = [w for w in self.workouts if w["id"] != wid]
        self.save()

    def update_datetime(self, wid: int, new_dt: datetime):
        """Змінює час тренування, скидає прапорці нагадувань."""
        for w in self.workouts:
            if w["id"] == wid:
                w["datetime"] = new_dt.isoformat()
                w["notified_1h"] = False
                w["notified_start"] = False
                w["notified_end"] = False
                break
        self.save()

    def get(self, wid: int) -> dict | None:
        return next((w for w in self.workouts if w["id"] == wid), None)

    def upcoming(self) -> list[dict]:
        now = datetime.now(TIMEZONE).isoformat()
        return sorted([w for w in self.workouts if w["datetime"] > now], key=lambda x: x["datetime"])

    def get_pending_notifications(self) -> list[tuple[dict, str]]:
        now = datetime.now(TIMEZONE)
        result, changed = [], False
        for w in self.workouts:
            dt = datetime.fromisoformat(w["datetime"])
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=TIMEZONE)
            diff = (dt - now).total_seconds()
            if not w["notified_1h"] and 3300 <= diff <= 3900:
                result.append((w, "1h")); w["notified_1h"] = True; changed = True
            if not w["notified_start"] and -300 <= diff <= 300:
                result.append((w, "start")); w["notified_start"] = True; changed = True
            # Повідомлення після завершення тренування (60 хв тренування + 5 хв = 65 хв після старту)
            if not w.get("notified_end", False) and -4200 <= diff <= -3600:
                result.append((w, "end")); w["notified_end"] = True; changed = True
        if changed:
            self.save()
        return result

    def count_paid(self, wid: int) -> int:
        return len(pay.get_paid_workout_ids(wid))


# ═══════════════════════════════════════════════════════════════
#  МЕНЕДЖЕР ПЕРСОНАЛЬНИХ ТРЕНУВАНЬ
# ═══════════════════════════════════════════════════════════════

class PersonalManager:
    def __init__(self):
        self.slots: list[dict] = []
        self._next_id: int = 1
        self._storage = GistStorageManager(
            local_file=LOCAL_PERSONAL_FILE,
            gist_filename="fit_personal.json"
        )

    def load(self):
        raw = self._storage.load_raw()
        if raw:
            self.slots    = raw.get("slots", [])
            self._next_id = raw.get("next_id", 1)
        logger.info(f"Персональних слотів: {len(self.slots)}")

    def save(self):
        self._storage.save_raw({"slots": self.slots, "next_id": self._next_id})

    def add_slot(self, date_str: str, time_str: str, teams_link: str) -> dict:
        slot = {
            "id": f"p_{self._next_id}", "date": date_str, "time": time_str,
            "teams_link": teams_link, "booked_by": None,
            "notified_1h": False, "notified_start": False, "notified_end": False,
        }
        self.slots.append(slot)
        self._next_id += 1
        self.save()
        return slot

    def delete_slot(self, sid: str):
        self.slots = [s for s in self.slots if s["id"] != sid]
        self.save()

    def get(self, sid: str) -> dict | None:
        return next((s for s in self.slots if s["id"] == sid), None)

    def book(self, sid: str, uid: int):
        for s in self.slots:
            if s["id"] == sid:
                s["booked_by"] = uid; break
        self.save()

    def unbook(self, sid: str):
        for s in self.slots:
            if s["id"] == sid:
                s["booked_by"] = None; break
        self.save()

    def is_booked(self, sid: str) -> bool:
        s = self.get(sid)
        return s is not None and s["booked_by"] is not None

    def available_dates_in_month(self, year: int, month: int) -> set[int]:
        days, now = set(), datetime.now(TIMEZONE)
        for s in self.slots:
            # Слот недоступний тільки якщо оплачений
            if s["booked_by"] is not None and pay.has_paid_personal(s["booked_by"], s["id"]):
                continue
            try:
                d = datetime.strptime(s["date"], "%Y-%m-%d")
                if d.year == year and d.month == month:
                    dt_full = datetime.strptime(f"{s['date']} {s['time']}", "%Y-%m-%d %H:%M").replace(tzinfo=TIMEZONE)
                    if dt_full > now:
                        days.add(d.day)
            except Exception:
                pass
        return days

    def slots_for_date(self, date_str: str) -> list[dict]:
        now, result = datetime.now(TIMEZONE), []
        for s in self.slots:
            if s["date"] != date_str:
                continue
            # Слот недоступний тільки якщо оплачений
            if s["booked_by"] is not None and pay.has_paid_personal(s["booked_by"], s["id"]):
                continue
            try:
                dt_full = datetime.strptime(f"{s['date']} {s['time']}", "%Y-%m-%d %H:%M").replace(tzinfo=TIMEZONE)
                if dt_full > now:
                    result.append(s)
            except Exception:
                pass
        return sorted(result, key=lambda x: x["time"])

    def all_upcoming(self) -> list[dict]:
        now, result = datetime.now(TIMEZONE), []
        for s in self.slots:
            try:
                dt = datetime.strptime(f"{s['date']} {s['time']}", "%Y-%m-%d %H:%M").replace(tzinfo=TIMEZONE)
                if dt > now:
                    result.append(s)
            except Exception:
                pass
        return sorted(result, key=lambda x: f"{x['date']} {x['time']}")

    def get_pending_notifications(self) -> list[tuple[dict, str]]:
        now = datetime.now(TIMEZONE)
        result, changed = [], False
        for s in self.slots:
            if s["booked_by"] is None:
                continue
            if not pay.has_paid_personal(s["booked_by"], s["id"]):
                continue
            try:
                dt = datetime.strptime(f"{s['date']} {s['time']}", "%Y-%m-%d %H:%M").replace(tzinfo=TIMEZONE)
            except Exception:
                continue
            diff = (dt - now).total_seconds()
            if not s.get("notified_1h", False) and 3300 <= diff <= 3900:
                result.append((s, "1h")); s["notified_1h"] = True; changed = True
            if not s.get("notified_start", False) and -300 <= diff <= 300:
                result.append((s, "start")); s["notified_start"] = True; changed = True
            # Повідомлення після завершення (60 хв + 5 хв = 65 хв після старту)
            if not s.get("notified_end", False) and -4200 <= diff <= -3600:
                result.append((s, "end")); s["notified_end"] = True; changed = True
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

def _is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS

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

# Кнопка «Головне меню» для вставки в будь-яке повідомлення
HOME_BTN = [InlineKeyboardButton("🏠 Головне меню", callback_data="main_menu")]
HOME_MARKUP = InlineKeyboardMarkup([HOME_BTN])


def _build_calendar(year: int, month: int, available_days: set[int],
                     cb_prefix: str) -> list[list[InlineKeyboardButton]]:
    """Календар для клієнта (персональні тренування) — без навігації."""
    kb = []
    kb.append([InlineKeyboardButton(
        f"📅 {MONTH_NAMES_UK[month]} {year}", callback_data="cal_ignore"
    )])
    kb.append([InlineKeyboardButton(d, callback_data="cal_ignore") for d in DAY_HEADERS])

    for week in calendar.monthcalendar(year, month):
        row = []
        for day in week:
            if day == 0:
                row.append(InlineKeyboardButton(" ", callback_data="cal_ignore"))
            elif day in available_days:
                row.append(InlineKeyboardButton(
                    f"✅{day}", callback_data=f"{cb_prefix}_{year}_{month}_{day}"
                ))
            else:
                row.append(InlineKeyboardButton(str(day), callback_data="cal_ignore"))
        kb.append(row)
    return kb


def _build_admin_calendar(year: int, month: int, cb_prefix: str,
                           nav_prefix: str = "") -> list[list[InlineKeyboardButton]]:
    """Календар для адміна — всі майбутні дні клікабельні. Стрілки ◀️▶️ тільки якщо nav_prefix задано."""
    now = datetime.now(TIMEZONE)
    future_days = set()
    _, days_in_month = calendar.monthrange(year, month)
    for d in range(1, days_in_month + 1):
        day_date = datetime(year, month, d, 23, 59, tzinfo=TIMEZONE)
        if day_date >= now:
            future_days.add(d)

    kb = []
    if nav_prefix:
        prev_m = month - 1 if month > 1 else 12
        prev_y = year if month > 1 else year - 1
        next_m = month + 1 if month < 12 else 1
        next_y = year if month < 12 else year + 1
        kb.append([
            InlineKeyboardButton("◀️", callback_data=f"{nav_prefix}_{prev_y}_{prev_m}"),
            InlineKeyboardButton(f"📅 {MONTH_NAMES_UK[month]} {year}", callback_data="cal_ignore"),
            InlineKeyboardButton("▶️", callback_data=f"{nav_prefix}_{next_y}_{next_m}"),
        ])
    else:
        kb.append([InlineKeyboardButton(
            f"📅 {MONTH_NAMES_UK[month]} {year}", callback_data="cal_ignore"
        )])
    kb.append([InlineKeyboardButton(d, callback_data="cal_ignore") for d in DAY_HEADERS])

    for week in calendar.monthcalendar(year, month):
        row = []
        for day in week:
            if day == 0:
                row.append(InlineKeyboardButton(" ", callback_data="cal_ignore"))
            elif day in future_days:
                row.append(InlineKeyboardButton(
                    str(day), callback_data=f"{cb_prefix}_{year}_{month}_{day}"
                ))
            else:
                row.append(InlineKeyboardButton("·", callback_data="cal_ignore"))
        kb.append(row)
    return kb


def _build_time_picker(cb_prefix: str) -> list[list[InlineKeyboardButton]]:
    """Сітка годин 00:00 — 23:00 (4 колонки)."""
    kb, row = [], []
    for h in range(24):
        row.append(InlineKeyboardButton(f"{h:02d}:00", callback_data=f"{cb_prefix}_{h}"))
        if len(row) == 4:
            kb.append(row); row = []
    if row:
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
        [InlineKeyboardButton("📋 Твої тренування",              callback_data="my_status")],
        [InlineKeyboardButton("📩 Зв'язок з тренером",     callback_data="contact_trainer")],
    ]
    if _is_admin(user.id):
        kb.append([InlineKeyboardButton("⚙️ Адмін-панель", callback_data="admin_panel")])

    await update.message.reply_text(
        f"👋 Привіт, <b>{user.first_name}</b>!\n\n"
        "Це бот онлайн-тренувань. Обери тип тренування:",
        reply_markup=InlineKeyboardMarkup(kb), parse_mode="HTML"
    )


async def _edit_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user  = update.effective_user
    context.user_data.pop("adm_state", None)
    context.user_data.pop("waiting_screenshot", None)

    kb = [
        [InlineKeyboardButton("👥 Групові тренування",     callback_data="group_menu")],
        [InlineKeyboardButton("🧑‍🏫 Персональне тренування", callback_data="personal_menu")],
        [InlineKeyboardButton("📋 Твої тренування",              callback_data="my_status")],
        [InlineKeyboardButton("📩 Зв'язок з тренером",     callback_data="contact_trainer")],
    ]
    if _is_admin(user.id):
        kb.append([InlineKeyboardButton("⚙️ Адмін-панель", callback_data="admin_panel")])
    await query.edit_message_text(
        "🏠 <b>Головне меню</b>\n\nОбери дію:",
        reply_markup=InlineKeyboardMarkup(kb), parse_mode="HTML"
    )


# ═══════════════════════════════════════════════════════════════
#  ПІДМЕНЮ: ГРУПОВІ ТРЕНУВАННЯ (клієнт)
# ═══════════════════════════════════════════════════════════════

async def show_group_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.edit_message_text(
        "👥 <b>Групові тренування</b>\n\n"
        f"Вартість: <b>{GROUP_PRICE} грн</b>\n\nОберіть дію:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🗓 Календар групових тренувань", callback_data="group_schedule")],
            [InlineKeyboardButton("💳 Оплатити тренування",            callback_data="group_pay_start")],
            [InlineKeyboardButton("◀️ Назад", callback_data="main_menu")],
            [InlineKeyboardButton("🏠 Головне меню", callback_data="main_menu")],
        ]), parse_mode="HTML"
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
            ]), parse_mode="HTML"
        ); return

    lines = ["🗓 <b>Календар групових тренувань:</b>\n"]
    for w in upcoming[:5]:
        cnt = wm.count_paid(w["id"])
        lines.append(f"• <b>{w['title']}</b>\n  📅 {_fmt_dt(w)}  👥 {cnt} оплачено")

    await query.edit_message_text(
        "\n\n".join(lines),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("💳 Оплатити тренування", callback_data="group_pay_start")],
            [InlineKeyboardButton("◀️ Назад", callback_data="group_menu")],
            [InlineKeyboardButton("🏠 Головне меню", callback_data="main_menu")],
        ]), parse_mode="HTML"
    )


async def group_pay_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user  = update.effective_user
    upcoming  = wm.upcoming()
    available = [w for w in upcoming if not pay.has_paid_group(user.id, w["id"])]

    if not upcoming or not available:
        msg = "😔 Немає тренувань." if not upcoming else "✅ Всі тренування оплачено!"
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("◀️ Назад", callback_data="group_menu")],
            [InlineKeyboardButton("🏠 Головне меню", callback_data="main_menu")],
        ])); return

    kb = [[InlineKeyboardButton(f"{w['title']} — {_fmt_dt(w)}", callback_data=f"gpay_sel_{w['id']}")] for w in available]
    kb.append([InlineKeyboardButton("◀️ Назад", callback_data="group_menu")])
    kb.append([InlineKeyboardButton("🏠 Головне меню", callback_data="main_menu")])

    await query.edit_message_text(
        f"💳 <b>Оплата групового тренування</b>\n\nВартість: <b>{GROUP_PRICE} грн</b>\n\nОберіть:",
        reply_markup=InlineKeyboardMarkup(kb), parse_mode="HTML"
    )


async def group_pay_details(update: Update, context: ContextTypes.DEFAULT_TYPE, wid: int):
    query   = update.callback_query
    user    = update.effective_user
    workout = wm.get(wid)
    if not workout:
        await query.answer("Не знайдено", show_alert=True); return

    pid = pay.set_pending(user.id, workout_id=wid, pay_type="group")
    context.user_data.update(waiting_screenshot=True, payment_id=pid, pay_type="group", workout_id=wid)

    await query.edit_message_text(
        f"💳 <b>Оплата групового тренування</b>\n\n"
        f"🏋️ {workout['title']}\n📅 {_fmt_dt(workout)}\n\n"
        f"Сума: <b>{GROUP_PRICE} грн</b>\n\n"
        f"Переказати на картку:\n<code>{CARD_NUMBER}</code>\n"
        f"Отримувач: <b>{CARD_OWNER}</b>\n\n"
        f"Після переказу надішліть 📸 <b>скріншот підтвердження</b>.\n"
        f"<i>⏱ Очікуйте підтвердження вашої участі у тренуванні.</i>",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("◀️ Назад", callback_data="group_pay_start"),
             InlineKeyboardButton("🏠 Меню", callback_data="main_menu")],
        ]), parse_mode="HTML"
    )


# ═══════════════════════════════════════════════════════════════
#  ПІДМЕНЮ: ПЕРСОНАЛЬНЕ ТРЕНУВАННЯ (клієнт)
# ═══════════════════════════════════════════════════════════════

async def show_personal_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.edit_message_text(
        "🧑‍🏫 <b>Персональне тренування</b>\n\n"
        f"Вартість: <b>{PERSONAL_PRICE} грн</b>\n"
        "Формат: 1-на-1 з тренером через Google Meet\n\n"
        "Оберіть дату та час у календарі, а потім оплатіть:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📅 Обрати та оплатити",  callback_data="personal_calendar")],
            [InlineKeyboardButton("◀️ Назад", callback_data="main_menu")],
            [InlineKeyboardButton("🏠 Головне меню", callback_data="main_menu")],
        ]), parse_mode="HTML"
    )


async def show_personal_calendar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    now   = datetime.now(TIMEZONE)
    available = pm.available_dates_in_month(now.year, now.month)

    cal_kb = _build_calendar(now.year, now.month, available, cb_prefix="cal_day")
    cal_kb.append([InlineKeyboardButton("◀️ Назад", callback_data="personal_menu")])
    cal_kb.append([InlineKeyboardButton("🏠 Головне меню", callback_data="main_menu")])

    hint = "Дні з ✅ — доступні. Натисніть на день." if available else "На цей місяць вільних слотів немає."
    await query.edit_message_text(
        f"📅 <b>Запис на персональне тренування</b>\n\n{hint}",
        reply_markup=InlineKeyboardMarkup(cal_kb), parse_mode="HTML"
    )


async def show_day_slots(update: Update, context: ContextTypes.DEFAULT_TYPE, year: int, month: int, day: int):
    query    = update.callback_query
    date_str = f"{year}-{month:02d}-{day:02d}"
    slots    = pm.slots_for_date(date_str)
    if not slots:
        await query.answer("На цей день слотів немає", show_alert=True); return

    d_fmt = f"{day:02d}.{month:02d}.{year}"
    kb = [[InlineKeyboardButton(f"🕐 {s['time']} — оплатити", callback_data=f"pslot_book_{s['id']}")] for s in slots]
    kb.append([InlineKeyboardButton("◀️ До календаря", callback_data="personal_calendar")])
    kb.append([InlineKeyboardButton("🏠 Головне меню", callback_data="main_menu")])

    await query.edit_message_text(
        f"📅 <b>{d_fmt}</b> — доступні слоти:\n\nВартість: <b>{PERSONAL_PRICE} грн</b>",
        reply_markup=InlineKeyboardMarkup(kb), parse_mode="HTML"
    )


async def book_personal_slot(update: Update, context: ContextTypes.DEFAULT_TYPE, sid: str):
    query = update.callback_query
    user  = update.effective_user
    slot  = pm.get(sid)
    if not slot:
        await query.answer("Слот не знайдено", show_alert=True); return
    # Перевірка: чи вже оплачений іншим клієнтом
    if slot["booked_by"] is not None and pay.has_paid_personal(slot["booked_by"], sid):
        await query.answer("Цей слот вже оплачений іншим клієнтом", show_alert=True); return

    # Одразу переходимо до оплати (без бронювання)
    pid = pay.set_pending(user.id, slot_id=sid, pay_type="personal")
    context.user_data.update(waiting_screenshot=True, payment_id=pid, pay_type="personal", slot_id=sid)

    await query.edit_message_text(
        f"💳 <b>Оплата персонального тренування</b>\n\n"
        f"📅 {_fmt_slot(slot)}\nСума: <b>{PERSONAL_PRICE} грн</b>\n\n"
        f"Переказати на картку:\n<code>{CARD_NUMBER}</code>\n"
        f"Отримувач: <b>{CARD_OWNER}</b>\n\n"
        f"Надішліть 📸 <b>скріншот підтвердження</b>.\n"
        f"<i>⏱ Перевірка до 1–3 годин.</i>",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("◀️ Назад", callback_data="personal_calendar"),
             InlineKeyboardButton("🏠 Меню", callback_data="main_menu")],
        ]), parse_mode="HTML"
    )


async def personal_pay_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Перенаправляє до календаря для вибору слоту та оплати."""
    await show_personal_calendar(update, context)


async def personal_pay_details(update: Update, context: ContextTypes.DEFAULT_TYPE, sid: str):
    query = update.callback_query
    user  = update.effective_user
    slot  = pm.get(sid)
    if not slot:
        await query.answer("Слот не знайдено", show_alert=True); return

    pid = pay.set_pending(user.id, slot_id=sid, pay_type="personal")
    context.user_data.update(waiting_screenshot=True, payment_id=pid, pay_type="personal", slot_id=sid)

    await query.edit_message_text(
        f"💳 <b>Оплата персонального тренування</b>\n\n"
        f"📅 {_fmt_slot(slot)}\nСума: <b>{PERSONAL_PRICE} грн</b>\n\n"
        f"Переказати на картку:\n<code>{CARD_NUMBER}</code>\n"
        f"Отримувач: <b>{CARD_OWNER}</b>\n\n"
        f"Надішліть 📸 <b>скріншот підтвердження</b>.\n"
        f"<i>⏱ Перевірка до 1–3 годин.</i>",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("◀️ Назад", callback_data="personal_calendar"),
             InlineKeyboardButton("🏠 Меню", callback_data="main_menu")],
        ]), parse_mode="HTML"
    )


# ═══════════════════════════════════════════════════════════════
#  МІЙ СТАТУС
# ═══════════════════════════════════════════════════════════════

async def show_my_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user  = update.effective_user
    upcoming = wm.upcoming()

    lines = [f"👤 <b>Статус: {user.full_name}</b>\n"]

    lines.append("<b>👥 Групові тренування:</b>")
    if not upcoming:
        lines.append("  Запланованих немає.")
    else:
        for w in upcoming[:5]:
            if pay.has_paid_group(user.id, w["id"]):
                lines.append(f"  ✅ {w['title']} ({_fmt_dt(w)})")
                lines.append(f"  🔗 <a href=\"{w['teams_link']}\">Підключитись</a>")
            else:
                lines.append(f"  ❌ {w['title']} ({_fmt_dt(w)})")

    lines.append("\n<b>🧑‍🏫 Персональні тренування:</b>")
    user_personal = [s for s in pm.all_upcoming()
                     if s["booked_by"] == user.id and pay.has_paid_personal(user.id, s["id"])]
    if not user_personal:
        lines.append("  Записів немає.")
    else:
        for s in user_personal[:5]:
            lines.append(f"  ✅ {_fmt_slot(s)}")
            lines.append(f"  🔗 <a href=\"{s['teams_link']}\">Підключитись</a>")

    await query.edit_message_text(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("👥 Групові", callback_data="group_menu"),
             InlineKeyboardButton("🧑‍🏫 Персональні", callback_data="personal_menu")],
            [InlineKeyboardButton("🏠 Головне меню", callback_data="main_menu")],
        ]), parse_mode="HTML", disable_web_page_preview=True
    )


# ═══════════════════════════════════════════════════════════════
#  ОБРОБНИК ФОТО
# ═══════════════════════════════════════════════════════════════

async def photo_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user

    # ── Адмін: фото для анонсу групового тренування (крок 6/6) ──
    if context.user_data.get("adm_state") == "group_photo" and _is_admin(user.id):
        photo = update.message.photo[-1]
        context.user_data["new_photo"] = photo.file_id

        async def _reply(text, markup, pm):
            await update.message.reply_text(text, reply_markup=markup, parse_mode=pm)
        await _finalize_group_workout(context, _reply)
        return

    # ── Адмін: фото для анонсу персонального тренування (крок 5/5) ──
    if context.user_data.get("adm_state") == "personal_photo" and _is_admin(user.id):
        photo = update.message.photo[-1]
        context.user_data["new_personal_photo"] = photo.file_id

        async def _reply_p(text, markup, pm):
            await update.message.reply_text(text, reply_markup=markup, parse_mode=pm)
        await _finalize_personal_slot(context, _reply_p)
        return

    if not context.user_data.get("waiting_screenshot"):
        return
    payment_id = context.user_data.get("payment_id")
    pay_type   = context.user_data.get("pay_type", "group")
    if not payment_id:
        await update.message.reply_text("⚠️ Немає активного платежу.",
                                       reply_markup=HOME_MARKUP); return

    photo = update.message.photo[-1]
    context.user_data["waiting_screenshot"] = False

    await update.message.reply_text(
        "✅ <b>Скріншот отримано!</b>\nПлатіж на перевірці (до 1–3 годин) 🙏\n\n"
        "<i>⚠️ Зверніть увагу: у разі пропуску оплаченого тренування "
        "з особистих причин кошти не повертаються.</i>",
        reply_markup=HOME_MARKUP, parse_mode="HTML"
    )

    if pay_type == "group":
        wid = context.user_data.get("workout_id")
        workout = wm.get(wid) if wid else None
        item_name = workout["title"] if workout else "—"
        dt_str = _fmt_dt(workout) if workout else "—"
        price = GROUP_PRICE
        cb_ok = f"adm_ok_{payment_id}_{user.id}_{wid or 0}_group"
    else:
        sid = context.user_data.get("slot_id")
        slot = pm.get(sid) if sid else None
        item_name = f"Персональне ({_fmt_slot(slot)})" if slot else "—"
        dt_str = _fmt_slot(slot) if slot else "—"
        price = PERSONAL_PRICE
        cb_ok = f"adm_ok_{payment_id}_{user.id}_{sid or 0}_personal"

    caption = (
        f"💳 <b>Новий платіж!</b>\n\n"
        f"👤 {user.full_name}{' (@' + user.username + ')' if user.username else ''}\n"
        f"🆔 <code>{user.id}</code>\n"
        f"🏋️ {item_name}\n📅 {dt_str}\n"
        f"💰 {price} грн ({pay_type})\n"
        f"🕐 {datetime.now(TIMEZONE).strftime('%d.%m.%Y %H:%M')}\n#pay{payment_id}"
    )
    admin_kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Підтвердити", callback_data=cb_ok),
        InlineKeyboardButton("❌ Відхилити", callback_data=f"adm_no_{payment_id}_{user.id}"),
    ]])
    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_photo(
                chat_id=admin_id, photo=photo.file_id, caption=caption,
                reply_markup=admin_kb, parse_mode="HTML"
            )
        except Exception as e:
            logger.warning(f"Не вдалось надіслати платіж адміну {admin_id}: {e}")


# ═══════════════════════════════════════════════════════════════
#  АДМІН-ПАНЕЛЬ
# ═══════════════════════════════════════════════════════════════

async def show_admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not _is_admin(update.effective_user.id):
        await query.answer("⛔", show_alert=True); return
    context.user_data.pop("adm_state", None)

    ug = wm.upcoming()
    up = pm.all_upcoming()
    total = len(pay.all_client_ids())

    await query.edit_message_text(
        f"⚙️ <b>Адмін-панель</b>\n\n"
        f"👥 Клієнтів: {total}\n"
        f"🏋️ Групових тренувань: {len(ug)}\n"
        f"🧘 Персональних слотів: {len(up)}\n\nОберіть дію:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🏋️➕ Додати групове",        callback_data="adm_add_group")],
            [InlineKeyboardButton("🏋️📋 Список групових",      callback_data="adm_list_group")],
            [InlineKeyboardButton("🧘➕ Додати персональний",   callback_data="adm_add_personal")],
            [InlineKeyboardButton("🧘📋 Список персональних",  callback_data="adm_list_personal")],
            [InlineKeyboardButton("📢 Розіслати всім",          callback_data="adm_bcast")],
            [InlineKeyboardButton("🏠 Головне меню",            callback_data="main_menu")],
        ]), parse_mode="HTML"
    )


# ── Адмін: додати ГРУПОВЕ тренування ──
# Крок 1: назва (текст) → Крок 2: дата (календар) → Крок 3: час (сітка) → Крок 4: Teams-лінк (текст)

async def adm_add_group_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not _is_admin(update.effective_user.id):
        await query.answer("⛔", show_alert=True); return
    await query.edit_message_text(
        "🏋️➕ <b>Нове групове тренування</b> — крок 1/6\n\nОберіть <b>назву</b>:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🧘‍♀️ Pilates",   callback_data="acg_title_Pilates")],
            [InlineKeyboardButton("🤸 Stretching", callback_data="acg_title_Stretching")],
            [InlineKeyboardButton("🌸 Women's health", callback_data="acg_title_Women's health")],
            [InlineKeyboardButton("💆 MFR",            callback_data="acg_title_MFR")],
            [InlineKeyboardButton("❌ Скасувати",  callback_data="admin_panel")],
        ]), parse_mode="HTML"
    )


async def _adm_group_show_calendar(query, context):
    now = datetime.now(TIMEZONE)
    cal_kb = _build_admin_calendar(now.year, now.month, cb_prefix="acg_day")
    cal_kb.append([InlineKeyboardButton("❌ Скасувати", callback_data="admin_panel")])
    title = context.user_data.get("new_title", "—")
    await query.edit_message_text(
        f"🏋️➕ <b>Нове групове тренування</b> — крок 2/6\n\n"
        f"Назва: <b>{title}</b>\n\nОберіть <b>дату</b>:",
        reply_markup=InlineKeyboardMarkup(cal_kb), parse_mode="HTML"
    )


async def _adm_group_show_time(query, context):
    title    = context.user_data.get("new_title", "—")
    date_str = context.user_data.get("new_date", "")
    d_fmt    = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d.%m.%Y")
    time_kb  = _build_time_picker("acg_time")
    time_kb.append([InlineKeyboardButton("❌ Скасувати", callback_data="admin_panel")])
    await query.edit_message_text(
        f"🏋️➕ <b>Нове групове тренування</b> — крок 3/6\n\n"
        f"Назва: <b>{title}</b>\n📅 Дата: <b>{d_fmt}</b>\n\nОберіть <b>час</b>:",
        reply_markup=InlineKeyboardMarkup(time_kb), parse_mode="HTML"
    )


async def _finalize_group_workout(context, reply_func):
    """
    Створює групове тренування та публікує анонс у канал.
    reply_func — async callable(text, reply_markup, parse_mode) для відповіді адміну.
    """
    title    = context.user_data.pop("new_title", "Тренування")
    date_str = context.user_data.pop("new_date", "")
    time_str = context.user_data.pop("new_time", "00:00")
    link     = context.user_data.pop("new_link", "")
    desc     = context.user_data.pop("new_desc", "")
    photo_id = context.user_data.pop("new_photo", "")
    context.user_data["adm_state"] = None

    dt = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M").replace(tzinfo=TIMEZONE)
    workout = wm.add(title, dt, link)

    bot_me = await context.bot.get_me()

    if desc:
        channel_text = (
            f"🏋️ <b>Онлайн-тренування!</b>\n\n📌 {title}\n"
            f"📅 {dt.strftime('%d.%m.%Y о %H:%M')}\n\n"
            f"{desc}\n\n"
            f"💳 Вартість: <b>{GROUP_PRICE} грн</b>\n\n"
            f"👉 @{bot_me.username} — оплата та посилання"
        )
    else:
        channel_text = (
            f"🏋️ <b>Онлайн-тренування!</b>\n\n📌 {title}\n"
            f"📅 {dt.strftime('%d.%m.%Y о %H:%M')}\n\n"
            f"💳 Вартість: <b>{GROUP_PRICE} грн</b>\n\n"
            f"👉 @{bot_me.username} — оплата та посилання"
        )

    ch_note = ""
    try:
        if photo_id:
            msg = await context.bot.send_photo(
                chat_id=CHANNEL_ID, photo=photo_id,
                caption=channel_text, parse_mode="HTML",
                message_thread_id=CHANNEL_TOPIC_ID or None,
            )
        else:
            msg = await context.bot.send_message(
                chat_id=CHANNEL_ID, text=channel_text, parse_mode="HTML",
                message_thread_id=CHANNEL_TOPIC_ID or None,
            )
        wm.set_channel_msg(workout["id"], msg.message_id)
        ch_note = f"✅ Анонс у {CHANNEL_ID}"
    except Exception as e:
        logger.error(f"Канал: {e}"); ch_note = f"⚠️ Канал: {e}"

    await reply_func(
        f"✅ <b>Групове тренування додано!</b>\n\n🏋️ {title}\n"
        f"📅 {dt.strftime('%d.%m.%Y о %H:%M')}\n\n{ch_note}",
        InlineKeyboardMarkup([
            [InlineKeyboardButton("🏋️📋 Список", callback_data="adm_list_group")],
            [InlineKeyboardButton("⚙️ Панель", callback_data="admin_panel")],
        ]),
        "HTML"
    )


# ── Адмін: додати ПЕРСОНАЛЬНИЙ слот ──
# Крок 1: дата → Крок 2: час → Крок 3: лінк → Крок 4: опис → Крок 5: фото

async def _finalize_personal_slot(context, reply_func):
    """
    Створює персональний слот та публікує анонс у канал (без Meet-лінку).
    """
    date_str = context.user_data.pop("new_personal_date", "")
    time_str = context.user_data.pop("new_personal_time", "00:00")
    link     = context.user_data.pop("new_personal_link", "")
    desc     = context.user_data.pop("new_personal_desc", "")
    photo_id = context.user_data.pop("new_personal_photo", "")
    context.user_data["adm_state"] = None

    slot = pm.add_slot(date_str, time_str, link)
    d_fmt = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d.%m.%Y")

    bot_me = await context.bot.get_me()

    channel_text = (
        f"🧑‍🏫 <b>Персональне тренування!</b>\n\n"
        f"📅 {d_fmt} о {time_str}\n\n"
    )
    if desc:
        channel_text += f"{desc}\n\n"
    channel_text += (
        f"💳 Вартість: <b>{PERSONAL_PRICE} грн</b>\n\n"
        f"👉 @{bot_me.username} — запис та оплата"
    )

    ch_note = ""
    try:
        if photo_id:
            msg = await context.bot.send_photo(
                chat_id=CHANNEL_ID, photo=photo_id,
                caption=channel_text, parse_mode="HTML",
                message_thread_id=CHANNEL_TOPIC_ID or None,
            )
        else:
            msg = await context.bot.send_message(
                chat_id=CHANNEL_ID, text=channel_text, parse_mode="HTML",
                message_thread_id=CHANNEL_TOPIC_ID or None,
            )
        ch_note = f"✅ Анонс у {CHANNEL_ID}"
    except Exception as e:
        logger.error(f"Канал (personal): {e}"); ch_note = f"⚠️ Канал: {e}"

    await reply_func(
        f"✅ <b>Персональний слот додано!</b>\n\n📅 {d_fmt} о {time_str}\n🆔 {slot['id']}\n\n{ch_note}",
        InlineKeyboardMarkup([
            [InlineKeyboardButton("🧘📋 Список", callback_data="adm_list_personal")],
            [InlineKeyboardButton("🧘➕ Ще один", callback_data="adm_add_personal")],
            [InlineKeyboardButton("⚙️ Панель", callback_data="admin_panel")],
        ]),
        "HTML"
    )

async def _adm_personal_show_calendar(query, context):
    now = datetime.now(TIMEZONE)
    cal_kb = _build_admin_calendar(now.year, now.month, cb_prefix="acp_day")
    cal_kb.append([InlineKeyboardButton("❌ Скасувати", callback_data="admin_panel")])
    await query.edit_message_text(
        "🧘➕ <b>Новий персональний слот</b> — крок 1/5\n\nОберіть <b>дату</b>:",
        reply_markup=InlineKeyboardMarkup(cal_kb), parse_mode="HTML"
    )


async def _adm_personal_show_time(query, context):
    date_str = context.user_data.get("new_personal_date", "")
    d_fmt    = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d.%m.%Y")
    time_kb  = _build_time_picker("acp_time")
    time_kb.append([InlineKeyboardButton("❌ Скасувати", callback_data="admin_panel")])
    await query.edit_message_text(
        f"🧘➕ <b>Новий персональний слот</b> — крок 2/5\n\n"
        f"📅 Дата: <b>{d_fmt}</b>\n\nОберіть <b>час</b>:",
        reply_markup=InlineKeyboardMarkup(time_kb), parse_mode="HTML"
    )


# ── Адмін: списки ──

async def adm_list_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not _is_admin(update.effective_user.id):
        await query.answer("⛔", show_alert=True); return
    upcoming = wm.upcoming()
    if not upcoming:
        await query.edit_message_text("🏋️📋 Групових тренувань немає.", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🏋️➕ Додати", callback_data="adm_add_group")],
            [InlineKeyboardButton("◀️ Назад", callback_data="admin_panel")],
        ])); return

    lines = ["🏋️📋 <b>Групові тренування:</b>\n"]
    kb = []
    for w in upcoming:
        cnt = wm.count_paid(w["id"])
        lines.append(f"#{w['id']} <b>{w['title']}</b> — {_fmt_dt(w)} | 👥{cnt}")
        kb.append([
            InlineKeyboardButton(f"🕐 Час #{w['id']}", callback_data=f"adm_etime_{w['id']}"),
            InlineKeyboardButton(f"👥 Оплатили #{w['id']}", callback_data=f"adm_paid_{w['id']}"),
            InlineKeyboardButton(f"🗑", callback_data=f"adm_del_g_{w['id']}"),
        ])
    kb.append([InlineKeyboardButton("◀️ Назад", callback_data="admin_panel")])
    await query.edit_message_text("\n".join(lines), reply_markup=InlineKeyboardMarkup(kb), parse_mode="HTML")


async def adm_show_paid(update: Update, context: ContextTypes.DEFAULT_TYPE, wid: int):
    """Показує розгорнутий список оплативших для конкретного тренування."""
    query   = update.callback_query
    workout = wm.get(wid)
    if not workout:
        await query.answer("Не знайдено", show_alert=True); return

    users = pay.get_paid_users_info(wid)
    lines = [f"👥 <b>Оплатили тренування #{wid}</b>\n"
             f"🏋️ {workout['title']} — {_fmt_dt(workout)}\n"
             f"Всього: <b>{len(users)}</b>\n"]
    if not users:
        lines.append("<i>Поки ніхто не оплатив.</i>")
    else:
        for i, u in enumerate(users, 1):
            name = u["full_name"] or "—"
            uname = f" (@{u['username']})" if u["username"] else ""
            lines.append(f"{i}. {name}{uname} | <code>{u['uid']}</code>")

    await query.edit_message_text(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("◀️ До списку", callback_data="adm_list_group")],
            [InlineKeyboardButton("⚙️ Панель", callback_data="admin_panel")],
        ]), parse_mode="HTML"
    )


async def adm_edit_time_start(update: Update, context: ContextTypes.DEFAULT_TYPE, wid: int):
    """Крок 1: показує календар для вибору нової дати тренування."""
    query   = update.callback_query
    workout = wm.get(wid)
    if not workout:
        await query.answer("Не знайдено", show_alert=True); return

    context.user_data["edit_wid"] = wid
    context.user_data["adm_state"] = "edit_group_cal"

    now = datetime.now(TIMEZONE)
    cal_kb = _build_admin_calendar(now.year, now.month, cb_prefix="edt_day")
    cal_kb.append([InlineKeyboardButton("❌ Скасувати", callback_data="adm_list_group")])

    await query.edit_message_text(
        f"🕐 <b>Змінити час тренування</b>\n\n"
        f"🏋️ {workout['title']}\n📅 Поточний: <b>{_fmt_dt(workout)}</b>\n\n"
        "Оберіть <b>нову дату</b>:",
        reply_markup=InlineKeyboardMarkup(cal_kb), parse_mode="HTML"
    )


async def adm_edit_time_show_hours(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Крок 2: показує сітку годин для вибору нового часу."""
    query    = update.callback_query
    wid      = context.user_data.get("edit_wid")
    workout  = wm.get(wid) if wid else None
    date_str = context.user_data.get("edit_new_date", "")
    d_fmt    = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d.%m.%Y")

    time_kb = _build_time_picker("edt_time")
    time_kb.append([InlineKeyboardButton("❌ Скасувати", callback_data="adm_list_group")])

    title = workout["title"] if workout else "—"
    await query.edit_message_text(
        f"🕐 <b>Змінити час тренування</b>\n\n"
        f"🏋️ {title}\n📅 Нова дата: <b>{d_fmt}</b>\n\nОберіть <b>час</b>:",
        reply_markup=InlineKeyboardMarkup(time_kb), parse_mode="HTML"
    )


async def adm_list_personal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not _is_admin(update.effective_user.id):
        await query.answer("⛔", show_alert=True); return
    upcoming = pm.all_upcoming()
    if not upcoming:
        await query.edit_message_text("🧘📋 Персональних слотів немає.", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🧘➕ Додати", callback_data="adm_add_personal")],
            [InlineKeyboardButton("◀️ Назад", callback_data="admin_panel")],
        ])); return

    lines = ["🧘📋 <b>Персональні слоти:</b>\n"]
    kb = []
    for s in upcoming:
        st = "🔒 зайнято" if s["booked_by"] else "🟢 вільно"
        paid = " ✅" if s["booked_by"] and pay.has_paid_personal(s["booked_by"], s["id"]) else ""
        lines.append(f"{s['id']}: <b>{_fmt_slot(s)}</b> — {st}{paid}")
        kb.append([InlineKeyboardButton(f"🗑 {s['id']} ({s['date']} {s['time']})", callback_data=f"adm_del_p_{s['id']}")])
    kb.append([InlineKeyboardButton("◀️ Назад", callback_data="admin_panel")])
    await query.edit_message_text("\n".join(lines), reply_markup=InlineKeyboardMarkup(kb), parse_mode="HTML")


async def adm_bcast_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not _is_admin(update.effective_user.id):
        await query.answer("⛔", show_alert=True); return
    context.user_data["adm_state"] = "bcast"
    await query.edit_message_text(
        "📢 <b>Розсилка</b>\n\nВведіть текст для <b>всіх клієнтів</b>:\n"
        "<i>HTML: &lt;b&gt;, &lt;i&gt;</i>",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("❌ Скасувати", callback_data="admin_panel")
        ]]), parse_mode="HTML"
    )


# ═══════════════════════════════════════════════════════════════
#  ОБРОБНИК ТЕКСТУ (для адміна: назва, Teams-лінк, розсилка)
# ═══════════════════════════════════════════════════════════════

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user  = update.effective_user
    text  = update.message.text.strip()
    state = context.user_data.get("adm_state")

    if context.user_data.get("waiting_screenshot"):
        await update.message.reply_text("📸 Надішліть <b>фото</b>, а не текст.",
                                       reply_markup=HOME_MARKUP, parse_mode="HTML")
        return

    if not _is_admin(user.id) or not state:
        return

    # ── Групове: Google Meet-лінк (крок 4/6) → перехід до опису ──
    if state == "group_link":
        if not text.startswith("http"):
            await update.message.reply_text("⚠️ Посилання має починатися з https://"); return

        context.user_data["new_link"] = text
        context.user_data["adm_state"] = "group_desc"

        title    = context.user_data.get("new_title", "—")
        date_str = context.user_data.get("new_date", "")
        time_str = context.user_data.get("new_time", "00:00")
        d_fmt    = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d.%m.%Y")

        await update.message.reply_text(
            f"🏋️➕ <b>Нове групове тренування</b> — крок 5/6\n\n"
            f"Назва: <b>{title}</b>\n📅 {d_fmt} о <b>{time_str}</b>\n"
            f"🔗 Лінк: ✅\n\n"
            f"Введіть <b>опис для анонсу</b> в каналі (довільний текст)\n"
            f"або натисніть <b>Пропустити</b> для стандартного шаблону:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⏭ Пропустити", callback_data="acg_skip_desc")],
                [InlineKeyboardButton("❌ Скасувати",  callback_data="admin_panel")],
            ]), parse_mode="HTML"
        )
        return

    # ── Групове: опис для анонсу (крок 5/6) → перехід до фото ──
    if state == "group_desc":
        context.user_data["new_desc"] = text
        context.user_data["adm_state"] = "group_photo"

        title    = context.user_data.get("new_title", "—")
        date_str = context.user_data.get("new_date", "")
        time_str = context.user_data.get("new_time", "00:00")
        d_fmt    = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d.%m.%Y")

        await update.message.reply_text(
            f"🏋️➕ <b>Нове групове тренування</b> — крок 6/6\n\n"
            f"Назва: <b>{title}</b>\n📅 {d_fmt} о <b>{time_str}</b>\n"
            f"🔗 Лінк: ✅\n📝 Опис: ✅\n\n"
            f"Надішліть <b>фото для анонсу</b>\n"
            f"або натисніть <b>Пропустити</b>:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⏭ Пропустити", callback_data="acg_skip_photo")],
                [InlineKeyboardButton("❌ Скасувати",  callback_data="admin_panel")],
            ]), parse_mode="HTML"
        )
        return

    # ── Групове: очікується фото, а не текст ──
    if state == "group_photo":
        await update.message.reply_text(
            "📸 Надішліть <b>фото</b>, а не текст.\n"
            "Або натисніть <b>Пропустити</b> вище ⬆️",
            parse_mode="HTML"
        )
        return

    # ── Персональне: Google Meet-лінк (крок 3/5) → перехід до опису ──
    if state == "personal_link":
        if not text.startswith("http"):
            await update.message.reply_text("⚠️ Посилання має починатися з https://"); return

        context.user_data["new_personal_link"] = text
        context.user_data["adm_state"] = "personal_desc"

        date_str = context.user_data.get("new_personal_date", "")
        time_str = context.user_data.get("new_personal_time", "00:00")
        d_fmt    = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d.%m.%Y")

        await update.message.reply_text(
            f"🧘➕ <b>Новий персональний слот</b> — крок 4/5\n\n"
            f"📅 {d_fmt} о <b>{time_str}</b>\n"
            f"🔗 Лінк: ✅\n\n"
            f"Введіть <b>опис для анонсу</b> в каналі (довільний текст)\n"
            f"або натисніть <b>Пропустити</b> для стандартного шаблону:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⏭ Пропустити", callback_data="acp_skip_desc")],
                [InlineKeyboardButton("❌ Скасувати",  callback_data="admin_panel")],
            ]), parse_mode="HTML"
        )
        return

    # ── Персональне: опис для анонсу (крок 4/5) → перехід до фото ──
    if state == "personal_desc":
        context.user_data["new_personal_desc"] = text
        context.user_data["adm_state"] = "personal_photo"

        date_str = context.user_data.get("new_personal_date", "")
        time_str = context.user_data.get("new_personal_time", "00:00")
        d_fmt    = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d.%m.%Y")

        await update.message.reply_text(
            f"🧘➕ <b>Новий персональний слот</b> — крок 5/5\n\n"
            f"📅 {d_fmt} о <b>{time_str}</b>\n"
            f"🔗 Лінк: ✅\n📝 Опис: ✅\n\n"
            f"Надішліть <b>фото для анонсу</b>\n"
            f"або натисніть <b>Пропустити</b>:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⏭ Пропустити", callback_data="acp_skip_photo")],
                [InlineKeyboardButton("❌ Скасувати",  callback_data="admin_panel")],
            ]), parse_mode="HTML"
        )
        return

    # ── Персональне: очікується фото, а не текст ──
    if state == "personal_photo":
        await update.message.reply_text(
            "📸 Надішліть <b>фото</b>, а не текст.\n"
            "Або натисніть <b>Пропустити</b> вище ⬆️",
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
                await context.bot.send_message(uid, f"📢 <b>Від тренера:</b>\n\n{text}",
                                               reply_markup=HOME_MARKUP, parse_mode="HTML")
                sent += 1; await asyncio.sleep(0.05)
            except Exception:
                failed += 1
        await update.message.reply_text(
            f"📢 <b>Розсилку завершено</b>\n✅ {sent}  ❌ {failed}", parse_mode="HTML"
        )


# ═══════════════════════════════════════════════════════════════
#  КОМАНДА /add_workout
# ═══════════════════════════════════════════════════════════════

async def cmd_add_workout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update.effective_user.id):
        return
    await update.message.reply_text(
        "🏋️➕ <b>Нове групове тренування</b> — крок 1/6\n\nОберіть <b>назву</b>:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🧘‍♀️ Pilates",   callback_data="acg_title_Pilates")],
            [InlineKeyboardButton("🤸 Stretching", callback_data="acg_title_Stretching")],
            [InlineKeyboardButton("🌸 Women's health", callback_data="acg_title_Women's health")],
            [InlineKeyboardButton("💆 MFR",            callback_data="acg_title_MFR")],
            [InlineKeyboardButton("❌ Скасувати",  callback_data="admin_panel")],
        ]),
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

    if data == "cal_ignore":
        return

    # ── Навігація ──
    if data == "main_menu":
        await _edit_main_menu(update, context); return

    if data == "contact_trainer":
        await query.edit_message_text(
            "📩 <b>Зв'язок з тренером</b>\n\n"
            "Якщо у вас є питання, будь ласка, звертайтесь "
            "<a href=\"https://www.instagram.com/vviolas13/\">сюди</a> 😊",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🏠 Головне меню", callback_data="main_menu")],
            ]),
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
        return

    # ── Групові (клієнт) ──
    if data == "group_menu":
        await show_group_menu(update, context); return
    if data == "group_schedule":
        await show_group_schedule(update, context); return
    if data == "group_pay_start":
        await group_pay_start(update, context); return
    if data.startswith("gpay_sel_"):
        await group_pay_details(update, context, int(data[9:])); return

    # ── Персональні (клієнт) ──
    if data == "personal_menu":
        await show_personal_menu(update, context); return
    if data == "personal_calendar":
        await show_personal_calendar(update, context); return
    if data.startswith("cal_day_"):
        _, _, y, m, d = data.split("_")
        await show_day_slots(update, context, int(y), int(m), int(d)); return
    if data.startswith("pslot_book_"):
        await book_personal_slot(update, context, data[11:]); return
    if data == "personal_pay_start":
        await personal_pay_start(update, context); return
    if data.startswith("ppay_slot_"):
        await personal_pay_details(update, context, data[10:]); return

    # ── Статус ──
    if data == "my_status":
        await show_my_status(update, context); return

    # ═══ АДМІН: підтвердити/відхилити оплату ═══
    if data.startswith("adm_ok_"):
        if not _is_admin(user.id):
            await query.answer("⛔", show_alert=True); return
        parts = data.split("_")  # adm_ok_{pid}_{uid}_{item}_{type}
        client_uid = int(parts[3])
        item_id    = parts[4]
        pay_type   = parts[5] if len(parts) > 5 else "group"

        pay.approve(client_uid)
        try:
            await query.edit_message_caption(
                caption=(query.message.caption or "") + "\n\n✅ <b>ПІДТВЕРДЖЕНО</b>", parse_mode="HTML")
        except Exception:
            pass

        REFUND_NOTE = "\n\n<i>⚠️ У разі пропуску оплаченого тренування з особистих причин кошти не повертаються.</i>"

        if pay_type == "group":
            wid = int(item_id) if item_id != "0" else None
            workout = wm.get(wid) if wid else None
            if workout:
                txt = (f"🎉 <b>Оплату підтверджено!</b>\n\n🏋️ <b>{workout['title']}</b>\n"
                       f"📅 {_fmt_dt(workout)}\n\n🔗 {workout['teams_link']}\n\n"
                       f"<i>До зустрічі! 💪</i>{REFUND_NOTE}")
            else:
                txt = f"🎉 <b>Оплату підтверджено!</b>{REFUND_NOTE}"
        else:
            slot = pm.get(item_id)
            if slot:
                # Закріплюємо слот за клієнтом після підтвердження оплати
                pm.book(item_id, client_uid)
                txt = (f"🎉 <b>Оплату підтверджено!</b>\n\n🧑‍🏫 Персональне\n"
                       f"📅 {_fmt_slot(slot)}\n\n🔗 {slot['teams_link']}\n\n"
                       f"<i>До зустрічі! 💪</i>{REFUND_NOTE}")
            else:
                txt = f"🎉 <b>Оплату підтверджено!</b>{REFUND_NOTE}"

        await context.bot.send_message(client_uid, txt, parse_mode="HTML",
                                       reply_markup=HOME_MARKUP, disable_web_page_preview=False)
        return

    if data.startswith("adm_no_"):
        if not _is_admin(user.id):
            await query.answer("⛔", show_alert=True); return
        parts = data.split("_")
        client_uid = int(parts[3])
        pay.clear_pending(client_uid)
        try:
            await query.edit_message_caption(
                caption=(query.message.caption or "") + "\n\n❌ <b>ВІДХИЛЕНО</b>", parse_mode="HTML")
        except Exception:
            pass
        await context.bot.send_message(
            client_uid,
            "❌ <b>Платіж не підтверджено.</b>\nСпробуйте ще раз або зверніться до тренера.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Меню", callback_data="main_menu")]]),
            parse_mode="HTML"
        )
        return

    # ═══ АДМІН-ПАНЕЛЬ ═══
    if data == "admin_panel":
        if _is_admin(user.id):
            await show_admin_panel(update, context)
        return

    # ── Додати групове: назва (кнопка) → календар → час → лінк ──
    if data == "adm_add_group":
        await adm_add_group_start(update, context); return

    if data.startswith("acg_title_"):
        # Адмін обрав назву тренування (кнопка)
        title = data[10:]  # "Pilates" або "Stretching"
        context.user_data["new_title"] = title
        context.user_data["adm_state"] = "group_cal"
        await _adm_group_show_calendar(query, context); return

    if data.startswith("acg_day_"):
        # Адмін обрав день (групове)
        parts = data.split("_")  # acg_day_YYYY_MM_DD
        date_str = f"{int(parts[2])}-{int(parts[3]):02d}-{int(parts[4]):02d}"
        context.user_data["new_date"]  = date_str
        context.user_data["adm_state"] = "group_time"
        await _adm_group_show_time(query, context); return

    if data.startswith("acg_time_"):
        # Адмін обрав годину (групове) → запит лінка
        h = int(data.split("_")[-1])
        context.user_data["new_time"]  = f"{h:02d}:00"
        context.user_data["adm_state"] = "group_link"

        title    = context.user_data.get("new_title", "—")
        date_str = context.user_data.get("new_date", "")
        d_fmt    = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d.%m.%Y")

        await query.edit_message_text(
            f"🏋️➕ <b>Нове групове тренування</b> — крок 4/6\n\n"
            f"Назва: <b>{title}</b>\n📅 {d_fmt} о <b>{h:02d}:00</b>\n\n"
            "Введіть <b>посилання Google Meet</b>:",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Скасувати", callback_data="admin_panel")
            ]]), parse_mode="HTML"
        )
        return

    # ── Групове: пропустити опис → перейти до фото ──
    if data == "acg_skip_desc":
        if not _is_admin(user.id): return
        context.user_data["new_desc"] = ""
        context.user_data["adm_state"] = "group_photo"

        title    = context.user_data.get("new_title", "—")
        date_str = context.user_data.get("new_date", "")
        time_str = context.user_data.get("new_time", "00:00")
        d_fmt    = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d.%m.%Y")

        await query.edit_message_text(
            f"🏋️➕ <b>Нове групове тренування</b> — крок 6/6\n\n"
            f"Назва: <b>{title}</b>\n📅 {d_fmt} о <b>{time_str}</b>\n"
            f"🔗 Лінк: ✅\n📝 Опис: пропущено\n\n"
            f"Надішліть <b>фото для анонсу</b>\n"
            f"або натисніть <b>Пропустити</b>:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⏭ Пропустити", callback_data="acg_skip_photo")],
                [InlineKeyboardButton("❌ Скасувати",  callback_data="admin_panel")],
            ]), parse_mode="HTML"
        )
        return

    # ── Групове: пропустити фото → фіналізація ──
    if data == "acg_skip_photo":
        if not _is_admin(user.id): return
        context.user_data["new_photo"] = ""

        async def _reply(text, markup, pm):
            await query.edit_message_text(text, reply_markup=markup, parse_mode=pm)
        await _finalize_group_workout(context, _reply)
        return

    # ── Додати персональне: календар + час ──
    if data == "adm_add_personal":
        if not _is_admin(update.effective_user.id):
            await query.answer("⛔", show_alert=True); return
        await _adm_personal_show_calendar(query, context); return

    if data.startswith("acp_day_"):
        parts = data.split("_")  # acp_day_YYYY_MM_DD
        date_str = f"{int(parts[2])}-{int(parts[3]):02d}-{int(parts[4]):02d}"
        context.user_data["new_personal_date"] = date_str
        context.user_data["adm_state"] = "personal_time"
        await _adm_personal_show_time(query, context); return

    if data.startswith("acp_time_"):
        h = int(data.split("_")[-1])
        context.user_data["new_personal_time"] = f"{h:02d}:00"
        context.user_data["adm_state"] = "personal_link"

        date_str = context.user_data.get("new_personal_date", "")
        d_fmt    = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d.%m.%Y")

        await query.edit_message_text(
            f"🧘➕ <b>Новий персональний слот</b> — крок 3/5\n\n"
            f"📅 {d_fmt} о <b>{h:02d}:00</b>\n\n"
            "Введіть <b>посилання Google Meet</b>:",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Скасувати", callback_data="admin_panel")
            ]]), parse_mode="HTML"
        )
        return

    # ── Персональне: пропустити опис → перейти до фото ──
    if data == "acp_skip_desc":
        if not _is_admin(user.id): return
        context.user_data["new_personal_desc"] = ""
        context.user_data["adm_state"] = "personal_photo"

        date_str = context.user_data.get("new_personal_date", "")
        time_str = context.user_data.get("new_personal_time", "00:00")
        d_fmt    = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d.%m.%Y")

        await query.edit_message_text(
            f"🧘➕ <b>Новий персональний слот</b> — крок 5/5\n\n"
            f"📅 {d_fmt} о <b>{time_str}</b>\n"
            f"🔗 Лінк: ✅\n📝 Опис: пропущено\n\n"
            f"Надішліть <b>фото для анонсу</b>\n"
            f"або натисніть <b>Пропустити</b>:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⏭ Пропустити", callback_data="acp_skip_photo")],
                [InlineKeyboardButton("❌ Скасувати",  callback_data="admin_panel")],
            ]), parse_mode="HTML"
        )
        return

    # ── Персональне: пропустити фото → фіналізація ──
    if data == "acp_skip_photo":
        if not _is_admin(user.id): return
        context.user_data["new_personal_photo"] = ""

        async def _reply_p(text, markup, pm):
            await query.edit_message_text(text, reply_markup=markup, parse_mode=pm)
        await _finalize_personal_slot(context, _reply_p)
        return

    # ── Списки та видалення ──
    if data == "adm_list_group":
        await adm_list_group(update, context); return
    if data == "adm_list_personal":
        await adm_list_personal(update, context); return
    if data == "adm_bcast":
        await adm_bcast_start(update, context); return

    # ── Показати оплативших ──
    if data.startswith("adm_paid_"):
        if not _is_admin(user.id): return
        await adm_show_paid(update, context, int(data[9:])); return

    # ── Редагувати час групового тренування ──
    if data.startswith("adm_etime_"):
        if not _is_admin(user.id): return
        await adm_edit_time_start(update, context, int(data[10:])); return

    if data.startswith("edt_day_"):
        # Адмін обрав нову дату
        parts = data.split("_")  # edt_day_YYYY_MM_DD
        date_str = f"{int(parts[2])}-{int(parts[3]):02d}-{int(parts[4]):02d}"
        context.user_data["edit_new_date"] = date_str
        context.user_data["adm_state"] = "edit_group_time"
        await adm_edit_time_show_hours(update, context); return

    if data.startswith("edt_time_"):
        # Адмін обрав новий час → зберігаємо та розсилаємо
        if not _is_admin(user.id): return
        h = int(data.split("_")[-1])
        wid      = context.user_data.pop("edit_wid", None)
        date_str = context.user_data.pop("edit_new_date", "")
        context.user_data["adm_state"] = None

        if not wid or not date_str:
            await query.answer("Помилка", show_alert=True); return

        new_dt = datetime.strptime(f"{date_str} {h:02d}:00", "%Y-%m-%d %H:%M").replace(tzinfo=TIMEZONE)
        workout = wm.get(wid)
        if not workout:
            await query.answer("Тренування не знайдено", show_alert=True); return

        old_dt_str = _fmt_dt(workout)
        wm.update_datetime(wid, new_dt)
        new_dt_str = new_dt.strftime("%d.%m.%Y о %H:%M")

        # Розсилка оплатившим про зміну часу
        paid_users = pay.get_paid_workout_ids(wid)
        sent = 0
        for uid in paid_users:
            try:
                await context.bot.send_message(
                    uid,
                    f"⚠️ <b>Увага! Зміна часу тренування!</b>\n\n"
                    f"🏋️ <b>{workout['title']}</b>\n"
                    f"❌ Було: {old_dt_str}\n"
                    f"✅ Стало: <b>{new_dt_str}</b>\n\n"
                    f"<i>Будь ласка, зверніть увагу на новий час.</i>",
                    reply_markup=HOME_MARKUP, parse_mode="HTML"
                )
                sent += 1
                await asyncio.sleep(0.05)
            except Exception:
                pass

        await query.edit_message_text(
            f"✅ <b>Час змінено!</b>\n\n"
            f"🏋️ {workout['title']}\n"
            f"📅 Новий час: <b>{new_dt_str}</b>\n\n"
            f"📢 Повідомлено оплативших: {sent} з {len(paid_users)}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🏋️📋 Список", callback_data="adm_list_group")],
                [InlineKeyboardButton("⚙️ Панель", callback_data="admin_panel")],
            ]), parse_mode="HTML"
        ); return

    if data.startswith("adm_del_g_"):
        if not _is_admin(user.id): return
        wm.delete(int(data[10:]))
        await query.answer("✅ Видалено")
        await adm_list_group(update, context); return

    if data.startswith("adm_del_p_"):
        if not _is_admin(user.id): return
        pm.delete_slot(data[10:])
        await query.answer("✅ Видалено")
        await adm_list_personal(update, context); return


# ═══════════════════════════════════════════════════════════════
#  ФОНОВИЙ ЦИКЛ НАГАДУВАНЬ
# ═══════════════════════════════════════════════════════════════

async def notification_loop(app: Application):
    while True:
        try:
            # ── Групові тренування ──
            for workout, ntype in wm.get_pending_notifications():
                ids = pay.get_paid_workout_ids(workout["id"])
                if not ids: continue
                dt = datetime.fromisoformat(workout["datetime"])
                if dt.tzinfo is None: dt = dt.replace(tzinfo=TIMEZONE)
                if ntype == "1h":
                    txt = (
                        f"🔔 <b>Через годину — час тренування!</b>\n\n"
                        f"🏋️ <b>{workout['title']}</b>\n"
                        f"📅 {dt.strftime('%d.%m.%Y о %H:%M')}\n\n"
                        f"Вже скоро починаємо! Підготуй зручний одяг, "
                        f"водичку і гарний настрій 😊\n\n"
                        f"🔗 <a href=\"{workout['teams_link']}\">Підключитись через Google Meet</a>\n\n"
                        f"До зустрічі! 💪🔥"
                    )
                elif ntype == "end":
                    txt = (
                        f"🎉 <b>Тренування завершено — ти молодець!</b>\n\n"
                        f"🏋️ <b>{workout['title']}</b>\n"
                        f"📅 {dt.strftime('%d.%m.%Y о %H:%M')}\n\n"
                        f"Дякуємо, що була з нами сьогодні! 🙏\n"
                        f"Кожне тренування — це крок до кращої версії себе, "
                        f"і ти вже на цьому шляху. Пишаємось твоєю наполегливістю! 💪\n\n"
                        f"Не забудь випити водички та трішки відпочити 😊\n\n"
                        f"📢 <b>Слідкуй за нашими оновленнями</b> — незабаром "
                        f"анонсуємо нові тренування! До зустрічі! 🔥"
                    )
                else:
                    txt = (
                        f"🚀 <b>Тренування починається ЗАРАЗ!</b>\n\n"
                        f"🏋️ <b>{workout['title']}</b>\n\n"
                        f"Час рухатись! Сьогодні ти стаєш ще сильнішою 💪\n\n"
                        f"🔗 <a href=\"{workout['teams_link']}\">Приєднатись до тренування</a>\n\n"
                        f"Тренер вже чекає — вперед! 🔥"
                    )
                for uid in ids:
                    try:
                        await app.bot.send_message(uid, txt, parse_mode="HTML",
                                                   reply_markup=HOME_MARKUP, disable_web_page_preview=False)
                        await asyncio.sleep(0.05)
                    except Exception as e:
                        logger.warning(f"notify group → {uid}: {e}")

            # ── Персональні тренування ──
            for slot, ntype in pm.get_pending_notifications():
                uid = slot["booked_by"]
                if not uid: continue
                if ntype == "1h":
                    txt = (
                        f"🔔 <b>Через годину — твоє персональне тренування!</b>\n\n"
                        f"📅 {_fmt_slot(slot)}\n\n"
                        f"Підготуйся: зручний одяг, вода, і налаштуйся "
                        f"на продуктивну роботу 😊\n\n"
                        f"🔗 <a href=\"{slot['teams_link']}\">Підключитись через Google Meet</a>\n\n"
                        f"Тренер вже готується до заняття саме з тобою! 🤝"
                    )
                elif ntype == "end":
                    txt = (
                        f"🎉 <b>Персональне тренування завершено!</b>\n\n"
                        f"📅 {_fmt_slot(slot)}\n\n"
                        f"Дякуємо за сьогоднішнє заняття! 🙏\n"
                        f"Ти чудово попрацювала — кожне тренування наближає "
                        f"тебе до твоєї мети. Так тримати! 💪✨\n\n"
                        f"Не забудь відпочити та відновити сили 😊\n\n"
                        f"📢 <b>Слідкуй за розкладом</b> — бронюй наступне "
                        f"персональне тренування та продовжуй свій шлях до результату! 🔥"
                    )
                else:
                    txt = (
                        f"🚀 <b>Персональне тренування починається ЗАРАЗ!</b>\n\n"
                        f"📅 {_fmt_slot(slot)}\n\n"
                        f"Час для себе — найкраща інвестиція! 💫\n\n"
                        f"🔗 <a href=\"{slot['teams_link']}\">Приєднатись до тренування</a>\n\n"
                        f"Вперед до нових результатів! 💪🔥"
                    )
                try:
                    await app.bot.send_message(uid, txt, parse_mode="HTML",
                                               reply_markup=HOME_MARKUP, disable_web_page_preview=False)
                except Exception as e:
                    logger.warning(f"notify personal → {uid}: {e}")
        except Exception as e:
            logger.error(f"notification_loop: {e}")

        # Завантажуємо відкладені бекапи (якщо throttle минув)
        try:
            pay._storage.flush_pending()
            wm._storage.flush_pending()
            pm._storage.flush_pending()
        except Exception:
            pass

        await asyncio.sleep(60)


# ═══════════════════════════════════════════════════════════════
#  HEALTH-CHECK + ЗАПУСК
# ═══════════════════════════════════════════════════════════════

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200); self.end_headers(); self.wfile.write(b"OK")
    def log_message(self, *a): pass

def run_health_server():
    port = int(os.environ.get("PORT", 8000))
    HTTPServer(("0.0.0.0", port), HealthHandler).serve_forever()

def main():
    # Діагностика storage
    if not GITHUB_TOKEN:
        logger.warning("⚠️ GITHUB_TOKEN не задано! Дані зберігаються ТІЛЬКИ локально і ЗНИКНУТЬ при рестарті!")
    else:
        gist_id = os.environ.get("GIST_ID", "")
        logger.info(f"GitHub Gist storage: token=✅, GIST_ID={'✅ ' + gist_id[:8] + '...' if gist_id else '❌ (буде створено автоматично)'}")

    pay.load(); wm.load(); pm.load()

    logger.info(f"Завантажено: клієнтів={len(pay.payments)}, "
                f"групових={len(wm.workouts)}, персональних={len(pm.slots)}")

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("add_workout", cmd_add_workout))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.PHOTO, photo_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))

    async def post_init(application: Application):
        asyncio.create_task(notification_loop(application))
    app.post_init = post_init

    WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "")
    PORT = int(os.environ.get("PORT", 8000))
    if WEBHOOK_URL:
        app.run_webhook(listen="0.0.0.0", port=PORT, webhook_url=WEBHOOK_URL,
                        allowed_updates=["message", "callback_query"], drop_pending_updates=True)
    else:
        app.run_polling(allowed_updates=["message", "callback_query"], drop_pending_updates=True)

if __name__ == "__main__":
    threading.Thread(target=run_health_server, daemon=True).start()
    main()
