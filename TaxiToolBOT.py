import logging
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, InputMediaPhoto, InlineKeyboardButton, InlineKeyboardMarkup,LabeledPrice
from telegram.ext import Application, ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters, ConversationHandler, CallbackQueryHandler, PreCheckoutQueryHandler
from telegram.error import TimedOut, RetryAfter, NetworkError
from supabase import create_client, Client
from datetime import datetime, timedelta, date, timezone
from openai import AsyncOpenAI
from geopy.distance import geodesic
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from calendar import monthrange
import json
import os
from pathlib import Path
from dotenv import load_dotenv
import re
import base64
import asyncio
from functools import lru_cache
from typing import Dict
from postgrest.exceptions import APIError as PostgrestAPIError
import uuid

#Check
# === Configuration ===
load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env")  # used locally; Cloud Run ignores .env

def require_env(name: str) -> str:
    v = os.getenv(name)
    if not v or not v.strip():
        raise RuntimeError(f"[startup] Missing required env var: {name}")
    return v.strip()  # trim newlines/spaces that Secret Manager values sometimes carry

TELEGRAM_BOT_TOKEN = require_env("TELEGRAM_TOKEN")
SUPABASE_URL       = require_env("SUPABASE_URL")
SUPABASE_KEY       = require_env("SUPABASE_KEY")
OPENAI_KEY         = require_env("OPENAI_KEY")

WEBHOOK_MODE = os.getenv("MODE", "polling") == "webhook"  # True on Cloud Run

# === Initialize clients (with clear error logging) ===
try:
    aclient = AsyncOpenAI(api_key=OPENAI_KEY)
except Exception as e:
    raise RuntimeError(f"[startup] OpenAI client init failed: {e}")

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception as e:
    raise RuntimeError(f"[startup] Supabase client init failed: {e}")

async def run_io(fn):
    return await asyncio.to_thread(fn)

# === Logging ===
logging.basicConfig(level=logging.INFO)

# === States ===
GET_CATEGORY, GET_ITEM_TITLE, GET_SPECS, GET_DESCRIPTION, GET_CONDITION, GET_PRICE, GET_PHOTOS, GET_LOCATION, GET_AVAILABILITY, BROWSE_SEARCH = range(10)

EDIT_DESCRIPTION,EDIT_CHOICE, CONFIRM_DELETE, AWAIT_NEW_DESCRIPTION, AWAIT_NEW_PRICE, AWAIT_NEW_LOCATION, AWAIT_NEW_CATEGORY, AWAIT_NEW_CONDITION, AWAIT_NEW_ITEM_TITLE, AWAIT_NEW_BRAND_MODEL, AWAIT_NEW_SPECS, AWAIT_NEW_PHOTOS = range(100, 112)  # Avoid overlap with existing states

AWAIT_SEARCH_QUERY, SETTINGS_MENU, AWAIT_LOCATION_CHOICE = range(300, 303)
LANG_SELECT = 350  # new: pick language flow (if you later want a conversation step)

# === i18n (English / Українська / Polski) ===

LOCALES = {
    "en": {
        "choose_language": "Choose your language:",
        "lang_en": "English",
        "lang_uk": "Українська",
        "lang_pl": "Polski",
        "greeting": (
            "Hi! I’m RentoTo — your tool-sharing assistant. Rent tools from others or list your own.\n\n"
            "Pick a language below. You can change it anytime with /language."
        ),
        "home_tip": "Welcome! Here's your main menu:",
        "menu_main": "Main Menu:",
        "btn_my_account": "My Account",
        "btn_create_listing": "Create a Listing",
        "btn_browse": "Browse",
        "btn_my_listings": "My Listings",
        "edit_what": "What would you like to edit?",
        "no_listing_in_ctx": "No listing in context.",
        "account_title": "My Account",
        "account_your_listings": "Your listings",
        "account_pending_requests": "Pending requests",
        "account_upcoming_bookings": "Upcoming bookings",
        "account_bookable_30": "Bookable days (next 30d)",
        "account_est_earnings": "Est. earnings (next 30d):",
        "btn_check_requests": "📬 Check outstanding requests",
        "btn_upcoming_borrowings": "📅 Upcoming borrowings",
        "no_upcoming_borrowings": "You have no upcoming borrowings",
        "from": "From",
        "when": "When",
        "back": "Back",
        "no_outstanding_requests": "🎉 No outstanding requests right now.",
        "request_prefix": "Request {i} of {n}",
        "borrower_request": "Borrower’s request",
        "price_total": "Total",
        "accept": "Accept",
        "decline": "Decline",
        "back_to_requests": "⬅️ Back to requests",
        "request_not_found": "Request not found.",
        "accepted_and_booked": "Request accepted and days booked.",
        "accepted_title": "Accepted!",
        "nice_borrower_is": "🙌 Nice! The borrower is {username}.",
        # NOTE: strings below are used with MarkdownV2, keep escapes
        "nudge_text_v2": "Great! Now text the borrower to agree on a convenient pickup time and place.\nA quick hello goes a long way 🙂",
        "no_public_username": "🙌 Nice! The borrower doesn’t have a public @username.\n\nGreat! Now text the borrower to agree on a convenient pickup time and place:\n",
        "request_declined": "Request declined",
        "request_declined_borrower": "😕 Your request for *{item}* was declined",

        # Edit menu
        "edit_photos": "🖼 Photos",
        "edit_category": "📂 Category",
        "edit_item_title": "📝 Item title",
        "edit_brand_model": "🏷 Brand/Model",
        "edit_specs": "⚙️ Specs",
        "edit_description": "✏️ Description",
        "edit_condition": "🛠 Condition",
        "edit_price": "💰 Price",
        "edit_location": "📍 Location",
        "edit_back": "🔙 Back",
        "finish_listing_first": "Please finish creating your listing first.",
    },
    "pl": {
        "choose_language": "Wybierz język:",
        "lang_en": "English",
        "lang_uk": "Українська",
        "lang_pl": "Polski",
        "greeting": "Cześć! Jestem RentoTo — asystent do współdzielenia sprzętu. Możesz wypożyczać narzędzia od innych albo dodać własną ofertę.",
        "home_tip": "Witaj! Oto twoje menu główne:",
        "menu_main": "Menu główne:",
        "btn_my_account": "Moje konto",
        "btn_create_listing": "Dodaj ogłoszenie",
        "btn_browse": "Przeglądaj",
        "btn_my_listings": "Moje ogłoszenia",
        "edit_what": "Co chcesz edytować?",
        "no_listing_in_ctx": "Brak kontekstu ogłoszenia.",
        "account_title": "Moje konto",
        "account_your_listings": "Twoje ogłoszenia",
        "account_pending_requests": "Oczekujące prośby",
        "account_upcoming_bookings": "Nadchodzące rezerwacje",
        "account_bookable_30": "Dni dostępne (30 dni)",
        "account_est_earnings": "Szac. zarobek (30 dni):",
        "btn_check_requests": "📬 Sprawdź oczekujące prośby",
        "btn_upcoming_borrowings": "📅 Nadchodzące wypożyczenia",
        "no_upcoming_borrowings": "Nie masz nadchodzących wypożyczeń",
        "from": "Od",
        "when": "Kiedy",
        "back": "Wstecz",
        "no_outstanding_requests": "🎉 Brak oczekujących próśb.",
        "request_prefix": "Prośba {i} z {n}",
        "borrower_request": "Prośba wypożyczającego",
        "price_total": "Suma",
        "accept": "Akceptuj",
        "decline": "Odrzuć",
        "back_to_requests": "⬅️ Wróć do próśb",
        "request_not_found": "Nie znaleziono prośby.",
        "accepted_and_booked": "Prośba zaakceptowana i dni zarezerwowane.",
        "accepted_title": "Zaakceptowano!",
        "nice_borrower_is": "🙌 Super! Wypożyczający to {username}.",
        "nudge_text_v2": "Świetnie! Teraz napisz do wypożyczającego, aby ustalić dogodny czas i miejsce odbioru.\nKrótka wiadomość wiele znaczy 🙂",
        "no_public_username": "🙌 Super! Wypożyczający nie ma publicznej nazwy użytkownika.\n\nNapisz do niego, aby ustalić szczegóły odbioru:\n",
        "request_declined": "Prośba odrzucona",
        "request_declined_borrower": "😕 Twoja prośba o *{item}* została odrzucona",

        "edit_photos": "🖼 Zdjęcia",
        "edit_category": "📂 Kategoria",
        "edit_item_title": "📝 Tytuł",
        "edit_brand_model": "🏷 Marka/Model",
        "edit_specs": "⚙️ Specyfikacja",
        "edit_description": "✏️ Opis",
        "edit_condition": "🛠 Stan",
        "edit_price": "💰 Cena",
        "edit_location": "📍 Lokalizacja",
        "edit_back": "🔙 Wstecz",
        "finish_listing_first": "Proszę najpierw dokończ tworzenie ogłoszenia.",
    },
    "uk": {
        "choose_language": "Оберіть мову:",
        "lang_en": "English",
        "lang_uk": "Українська",
        "lang_pl": "Polski",
        "greeting": "Привіт! Я RentoTo — асистент зі позичання речей і інструментів. Ти можеш орендувати речі або додати власне оголошення.",
        "home_tip": "Привіт! Ось твоє головне меню:",
        "menu_main": "Головне меню:",
        "btn_my_account": "Мій акаунт",
        "btn_create_listing": "Створити оголошення",
        "btn_browse": "Пошук",
        "btn_my_listings": "Мої оголошення",
        "edit_what": "Що хочете змінити?",
        "no_listing_in_ctx": "Немає оголошення в контексті.",
        "account_title": "Мій акаунт",
        "account_your_listings": "Ваші оголошення",
        "account_pending_requests": "Очікують підтвердження",
        "account_upcoming_bookings": "Майбутні бронювання",
        "account_bookable_30": "Доступні дні (30 днів)",
        "account_est_earnings": "Орієнт. дохід (30 днів):",
        "btn_check_requests": "📬 Переглянути заявки",
        "btn_upcoming_borrowings": "📅 Майбутні оренди",
        "no_upcoming_borrowings": "У вас немає майбутніх оренд",
        "from": "Від",
        "when": "Коли",
        "back": "Назад",
        "no_outstanding_requests": "🎉 Зараз немає заявок.",
        "request_prefix": "Заявка {i} з {n}",
        "borrower_request": "Повідомлення від орендаря",
        "price_total": "Разом",
        "accept": "Прийняти",
        "decline": "Відхилити",
        "back_to_requests": "⬅️ Назад до заявок",
        "request_not_found": "Заявку не знайдено.",
        "accepted_and_booked": "Заявку прийнято, дні заброньовано.",
        "accepted_title": "Прийнято!",
        "nice_borrower_is": "🙌 Клас! Орендар — {username}.",
        "nudge_text_v2": "Чудово! Тепер напишіть орендареві, щоб узгодити зручні час і місце передачі.\nКоротке привітання — це завжди доречно 🙂",
        "no_public_username": "🙌 Клас! У орендаря немає публічного @username.\n\nНапишіть йому, щоб узгодити деталі видачі:\n",
        "request_declined": "Заявку відхилено",
        "request_declined_borrower": "😕 Вашу заявку на *{item}* відхилено",

        "edit_photos": "🖼 Фото",
        "edit_category": "📂 Категорія",
        "edit_item_title": "📝 Назва",
        "edit_brand_model": "🏷 Бренд/Модель",
        "edit_specs": "⚙️ Характеристики",
        "edit_description": "✏️ Опис",
        "edit_condition": "🛠 Стан",
        "edit_price": "💰 Ціна",
        "edit_location": "📍 Локація",
        "edit_back": "🔙 Назад",
        "finish_listing_first": "Будь ласка, спочатку завершіть створення оголошення.",
    },
}

# === i18n helpers (sync) ===

DEFAULT_LANG = "en"
LANG_CACHE: Dict[str, str] = {}

def get_user_lang(user_id: str) -> str:
    lang = LANG_CACHE.get(user_id)
    if lang:
        return lang
    try:
        res = supabase.table("users").select("language").eq("id", user_id).execute()
        if res.data:
            lang = (res.data[0].get("language") or DEFAULT_LANG)
        else:
            lang = DEFAULT_LANG
    except Exception as e:
        logging.warning(f"get_user_lang fallback: {e}")
        lang = DEFAULT_LANG
    LANG_CACHE[user_id] = lang
    return lang


def set_user_lang(uid: str, lang: str) -> None:
    LANG_CACHE[uid] = lang
    try:
        supabase.table("users").upsert({"id": uid, "language": lang}).execute()
    except Exception as e:
        logging.warning(f"[lang] failed to persist language for {uid}: {e}")

def tr(uid: str, key: str, **fmt) -> str:
    # Lazy-load language from DB if cache is cold (e.g., after restart)
    lang = LANG_CACHE.get(uid) or get_user_lang(uid)
    bucket = LOCALES.get(lang) or LOCALES.get(DEFAULT_LANG, {})
    txt = bucket.get(key, LOCALES.get(DEFAULT_LANG, {}).get(key, key))
    return txt.format(**fmt)

def get_entitlement(user_id: str) -> tuple[int | None, int, bool]:
    try:
        row = supabase.table("users").select("paid_slots, subscription_until").eq("id", user_id).execute().data
        row = (row or [{}])[0]
        paid = int(row.get("paid_slots") or 0)
        until = _parse_ts_iso(row.get("subscription_until"))
        sub_active = bool(until and until > datetime.utcnow())
        max_allowed = None if sub_active else 2 + paid
        return max_allowed, paid, sub_active
    except Exception:
        return 2, 0, False

def lang_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🇬🇧 English", callback_data="set_lang_en"),
            InlineKeyboardButton("🇺🇦 Українська", callback_data="set_lang_uk"),
            InlineKeyboardButton("🇵🇱 Polski", callback_data="set_lang_pl"),
        ]
    ])

# === Helpers ===
geolocator = Nominatim(user_agent="rento-to-bot/1.0 (contact: RentoToBOT@gmail.com)", timeout=5)
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1, max_retries=2)
reverse = RateLimiter(geolocator.reverse, min_delay_seconds=1, max_retries=2)

async def supa_exec_with_retry(op, tries: int = 3, base_delay: float = 0.7):
    """
    Run a synchronous PostgREST .execute() call with simple async retries.
    Use: await supa_exec_with_retry(lambda: supabase.table(...).insert(...).execute())
    """
    for i in range(tries):
        try:
            return op()
        except PostgrestAPIError as e:
            # When Cloudflare/Supabase briefly fails, PostgREST wraps it as APIError
            # and sometimes the 'details' contains an HTML page. Retry those.
            text = f"{getattr(e, 'message', '')} {getattr(e, 'details', '')}"
            transient = (
                "DOCTYPE html" in text
                or "Could not find host" in text
                or "timed out" in text.lower()
            )
            if transient and i < tries - 1:
                await asyncio.sleep(base_delay * (2 ** i))
                continue
            logging.exception("Supabase error (no more retries)")
            raise

@lru_cache(maxsize=2000)
def _loc_label_cached(input_str: str) -> str:
    return _location_name_from_coords_uncached(input_str)

def location_name_from_coords(input_str):
    return _loc_label_cached(input_str)

def _location_name_from_coords_uncached(input_str):
    try:
        if any(ch.isdigit() for ch in input_str) and "," in input_str:
            lat, lon = map(float, input_str.split(","))
        else:
            location = geocode(input_str)
            if not location:
                return input_str
            lat, lon = location.latitude, location.longitude

        location = reverse((lat, lon), exactly_one=True)
        if location:
            addr = location.raw.get("address", {})
            city = addr.get("city") or addr.get("town") or addr.get("village") or ""
            region = addr.get("suburb") or addr.get("city_district") or addr.get("state_district") or ""
            if city:
                return f"{city}, {region}" if region else city
        return f"{lat},{lon}"
    except Exception as e:
        logging.warning(f"[Geo Error] {e}")
        return input_str

async def async_geocode(q):
    return await run_io(lambda: geocode(q))

async def async_reverse(latlon):
    return await run_io(lambda: reverse(latlon, exactly_one=True))

_MD2_SPECIAL = r'[_*[\]()~`>#+\-=|{}.!]'

async def safe_send(func, *args, **kwargs):
    """Call a PTB send/edit method with minimal retries.

    In webhook mode (MODE=webhook) retries are skipped entirely to avoid
    blocking the aiohttp event loop with asyncio.sleep(), which can trigger
    'Event loop is closed' errors on Cloud Run.
    """
    try:
        return await func(*args, **kwargs)
    except RetryAfter as e:
        if WEBHOOK_MODE:
            logging.warning(f"[send] rate limited in webhook mode, not retrying: {e}")
            raise
        # Telegram is throttling — wait the suggested time
        await asyncio.sleep(getattr(e, "retry_after", 1) + 0.5)
        try:
            return await func(*args, **kwargs)
        except Exception as e2:
            logging.warning(f"[send] retry failed: {e2}")
            raise
    except (TimedOut, NetworkError) as e:
        # Don't retry network errors in webhook mode - just log and fail fast
        logging.warning(f"[send] network error (not retrying): {e}")
        raise
        
def _parse_ts_iso(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        s2 = s.rstrip("Z")
        dt = datetime.fromisoformat(s2)
        # normalize to naive UTC for consistent comparisons/formatting
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except Exception:
        return None

def get_used_listings(user_id: str) -> int:
    try:
        return len(supabase.table("listings").select("id").eq("owner_id", user_id).execute().data or [])
    except Exception:
        return 0

def nz(x) -> str:
    """None-safe to-string (None -> '', preserves strings)."""
    if x is None:
        return ""
    return x if isinstance(x, str) else str(x)

async def _debug_all_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    logging.warning(f"[DBG] Unhandled callback: {q.data!r}")
    await q.answer()  # avoid spinner

async def dbg_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = getattr(update.effective_user, "id", None)
    mid = getattr(update.effective_message, "message_id", None)
    txt = getattr(update.effective_message, "text", None)
    logging.warning("[DBG_UPDATE] uid=%s mid=%s text=%r has_photo=%s has_loc=%s",
                    uid, mid, txt,
                    bool(getattr(update.effective_message, "photo", None)),
                    bool(getattr(update.effective_message, "location", None)))

async def edit_menu_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    listing_id = context.user_data.get("edit_listing_id")
    me = str(update.effective_user.id)
    if not listing_id:
        # try to answer if it was a callback; otherwise just send a message
        q = getattr(update, "callback_query", None)
        if q:
            await q.answer()
            await q.message.edit_text(tr(me, "no_listing_in_ctx"))
        else:
            if update.message:
                await update.message.reply_text(tr(me, "no_listing_in_ctx"))
        return ConversationHandler.END

    kb = build_edit_menu_keyboard(me)
    q = getattr(update, "callback_query", None)
    if q:
        await q.answer()
        await q.message.edit_text(tr(me, "edit_what"), reply_markup=kb)
    else:
        if update.message:
            await update.message.reply_text(tr(me, "edit_what"), reply_markup=kb)
    return EDIT_CHOICE

CANON_CATEGORIES = [
    "Electronics", "Recreation", "Construction", "Home Improvement", "Events & Party", "Gardening"
]

def _t_for_lang(lang: str, key: str) -> str:
    """Lookup i18n key with fallback to English and then the key name."""
    if lang not in LOCALES:
        lang = "en"
    table = LOCALES.get(lang, {})
    if key in table:
        return table[key]
    # fallback to English
    en = LOCALES.get("en", {})
    if key in en:
        return en[key]
    logging.warning(f"[i18n] missing key {key!r} for lang {lang!r}")
    return key

def _quota_line_text(user_id: str) -> str:
    """
    Renders a localized quota line using your existing get_entitlement().
    - Unlimited → 'quota_unlimited' (with date if present)
    - Limited   → 'quota_limited' (used/limit)
    """
    max_allowed, _, sub_active = get_entitlement(user_id)
    used = get_used_listings(user_id)

    if sub_active:
        # show the subscription-until date if available
        try:
            row = supabase.table("users").select("subscription_until").eq("id", user_id).execute().data
            until = _parse_ts_iso((row or [{}])[0].get("subscription_until"))
            until_str = until.strftime("%Y-%m-%d") if until else "—"
        except Exception:
            until_str = "—"
        return tr(user_id, "quota_unlimited", until=until_str)

    # finite plan
    limit = max_allowed if max_allowed is not None else 2
    return tr(user_id, "quota_limited", used=used, limit=limit)

def photo_edit_keyboard(me: str) -> ReplyKeyboardMarkup:
    del_word = tr(me, "photos_delete_word")  # localized base word
    # Buttons: Delete 1 / Delete 2 / Delete 3 | Clear / Done | Cancel
    rows = [
        [f"{del_word} 1", f"{del_word} 2", f"{del_word} 3"],
        [tr(me, "photos_edit_clear"), tr(me, "photos_edit_done")],
        [tr(me, "photos_edit_cancel")],
    ]
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

def _cat_label(canon: str, me: str) -> str:
    key = {
        "Electronics": "cat_Electronics",
        "Recreation": "cat_Recreation",
        "Construction": "cat_Construction",
        "Home Improvement": "cat_HomeImprovement",
        "Events & Party": "cat_EventsParty",
        "Gardening": "cat_Gardening",
    }[canon]
    return tr(me, key)

def category_keyboard(me: str) -> ReplyKeyboardMarkup:
    labels = [_cat_label(c, me) for c in CANON_CATEGORIES]
    # 2 per row
    rows = [labels[i:i+2] for i in range(0, len(labels), 2)]
    rows.append([tr(me, "back")])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

def to_canonical_category(user_text: str, me: str) -> str | None:
    """Map a typed or clicked category (any language) to canonical."""
    low = (user_text or "").strip().lower()
    # map all localized labels back to canonical
    for canon in CANON_CATEGORIES:
        if low == _cat_label(canon, me).lower():
            return canon
    # fall back to alias table you already had
    return CATEGORY_ALIASES.get(low) or None

def esc_md2(s: str | None) -> str:
    if s is None:
        return ""
    return re.sub(f"({_MD2_SPECIAL})", r"\\\1", str(s))

def photo_stage_keyboard(me: str) -> ReplyKeyboardMarkup:
    del_word = tr(me, "photos_delete_word")
    return ReplyKeyboardMarkup([
        [tr(me, "photos_add_more"), tr(me, "photos_continue")],
        [f"{del_word} 1", f"{del_word} 2", f"{del_word} 3"],
        [tr(me, "photos_cancel_listing")]
    ], resize_keyboard=True)

def parse_delete_idx(text: str, me: str) -> int | None:
    t = (text or "").strip().lower()
    del_word = tr(me, "photos_delete_word").lower()
    if t.startswith(del_word.lower()):
        parts = t.split()
        if parts and parts[-1].isdigit():
            return int(parts[-1]) - 1
    # also accept English "delete" as a fallback
    if t.startswith("delete"):
        parts = t.split()
        if parts and parts[-1].isdigit():
            return int(parts[-1]) - 1
    return None

def _to_iso_list(tokens: list[str]) -> list[str]:
    """Normalize any 'YYYY-MM-DD' or 'DD/MM/YYYY' tokens to ISO 'YYYY-MM-DD'."""
    out = []
    for t in tokens or []:
        d = _parse_date_any(t)
        if d:
            out.append(d.strftime("%Y-%m-%d"))
    return out

def _ranges_from_iso(iso_dates: list[str]) -> tuple[str, int]:
    """
    Collapse ISO dates into human-friendly ranges.
    Returns (pretty_string, total_day_count).
    """
    if not iso_dates:
        return "—", 0
    days = sorted(datetime.strptime(x, "%Y-%m-%d").date() for x in iso_dates)
    total = len(days)

    ranges = []
    start = end = days[0]
    for d in days[1:]:
        if d == end + timedelta(days=1):
            end = d
        else:
            ranges.append((start, end))
            start = end = d
    ranges.append((start, end))

    pretty = ", ".join(
        f"{a.strftime('%d/%m/%Y')}" + (f" - {b.strftime('%d/%m/%Y')}" if a != b else "")
        for a, b in ranges
    )
    return pretty, total

def format_date_ranges_from_tokens(tokens: list[str]) -> str:
    """
    Takes a list of date tokens ('YYYY-MM-DD' or 'DD/MM/YYYY') and returns
    collapsed ranges in 'DD/MM/YYYY - DD/MM/YYYY' format, one per line.
    """
    # Parse & sort to date
    parsed = []
    for t in tokens or []:
        d = _parse_date_any(t)
        if d:
            parsed.append(d)
    if not parsed:
        return "Not provided"
    parsed = sorted(set(parsed))
    # Collapse consecutive dates
    blocks = []
    start = end = parsed[0]
    for d in parsed[1:]:
        if d == end + timedelta(days=1):
            end = d
        else:
            blocks.append((start, end))
            start = end = d
    blocks.append((start, end))
    # Render as DD/MM/YYYY - DD/MM/YYYY
    return "\n".join(f"{s.strftime('%d/%m/%Y')} - {e.strftime('%d/%m/%Y')}" for s, e in blocks)

def _in_next_n_days(d: date, n: int = 30) -> bool:
    today = datetime.utcnow().date()
    return today <= d <= (today + timedelta(days=n))

def _count_bookable_days_next_30(listing: dict) -> int:
    free = _collect_available_days(listing)  # tokens in source format
    count = 0
    for t in free:
        d = _parse_date_any(t)
        if d and _in_next_n_days(d, 30):
            count += 1
    return count

def coerce_list(v):
    """Return a list for DB fields that may arrive as list, JSON string, comma string or None."""
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x).strip() for x in v if str(x).strip()]
    if isinstance(v, str):
        s = v.strip()
        if s.lower() in {"", "none", "null", "nan"}:
            return []
        # JSON array?
        if s.startswith("[") and s.endswith("]"):
            try:
                arr = json.loads(s)
                return [str(x).strip() for x in arr if str(x).strip()]
            except Exception:
                pass
        # Fallback: comma-separated
        return [p.strip() for p in s.split(",") if p.strip()]
    return []

def _parse_date_any(fmt_str: str) -> date | None:
    """Accepts either DD/MM/YYYY or YYYY-MM-DD; returns date or None."""
    s = fmt_str.strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    return None

def _fmt_like_source(d: date, availability_list: list[str]) -> str:
    uses_slash = any("/" in x for x in (availability_list or []))
    return d.strftime("%d/%m/%Y") if uses_slash else d.strftime("%Y-%m-%d")

def _collect_available_days(listing: dict) -> set[str]:
    """Available days = availability minus booked_days (normalize formats)."""
    avail_raw = listing.get("availability") or []
    booked_raw = listing.get("booked_days") or []  # text[] in DB; create this column
    avail = {_fmt_like_source(_parse_date_any(a), avail_raw) for a in avail_raw if _parse_date_any(a)}
    booked = {_fmt_like_source(_parse_date_any(b), avail_raw) for b in booked_raw if _parse_date_any(b)}
    return {d for d in avail if d not in booked}

def _group_available_by_year_month(listing: dict) -> dict[int, dict[int, list[tuple[int, str]]]]:
    """
    Returns {year: {month: [(day, token), ...]}} where token is the original date
    string from availability (e.g., '2025-10-12' or '12/10/2025').
    Sorted by day within each month.
    """
    by: dict[int, dict[int, list[tuple[int, str]]]] = {}
    for token in _collect_available_days(listing):
        d = _parse_date_any(token)
        if not d:
            continue
        by.setdefault(d.year, {}).setdefault(d.month, []).append((d.day, token))
    for y in by:
        for m in by[y]:
            by[y][m].sort(key=lambda t: t[0])
    return by

def _split_week_ranges(year: int, month: int) -> list[tuple[int, int]]:
    """1–7, 8–14, 15–21, 22–lastDay for that month."""
    last_day = monthrange(year, month)[1]
    return [(1,7), (8,14), (15,21), (22,last_day)]

def _month_name(m: int) -> str:
    return datetime(2000, m, 1).strftime("%B")

def _add_selected_day(context: ContextTypes.DEFAULT_TYPE, listing_id: str, day_token: str):
    key = f"rent_selected_{listing_id}"
    sel = context.user_data.get(key) or []
    if day_token not in sel:
        sel.append(day_token)
    context.user_data[key] = sel
    return sel

def _get_selected_days(context: ContextTypes.DEFAULT_TYPE, listing_id: str) -> list[str]:
    return context.user_data.get(f"rent_selected_{listing_id}", []) or []

def _clear_selected_days(context: ContextTypes.DEFAULT_TYPE, listing_id: str):
    context.user_data.pop(f"rent_selected_{listing_id}", None)

def _pending_requests_count_for_lender(user_id: str) -> int:
    try:
        # all my listings
        ids = [r["id"] for r in (supabase.table("listings").select("id").eq("owner_id", user_id).execute().data or [])]
        if not ids:
            return 0
        # pending requests addressed to me (as lender)
        rr = supabase.table("rental_requests").select("id").in_("listing_id", ids)\
             .eq("lender_id", user_id).eq("status", "pending").execute().data or []
        return len(rr)
    except Exception:
        return 0

def main_menu_keyboard(user_id: str) -> ReplyKeyboardMarkup:
    n = _pending_requests_count_for_lender(user_id)
    base = tr(user_id, "btn_my_account")
    label = base if n == 0 else f"{base} [{n}]"
    return ReplyKeyboardMarkup([[label, tr(user_id, "btn_create_listing")], [tr(user_id, "btn_browse"), tr(user_id, "btn_my_listings")]], resize_keyboard=True)

def build_edit_menu_keyboard(user_id: str):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(tr(user_id, "edit_photos"), callback_data="edit_field_photos")],
        [InlineKeyboardButton(tr(user_id, "edit_category"), callback_data="edit_field_category")],
        [InlineKeyboardButton(tr(user_id, "edit_item_title"), callback_data="edit_field_item")],
        [InlineKeyboardButton(tr(user_id, "edit_brand_model"), callback_data="edit_field_brand_model")],
        [InlineKeyboardButton(tr(user_id, "edit_specs"), callback_data="edit_field_specs")],
        [InlineKeyboardButton(tr(user_id, "edit_description"), callback_data="edit_field_description")],
        [InlineKeyboardButton(tr(user_id, "edit_condition"), callback_data="edit_field_condition")],
        [InlineKeyboardButton(tr(user_id, "edit_price"), callback_data="edit_field_price")],
        [InlineKeyboardButton(tr(user_id, "edit_location"), callback_data="edit_field_location")],
        [InlineKeyboardButton(tr(user_id, "edit_back"), callback_data="cancel_edit")]
    ])

async def _back_to_listing_from_edit(update_or_query, context: ContextTypes.DEFAULT_TYPE):
    """Jump back to the listing card that opened the edit menu."""
    listing_id = context.user_data.get("edit_listing_id")
    if listing_id:
        listings = context.user_data.get("my_listings", [])
        for i, l in enumerate(listings or []):
            if l.get("id") == listing_id:
                context.user_data["listing_index"] = i
                break
    await send_single_listing(update_or_query, context)
    return ConversationHandler.END

# === Moderation ===
BAD_WORDS = {
    # sexual content / toys
    "dildo", "vibrator", "sex toy", "buttplug", "butt plug",
    "anal", "bdsm", "porn", "porno", "pornography", "xxx", "nsfw",
    "nude", "nudity", "stripper", "camgirl", "cam boy", "onlyfans",
    "handjob", "blowjob", "blow job", "cum", "ejaculate",
    "orgy", "gangbang", "milf", "fetish", "deepthroat",
    "penis", "cock", "dick", "vagina", "pussy", "boobs", "tits",
    # sexual minors / prohibited
    "pedo", "pedophile", "child porn", "loli", "incest", "bestiality",
    # violence / self-harm
    "kill", "murder", "behead", "suicide", "self harm",
}

_bad_word_re = re.compile(r"\b(" + "|".join(re.escape(w) for w in BAD_WORDS) + r")\b", re.I)

def _bad_word_hit(text: str) -> bool:
    return bool(_bad_word_re.search(text or ""))

async def moderate_text(text: str) -> tuple[bool, str]:
    try:
        resp = await aclient.moderations.create(
            model="omni-moderation-latest",
            input=text[:5000] if text else ""
        )
        res = resp.results[0]
        if getattr(res, "flagged", False):
            # Build a short reason from tripped categories, if available
            cats = []
            try:
                cats = [k for k, v in res.categories.items() if v]  # best effort
            except Exception:
                pass
            return False, ("Inappropriate content" + (f" ({', '.join(cats)})" if cats else ""))
    except Exception as e:
        logging.warning(f"[moderation] text API failed: {e}")

    # Fallback wordlist
    if _bad_word_hit(text or ""):
        return False, "Inappropriate content (word filter)"
    return True, ""

async def moderate_telegram_photo(file_id: str, bot) -> tuple[bool, str]:
    tmp_path = f"/tmp/{file_id}.jpg"
    try:
        tg_file = await bot.get_file(file_id)
        await tg_file.download_to_drive(tmp_path)
        with open(tmp_path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode("utf-8")

        # Use a small model to classify; response format we enforce is 'OK' or 'BLOCK: ...'
        resp = await aclient.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0,
            messages=[
                {"role": "system",
                "content": "You are an image safety checker. Reply exactly 'OK' if the image is safe for a general audience. "
                            "Reply 'BLOCK: <short reason>' if it contains nudity/sexual content (incl. toys), minors, graphic violence, self-harm, or hate symbols."},
                {"role": "user", "content": [
                    {"type": "text", "text": "Check this image for safety."},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}"}}
                ]}
            ]
        )
        decision = (resp.choices[0].message.content or "").strip()
        if decision.upper().startswith("OK"):
            return True, ""
        if decision.upper().startswith("BLOCK"):
            reason = decision.split(":", 1)[1].strip() if ":" in decision else "Unsafe image"
            return False, reason
    except Exception as e:
        logging.warning(f"[moderation] image moderation failed: {e}")
        return True, ""
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass

# === Language selection handlers (callable from /start or a separate /language later) ===
# /language
async def prompt_language(update: Update, context: ContextTypes.DEFAULT_TYPE):
    me = str(update.effective_user.id)
    if context.user_data.get('_in_listing_creation'):
        context.application.create_task(
            update.message.reply_text(tr(me, "finish_listing_first")),
            update=update
        )
        return
    context.application.create_task(
        update.message.reply_text(
        tr(me, "choose_language"),
        reply_markup=lang_keyboard()
        ),
        update=update
    )

# set_lang_<xx> callback
async def set_language_from_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    me = str(update.effective_user.id)
    data = q.data
    lang = data.split("_")[-1]
    if lang not in ("en", "pl", "uk"):
        lang = "en"
    set_user_lang(me, lang)
    context.application.create_task(
        q.message.reply_text(
        tr(me, "greeting"),
        reply_markup=main_menu_keyboard(me)
        ),
        update=update
    )


# === Start Command ===

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    me = str(update.effective_user.id)

    # 1) NEVER force a language for existing users
    #    This reads from cache/DB and seeds LANG_CACHE if needed.
    lang = get_user_lang(me)

    # 2) Ensure the user row exists, but don't override their language
    try:
        rows = supabase.table("users").select("id, language").eq("id", me).limit(1).execute().data
    except Exception as e:
        # If DB hiccups, still keep current lang and show menu
        rows = []

    if not rows:
        # First time we've seen this user — try to guess from Telegram
        guess = (update.effective_user.language_code or "").split("-")[0].lower()
        if guess in ("en", "pl", "uk"):
            set_user_lang(me, guess)
            lang = guess
        else:
            # keep whatever get_user_lang() returned (likely DEFAULT_LANG)
            set_user_lang(me, lang)

        # Create the user row (don’t re-override language)
        try:
            supabase.table("users").insert({"id": me, "language": lang}).execute()
        except Exception:
            pass

        # Show language picker only once (on first start)
        context.application.create_task(
            update.effective_message.reply_text(
                tr(me, "choose_language"),
                reply_markup=lang_keyboard()
            ),
            update=update
        )
        return

    # 3) Existing user — keep their saved language exactly as-is
    saved = rows[0].get("language") or lang
    LANG_CACHE[me] = saved  # refresh cache; do NOT set to 'en'

    # 4) Show your normal home/menu in the user’s current language
    context.application.create_task(
        update.effective_message.reply_text(
            tr(me, "home_tip"),
            reply_markup=main_menu_keyboard(me)
        ),
        update=update
    )


# === Back to Menu ===
_LISTING_CREATION_KEYS = (
    '_in_listing_creation',
    'category',
    'item_title',
    'brand_model',
    'specs',
    'description',
    'condition',
    'price_per_day',
    'currency',
    'photos',
    'location',
    'availability',
    'edit_photos',
    'create_idem_key'
)

async def go_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    me = str(update.effective_user.id)
    context.user_data.pop('_in_listing_creation', None)
    context.application.create_task(update.message.reply_text(tr(me, "menu_main"), reply_markup=main_menu_keyboard(me)), update=update)
    return ConversationHandler.END


# === Stop Listing Creation ===
async def stop_listing_creation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    me = str(update.effective_user.id)

    for key in list(context.user_data.keys()):
        if key in _LISTING_CREATION_KEYS:
            context.user_data.pop(key, None)

    await update.message.reply_text(
        tr(me, "stop_cancelled_creation"),
        reply_markup=main_menu_keyboard(me)
    )

    return ConversationHandler.END


# === My Account ===
async def handle_my_account(update: Update, context: ContextTypes.DEFAULT_TYPE):
    me = str(update.effective_user.id)

    # Listings of this lender
    listings = supabase.table("listings").select("*").eq("owner_id", me).execute().data or []
    listing_ids = [l["id"] for l in listings]

    # Pending + accepted requests
    pending = []
    accepted = []
    if listing_ids:
        # pending
        pending = supabase.table("rental_requests").select("*").in_("listing_id", listing_ids)\
                 .eq("lender_id", me).eq("status", "pending").execute().data or []
        # accepted
        accepted = supabase.table("rental_requests").select("*").in_("listing_id", listing_ids)\
                   .eq("lender_id", me).eq("status", "accepted").execute().data or []

    # Stats
    total_listings = len(listings)
    pending_count = len(pending)

    # Upcoming bookings = accepted requests with any date >= today
    today = datetime.utcnow().date()
    def _any_upcoming(rr):
        return any((_parse_date_any(t) or date.min) >= today for t in rr.get("dates", []))
    upcoming_count = sum(1 for rr in accepted if _any_upcoming(rr))

    # Bookable days next 30
    bookable_30 = sum(_count_bookable_days_next_30(l) for l in listings)

    # Estimated earnings next 30 by currency
    listing_by_id = {l["id"]: l for l in listings}
    earn_by_cur: dict[str, float] = {}

    for rr in accepted:
        l = listing_by_id.get(rr["listing_id"])
        if not l:
            continue
        cur = (l.get("currency") or "PLN").upper()
        p = float(l.get("price_per_day") or 0)
        for t in rr.get("dates", []):
            d = _parse_date_any(t)
            if d and _in_next_n_days(d, 30):
                earn_by_cur[cur] = round(earn_by_cur.get(cur, 0.0) + p, 2)

    if earn_by_cur:
        earn_lines = "\n".join(f"• {amt:.2f} {cur}" for cur, amt in earn_by_cur.items())
    else:
        earn_lines = "• 0" if not earn_by_cur else "\n".join(f"• {amt:.2f} {cur}" for cur, amt in earn_by_cur.items())

    # NEW: quota line
    quota = _quota_line_text(me)

    text = (
        f"👤 *{tr(me, 'account_title')}*\n\n"
        f"📦 {tr(me, 'account_your_listings')}: *{total_listings}*\n"
        f"⏳ {tr(me, 'account_pending_requests')}: *{pending_count}*\n"
        f"📅 {tr(me, 'account_upcoming_bookings')}: *{upcoming_count}*\n"
        f"🗓 {tr(me, 'account_bookable_30')}: *{bookable_30}*\n"
        f"💰 {tr(me, 'account_est_earnings')}\n{earn_lines}\n\n"
        f"{quota}"
    )

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(tr(me, "btn_check_requests"), callback_data="account_requests")],
        [InlineKeyboardButton(tr(me, "btn_upcoming_borrowings"), callback_data="my_borrowings")],
        # CHANGED: use your purchase label from _I18N_PURCHASE_PATCH
        [InlineKeyboardButton(tr(me, "btn_purchase"), callback_data="shop_open")]
    ])
    context.application.create_task(update.message.reply_text(text, parse_mode="Markdown", reply_markup=kb), update=update)

async def account_my_borrowings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    me = str(update.effective_user.id)

    # Accepted requests where I'm the borrower
    rrs = supabase.table("rental_requests")\
        .select("listing_id, dates, status")\
        .eq("borrower_id", me).eq("status", "accepted")\
        .execute().data or []

    if not rrs:
        context.application.create_task(q.message.edit_text(tr(me, "no_upcoming_borrowings")), update=update)
        return

    # Gather upcoming days per listing
    today = datetime.utcnow().date()
    by_listing: dict[str, set[str]] = {}
    for rr in rrs:
        keep = []
        for t in rr.get("dates") or []:
            d = _parse_date_any(t)
            if d and d >= today:
                keep.append(d.strftime("%Y-%m-%d"))
        if keep:
            by_listing.setdefault(rr["listing_id"], set()).update(keep)

    if not by_listing:
        context.application.create_task(q.message.edit_text(tr(me, "no_upcoming_borrowings")), update=update)
        return

    # Fetch listings and owners
    listing_ids = list(by_listing.keys())
    listings = supabase.table("listings").select("id,item,description,owner_id").in_("id", listing_ids).execute().data or []
    by_id = {l["id"]: l for l in listings}

    owner_ids = list({l["owner_id"] for l in listings if l.get("owner_id")})
    owners = supabase.table("users").select("id,telegram_username").in_("id", owner_ids).execute().data or []
    owner_name = {u["id"]: (u.get("telegram_username") or "") for u in owners}

    blocks = []
    for lid, days in by_listing.items():
        l = by_id.get(lid)
        if not l:
            continue
        item_e = esc_md2(l.get("item") or "Item")
        desc_e = esc_md2(l.get("description") or "")
        lender_un = owner_name.get(l.get("owner_id", ""), "")
        lender_tag = esc_md2(f"@{lender_un}") if lender_un else esc_md2("—")
        ranges_str = format_date_ranges_from_tokens(sorted(days))
        ranges_e = esc_md2(ranges_str)

        block = (
            f"🏷️ *{item_e}*\n"
            f"📄 {desc_e}\n"
            f"👤 {tr(me, 'from')}: {lender_tag}\n"
            f"🗓 {tr(me, 'when')}:\n{ranges_e}"
        )
        blocks.append(block)

    if not rrs or not by_listing:
        context.application.create_task(
            q.message.edit_text(
                tr(me, "no_upcoming_borrowings"),
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(f"⬅️ {tr(me, 'back')}", callback_data="account_overview")]])
            ),
            update=update
        )
        return

    text = "📅 *" + esc_md2(tr(me, "btn_upcoming_borrowings").replace("📅 ", "")) + "*\n\n" + "\n\n".join(blocks)
    kb = InlineKeyboardMarkup([[InlineKeyboardButton(f"⬅️ {tr(me, 'back')}", callback_data="account_overview")]])
    context.application.create_task(q.message.edit_text(text, parse_mode="MarkdownV2", reply_markup=kb), update=update)

async def account_overview(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    me = str(update.effective_user.id)

    # ==== same stats logic as handle_my_account() ====
    listings = supabase.table("listings").select("*").eq("owner_id", me).execute().data or []
    listing_ids = [l["id"] for l in listings]
    pending = accepted = []
    if listing_ids:
        pending = supabase.table("rental_requests").select("*").in_("listing_id", listing_ids)\
                 .eq("lender_id", me).eq("status", "pending").execute().data or []
        accepted = supabase.table("rental_requests").select("*").in_("listing_id", listing_ids)\
                   .eq("lender_id", me).eq("status", "accepted").execute().data or []

    today = datetime.utcnow().date()
    def _any_upcoming(rr): return any((_parse_date_any(t) or date.min) >= today for t in rr.get("dates", []))
    upcoming_count = sum(1 for rr in accepted if _any_upcoming(rr))
    total_listings = len(listings)
    pending_count = len(pending)
    bookable_30 = sum(_count_bookable_days_next_30(l) for l in listings)

    listing_by_id = {l["id"]: l for l in listings}
    earn_by_cur = {}
    for rr in accepted:
        l = listing_by_id.get(rr["listing_id"])
        if not l: continue
        cur = (l.get("currency") or "PLN").upper()
        p = float(l.get("price_per_day") or 0)
        for t in rr.get("dates", []):
            d = _parse_date_any(t)
            if d and d >= today and d <= today + timedelta(days=30):
                earn_by_cur[cur] = round(earn_by_cur.get(cur, 0.0) + p, 2)
    earn_lines = "• 0" if not earn_by_cur else "\n".join(f"• {amt:.2f} {cur}" for cur, amt in earn_by_cur.items())

    # NEW: quota line
    quota = _quota_line_text(me)

    text = (
        f"👤 *{tr(me, 'account_title')}*\n\n"
        f"📦 {tr(me, 'account_your_listings')}: *{total_listings}*\n"
        f"⏳ {tr(me, 'account_pending_requests')}: *{pending_count}*\n"
        f"📅 {tr(me, 'account_upcoming_bookings')}: *{upcoming_count}*\n"
        f"🗓 {tr(me, 'account_bookable_30')}: *{bookable_30}*\n"
        f"💰 {tr(me, 'account_est_earnings')}\n{earn_lines}\n\n"
        f"{quota}"
    )

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(tr(me, "btn_check_requests"), callback_data="account_requests")],
        [InlineKeyboardButton(tr(me, "btn_upcoming_borrowings"), callback_data="my_borrowings")],
        # CHANGED: same as above
        [InlineKeyboardButton(tr(me, "btn_purchase"), callback_data="shop_open")]
    ])
    context.application.create_task(q.message.edit_text(text, parse_mode="Markdown", reply_markup=kb), update=update)

async def account_requests(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    me = str(update.effective_user.id)

    # Load my pending requests (lender)
    rr = supabase.table("rental_requests").select("*").eq("lender_id", me).eq("status", "pending")\
         .order("created_at", desc=False).execute().data or []
    if not rr:
        context.application.create_task(
            q.message.edit_text(
                tr(me, "no_outstanding_requests"),
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(f"⬅️ {tr(me, 'back')}", callback_data="account_overview")]])
            ),
            update=update
        )
        return

    # Fetch listings for enrichment (titles/prices)
    listing_ids = list({x["listing_id"] for x in rr})
    listings = supabase.table("listings").select("*").in_("id", listing_ids).execute().data or []
    by_id = {l["id"]: l for l in listings}
    enriched = []
    for r in rr:
        l = by_id.get(r["listing_id"])
        if l:
            r["_listing"] = l
            enriched.append(r)

    context.user_data["pending_reqs"] = enriched
    context.user_data["pending_req_idx"] = 0
    await _show_request_card(q, context)

async def account_req_next(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    arr = context.user_data.get("pending_reqs", [])
    if not arr:
        context.application.create_task(q.message.edit_text("No requests."), update=update)
        return
    context.user_data["pending_req_idx"] = (context.user_data.get("pending_req_idx", 0) + 1) % len(arr)
    await _show_request_card(q, context)

async def account_req_prev(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    arr = context.user_data.get("pending_reqs", [])
    if not arr:
        context.application.create_task(q.message.edit_text("No requests."), update=update)
        return
    context.user_data["pending_req_idx"] = (context.user_data.get("pending_req_idx", 0) - 1) % len(arr)
    await _show_request_card(q, context)

async def _show_request_card(update_or_query, context: ContextTypes.DEFAULT_TYPE):
    q = getattr(update_or_query, "callback_query", None) or update_or_query
    me = str(q.from_user.id if getattr(q, "from_user", None) else update_or_query.effective_user.id)
    arr = context.user_data.get("pending_reqs", [])
    idx = context.user_data.get("pending_req_idx", 0)
    if not arr:
        await q.message.edit_text(tr(me, "no_outstanding_requests"))
        return

    req = arr[idx]
    l = req["_listing"]
    currency = l.get("currency", "PLN")
    price_per_day = float(l.get("price_per_day") or 0)
    days = len(req.get("dates", []))
    total = round(price_per_day * days, 2)

    # Escape dynamic bits for MarkdownV2
    category = esc_md2(l.get("category", ""))
    item = esc_md2(l.get("item") or "Item")
    dates_block = esc_md2(format_date_ranges_from_tokens(req.get("dates", [])))
    currency_e = esc_md2(currency)
    price_per_day_str = esc_md2(f"{price_per_day:.2f}")
    total_str = esc_md2(f"{total:.2f}")

    note = req.get("message_from_borrower") or ""
    note_e = esc_md2(note) if note else "—"

    header = esc_md2(tr(me, "request_prefix", i=idx+1, n=len(arr)))
    text = (
        f"📬 *{header}*\n\n"
        f"🧰 *{category}*\n"
        f"🏷️ *{item}*\n"
        f"📝 {esc_md2(tr(me, 'borrower_request'))}: {note_e}\n"
        f"📅\n{dates_block}\n"
        f"💰 {price_per_day_str} {currency_e}/day · {esc_md2(tr(me, 'price_total'))}: {total_str} {currency_e}"
    )

    nav = []
    if len(arr) > 1:
        nav = [InlineKeyboardButton(tr(me, "req_nav_prev"), callback_data="account_req_prev"),
            InlineKeyboardButton(tr(me, "req_nav_next"), callback_data="account_req_next")]

    kb = [
        [InlineKeyboardButton(f"✅ {tr(me, 'accept')}", callback_data=f"req_accept_{req['id']}"),
         InlineKeyboardButton(f"❌ {tr(me, 'decline')}", callback_data=f"req_decline_{req['id']}")]
    ]
    if nav:
        kb.append(nav)
    kb.append([InlineKeyboardButton(f"⬅️ {tr(me, 'back')}", callback_data="account_overview")])

    await q.message.edit_text(text, parse_mode="MarkdownV2", reply_markup=InlineKeyboardMarkup(kb))

async def handle_request_accept(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return

    me = str(q.from_user.id)  # <-- define before using "me"
    await q.answer(tr(me, "accepted_and_booked"), show_alert=True)  # nice popup

    data = q.data or ""
    req_id = data.split("_", 2)[-1]

    rr_list = supabase.table("rental_requests").select("*").eq("id", req_id).execute().data
    if not rr_list:
        context.application.create_task(
            q.message.edit_text(
                tr(me, "request_not_found"),
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton(tr(me, "back_to_requests"), callback_data="account_requests")]]
                )
            ),
            update=update
        )
        return
    rr = rr_list[0]

    listing = supabase.table("listings").select("*").eq("id", rr["listing_id"]).execute().data[0]
    requested = rr.get("dates", []) or []

    # 1) Update status + mark booked days
    supabase.table("rental_requests").update({"status": "accepted"}).eq("id", req_id).execute()
    booked = (listing.get("booked_days") or [])
    new_booked = sorted(set(booked) | set(_to_iso_list(requested)))
    supabase.table("listings").update({"booked_days": new_booked}).eq("id", listing["id"]).execute()

    # 2) Compute UI numbers
    currency = (listing.get("currency") or "PLN").upper()
    price_per_day = float(listing.get("price_per_day") or 0)
    total = round(price_per_day * len(requested), 2)
    pretty_ranges = format_date_ranges_from_tokens(requested)

    item_e   = esc_md2(listing.get('item') or 'Item')
    ranges_e = esc_md2(pretty_ranges)
    cur_e    = esc_md2(currency)
    tot_e    = esc_md2(f"{total:.2f}")

    # 3) Borrower contact line
    borrower_username = (rr.get("borrower_username") or "").strip()
    borrower_tg_id = rr.get("borrower_id")

    if borrower_username:
        borrower_contact_line = f"👤 {esc_md2('@' + borrower_username)}"
        borrower_intro_line = ""  # no intro needed
    else:
        borrower_contact_line = f"[👤](tg://user?id={borrower_tg_id})"
        borrower_intro_line = esc_md2(tr(me, "no_public_username"))

    # 4) Final lender-facing message (with nudge)
    text = (
        f"✅ {esc_md2(tr(me, 'accepted_and_booked'))}\n\n"
        f"🏷️ *{item_e}*\n"
        f"📅\n{ranges_e}\n"
        f"💰 {esc_md2(tr(me, 'price_total'))}: {tot_e} {cur_e}\n\n"
        f"{borrower_intro_line}\n"
        f"{borrower_contact_line}\n\n"
        f"{esc_md2(tr(me, 'nudge_text_v2'))}"
    )

    context.application.create_task(
        q.message.edit_text(
            text,
            parse_mode="MarkdownV2",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton(tr(me, 'back_to_requests'), callback_data='account_requests')]]
            ),
        ),
        update=update
    )

    # 5) Notify borrower
    try:
        borrower_id = int(rr["borrower_id"])
        context.application.create_task(
            context.bot.send_message(
                chat_id=borrower_id,
                text=(
                    f"🎉 *{esc_md2(tr(str(borrower_id), 'accepted_title'))}*\n\n"
                    f"🏷️ *{item_e}*\n"
                    f"📅\n{ranges_e}\n"
                    f"💰 {esc_md2(tr(str(borrower_id), 'price_total'))}: {tot_e} {cur_e}"
                ),
                parse_mode="MarkdownV2"
            ),
            update=update
        )
    except Exception as e:
        logging.warning(f"Failed to notify borrower: {e}")

async def handle_request_decline(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    me = str(update.effective_user.id)
    req_id = q.data.split("_", 2)[2]

    rr_list = supabase.table("rental_requests").select("*").eq("id", req_id).execute().data
    if not rr_list:
        context.application.create_task(
            q.message.edit_text(
                tr(me, "request_not_found"),
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton(tr(me, "back_to_requests"), callback_data="account_requests")]]
                ),
            ),
            update=update
        )
        return
    rr = rr_list[0]

    supabase.table("rental_requests").update({"status": "declined"}).eq("id", req_id).execute()

    context.application.create_task(
        q.message.edit_text(
            f"❌ {tr(me, 'request_declined')}",
            parse_mode="MarkdownV2",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton(tr(me, "back_to_requests"), callback_data="account_requests")]]
            ),
        ),
        update=update
    )

    # Try to notify borrower (escape item name)
    try:
        listing = supabase.table("listings").select("item").eq("id", rr["listing_id"]).execute().data[0]
        item_e = esc_md2((listing or {}).get("item") or "Item")
        borrower_id = int(rr["borrower_id"])
        context.application.create_task(
            context.bot.send_message(
                chat_id=borrower_id,
                text=tr(str(borrower_id), "request_declined_borrower", item=item_e),
                parse_mode="MarkdownV2",
            ),
            update=update
        )
    except Exception as e:
        logging.warning(f"Failed to notify borrower about decline: {e}")

# === Shop ===
async def shop_open(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    me = str(update.effective_user.id)

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(tr(me, "purchase_pack2"), callback_data="shop_buy_2")],
        [InlineKeyboardButton(tr(me, "purchase_pack5"), callback_data="shop_buy_5")],
        [InlineKeyboardButton(tr(me, "purchase_unlm"),  callback_data="shop_buy_sub")],
        [InlineKeyboardButton(tr(me, "purchase_back"),  callback_data="account_overview")],
    ])
    txt = f"⭐ *{tr(me,'purchase_title')}*\n\n{_quota_line_text(me)}\n\n{tr(me,'purchase_pick')}"
    context.application.create_task(q.message.edit_text(txt, parse_mode="Markdown", reply_markup=kb), update=update)


# replace your helper with this
async def _send_stars_invoice(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    title: str,
    description: str,
    payload: str,
    amount_stars: int,
):
    q = update.callback_query
    await q.answer()
    await context.bot.send_invoice(
        chat_id=q.message.chat.id,
        title=title,
        description=description,
        payload=payload,     # used later in payment_success
        provider_token="",   # Stars
        currency="XTR",
        prices=[LabeledPrice(label=title, amount=amount_stars)],
        is_flexible=False,
    )


async def shop_buy_2(update: Update, context: ContextTypes.DEFAULT_TYPE):
    me = str(update.effective_user.id)
    await _send_stars_invoice(update, context, tr(me,"purchase_pack2"), tr(me,"purchase_pack2"), "slots2", 100)

async def shop_buy_5(update: Update, context: ContextTypes.DEFAULT_TYPE):
    me = str(update.effective_user.id)
    await _send_stars_invoice(update, context, tr(me,"purchase_pack5"), tr(me,"purchase_pack5"), "slots5", 250)

async def shop_buy_sub(update: Update, context: ContextTypes.DEFAULT_TYPE):
    me = str(update.effective_user.id)
    await _send_stars_invoice(update, context, tr(me,"purchase_unlm"), tr(me,"purchase_unlm"), "sub1m", 350)

async def payment_precheckout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.pre_checkout_query.answer(ok=True)

async def payment_success(update: Update, context: ContextTypes.DEFAULT_TYPE):
    me = str(update.effective_user.id)
    sp = update.message.successful_payment
    payload = sp.invoice_payload
    try:
        if payload in ("slots2", "slots5"):
            inc = 2 if payload == "slots2" else 5
            cur = (await run_io(lambda: supabase.table("users")
                                .select("paid_slots")
                                .eq("id", me).execute())).data
            cur_val = int((cur or [{}])[0].get("paid_slots") or 0)
            new_paid = cur_val + inc
            await run_io(lambda: supabase.table("users")
                         .update({"paid_slots": new_paid})
                         .eq("id", me).execute())

            # Always compute a finite slot count for the message
            limit_display = 2 + new_paid
            # If Unlimited is active, clarify this is for after Unlimited ends
            _, _, sub_active = get_entitlement(me)
            note = " (applies when Unlimited ends)" if sub_active else ""
            context.application.create_task(update.message.reply_text(tr(me, "purchase_thanks_slots", limit=limit_display) + note), update=update)

        elif payload == "sub1m":
            row = (await run_io(lambda: supabase.table("users")
                                .select("subscription_until")
                                .eq("id", me).execute())).data
            cur_until = _parse_ts_iso((row or [{}])[0].get("subscription_until"))
            base = cur_until if (cur_until and cur_until > datetime.utcnow()) else datetime.utcnow()
            new_until_dt = base + timedelta(days=30)
            await run_io(lambda: supabase.table("users")
                         .update({"subscription_until": new_until_dt.isoformat() + "Z"})
                         .eq("id", me).execute())
            context.application.create_task(update.message.reply_text(tr(me, "purchase_thanks_unlm", until=new_until_dt.strftime("%Y-%m-%d"))), update=update)
        else:
            context.application.create_task(update.message.reply_text("❌"), update=update)
            return
    except Exception:
        logging.exception("Purchase update failed")
        context.application.create_task(update.message.reply_text("❌"), update=update)
        return

# --- i18n additions for Settings/Browse (patch LOCALES defined above) ---
_I18N_PATCH = {
    "en": {
        # Settings / location
        "settings_title": "Settings:",
        "settings_change_location": "Change Location",
        "prompt_location_how": "How would you like to set your location?",
        "share_location_btn": "Share Location with Telegram",
        "type_location_btn": "Type in Location Manually",
        "location_saved": "✅ Location saved!",
        "location_saved_named": "✅ Location '{address}' saved!",
        "location_not_found": "❗ Could not find that location. Please try again with a city name.",
        # Browse
        "browse_prompt": "Please type what you need. For example: “I want to rent a dirt bike in Zabrze”",
        "couldnt_understand": "❗ Sorry, I couldn’t understand your request.",
        "no_listings_db": "No listings found in the database.",
        "nothing_found_try_again": "😕 Sorry, nothing found — try a different keyword.",
        "not_provided": "Not provided",
        "label_condition": "Condition",
        "label_location": "Location",
        "label_availability": "Availability",
        "per_day": "day",
        "btn_prev": "⬅️ Previous",
        "btn_next": "➡️ Next",
        "btn_rent": "🗓 Rent",
        "thanks_photos_received": "Thanks! Photos received.",
        "send_photo_or_cmd": "❗ Please send a photo or use text commands like 'Delete X', 'Add More', 'Continue', or 'Cancel Listing'.",
    },
    "pl": {
        # Settings / location
        "settings_title": "Ustawienia:",
        "settings_change_location": "Zmień lokalizację",
        "prompt_location_how": "W jaki sposób chcesz ustawić lokalizację?",
        "share_location_btn": "Udostępnij lokalizację przez Telegram",
        "type_location_btn": "Wpisz lokalizację ręcznie",
        "location_saved": "✅ Lokalizacja zapisana!",
        "location_saved_named": "✅ Zapisano lokalizację „{address}”!",
        "location_not_found": "❗ Nie udało się znaleźć tej lokalizacji. Spróbuj nazwę miasta.",
        # Browse
        "browse_prompt": "Napisz czego potrzebujesz. Przykład: „Chcę wypożyczyć crossa w Zabrzu”",
        "couldnt_understand": "❗ Przepraszam, nie zrozumiałem prośby.",
        "no_listings_db": "Brak ogłoszeń w bazie.",
        "nothing_found_try_again": "😕 Nic nie znaleziono — spróbuj inne słowo kluczowe.",
        "not_provided": "Brak danych",
        "label_condition": "Stan",
        "label_location": "Lokalizacja",
        "label_availability": "Dostępność",
        "per_day": "dzień",
        "btn_prev": "⬅️ Poprzednie",
        "btn_next": "➡️ Następne",
        "btn_rent": "🗓 Zarezerwuj",
        "thanks_photos_received": "Dzięki! Zdjęcia otrzymane.",
        "send_photo_or_cmd": "❗ Wyślij zdjęcie lub użyj poleceń: 'Delete X', 'Add More', 'Continue', 'Cancel Listing'.",
    },
    "uk": {
        # Settings / location
        "settings_title": "Налаштування:",
        "settings_change_location": "Змінити локацію",
        "prompt_location_how": "Як ви хочете встановити свою локацію?",
        "share_location_btn": "Поділитися геопозицією в Telegram",
        "type_location_btn": "Ввести локацію вручну",
        "location_saved": "✅ Локацію збережено!",
        "location_saved_named": "✅ Локацію «{address}» збережено!",
        "location_not_found": "❗ Не вдалося знайти цю локацію. Спробуйте назву міста.",
        # Browse
        "browse_prompt": "Опишіть, що вам потрібно. Наприклад: «Хочу орендувати фото комеру в Луцьку»",
        "couldnt_understand": "❗ Вибачте, я не зміг зрозуміти запит.",
        "no_listings_db": "У базі немає оголошень.",
        "nothing_found_try_again": "😕 Нічого не знайдено — спробуйте інше ключове слово.",
        "not_provided": "Немає даних",
        "label_condition": "Стан",
        "label_location": "Локація",
        "label_availability": "Доступність",
        "per_day": "доба",
        "btn_prev": "⬅️ Попереднє",
        "btn_next": "➡️ Наступне",
        "btn_rent": "🗓 Забронювати",
        "thanks_photos_received": "Дякую! Фото отримано.",
        "send_photo_or_cmd": "❗ Надішліть фото або використайте команди: 'Delete X', 'Add More', 'Continue', 'Cancel Listing'.",
    },
}
try:
    for _lng, _patch in _I18N_PATCH.items():
        if _lng in LOCALES:
            LOCALES[_lng].update(_patch)
except Exception as _e:
    logging.warning(f"[i18n] LOCALES patch failed: {_e}")

_PHOTO_EDIT_I18N = {
    "en": {
        "photos_edit_help": "Edit photos: send up to 3 photos or use the buttons. Changes are saved only when you press *Done*.",
        "photos_edit_clear": "Clear",
        "photos_edit_done": "Done",
        "photos_edit_cancel": "Cancel",
    },
    "pl": {
        "photos_edit_help": "Edytuj zdjęcia: wyślij do 3 zdjęć lub użyj przycisków. Zmiany zapisują się dopiero po naciśnięciu *Done*.",
        "photos_edit_clear": "Wyczyść",
        "photos_edit_done": "Zapisz",
        "photos_edit_cancel": "Anuluj",
    },
    "uk": {
        "photos_edit_help": "Редагуйте фото: надішліть до 3 фото або скористайтесь кнопками. Зміни зберігаються лише після натискання *Done*.",
        "photos_edit_clear": "Очистити",
        "photos_edit_done": "Готово",
        "photos_edit_cancel": "Скасувати",
    },
}
for _lng, _patch in _PHOTO_EDIT_I18N.items():
    if _lng in LOCALES:
        LOCALES[_lng].update(_patch)

# === Settings ===
async def save_location_from_gps(update: Update, context: ContextTypes.DEFAULT_TYPE):
    loc = update.message.location
    user_id = str(update.effective_user.id)
    lat_lon = f"{loc.latitude},{loc.longitude}"
    supabase.table("users").update({"location": lat_lon}).eq("id", user_id).execute()
    context.application.create_task(update.message.reply_text(tr(user_id, "location_saved"), reply_markup=ReplyKeyboardRemove()), update=update)
    return await go_back(update, context)

async def save_location_from_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_input = update.message.text
    user_id = str(update.effective_user.id)

    location = await async_geocode(user_input)
    if location:
        lat_lon = f"{location.latitude},{location.longitude}"
        supabase.table("users").update({"location": lat_lon}).eq("id", user_id).execute()
        context.application.create_task(
            update.message.reply_text(
                tr(user_id, "location_saved_named", address=location.address),
                reply_markup=ReplyKeyboardRemove()
            ),
            update=update
        )
        return await go_back(update, context)

    context.application.create_task(update.message.reply_text(tr(user_id, "location_not_found")), update=update)
    return AWAIT_LOCATION_CHOICE


async def prompt_location_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    me = str(update.effective_user.id)
    keyboard = [
        [KeyboardButton(tr(me, "share_location_btn"), request_location=True)],
        [tr(me, "type_location_btn")],
        [tr(me, "back")]
    ]
    context.application.create_task(
        update.message.reply_text(
            tr(me, "prompt_location_how"),
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        ),
        update=update
    )
    return AWAIT_LOCATION_CHOICE

async def handle_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    me = str(update.effective_user.id)
    keyboard = [[tr(me, "settings_change_location")], [tr(me, "back")]]
    context.application.create_task(
        update.message.reply_text(
            tr(me, "settings_title"),
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        ),
        update=update
    )
    return SETTINGS_MENU

CATEGORY_ALIASES = {
    "party": "Events & Party",
    "party equipment": "Events & Party",
    "home tools": "Home Improvement",
    "home appliances": "Home Improvement",
    "electronics": "Electronics",
    "recreation": "Recreation",
    "construction": "Construction",
    "tools": "Construction",
    "garden": "Gardening",
    "gardening": "Gardening",
}

# === Browse ===
async def generate_embedding(text: str):
    response = await aclient.embeddings.create(
        model="text-embedding-3-small",
        input=text,
    )
    return response.data[0].embedding

async def handle_browse(update: Update, context: ContextTypes.DEFAULT_TYPE):
    me = str(update.effective_user.id)
    if context.user_data.get('_in_listing_creation'):
        context.application.create_task(update.message.reply_text(tr(me, "finish_listing_first")), update=update)
        return None
    context.application.create_task(update.message.reply_text(tr(me, "browse_prompt")), update=update)
    return AWAIT_SEARCH_QUERY

async def handle_natural_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_query = update.message.text
    context.user_data["last_search_query"] = user_query
    user_id = str(update.effective_user.id)

    prompt = f"""
    You are a smart assistant helping users rent tools. From this request:
    "{user_query}"

    Extract:
    - category (e.g. 'Construction', 'Recreation', 'Electronics', etc.)
    - keyword (specific item like 'bike', 'guitar', 'drill')
    - location (city only)

    Respond strictly in this JSON format with double quotes:

    {{ 
    "category": "...", 
    "keyword": "...", 
    "location": "..." 
    }}
    """

    # ---- LLM parsing section ----
    try:
        llm_resp = await aclient.chat.completions.create(
            model="gpt-4o-mini",
            response_format={"type": "json_object"},
            temperature=0,
            messages=[{"role": "user", "content": prompt}],
        )
        raw_output = (llm_resp.choices[0].message.content or "").strip()
        if "```json" in raw_output:
            raw_output = raw_output.split("```json")[1].split("```")[0].strip()
        elif "```" in raw_output:
            raw_output = raw_output.split("```")[1].strip()
        data = json.loads(raw_output)
    except Exception as e:
        print("LLM Output Error:", e)
        context.application.create_task(update.message.reply_text(tr(user_id, "couldnt_understand")), update=update)
        return ConversationHandler.END


    keyword = data.get("keyword", "").lower()
    location = data.get("location", "")
    category = data.get("category", "").lower()
    normalized_category = CATEGORY_ALIASES.get(category.lower(), category.title())
    print("Normalized category:", normalized_category)

    # Step 2: Fetch all listings
    today_str = datetime.today().strftime("%Y-%m-%d")
    result = supabase.table("listings").select("*").execute()
    all_listings = result.data or []

    # Parse location into user coordinates (optional proximity logic later)
    try:
        target_city = await async_geocode(location) if location else None
        user_coords = (target_city.latitude, target_city.longitude) if target_city else None
    except:
        user_coords = None

    # Convert location strings to coordinates
    def parse_coords(loc_str):
        try:
            lat, lon = map(float, loc_str.split(","))
            return (lat, lon)
        except:
            return None

    for l in all_listings:
        l["location_coords"] = parse_coords(l.get("location", ""))
        l["is_available"] = today_str in (l.get("availability") or [])

    # ---- Embedding + match section ----
    query_embedding = (await aclient.embeddings.create(
        model="text-embedding-3-small",
        input=user_query
    )).data[0].embedding


    print("Calling match_listings() with:", query_embedding[:5], "...", location)
    match_resp = supabase.rpc("match_listings", {
        "query_embedding": query_embedding,
        "match_threshold": 0.0,
        "match_count": 5
    }).execute()

    logging.info(
        "[EMB TEST] top hits: %s",
        [(r["id"], round(r.get("similarity", 0), 3), r.get("description", "")) for r in (match_resp.data or [])]
    )

    matched_listings = match_resp.data or []
    score_by_id = {r["id"]: r.get("similarity", 0.0) for r in matched_listings}

    # 🔎 DEBUG: see what columns the RPC returned
    if matched_listings:
        logging.info("[BROWSE DEBUG] RPC keys: %s", list(matched_listings[0].keys()))

    # ⬇️ Rehydrate with full records so 'item' and 'specs' are present
    ids = [r["id"] for r in matched_listings]
    if ids:
        full = (await run_io(lambda: supabase.table("listings").select("*").in_("id", ids).execute())).data or []
        for r in full:
            r["similarity"] = score_by_id.get(r["id"], 0.0)
        matched_listings = sorted(full, key=lambda r: r.get("similarity", 0.0), reverse=True)

    for i, l in enumerate(matched_listings):
        print(f"{i+1}. {l.get('description','')} → similarity: {round(l.get('similarity', 0), 3)}")

    # Fallback if nothing matched
    if not matched_listings:
        fallback = [
            l for l in all_listings
            if normalized_category.lower() in (l.get("category","")).lower()
            and (keyword in (l.get("description","")).lower() or keyword in (l.get("item","")).lower())
            and l.get("is_available")
        ]
        matched_listings = fallback

    print(f"Matched {len(matched_listings)} listings")
    for l in matched_listings:
        print("→", l.get('description',''), "|", l.get('category',''), "|", l.get('location',''))

    # Store in context
    context.user_data["matched_listings"] = matched_listings
    context.user_data["browse_index"] = 0

    if not all_listings:
        context.application.create_task(update.message.reply_text(tr(user_id, "no_listings_db")), update=update)
        return ConversationHandler.END

    if not matched_listings:
        # Fallback 2: very naive keyword+location check
        all_listings2 = supabase.table("listings").select("*").execute().data
        matched_listings = [
            l for l in all_listings2
            if keyword.lower() in (l.get("description","")).lower()
            and location.lower() in (l.get("location","")).lower()
            and today_str in (l.get("availability") or [])
        ]
        if matched_listings:
            print(f"Fallback matched {len(matched_listings)} listings.")
        else:
            context.application.create_task(update.message.reply_text(tr(user_id, "nothing_found_try_again")), update=update)
            return ConversationHandler.END

    await send_browse_listing(update, context)
    return ConversationHandler.END

async def browse_next_match(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    listings = context.user_data.get("matched_listings", [])
    if not listings:
        context.application.create_task(q.message.edit_text("No results to browse."), update=update)
        return
    context.user_data["browse_index"] = (context.user_data.get("browse_index", 0) + 1) % len(listings)
    await send_browse_listing(update, context)

async def browse_prev_match(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    listings = context.user_data.get("matched_listings", [])
    if not listings:
        context.application.create_task(q.message.edit_text("No results to browse."), update=update)
        return
    context.user_data["browse_index"] = (context.user_data.get("browse_index", 0) - 1) % len(listings)
    await send_browse_listing(update, context)

async def send_browse_listing(update_or_query, context):
    listings = context.user_data.get("matched_listings", [])
    index = context.user_data.get("browse_index", 0)
    if not listings:
        return

    # --- unify who/where we’re replying to (works for Update or raw CallbackQuery) ---
    q = getattr(update_or_query, "callback_query", None)
    if q is not None:
        # Case: Update with a callback_query
        me = str(q.from_user.id)
        chat = q.message.chat
    elif hasattr(update_or_query, "from_user") and hasattr(update_or_query, "message"):
        # Case: raw CallbackQuery was passed in directly
        q = update_or_query
        me = str(q.from_user.id)
        chat = q.message.chat
    else:
        # Case: plain Update (no callback)
        me = str(update_or_query.effective_user.id)
        chat = update_or_query.effective_chat

    listing = listings[index]

    # Quick wordlist guard for legacy content (optional)
    joined_text = " ".join(filter(None, [
        nz(listing.get("category")),
        nz(listing.get("item")),
        ", ".join(coerce_list(listing.get("specs"))),
        nz(listing.get("description")),
        nz(listing.get("condition")),
        location_name_from_coords(nz(listing.get("location"))),
    ])).strip()
    if _bad_word_hit(joined_text):
        nxt = index + 1
        context.user_data["browse_index"] = nxt
        if nxt < len(listings):
            return await send_browse_listing(update_or_query, context)
        return

    photos = coerce_list(listing.get("photos"))
    title  = listing.get("item") or "Item" or listing.get("title")
    specs  = coerce_list(listing.get("specs"))
    cur    = listing.get("currency", "PLN")

    # 🔧 use `me` everywhere for translations (no direct effective_user access)
    not_prov = tr(me, "not_provided")
    cond_lbl = tr(me, "label_condition")
    per_day  = tr(me, "per_day")

    def format_availability(dates):
        if not dates:
            return not_prov
        dates = sorted(dates)
        result = []
        current_start = current_end = datetime.strptime(dates[0], "%Y-%m-%d")
        for d in dates[1:]:
            d_parsed = datetime.strptime(d, "%Y-%m-%d")
            if d_parsed == current_end + timedelta(days=1):
                current_end = d_parsed
            else:
                result.append(f"{current_start.strftime('%d/%m/%Y')} - {current_end.strftime('%d/%m/%Y')}")
                current_start = current_end = d_parsed
        result.append(f"{current_start.strftime('%d/%m/%Y')} - {current_end.strftime('%d/%m/%Y')}")
        return "\n".join(result)

    availability_str = format_availability(listing.get("availability"))

    # escape AFTER availability_str is built
    cat_e   = esc_md2(listing.get('category', ''))
    title_e = esc_md2(title)
    specs_e = esc_md2(", ".join(specs[:4])) if specs else ""
    desc_e  = esc_md2(listing.get('description', ''))
    cond_e  = esc_md2(listing.get('condition', not_prov))
    price_e = esc_md2(f"{float(listing.get('price_per_day', 0)):.2f}")
    cur_e   = esc_md2(cur)
    loc_raw = listing.get("location") or ""
    loc_e   = esc_md2(location_name_from_coords(loc_raw))
    avail_e = esc_md2(availability_str)

    msg = (
        f"🧰 *{cat_e}*\n"
        f"🏷️ *{title_e}*\n"
        + (f"🔧 {specs_e}\n" if specs_e else "")
        + f"📄 {desc_e}\n"
        + f"📦 {esc_md2(cond_lbl)}: {cond_e}\n"
        + f"💰 {price_e} {cur_e}/{esc_md2(per_day)}\n"
        + f"📍 {esc_md2('Location')}: {loc_e}\n"
        + f"📅 {esc_md2('Availability')}:\n{avail_e}"
    )

    # Buttons
    buttons = []
    if index > 0:
        buttons.append(InlineKeyboardButton(tr(me, "btn_prev"), callback_data="browse_prev"))
    buttons.append(InlineKeyboardButton(tr(me, "btn_rent"), callback_data=f"rent_year_{listing['id']}"))
    if index < len(listings) - 1:
        buttons.append(InlineKeyboardButton(tr(me, "btn_next"), callback_data="browse_next"))
    reply_markup = InlineKeyboardMarkup([buttons])

    # --- Callback case (works for Update.callback_query OR raw CallbackQuery) ---
    if q is not None:
        await q.answer()
        # delete old media group
        for msg_id in context.user_data.get("browse_media_ids", []):
            try:
                await context.bot.delete_message(chat_id=chat.id, message_id=msg_id)
            except Exception as e:
                print(f"[browse] failed to delete media {msg_id}: {e}")
        context.user_data["browse_media_ids"] = []

        # delete old text message
        try:
            await q.message.delete()
        except Exception as e:
            print(f"[browse] failed to delete text message: {e}")

        # send photos
        if photos:
            media_group = await chat.send_media_group([InputMediaPhoto(p) for p in photos[:3]])
            context.user_data["browse_media_ids"] = [m.message_id for m in media_group]
        else:
            context.user_data["browse_media_ids"] = []

        # send text card
        await chat.send_message(text=msg, parse_mode="MarkdownV2", reply_markup=reply_markup)

    # --- First render (message case): reply with media group + text ---
    else:
        context.user_data["browse_media_ids"] = []
        if photos:
            media_messages = await update_or_query.message.reply_media_group([InputMediaPhoto(p) for p in photos[:3]])
            context.user_data["browse_media_ids"] = [m.message_id for m in media_messages]
        await update_or_query.message.reply_text(msg, parse_mode="MarkdownV2", reply_markup=reply_markup)


# === View My Listings ===
async def send_single_listing(update_or_query, context):
    listings = context.user_data.get("my_listings", [])
    index = context.user_data.get("listing_index", 0)
    if not listings:
        return
    if index < 0 or index >= len(listings):
        index = index % len(listings)
        context.user_data["listing_index"] = index

    if not listings:
        return

    listing = listings[index]
    photos = coerce_list(listing.get("photos"))

    def format_availability(dates):
        if not dates:
            return "Not provided"

        dates = sorted(dates)
        result = []
        current_start = current_end = datetime.strptime(dates[0], "%Y-%m-%d")

        for d in dates[1:]:
            d_parsed = datetime.strptime(d, "%Y-%m-%d")
            if d_parsed == current_end + timedelta(days=1):
                current_end = d_parsed
            else:
                result.append(f"{current_start.strftime('%d/%m/%Y')} - {current_end.strftime('%d/%m/%Y')}")
                current_start = current_end = d_parsed

        result.append(f"{current_start.strftime('%d/%m/%Y')} - {current_end.strftime('%d/%m/%Y')}")
        return "\n".join(result)

    availability_str = format_availability(listing.get("availability"))
    title = listing.get("item") or "Item" or listing.get("title")
    specs = coerce_list(listing.get("specs"))
    cur = listing.get("currency", "PLN")
    cat_e   = esc_md2(listing.get('category', ''))
    title_e = esc_md2(title)
    specs_e = esc_md2(", ".join(specs[:4])) if specs else ""
    desc_e  = esc_md2(listing.get('description', ''))
    cond_e  = esc_md2(listing.get('condition', 'Not specified'))
    price_e = esc_md2(f"{float(listing.get('price_per_day', 0)):.2f}")
    cur_e   = esc_md2(cur)
    loc_raw = listing.get("location") or ""
    loc_e = esc_md2(location_name_from_coords(loc_raw))
    avail_e = esc_md2(availability_str)
    msg = (
        f"🧰 *{cat_e}*\n"
        f"🏷️ *{title_e}*\n"
        + (f"🔧 {specs_e}\n" if specs_e else "")
        + f"📄 {desc_e}\n"
        + f"📦 Condition: {cond_e}\n"
        + f"💰 {price_e} {cur_e}/day\n"
        + f"📍 Location: {loc_e}\n"
        + f"📅 Availability:\n{avail_e}"
    )

    buttons = []
    if index > 0:
        buttons.append(InlineKeyboardButton(tr(str(update_or_query.effective_user.id), "btn_prev_listing"), callback_data="prev_listing"))
    if index < len(listings) - 1:
        buttons.append(InlineKeyboardButton(tr(str(update_or_query.effective_user.id), "btn_next_listing"), callback_data="next_listing"))
    nav_row = buttons

    edit_row = [
        InlineKeyboardButton(tr(str(update_or_query.effective_user.id), "btn_edit"),   callback_data=f"edit_{listing['id']}"),
        InlineKeyboardButton(tr(str(update_or_query.effective_user.id), "btn_delete"), callback_data=f"delete_{listing['id']}")
    ]
    schedule_row = [InlineKeyboardButton(tr(str(update_or_query.effective_user.id), "btn_lending_schedule"), callback_data=f"schedule_{listing['id']}")]


    reply_markup = InlineKeyboardMarkup([nav_row, edit_row, schedule_row])

    # Handle CallbackQuery
    if update_or_query.callback_query:
        query = update_or_query.callback_query
        await query.answer()

        chat = query.message.chat

        # 🧹 Delete old media messages (photos)
        for msg_id in context.user_data.get("last_media_messages", []):
            try:
                await context.bot.delete_message(chat_id=chat.id, message_id=msg_id)
            except Exception as e:
                print(f"Failed to delete media message {msg_id}: {e}")
        context.user_data["last_media_messages"] = []

        # 🧹 Delete old text message (if exists)
        try:
            await query.message.delete()
        except Exception as e:
            print(f"Failed to delete main message: {e}")

        # 📸 Send new photos
        if photos:
            media_group = await chat.send_media_group([InputMediaPhoto(p) for p in photos[:3]])
            context.user_data["last_media_messages"] = [m.message_id for m in media_group]

        # 📝 Send new text block
        await chat.send_message(text=msg, parse_mode="MarkdownV2", reply_markup=reply_markup)
        
    # Handle Message (initial case)
    elif update_or_query.message:
        if photos:
            media_messages = await update_or_query.message.reply_media_group([InputMediaPhoto(p) for p in photos[:3]])
            context.user_data["last_media_messages"] = [m.message_id for m in media_messages]
        else:
            context.user_data["last_media_messages"] = []

        await update_or_query.message.reply_text(msg, parse_mode="MarkdownV2", reply_markup=reply_markup)

async def show_lending_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    me = str(update.effective_user.id)
    listing_id = q.data.split("_", 1)[1]

    listing = supabase.table("listings").select("item,description").eq("id", listing_id).execute().data
    if not listing:
        context.application.create_task(q.message.edit_text(tr(me, "something_wrong")), update=update)
        return
    listing = listing[0]

    rrs = supabase.table("rental_requests")\
        .select("borrower_id, borrower_username, dates, status")\
        .eq("listing_id", listing_id).eq("status", "accepted")\
        .execute().data or []

    today = datetime.utcnow().date()
    by_borrower: dict[str, dict] = {}
    for rr in rrs:
        upcoming = []
        for t in rr.get("dates") or []:
            d = _parse_date_any(t)
            if d and d >= today:
                upcoming.append(d.strftime("%Y-%m-%d"))
        if upcoming:
            k = rr["borrower_id"]
            g = by_borrower.setdefault(k, {"username": rr.get("borrower_username") or "", "dates": set()})
            g["dates"].update(upcoming)

    if not by_borrower:
        kb = InlineKeyboardMarkup([[InlineKeyboardButton(tr(me, "back"), callback_data=f"schedule_back_{listing_id}")]])
        context.application.create_task(q.message.edit_text(tr(me, "no_upcoming_for_listing"), reply_markup=kb), update=update)
        return

    item_e = esc_md2(listing.get("item") or "Item")
    desc_e = esc_md2(listing.get("description") or "")

    blocks = []
    for _, info in by_borrower.items():
        uname = (info.get("username") or "").strip()
        tag = esc_md2(f"@{uname}") if uname else esc_md2("—")
        ranges_str = format_date_ranges_from_tokens(sorted(info["dates"]))
        ranges_e = esc_md2(ranges_str)
        blocks.append(f"🏷️ *{item_e}*\n📄 {desc_e}\n👤 By: {tag}\n🗓 Rented out:\n{ranges_e}")

    text = f"{esc_md2(tr(me, 'lending_schedule_title'))}\n\n" + "\n\n".join(blocks)
    kb = InlineKeyboardMarkup([[InlineKeyboardButton(tr(me, "back"), callback_data=f"schedule_back_{listing_id}")]])
    context.application.create_task(q.message.edit_text(text, parse_mode="MarkdownV2", reply_markup=kb), update=update)

async def schedule_back_to_listing(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    # schedule_back_<listing_id>
    listing_id = q.data.split("_", 2)[2]

    # Move index to this listing if it's present in memory
    listings = context.user_data.get("my_listings", [])
    for i, l in enumerate(listings):
        if l.get("id") == listing_id:
            context.user_data["listing_index"] = i
            break

    await send_single_listing(update, context)

async def view_my_listings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    result = supabase.table("listings").select("*").eq("owner_id", user_id).execute()
    listings = result.data

    if not listings:
        context.application.create_task(update.message.reply_text("You don’t have any listings yet."), update=update)
        return

    context.user_data["my_listings"] = listings
    context.user_data["listing_index"] = 0

    await send_single_listing(update, context)

async def browse_next_listing(update: Update, context: ContextTypes.DEFAULT_TYPE):
    listings = context.user_data.get("my_listings", [])
    if not listings:
        return
    idx = (context.user_data.get("listing_index", 0) + 1) % len(listings)
    context.user_data["listing_index"] = idx
    await send_single_listing(update, context)

async def browse_prev_listing(update: Update, context: ContextTypes.DEFAULT_TYPE):
    listings = context.user_data.get("my_listings", [])
    if not listings:
        return
    idx = (context.user_data.get("listing_index", 0) - 1) % len(listings)
    context.user_data["listing_index"] = idx
    await send_single_listing(update, context)

# === Edit or Delete Listing Choice ===
_I18N_EDIT_PATCH = {
    "en": {
        # generic
        "something_wrong": "Something went wrong.",
        "cancelled": "Cancelled.",
        "unknown_option": "Unknown option.",
        "back": "Back",
        "btn_cancel": "❌ Cancel",
        # edit menu
        "edit_menu_title": "What would you like to edit?",
        # photos
        "photos_help": "Send up to 3 photos. You can also type:\n• Delete 1 / Delete 2 / Delete 3\n• Clear\n• Done\n• Cancel",
        "photo_deleted": "✅ Deleted.",
        "photo_invalid_number": "Invalid number.",
        "photo_type_delete": "Type e.g. 'Delete 1'.",
        "photos_cleared": "Cleared all staged photos.",
        "photos_already_three": "You already have 3 photos staged. Delete one or type 'Done'.",
        "photo_rejected": "🚫 Photo rejected: {why}. Please send a different one.",
        "photo_added": "Added. Currently staged: {n}. You can add up to {remaining} more, or type 'Done'.",
        "photos_send_prompt": "Send a photo, or type Delete X / Clear / Done / Cancel.",
        "photos_updated": "✅ Photos updated.",
        # edit field prompts
        "prompt_edit_desc": "Please type in chat to what we will change the *description*:",
        "prompt_edit_price": "Please type the new *price per day* with currency (e.g., `100 PLN`):",
        "prompt_edit_location": "Please type in chat to what we will change the *location*:",
        "prompt_select_category": "Please select your category or type it in chat:",
        "prompt_edit_condition": "Please describe the item's current condition or any damages:",
        "prompt_edit_item": "Please send the new *item title* (e.g., Electric guitar):",
        "prompt_edit_brand_model": "Please send the new *brand/model* (e.g., Ibanez RG370), or type 'Skip' to clear:",
        "prompt_edit_specs": "Send 1–6 *specs* comma-separated (e.g., 24 frets, tremolo, HSH):",
        "cancel": "Cancel",
        # moderation rejections
        "reject_item": "🚫 Item title rejected: {why}. Please rephrase.",
        "reject_brand_model": "🚫 Brand/Model rejected: {why}. Please rephrase or type 'Skip'.",
        "reject_specs": "🚫 Specs rejected: {why}. Please rephrase.",
        "reject_desc": "🚫 Description rejected: {why}. Please rephrase.",
        "reject_condition": "🚫 Condition text rejected: {why}. Please rephrase.",
        # success updates
        "ok_item_updated": "✅ Item title updated and embedding refreshed.",
        "ok_brand_updated": "✅ Brand/Model updated and embedding refreshed.",
        "ok_specs_updated": "✅ Specs updated and embedding refreshed.",
        "ok_category_updated": "✅ Category updated to '{cat}' and embedding refreshed.",
        "ok_desc_updated": "✅ Description updated and embedding refreshed.",
        "ok_condition_updated": "✅ Condition updated.",
        "ok_price_updated": "✅ Price updated.",
        "ok_location_updated": "✅ Location updated and embedding refreshed.",
        "ok_desc_updated_simple": "✅ Description updated.",
        # price format
        "price_format_hint": "Please enter price per day with currency, e.g. `100 PLN` or `99.90 EUR`",
        # delete listing
        "delete_confirm": "Are you sure you want to delete this listing?",
        "delete_yes": "✅ Yes, delete",
        "delete_no": "❌ No, cancel",
        "delete_done_none_left": "✅ Listing deleted. You don’t have any other listings.",
        "delete_done": "✅ Listing deleted.",
        "delete_cancelled": "Deletion cancelled.",
    },
    "pl": {
        # generic
        "something_wrong": "Coś poszło nie tak.",
        "cancelled": "Anulowano.",
        "unknown_option": "Nieznana opcja.",
        "back": "Wstecz",
        "btn_cancel": "❌ Anuluj",
        # edit menu
        "edit_menu_title": "Co chcesz edytować?",
        # photos
        "photos_help": "Wyślij do 3 zdjęć. Możesz też napisać:\n• Delete 1 / Delete 2 / Delete 3\n• Clear\n• Done\n• Cancel",
        "photo_deleted": "✅ Usunięto.",
        "photo_invalid_number": "Nieprawidłowy numer.",
        "photo_type_delete": "Napisz np. 'Delete 1'.",
        "photos_cleared": "Wyczyszczono wszystkie zdjęcia.",
        "photos_already_three": "Masz już 3 zdjęcia. Usuń jedno lub wpisz 'Done'.",
        "photo_rejected": "🚫 Zdjęcie odrzucone: {why}. Wyślij inne.",
        "photo_added": "Dodano. Obecnie: {n}. Możesz dodać jeszcze {remaining} lub wpisz 'Done'.",
        "photos_send_prompt": "Wyślij zdjęcie lub wpisz Delete X / Clear / Done / Cancel.",
        "photos_updated": "✅ Zaktualizowano zdjęcia.",
        # edit field prompts
        "prompt_edit_desc": "Napisz na co zmieniamy *opis*:",
        "prompt_edit_price": "Podaj nową *cenę za dzień* z walutą (np. `100 PLN`):",
        "prompt_edit_location": "Napisz na co zmieniamy *lokalizację*:",
        "prompt_select_category": "Wybierz kategorię lub wpisz ją ręcznie:",
        "prompt_edit_condition": "Opisz aktualny stan lub uszkodzenia przedmiotu:",
        "prompt_edit_item": "Wyślij nowy *tytuł przedmiotu* (np. Gitara elektryczna):",
        "prompt_edit_brand_model": "Wyślij nowe *marka/model* (np. Ibanez RG370) lub wpisz 'Skip', aby wyczyścić:",
        "prompt_edit_specs": "Wyślij 1–6 *specyfikacji* po przecinku (np. 24 progi, tremolo, HSH):",
        "cancel": "Anuluj",
        # moderation rejections
        "reject_item": "🚫 Tytuł odrzucony: {why}. Zredaguj proszę.",
        "reject_brand_model": "🚫 Marka/Model odrzucone: {why}. Zredaguj lub wpisz 'Skip'.",
        "reject_specs": "🚫 Specyfikacje odrzucone: {why}. Zredaguj proszę.",
        "reject_desc": "🚫 Opis odrzucony: {why}. Zredaguj proszę.",
        "reject_condition": "🚫 Opis stanu odrzucony: {why}. Zredaguj proszę.",
        # success updates
        "ok_item_updated": "✅ Zaktualizowano tytuł i embedding.",
        "ok_brand_updated": "✅ Zaktualizowano markę/model i embedding.",
        "ok_specs_updated": "✅ Zaktualizowano specyfikacje i embedding.",
        "ok_category_updated": "✅ Zmieniono kategorię na '{cat}' i odświeżono embedding.",
        "ok_desc_updated": "✅ Zaktualizowano opis i embedding.",
        "ok_condition_updated": "✅ Zaktualizowano stan.",
        "ok_price_updated": "✅ Zaktualizowano cenę.",
        "ok_location_updated": "✅ Zaktualizowano lokalizację i embedding.",
        "ok_desc_updated_simple": "✅ Opis zaktualizowany.",
        # price format
        "price_format_hint": "Podaj cenę za dzień z walutą, np. `100 PLN` lub `99.90 EUR`",
        # delete listing
        "delete_confirm": "Na pewno usunąć to ogłoszenie?",
        "delete_yes": "✅ Tak, usuń",
        "delete_no": "❌ Nie, anuluj",
        "delete_done_none_left": "✅ Usunięto. Nie masz więcej ogłoszeń.",
        "delete_done": "✅ Ogłoszenie usunięte.",
        "delete_cancelled": "Usunięcie anulowane.",
    },
    "uk": {
        # generic
        "something_wrong": "Щось пішло не так.",
        "cancelled": "Скасовано.",
        "unknown_option": "Невідома опція.",
        "back": "Назад",
        "btn_cancel": "❌ Скасувати",
        # edit menu
        "edit_menu_title": "Що ви хочете змінити?",
        # photos
        "photos_help": "Надішліть до 3 фото. Також можна написати:\n• Delete 1 / Delete 2 / Delete 3\n• Clear\n• Done\n• Cancel",
        "photo_deleted": "✅ Видалено.",
        "photo_invalid_number": "Неправильний номер.",
        "photo_type_delete": "Напишіть напр. 'Delete 1'.",
        "photos_cleared": "Усі фото очищено.",
        "photos_already_three": "У вас вже 3 фото. Видаліть одне або натисніть 'Продовжити'.",
        "photo_rejected": "🚫 Фото відхилено: {why}. Надішліть інше.",
        "photo_added": "Додано. Зараз: {n}. Можна додати ще {remaining}, або натисніть 'Продовжити'.",
        "photos_send_prompt": "Надішліть фото або введіть Delete X / Clear / Done / Cancel.",
        "photos_updated": "✅ Фото оновлено.",
        # edit field prompts
        "prompt_edit_desc": "Напишіть, на що змінюємо *опис*:",
        "prompt_edit_price": "Укажіть нову *ціну за добу* з валютою (наприклад, `100 PLN`):",
        "prompt_edit_location": "Напишіть, на що змінюємо *локацію*:",
        "prompt_select_category": "Оберіть категорію або введіть вручну:",
        "prompt_edit_condition": "Опишіть поточний стан речі або пошкодження:",
        "prompt_edit_item": "Надішліть нову *назву предмета* (наприклад, Електрогітара):",
        "prompt_edit_brand_model": "Надішліть нові *бренд/модель* (наприклад, Ibanez RG370) або введіть 'Skip', щоб очистити:",
        "prompt_edit_specs": "Надішліть 1–6 *характеристик* через кому (наприклад, 24 лади, тремоло, HSH):",
        "cancel": "Скасувати",
        # moderation rejections
        "reject_item": "🚫 Назву відхилено: {why}. Перефразуйте.",
        "reject_brand_model": "🚫 Бренд/Модель відхилено: {why}. Перефразуйте або введіть 'Skip'.",
        "reject_specs": "🚫 Характеристики відхилено: {why}. Перефразуйте.",
        "reject_desc": "🚫 Опис відхилено: {why}. Перефразуйте.",
        "reject_condition": "🚫 Опис стану відхилено: {why}. Перефразуйте.",
        # success updates
        "ok_item_updated": "✅ Оновлено назву та embedding.",
        "ok_brand_updated": "✅ Оновлено бренд/модель та embedding.",
        "ok_specs_updated": "✅ Оновлено характеристики та embedding.",
        "ok_category_updated": "✅ Змінено категорію на '{cat}' та оновлено embedding.",
        "ok_desc_updated": "✅ Оновлено опис та embedding.",
        "ok_condition_updated": "✅ Оновлено стан.",
        "ok_price_updated": "✅ Оновлено ціну.",
        "ok_location_updated": "✅ Оновлено локацію та embedding.",
        "ok_desc_updated_simple": "✅ Опис оновлено.",
        # price format
        "price_format_hint": "Вкажіть ціну за добу з валютою, напр. `100 PLN` або `99.90 EUR`",
        # delete listing
        "delete_confirm": "Ви впевнені, що хочете видалити це оголошення?",
        "delete_yes": "✅ Так, видалити",
        "delete_no": "❌ Ні, скасувати",
        "delete_done_none_left": "✅ Видалено. В інших оголошень немає.",
        "delete_done": "✅ Оголошення видалено.",
        "delete_cancelled": "Видалення скасовано.",
    },
}
try:
    for _lng, _patch in _I18N_EDIT_PATCH.items():
        if _lng in LOCALES:
            LOCALES[_lng].update(_patch)
except Exception as _e:
    logging.warning(f"[i18n] LOCALES edit patch failed: {_e}")

I18N_MORE = {
  "en": {
    "req_nav_prev": "⬅️ Previous",
    "req_nav_next": "➡️ Next",
    "lending_schedule_title": "📆 Lending schedule",
    "no_upcoming_for_listing": "📆 No upcoming rentals for this listing",
    "back_to_years": "⬅️ Back to years",
    "are_you_sure": "Are you sure?",
  },
  "pl": {
    "req_nav_prev": "⬅️ Poprzednia",
    "req_nav_next": "➡️ Następna",
    "lending_schedule_title": "📆 Harmonogram wypożyczeń",
    "no_upcoming_for_listing": "📆 Brak nadchodzących wypożyczeń dla tego ogłoszenia",
    "back_to_years": "⬅️ Wróć do lat",
    "are_you_sure": "Na pewno?",
  },
  "uk": {
    "req_nav_prev": "⬅️ Попередня",
    "req_nav_next": "➡️ Наступна",
    "lending_schedule_title": "📆 Графік видачі",
    "no_upcoming_for_listing": "📆 Немає майбутніх оренд для цього оголошення",
    "back_to_years": "⬅️ Назад до років",
    "are_you_sure": "Ви впевнені?",
  }
}
for lng, patch in I18N_MORE.items():
    LOCALES[lng].update(patch)

# === Edit or Delete Listing Choice ===
async def receive_new_photos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    me = str(update.effective_user.id)
    listing_id = context.user_data.get("edit_listing_id")
    buf = context.user_data.get("edit_photos") or []
    in_edit_mode = context.user_data.get("photo_edit_mode", False)

    # --- TEXT / BUTTONS ---
    if update.message and update.message.text:
        raw = update.message.text.strip()
        low = raw.lower()

        # DELETE 1/2/3 (localized)
        idx = parse_delete_idx(raw, me)
        if idx is not None:
            if 0 <= idx < len(buf):
                buf.pop(idx)
                context.user_data["edit_photos"] = buf
                context.application.create_task(update.message.reply_text(tr(me, "photo_deleted"), reply_markup=photo_edit_keyboard(me) if in_edit_mode else photo_stage_keyboard(me)), update=update)
            else:
                context.application.create_task(update.message.reply_text(tr(me, "photo_invalid_number"), reply_markup=photo_edit_keyboard(me) if in_edit_mode else photo_stage_keyboard(me)), update=update)
            return AWAIT_NEW_PHOTOS

        # CLEAR
        if low == tr(me, "photos_edit_clear").lower() or low == "clear":
            buf = []
            context.user_data["edit_photos"] = buf
            context.application.create_task(update.message.reply_text(tr(me, "photos_cleared"), reply_markup=photo_edit_keyboard(me) if in_edit_mode else photo_stage_keyboard(me)), update=update)
            return AWAIT_NEW_PHOTOS

        # DONE (commit + back to EDIT MENU)
        if low == tr(me, "photos_edit_done").lower() or low == "done":
            if in_edit_mode:
                supabase.table("listings").update({"photos": buf[:3]}).eq("id", listing_id).execute()
                context.user_data.pop("photo_edit_mode", None)
                # remove the reply keyboard, then reopen the edit menu
                context.application.create_task(update.message.reply_text(tr(me, "photos_updated"), reply_markup=ReplyKeyboardRemove()), update=update)
                return await edit_menu_back(update, context)
            else:
                # create flow doesn't use "Done" here; ignore
                context.application.create_task(update.message.reply_text(tr(me, "send_photo_or_cmd"), reply_markup=photo_stage_keyboard(me)), update=update)
                return AWAIT_NEW_PHOTOS

        # CANCEL (discard staged & back to LISTING)
        if low == tr(me, "photos_edit_cancel").lower() or low == "cancel":
            if in_edit_mode:
                context.user_data.pop("photo_edit_mode", None)
                # discard staged by reloading from DB (safety)
                try:
                    row = supabase.table("listings").select("photos").eq("id", listing_id).execute().data[0]
                    context.user_data["edit_photos"] = coerce_list((row or {}).get("photos"))
                except Exception:
                    pass
                context.application.create_task(update.message.reply_text(tr(me, "cancelled"), reply_markup=ReplyKeyboardRemove()), update=update)
                return await _back_to_listing_from_edit(update, context)
            else:
                # create flow cancel listing
                context.user_data.clear()
                context.application.create_task(update.message.reply_text(tr(me, "cancelled"), reply_markup=ReplyKeyboardRemove()), update=update)
                return await go_back(update, context)

        # CREATE-FLOW commands preserved (not used in edit mode)
        if not in_edit_mode:
            if low == tr(me, "photos_add_more").lower():
                context.application.create_task(update.message.reply_text(tr(me, "photos_send_prompt"), reply_markup=photo_stage_keyboard(me)), update=update)
                return GET_PHOTOS
            if low == tr(me, "photos_continue").lower():
                context.application.create_task(
                    update.message.reply_text(
                        tr(me, "share_location_or_type"),
                        reply_markup=ReplyKeyboardMarkup(
                            [[KeyboardButton(tr(me, "send_location_btn"), request_location=True),
                              tr(me, "cancel_listing_btn")]],
                            resize_keyboard=True
                        )
                    ),
                    update=update
                )
                return GET_LOCATION
            if low == tr(me, "photos_cancel_listing").lower():
                context.user_data.clear()
                context.application.create_task(update.message.reply_text(tr(me, "cancelled"), reply_markup=ReplyKeyboardRemove()), update=update)
                return await go_back(update, context)

        # Unknown text → repeat prompt with correct keyboard
        context.application.create_task(
            update.message.reply_text(
                tr(me, "photos_edit_help") if in_edit_mode else tr(me, "send_photo_or_cmd"),
                reply_markup=photo_edit_keyboard(me) if in_edit_mode else photo_stage_keyboard(me),
                parse_mode="Markdown"
            ),
            update=update
        )
        return AWAIT_NEW_PHOTOS

    # --- PHOTO MEDIA ---
    if update.message and update.message.photo:
        if len(buf) >= 3:
            context.application.create_task(
                update.message.reply_text(
                    tr(me, "photos_already_three"),
                    reply_markup=photo_edit_keyboard(me) if in_edit_mode else photo_stage_keyboard(me)
                ),
                update=update
            )
            return AWAIT_NEW_PHOTOS

        file_id = update.message.photo[-1].file_id
        ok, why = await moderate_telegram_photo(file_id, context.bot)
        if not ok:
            context.application.create_task(
                update.message.reply_text(tr(me, "photo_rejected", why=why),
                                                reply_markup=photo_edit_keyboard(me) if in_edit_mode else photo_stage_keyboard(me)),
                update=update
            )
            return AWAIT_NEW_PHOTOS

        buf.append(file_id)
        context.user_data["edit_photos"] = buf
        context.application.create_task(
            update.message.reply_text(
                tr(me, "photo_added", n=len(buf), remaining=max(0, 3-len(buf))),
                reply_markup=photo_edit_keyboard(me) if in_edit_mode else photo_stage_keyboard(me)
            ),
            update=update
        )
        return AWAIT_NEW_PHOTOS

    # Fallback
    context.application.create_task(
        update.message.reply_text(
            tr(me, "photos_edit_help") if in_edit_mode else tr(me, "send_photo_or_cmd"),
            reply_markup=photo_edit_keyboard(me) if in_edit_mode else photo_stage_keyboard(me),
            parse_mode="Markdown"
        ),
        update=update
    )
    return AWAIT_NEW_PHOTOS

def build_embedding_for_listing(
    item_title: str,
    brand: str,
    model: str,
    specs: list[str],
    tags: list[str],
    location: str,
    description: str,
    category: str
    ):
    parts = []

    if item_title:
        parts.append(f"Item: {item_title}")
    if brand or model:
        parts.append(f"Brand/Model: {brand} {model}".strip())
    if specs:
        parts.append("Specs: " + ", ".join(specs))
    if tags:
        parts.append("Tags: " + ", ".join(tags))
    if location:
        parts.append(f"Location: {location}")
    if category:
        parts.append(f"Category: {category}")
    if description:
        parts.append(f"Description: {description}")

    return ". ".join(parts)

def build_embedding_input_from_row(
    row: dict,
    overrides: dict = None
) -> str:
    """
    Build a rich text string for embeddings from a listings table row.
    `overrides` lets you replace fields without mutating the original.
    """
    data = row.copy()
    overrides = overrides or {}
    data.update({k: v for k, v in overrides.items() if v is not None})

    item_title = data.get("item", "") or ""
    brand_model = data.get("brand_model", "") or ""
    brand = brand_model.split()[0] if brand_model else ""
    model = " ".join(brand_model.split()[1:]) if brand_model else ""
    specs = data.get("specs", []) or []
    tags = data.get("tags", []) or []
    location = data.get("location", "") or ""
    description = data.get("description", "") or ""
    category = data.get("category", "") or ""

    parts = []
    if item_title:
        parts.append(f"Item: {item_title}")
    if brand or model:
        parts.append(f"Brand/Model: {brand} {model}".strip())
    if specs:
        parts.append("Specs: " + ", ".join(specs))
    if tags:
        parts.append("Tags: " + ", ".join(tags))
    if location:
        parts.append(f"Location: {location}")
    if category:
        parts.append(f"Category: {category}")
    if description:
        parts.append(f"Description: {description}")

    return ". ".join(parts)

async def start_editing_listing(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    me = str(update.effective_user.id)

    parts = query.data.split("_")
    if len(parts) < 2:
        await query.edit_message_text("❗ Error: Invalid listing ID.")
        return ConversationHandler.END

    listing_id = parts[1]
    context.user_data["edit_listing_id"] = listing_id

    kb = build_edit_menu_keyboard(me)  # <-- pass user id
    context.application.create_task(
        query.message.reply_text(
            text=tr(me, "edit_menu_title"),
            reply_markup=kb
        ),
        update=update
    )
    return EDIT_CHOICE

async def handle_edit_description_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    me = str(update.effective_user.id)

    listing_id = context.user_data.get("edit_listing_id")
    if not listing_id:
        await query.edit_message_text("❗ Error: Listing not found.")
        return ConversationHandler.END

    await query.edit_message_text(tr(me, "prompt_edit_desc"))
    return AWAIT_NEW_DESCRIPTION

async def handle_edit_field_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    me = str(update.effective_user.id)

    if query.data == "cancel_edit":
        return await cancel_editing(update, context)

    listing_id = context.user_data.get("edit_listing_id")
    if not listing_id:
        await query.edit_message_text(tr(me, "something_wrong"))
        return ConversationHandler.END

    if query.data == "edit_field_description":
        context.user_data["edit_field"] = "description"
        prompt = tr(me, "prompt_edit_desc")
        next_state = AWAIT_NEW_DESCRIPTION
    elif query.data == "edit_field_price":
        context.user_data["edit_field"] = "price"
        prompt = tr(me, "prompt_edit_price")
        next_state = AWAIT_NEW_PRICE
    elif query.data == "edit_field_location":
        context.user_data["edit_field"] = "location"
        prompt = tr(me, "prompt_edit_location")
        next_state = AWAIT_NEW_LOCATION
    elif query.data == "edit_field_category":
        context.user_data["edit_field"] = "category"
        context.application.create_task(
            query.message.reply_text(
                tr(me, "prompt_select_category"),
                reply_markup=category_keyboard(me)
            ),
            update=update
        )
        return AWAIT_NEW_CATEGORY

    elif query.data == "edit_field_condition":
        context.user_data["edit_field"] = "condition"
        prompt = tr(me, "prompt_edit_condition")
        next_state = AWAIT_NEW_CONDITION
    elif query.data == "edit_field_item":
        context.user_data["edit_field"] = "item"
        prompt = tr(me, "prompt_edit_item")
        next_state = AWAIT_NEW_ITEM_TITLE
    elif query.data == "edit_field_brand_model":
        context.user_data["edit_field"] = "brand_model"
        prompt = tr(me, "prompt_edit_brand_model")
        next_state = AWAIT_NEW_BRAND_MODEL
    elif query.data == "edit_field_specs":
        context.user_data["edit_field"] = "specs"
        prompt = tr(me, "prompt_edit_specs")
        next_state = AWAIT_NEW_SPECS
    elif query.data == "edit_field_photos":
        context.user_data["edit_field"] = "photos"
        # preload current photos
        row = supabase.table("listings").select("photos").eq("id", listing_id).execute().data[0]
        context.user_data["edit_photos"] = coerce_list((row or {}).get("photos"))
        context.user_data["photo_edit_mode"] = True  # mark edit mode

        context.application.create_task(
            query.message.reply_text(
                tr(me, "photos_edit_help"),
                reply_markup=photo_edit_keyboard(me),
                parse_mode="Markdown"
            ),
            update=update
        )
        return AWAIT_NEW_PHOTOS

    else:
        await query.edit_message_text(
            tr(me, "unknown_option"),
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(tr(me, "back"), callback_data="edit_menu_back")]])
        )
        return EDIT_CHOICE

    cancel_button = InlineKeyboardMarkup([[InlineKeyboardButton(tr(me, "btn_cancel"), callback_data="cancel_edit")]])
    context.application.create_task(query.message.reply_text(prompt, reply_markup=cancel_button, parse_mode="Markdown"), update=update)
    return next_state

async def receive_new_item_title(update: Update, context: ContextTypes.DEFAULT_TYPE):
    me = str(update.effective_user.id)
    new_item = update.message.text.strip()
    ok, why = await moderate_text(new_item)
    if not ok:
        context.application.create_task(update.message.reply_text(tr(me, "reject_item", why=why)), update=update)
        return AWAIT_NEW_ITEM_TITLE
    listing_id = context.user_data.get("edit_listing_id")
    if not listing_id:
        context.application.create_task(update.message.reply_text(tr(me, "something_wrong")), update=update)
        return ConversationHandler.END

    listing = supabase.table("listings").select(
        "item,brand_model,specs,tags,location,description,category"
    ).eq("id", listing_id).execute().data[0]

    embedding_input = build_embedding_input_from_row(listing, overrides={"item": new_item})
    embedding = await generate_embedding(embedding_input)
    logging.info(f"[Embeddings] Item edit → input='{embedding_input[:120]}...'")

    supabase.table("listings").update({
        "item": new_item,
        "embedding": embedding
    }).eq("id", listing_id).execute()

    context.application.create_task(update.message.reply_text(tr(me, "ok_item_updated")), update=update)
    return await edit_menu_back(update, context)

async def receive_new_brand_model(update: Update, context: ContextTypes.DEFAULT_TYPE):
    me = str(update.effective_user.id)
    text = update.message.text.strip()
    new_brand_model = "" if text.lower() == "skip" else text
    if new_brand_model:
        ok, why = await moderate_text(new_brand_model)
        if not ok:
            context.application.create_task(update.message.reply_text(tr(me, "reject_brand_model", why=why)), update=update)
            return AWAIT_NEW_BRAND_MODEL
    listing_id = context.user_data.get("edit_listing_id")
    if not listing_id:
        context.application.create_task(update.message.reply_text(tr(me, "something_wrong")), update=update)
        return ConversationHandler.END

    listing = supabase.table("listings").select(
        "item,brand_model,specs,tags,location,description,category"
    ).eq("id", listing_id).execute().data[0]

    embedding_input = build_embedding_input_from_row(listing, overrides={"brand_model": new_brand_model})
    embedding = await generate_embedding(embedding_input)
    logging.info(f"[Embeddings] Brand/Model edit → input='{embedding_input[:120]}...'")

    supabase.table("listings").update({
        "brand_model": new_brand_model if new_brand_model else None,
        "embedding": embedding
    }).eq("id", listing_id).execute()

    context.application.create_task(update.message.reply_text(tr(me, "ok_brand_updated")), update=update)
    return await edit_menu_back(update, context)

async def receive_new_specs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    me = str(update.effective_user.id)
    raw = update.message.text.strip()
    ok, why = await moderate_text(raw)
    if not ok:
        context.application.create_task(update.message.reply_text(tr(me, "reject_specs", why=why)), update=update)
        return AWAIT_NEW_SPECS
    new_specs = [s.strip() for s in raw.split(",") if s.strip()]
    listing_id = context.user_data.get("edit_listing_id")
    if not listing_id:
        context.application.create_task(update.message.reply_text(tr(me, "something_wrong")), update=update)
        return ConversationHandler.END

    listing = supabase.table("listings").select(
        "item,brand_model,specs,tags,location,description,category"
    ).eq("id", listing_id).execute().data[0]

    embedding_input = build_embedding_input_from_row(listing, overrides={"specs": new_specs})
    embedding = await generate_embedding(embedding_input)
    logging.info(f"[Embeddings] Specs edit → input='{embedding_input[:120]}...'")

    supabase.table("listings").update({
        "specs": new_specs,
        "embedding": embedding
    }).eq("id", listing_id).execute()

    context.application.create_task(update.message.reply_text(tr(me, "ok_specs_updated")), update=update)
    return await edit_menu_back(update, context)

async def receive_new_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    me = str(update.effective_user.id)
    new_category_raw = update.message.text
    new_category = to_canonical_category(new_category_raw, me) or new_category_raw
    if new_category == tr(me, "cancel"):
        return await go_back(update, context)

    listing_id = context.user_data.get("edit_listing_id")
    if not listing_id:
        context.application.create_task(update.message.reply_text(tr(me, "something_wrong")), update=update)
        return ConversationHandler.END

    # Get current row (only the columns used by the embedding builder)
    listing = supabase.table("listings").select(
        "item,brand_model,specs,tags,location,description,category"
    ).eq("id", listing_id).execute().data[0]

    # Build embedding input with the new category
    embedding_input = build_embedding_input_from_row(listing, overrides={"category": new_category})
    embedding = await generate_embedding(embedding_input)
    logging.info(f"[Embeddings] Category edit → input='{embedding_input[:120]}...'")

    supabase.table("listings").update({
        "category": new_category,
        "embedding": embedding
    }).eq("id", listing_id).execute()

    context.application.create_task(
        update.message.reply_text(
            tr(me, "ok_category_updated", cat=new_category),
            reply_markup=main_menu_keyboard(str(update.effective_user.id))
        ),
        update=update
    )
    return await edit_menu_back(update, context)

async def receive_new_description(update: Update, context: ContextTypes.DEFAULT_TYPE):
    me = str(update.effective_user.id)
    new_desc = update.message.text
    ok, why = await moderate_text(new_desc)
    if not ok:
        context.application.create_task(update.message.reply_text(tr(me, "reject_desc", why=why)), update=update)
        return AWAIT_NEW_DESCRIPTION
    listing_id = context.user_data.get("edit_listing_id")
    if not listing_id:
        context.application.create_task(update.message.reply_text(tr(me, "something_wrong")), update=update)
        return ConversationHandler.END

    listing = supabase.table("listings").select(
        "item,brand_model,specs,tags,location,description,category"
    ).eq("id", listing_id).execute().data[0]

    embedding_input = build_embedding_input_from_row(listing, overrides={"description": new_desc})
    embedding = await generate_embedding(embedding_input)
    logging.info(f"[Embeddings] Description edit → input='{embedding_input[:120]}...'")

    supabase.table("listings").update({
        "description": new_desc,
        "embedding": embedding
    }).eq("id", listing_id).execute()

    context.application.create_task(
        update.message.reply_text(
            tr(me, "ok_desc_updated"),
            reply_markup=main_menu_keyboard(str(update.effective_user.id))
        ),
        update=update
    )
    # 👇 show the edit menu again
    return await edit_menu_back(update, context)

async def receive_new_condition(update: Update, context: ContextTypes.DEFAULT_TYPE):
    me = str(update.effective_user.id)
    new_condition = update.message.text
    ok, why = await moderate_text(new_condition)
    if not ok:
        context.application.create_task(update.message.reply_text(tr(me, "reject_condition", why=why)), update=update)
        return AWAIT_NEW_CONDITION
    listing_id = context.user_data.get("edit_listing_id")

    if not listing_id:
        context.application.create_task(update.message.reply_text(tr(me, "something_wrong")), update=update)
        return ConversationHandler.END

    supabase.table("listings").update({"condition": new_condition}).eq("id", listing_id).execute()
    context.application.create_task(update.message.reply_text(tr(me, "ok_condition_updated"), reply_markup=main_menu_keyboard(str(update.effective_user.id))), update=update)
    return await edit_menu_back(update, context)

async def receive_new_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    me = str(update.effective_user.id)
    listing_id = context.user_data.get("edit_listing_id")
    raw = (update.message.text or "").strip()
    m = re.match(r'^\s*([0-9]+(?:[.,][0-9]+)?)\s*([A-Za-z]{3})?\s*$', raw)
    if not m:
        context.application.create_task(update.message.reply_text(tr(me, "price_format_hint"), parse_mode="Markdown"), update=update)
        return AWAIT_NEW_PRICE

    amount = float(m.group(1).replace(",", "."))
    currency = (m.group(2) or "PLN").upper()

    supabase.table("listings").update({
        "price_per_day": amount,
        "currency": currency
    }).eq("id", listing_id).execute()

    context.application.create_task(update.message.reply_text(tr(me, "ok_price_updated"), reply_markup=main_menu_keyboard(str(update.effective_user.id))), update=update)
    return await edit_menu_back(update, context)

async def receive_new_location(update: Update, context: ContextTypes.DEFAULT_TYPE):
    me = str(update.effective_user.id)
    new_location = update.message.text
    listing_id = context.user_data.get("edit_listing_id")
    if not listing_id:
        context.application.create_task(update.message.reply_text(tr(me, "something_wrong")), update=update)
        return ConversationHandler.END

    listing = supabase.table("listings").select(
        "item,brand_model,specs,tags,location,description,category"
    ).eq("id", listing_id).execute().data[0]

    embedding_input = build_embedding_input_from_row(listing, overrides={"location": new_location})
    embedding = await generate_embedding(embedding_input)
    logging.info(f"[Embeddings] Location edit → input='{embedding_input[:120]}...'")

    supabase.table("listings").update({
        "location": new_location,
        "embedding": embedding
    }).eq("id", listing_id).execute()

    context.application.create_task(
        update.message.reply_text(
            tr(me, "ok_location_updated"),
            reply_markup=main_menu_keyboard(str(update.effective_user.id))
        ),
        update=update
    )
    return await edit_menu_back(update, context)

async def update_listing_description(update: Update, context: ContextTypes.DEFAULT_TYPE):
    me = str(update.effective_user.id)
    listing_id = context.user_data.get("edit_listing_id")
    new_text = update.message.text
    supabase.table("listings").update({"description": new_text}).eq("id", listing_id).execute()
    context.application.create_task(update.message.reply_text(tr(me, "ok_desc_updated_simple"), reply_markup=main_menu_keyboard(str(update.effective_user.id))), update=update)

    return await edit_menu_back(update, context)

async def cancel_editing(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    # Go straight back to the listing card (no “Cancelled” text)
    return await _back_to_listing_from_edit(update, context)

async def confirm_delete_listing(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    me = str(update.effective_user.id)

    listing_id = query.data.split("_")[1]
    context.user_data["delete_listing_id"] = listing_id

    chat = query.message.chat

    # 🧹 Delete old media messages
    for msg_id in context.user_data.get("last_media_messages", []):
        try:
            context.application.create_task(context.bot.delete_message(chat_id=chat.id, message_id=msg_id), update=update)
        except Exception as e:
            print(f"Failed to delete media message {msg_id}: {e}")
    context.user_data["last_media_messages"] = []

    # 🧹 Delete the listing text block
    try:
        context.application.create_task(query.message.delete(), update=update)
    except Exception as e:
        print(f"Failed to delete main listing message: {e}")

    # ✅ Show confirmation message
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(tr(me, "delete_yes"), callback_data="confirm_delete_yes")],
        [InlineKeyboardButton(tr(me, "delete_no"), callback_data="confirm_delete_no")]
    ])

    context.application.create_task(
        chat.send_message(
            tr(me, "delete_confirm"),
            reply_markup=keyboard
        ),
        update=update
    )

    return CONFIRM_DELETE

async def handle_delete_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    me = str(update.effective_user.id)
    decision = query.data

    if decision == "confirm_delete_yes":
        listing_id = context.user_data.get("delete_listing_id")
        if listing_id:
            supabase.table("listings").delete().eq("id", listing_id).execute()

        user_id = str(update.effective_user.id)
        result = supabase.table("listings").select("*").eq("owner_id", user_id).execute()
        listings = result.data or []

        if not listings:
            context.application.create_task(query.message.reply_text(tr(me, "delete_done_none_left")), update=update)
            return ConversationHandler.END

        context.user_data["my_listings"] = listings
        context.user_data["listing_index"] = 0
        context.application.create_task(query.message.reply_text(tr(me, "delete_done")), update=update)
        await send_single_listing(update, context)
        return ConversationHandler.END

    elif decision == "confirm_delete_no":
        context.application.create_task(query.message.reply_text(tr(me, "delete_cancelled")), update=update)
        return ConversationHandler.END

# --- i18n additions for Create Listing + Rental flow (patch LOCALES defined above) ---
_I18N_CREATE_RENT_PATCH = {
    "en": {
        # Create flow
        "create_pick_category": "Please select a category from the buttons below or type it in chat:",
        "create_item_prompt_with_examples": (
            "Please provide the item name plus brand and model (if it exists).\n"
            "Examples:\n"
            "• Electric guitar Ibanez RG370\n"
            "• VR headset Meta Quest 3\n"
            "• DJI drone Mini 3"
        ),
        "create_specs_prompt": "List 1–4 key specs (comma-separated):",
        "create_desc_prompt": "Got it! Now enter a short description:",
        "create_condition_prompt": "Please describe any visible damage or wear. If it's in perfect condition, just say so.",
        "create_price_prompt": "Please input the price per day and a currency. For example: 100 PLN",
        "create_send_photos": "Send up to 3 photos of your item:",
        "photos_thanks": "Thanks! Photos received.",
        "share_location_or_type": "Now share your location or type it manually:",
        "could_not_recognize_location": "❗ Could not recognize that location. Please try again with a valid city.",
        "availability_prompt": "Please type your availability in this format:\n01.01.2026 - 15.01.2026, 20.01.2026 - 30.01.2026",
        "availability_end_before_start": "❗ End date {end} is before start date {start}. Please try again.",
        "availability_parse_fail": (
            "❗ I couldn't find any valid ranges.\n"
            "Try formats like:\n"
            "• 05/09/2025 - 12/09/2025\n"
            "• 05-09-2025 - 12-09-2025\n"
            "• 05,09,2025 - 12,09,2025\n"
            "You can chain multiple ranges in one message."
        ),
        "stop_no_listing": "You're not creating a listing right now.",
        "stop_cancelled_creation": "❌ Listing creation cancelled. All progress lost.",
        "listing_created": "✅ Listing created.",
        "insurance_q": "Would you feel safer with insurance for unexpected damage or theft?",
        "insurance_yes": "Thanks! We're working hard to implement this feature soon.",
        "insurance_no": "Thanks for your feedback!",
        # Rental flow
        "rent_no_days": "No available days to book.",
        "choose_year": "Choose a year:",
        "back_to_listing": "⬅️ Back to listing",
        "cancel": "❌ Cancel",
        "year_title": "Year: {y}\nChoose a month:",
        "no_months": "Year: {y}\nNo available months.",
        "pick_day": "{month} {year}\nPick a day:",
        "no_free_days": "No free days here",
        "day_just_taken": "😕 Sorry, this day was just taken. Please pick another one.",
        "back_to_months": "Back to months",
        "cancel_reservation": "❌ Cancel Reservation",
        "day_added": "✅ Added *{day}*.\n\nSelected so far: {preview}\n\nSelect more days?",
        "select_more_days": "➕ Select more days",
        "finish_selecting": "✅ Finish selecting",
        "year_label": "Year:",
        "keep_selecting_ok": "Okay, keep selecting.",
        "review_selection_title": "🗓 *Review selection*\n\n",
        "yes_book_them": "✅ Yes, book them",
        "no_keep_selecting": "🙅 No, keep selecting",
        "req_sent_title": "📩 *Request sent to the owner*\n\n",
        "lbl_item": "• Item:",
        "lbl_dates": "• Dates:",
        "lbl_est_total": "• Estimated total:",
        "notify_when_accept": "You'll get a message here once the owner accepts or declines",
        "back_to_listing_btn": "⬅️ Back to listing",
        "notify_lender_new_req": "📬 New borrow request for “{item}”. Open My Account to review.",
    },
    "pl": {
        # Create flow
        "create_pick_category": "Wybierz kategorię z przycisków poniżej lub wpisz ją w czacie:",
        "create_item_prompt_with_examples": (
            "Podaj nazwę przedmiotu oraz markę i model (jeśli istnieje).\n"
            "Przykłady:\n"
            "• Gitara elektryczna Ibanez RG370\n"
            "• Gogle VR Meta Quest 3\n"
            "• Dron DJI Mini 3"
        ),
        "create_specs_prompt": "Wypisz 1–4 kluczowe cechy (po przecinku):",
        "create_desc_prompt": "Świetnie! Teraz krótki opis:",
        "create_condition_prompt": "Opisz widoczne ślady użycia lub uszkodzenia. Jeśli stan idealny — napisz to.",
        "create_price_prompt": "Podaj cenę za dzień i walutę. Na przykład: 100 PLN",
        "create_send_photos": "Wyślij do 3 zdjęć przedmiotu:",
        "photos_thanks": "Dzięki! Zdjęcia odebrane.",
        "share_location_or_type": "Udostępnij swoją lokalizację lub wpisz ją ręcznie:",
        "could_not_recognize_location": "❗ Nie rozpoznano lokalizacji. Spróbuj ponownie, podając poprawne miasto.",
        "availability_prompt": "Podaj dostępność w formacie:\n01.01.2026 - 15.01.2026, 20.01.2026 - 30.01.2026",
        "availability_end_before_start": "❗ Data końcowa {end} jest przed początkową {start}. Spróbuj ponownie.",
        "availability_parse_fail": (
            "❗ Nie znaleziono prawidłowych zakresów.\n"
            "Przykłady:\n"
            "• 05/09/2025 - 12/09/2025\n"
            "• 05-09-2025 - 12-09-2025\n"
            "• 05,09,2025 - 12,09,2025\n"
            "Możesz podać wiele zakresów w jednej wiadomości."
        ),
        "stop_no_listing": "Nie tworzysz teraz żadnego ogłoszenia.",
        "stop_cancelled_creation": "❌ Tworzenie ogłoszenia anulowane. Postęp utracony.",
        "listing_created": "✅ Ogłoszenie utworzone.",
        "insurance_q": "Czy czuł(a)byś się bezpieczniej z ubezpieczeniem od szkód lub kradzieży?",
        "insurance_yes": "Dzięki! Pracujemy nad tą funkcją.",
        "insurance_no": "Dziękujemy za opinię!",
        # Rental flow
        "rent_no_days": "Brak dostępnych dni do rezerwacji.",
        "choose_year": "Wybierz rok:",
        "back_to_listing": "⬅️ Powrót do ogłoszenia",
        "cancel": "❌ Anuluj",
        "year_title": "Rok: {y}\nWybierz miesiąc:",
        "no_months": "Rok: {y}\nBrak dostępnych miesięcy.",
        "pick_day": "{month} {year}\nWybierz dzień:",
        "no_free_days": "Brak wolnych dni",
        "day_just_taken": "😕 Niestety ten dzień został właśnie zajęty. Wybierz inny.",
        "back_to_months": "Powrót do miesięcy",
        "cancel_reservation": "❌ Anuluj rezerwację",
        "day_added": "✅ Dodano *{day}*.\n\nDotychczas wybrane: {preview}\n\nDodać kolejne dni?",
        "select_more_days": "➕ Wybierz kolejne dni",
        "finish_selecting": "✅ Zakończ wybór",
        "year_label": "Rok:",
        "keep_selecting_ok": "OK, kontynuuj wybór.",
        "review_selection_title": "🗓 *Przegląd wyboru*\n\n",
        "yes_book_them": "✅ Tak, zarezerwuj",
        "no_keep_selecting": "🙅 Nie, wybieram dalej",
        "req_sent_title": "📩 *Wysłano prośbę do właściciela*\n\n",
        "lbl_item": "• Przedmiot:",
        "lbl_dates": "• Terminy:",
        "lbl_est_total": "• Szacowany koszt:",
        "notify_when_accept": "Otrzymasz wiadomość, gdy właściciel zaakceptuje lub odrzuci",
        "back_to_listing_btn": "⬅️ Powrót do ogłoszenia",
        "notify_lender_new_req": "📬 Nowa prośba o wypożyczenie „{item}”. Otwórz My Account, aby sprawdzić.",
    },
    "uk": {
        # Create flow
        "create_pick_category": "Оберіть категорію нижче або напишіть у чаті:",
        "create_item_prompt_with_examples": (
            "Укажіть назву предмета та, за можливості, бренд і модель.\n"
            "Приклади:\n"
            "• Електрогітара Ibanez RG370\n"
            "• VR-гарнітура Meta Quest 3\n"
            "• Дрон DJI Mini 3"
        ),
        "create_specs_prompt": "Вкажіть 1–4 ключові характеристики (через кому):",
        "create_desc_prompt": "Чудово! Тепер короткий опис:",
        "create_condition_prompt": "Опишіть видимі пошкодження чи зношеність. Якщо стан ідеальний — просто скажіть про це.",
        "create_price_prompt": "Вкажіть ціну за добу та валюту. Наприклад: 100 PLN",
        "create_send_photos": "Надішліть до 3 фото предмета:",
        "photos_thanks": "Дякую! Фото отримано.",
        "share_location_or_type": "Надішліть свою локацію або введіть її вручну:",
        "could_not_recognize_location": "❗ Не вдалося розпізнати локацію. Спробуйте ще раз, вказавши коректне місто.",
        "availability_prompt": "Вкажіть доступність у форматі:\n01.01.2026 - 15.01.2026, 20.01.2026 - 30.01.2026",
        "availability_end_before_start": "❗ Кінцева дата {end} раніше початкової {start}. Спробуйте ще раз.",
        "availability_parse_fail": (
            "❗ Не знайшов жодних коректних проміжків.\n"
            "Спробуйте так:\n"
            "• 05/09/2025 - 12/09/2025\n"
            "• 05-09-2025 - 12-09-2025\n"
            "• 05,09,2025 - 12,09,2025\n"
            "Можна вказати кілька діапазонів в одному повідомленні."
        ),
        "stop_no_listing": "Ви зараз не створюєте оголошення.",
        "stop_cancelled_creation": "❌ Створення оголошення скасовано. Весь прогрес втрачено.",
        "listing_created": "✅ Оголошення створено.",
        "insurance_q": "Чи почувалися б ви безпечніше з страхуванням від пошкоджень або крадіжки?",
        "insurance_yes": "Дякуємо! Невдовзі додамо цю функцію.",
        "insurance_no": "Дякуємо за відгук!",
        # Rental flow
        "rent_no_days": "Немає доступних днів для бронювання.",
        "choose_year": "Оберіть рік:",
        "back_to_listing": "⬅️ Повернутися до оголошення",
        "cancel": "❌ Скасувати",
        "year_title": "Рік: {y}\nОберіть місяць:",
        "no_months": "Рік: {y}\nНемає доступних місяців.",
        "pick_day": "{month} {year}\nОберіть день:",
        "no_free_days": "Немає вільних днів",
        "day_just_taken": "😕 На жаль, цей день щойно зайняли. Оберіть інший.",
        "back_to_months": "Повернутися до місяців",
        "cancel_reservation": "❌ Скасувати бронювання",
        "day_added": "✅ Додано *{day}*.\n\nВибрано: {preview}\n\nДодати ще днів?",
        "select_more_days": "➕ Обрати ще дні",
        "finish_selecting": "✅ Завершити вибір",
        "year_label": "Рік:",
        "keep_selecting_ok": "Гаразд, продовжуйте вибір.",
        "review_selection_title": "🗓 *Перегляд вибору*\n\n",
        "yes_book_them": "✅ Так, забронювати",
        "no_keep_selecting": "🙅 Ні, продовжити вибір",
        "req_sent_title": "📩 *Запит надіслано власнику*\n\n",
        "lbl_item": "• Предмет:",
        "lbl_dates": "• Дати:",
        "lbl_est_total": "• Орієнтовна сума:",
        "notify_when_accept": "Ви отримаєте повідомлення після підтвердження або відхилення",
        "back_to_listing_btn": "⬅️ Назад до оголошення",
        "notify_lender_new_req": "📬 Новий запит на оренду «{item}». Відкрийте My Account, щоб переглянути.",
    },
}
try:
    for _lng, _patch in _I18N_CREATE_RENT_PATCH.items():
        if _lng in LOCALES:
            LOCALES[_lng].update(_patch)
except Exception as _e:
    logging.warning(f"[i18n] LOCALES create/rent patch failed: {_e}")

_I18N_UI_PATCH = {
    "en": {
        # listings / nav
        "btn_prev_listing": "⬅️ Previous",
        "btn_next_listing": "➡️ Next",
        "btn_edit": "✏️ Edit",
        "btn_delete": "🗑 Delete",
        "btn_lending_schedule": "📆 Lending schedule",

        # photo flow buttons
        "photos_add_more": "Add More",
        "photos_continue": "Continue",
        "photos_delete_word": "Delete",
        "photos_cancel_listing": "Cancel Listing",

        # location/share buttons
        "send_location_btn": "Send Location",
        "cancel_listing_btn": "Cancel Listing",

        # categories (canonical -> localized label)
        "cat_Electronics": "Electronics",
        "cat_Recreation": "Recreation",
        "cat_Construction": "Construction",
        "cat_HomeImprovement": "Home Improvement",
        "cat_EventsParty": "Events & Party",
        "cat_Gardening": "Gardening",
    },
    "pl": {
        "btn_prev_listing": "⬅️ Poprzednie",
        "btn_next_listing": "➡️ Następne",
        "btn_edit": "✏️ Edytuj",
        "btn_delete": "🗑 Usuń",
        "btn_lending_schedule": "📆 Harmonogram wypożyczeń",

        "photos_add_more": "Dodaj kolejne",
        "photos_continue": "Kontynuuj",
        "photos_delete_word": "Usuń",
        "photos_cancel_listing": "Anuluj ogłoszenie",

        "send_location_btn": "Wyślij lokalizację",
        "cancel_listing_btn": "Anuluj ogłoszenie",

        "cat_Electronics": "Elektronika",
        "cat_Recreation": "Rekreacja",
        "cat_Construction": "Budowlane",
        "cat_HomeImprovement": "Dom i remont",
        "cat_EventsParty": "Imprezy i eventy",
        "cat_Gardening": "Ogród",
    },
    "uk": {
        "btn_prev_listing": "⬅️ Попереднє",
        "btn_next_listing": "➡️ Наступне",
        "btn_edit": "✏️ Редагувати",
        "btn_delete": "🗑 Видалити",
        "btn_lending_schedule": "📆 Графік видачі",

        "photos_add_more": "Додати ще",
        "photos_continue": "Продовжити",
        "photos_delete_word": "Видалити",
        "photos_cancel_listing": "Скасувати оголошення",

        "send_location_btn": "Надіслати локацію",
        "cancel_listing_btn": "Скасувати оголошення",

        "cat_Electronics": "Електроніка",
        "cat_Recreation": "Відпочинок",
        "cat_Construction": "Будівництво",
        "cat_HomeImprovement": "Дім та ремонт",
        "cat_EventsParty": "Події та вечірки",
        "cat_Gardening": "Сад",
    },
}
try:
    for _lng, _patch in _I18N_UI_PATCH.items():
        if _lng in LOCALES:
            LOCALES[_lng].update(_patch)
except Exception as _e:
    logging.warning(f"[i18n] LOCALES UI patch failed: {_e}")

_I18N_PURCHASE_PATCH = {
    "en": {
        "btn_purchase": "⭐ Purchase listings",
        "quota_unlimited": "Limit: *Unlimited* (until {until})",
        "quota_limited":  "Limit: {used}/{limit} listings used",
        "purchase_title": "Get more listing slots",
        "purchase_pick":  "Choose a plan:",
        "purchase_pack2": "➕ +2 listings — 100⭐",
        "purchase_pack5": "➕ +5 listings — 250⭐",
        "purchase_unlm":  "♾ Unlimited 30 days — 350⭐/mo",
        "purchase_back":  "⬅️ Back",
        "purchase_thanks_slots": "✅ Purchase successful! You now have {limit} total slots.",
        "purchase_thanks_unlm":  "✅ Unlimited active until {until}.",
        "limit_hit": "You’ve reached your listing limit.\n\n{quota}\n\nBuy more slots to continue.",
        "open_purchase": "Open purchase options",
    },
    "pl": {
        "btn_purchase": "⭐ Kup miejsca na ogłoszenia",
        "quota_unlimited": "Limit: *Bez limitu* (do {until})",
        "quota_limited":  "Limit: wykorzystano {used}/{limit}",
        "purchase_title": "Kup więcej miejsc",
        "purchase_pick":  "Wybierz plan:",
        "purchase_pack2": "➕ +2 ogłoszenia — 100⭐",
        "purchase_pack5": "➕ +5 ogłoszeń — 250⭐",
        "purchase_unlm":  "♾ Bez limitu 30 dni — 350⭐/mies.",
        "purchase_back":  "⬅️ Wstecz",
        "purchase_thanks_slots": "✅ Zakup udany! Masz teraz {limit} miejsc.",
        "purchase_thanks_unlm":  "✅ Bez limitu aktywne do {until}.",
        "limit_hit": "Osiągnięto limit ogłoszeń.\n\n{quota}\n\nKup więcej miejsc, aby kontynuować.",
        "open_purchase": "Otwórz opcje zakupu",
    },
    "uk": {
        "btn_purchase": "⭐ Купити місця для оголошень",
        "quota_unlimited": "Ліміт: *Без обмежень* (до {until})",
        "quota_limited":  "Ліміт: використано {used}/{limit}",
        "purchase_title": "Отримати більше місць",
        "purchase_pick":  "Оберіть план:",
        "purchase_pack2": "➕ +2 оголошення — 100⭐",
        "purchase_pack5": "➕ +5 оголошень — 250⭐",
        "purchase_unlm":  "♾ Безліміт на 30 днів — 350⭐/міс.",
        "purchase_back":  "⬅️ Назад",
        "purchase_thanks_slots": "✅ Покупку виконано! Тепер у вас {limit} місць.",
        "purchase_thanks_unlm":  "✅ Безліміт активний до {until}.",
        "limit_hit": "Досягнено ліміт оголошень.\n\n{quota}\n\nПридбайте більше місць, щоб продовжити.",
        "open_purchase": "Відкрити варіанти покупки",
    },
}
try:
    for _lng, _patch in _I18N_PURCHASE_PATCH.items():
        if _lng in LOCALES:
            LOCALES[_lng].update(_patch)
except Exception as _e:
    logging.warning(f"[i18n] LOCALES purchase patch failed: {_e}")

# === Create Listing Flow ===
async def start_listing(update: Update, context: ContextTypes.DEFAULT_TYPE):
    me = str(update.effective_user.id)
    logging.info("[create] start_listing user=%s", me)

    # Enforce limit
    max_allowed, _, sub_active = get_entitlement(me)
    used = get_used_listings(me)
    quota = _quota_line_text(me)
    if (not sub_active) and max_allowed is not None and used >= max_allowed:
        kb = InlineKeyboardMarkup([[InlineKeyboardButton(tr(me, "btn_purchase"), callback_data="shop_open")]])
        await update.message.reply_text(tr(me, "limit_hit", quota=quota), reply_markup=kb)
        return ConversationHandler.END
    context.user_data['_in_listing_creation'] = True
    await update.message.reply_text(
        tr(me, "create_pick_category"),
        reply_markup=category_keyboard(me)
    )
    return GET_CATEGORY

async def get_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    me = str(update.effective_user.id)
    logging.info("[create] get_category user=%s text=%r", me, update.message.text)
    if update.message.text == tr(me, "back"):
        return await go_back(update, context)

    cat_raw = (update.message.text or "").strip()

    # safety: ignore empty category
    if not cat_raw:
        await update.message.reply_text(
            tr(me, "create_pick_category"),
            reply_markup=category_keyboard(me)
        )
        return GET_CATEGORY

    canon = to_canonical_category(cat_raw, me) or cat_raw  # allow free text, but prefer canonical
    context.user_data['_in_listing_creation'] = True
    context.user_data['category'] = canon

    await update.message.reply_text(
        tr(me, "create_item_prompt_with_examples"),
        reply_markup=ReplyKeyboardRemove()
    )
    return GET_ITEM_TITLE

async def get_item_title(update: Update, context: ContextTypes.DEFAULT_TYPE):
    me = str(update.effective_user.id)
    logging.info("[create] get_item_title user=%s text=%r", me, update.message.text)
    if (update.message.text or "").strip() == tr(me, "back"):
        return await go_back(update, context)
    text = update.message.text.strip()
    ok, why = await moderate_text(text)
    if not ok:
        await update.message.reply_text(tr(me, "reject_item", why=why))
        return GET_ITEM_TITLE
    context.user_data['item_title'] = text
    await update.message.reply_text(tr(me, "create_specs_prompt"))
    return GET_SPECS

async def get_specs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    me = str(update.effective_user.id)
    logging.info("[create] get_specs user=%s text=%r", me, update.message.text)
    if (update.message.text or "").strip() == tr(me, "back"):
      return await go_back(update, context)
    specs = [s.strip() for s in update.message.text.split(",") if s.strip()]
    # Check as a joined string
    ok, why = await moderate_text(", ".join(specs))
    if not ok:
        await update.message.reply_text(tr(me, "reject_specs", why=why))
        return GET_SPECS
    context.user_data['specs'] = specs
    await update.message.reply_text(tr(me, "create_desc_prompt"), reply_markup=ReplyKeyboardRemove())
    return GET_DESCRIPTION

async def get_description(update: Update, context: ContextTypes.DEFAULT_TYPE):
    me = str(update.effective_user.id)
    logging.info("[create] get_description user=%s text=%r", me, update.message.text)
    text = update.message.text
    ok, why = await moderate_text(text)
    if not ok:
        await update.message.reply_text(tr(me, "reject_desc", why=why))
        return GET_DESCRIPTION
    context.user_data['description'] = text
    await update.message.reply_text(
        tr(me, "create_condition_prompt"),
        reply_markup=ReplyKeyboardRemove()
    )
    return GET_CONDITION

async def get_condition(update: Update, context: ContextTypes.DEFAULT_TYPE):
    me = str(update.effective_user.id)
    logging.info("[create] get_condition user=%s text=%r", me, update.message.text)
    text = update.message.text
    ok, why = await moderate_text(text)
    if not ok:
        await update.message.reply_text(tr(me, "reject_condition", why=why))
        return GET_CONDITION
    context.user_data['condition'] = text
    await update.message.reply_text(tr(me, "create_price_prompt"), reply_markup=ReplyKeyboardRemove())
    return GET_PRICE

async def get_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    me = str(update.effective_user.id)
    logging.info("[create] get_price user=%s text=%r", me, update.message.text)
    raw = (update.message.text or "").strip()
    # Accept: "100", "100 PLN", "120.50 eur", "120,50 uah"
    m = re.match(r'^\s*([0-9]+(?:[.,][0-9]+)?)\s*([A-Za-z]{3})?\s*$', raw)
    if not m:
        await update.message.reply_text(tr(me, "price_format_hint"), parse_mode="Markdown")
        return GET_PRICE

    amount = float(m.group(1).replace(",", "."))
    currency = (m.group(2) or "PLN").upper()

    context.user_data['price_per_day'] = amount
    context.user_data['currency'] = currency

    await update.message.reply_text(tr(me, "create_send_photos"))
    context.user_data['photos'] = []
    return GET_PHOTOS

async def get_photos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    me = str(update.effective_user.id)
    logging.info("[create] get_photos user=%s has_photo=%s text=%r", me, bool(update.message.photo), update.message.text)
    if 'photos' not in context.user_data:
        context.user_data['photos'] = []

    if update.message.text:
        text = update.message.text.strip()

        # delete N (localized)
        idx = parse_delete_idx(text, me)
        if idx is not None:
            if 0 <= idx < len(context.user_data['photos']):
                context.user_data['photos'].pop(idx)
                await update.message.reply_text(tr(me, "photo_deleted"), reply_markup=photo_stage_keyboard(me))
            else:
                await update.message.reply_text(tr(me, "photo_invalid_number"), reply_markup=photo_stage_keyboard(me))
            return GET_PHOTOS

        # cancel listing
        if text.lower() == tr(me, "photos_cancel_listing").lower():
            context.user_data.clear()
            return await go_back(update, context)

        # continue -> ask for location (localized buttons)
        if text.lower() == tr(me, "photos_continue").lower():
            await update.message.reply_text(
                tr(me, "share_location_or_type"),
                reply_markup=ReplyKeyboardMarkup(
                    [[KeyboardButton(tr(me, "send_location_btn"), request_location=True),
                    tr(me, "cancel_listing_btn")]],
                    resize_keyboard=True
                )
            )
            return GET_LOCATION

        # add more: just re-show the keyboard
        if text.lower() == tr(me, "photos_add_more").lower():
            await update.message.reply_text(tr(me, "photos_send_prompt"), reply_markup=photo_stage_keyboard(me))
            return GET_PHOTOS

        await update.message.reply_text(tr(me, "send_photo_or_cmd"), reply_markup=photo_stage_keyboard(me))
        return GET_PHOTOS

    elif update.message.photo:
        file_id = update.message.photo[-1].file_id
        ok, why = await moderate_telegram_photo(file_id, context.bot)
        if not ok:
            await update.message.reply_text(tr(me, "photo_rejected", why=why), reply_markup=photo_stage_keyboard(me))
            return GET_PHOTOS
        context.user_data['photos'].append(file_id)
        await update.message.reply_text(tr(me, "photo_added", n=len(context.user_data['photos']), remaining=max(0, 3-len(context.user_data['photos']))), reply_markup=photo_stage_keyboard(me))
        return GET_PHOTOS

    await update.message.reply_text(tr(me, "send_photo_or_cmd"), reply_markup=photo_stage_keyboard(me))
    return GET_PHOTOS

async def get_location(update: Update, context: ContextTypes.DEFAULT_TYPE):
    me = str(update.effective_user.id)
    logging.info("[create] get_location user=%s has_location=%s text=%r", me, bool(update.message.location), update.message.text)
    if update.message.location:
        lat, lon = update.message.location.latitude, update.message.location.longitude
        context.user_data['location'] = f"{lat},{lon}"
    else:
        city_name = update.message.text
        location = await async_geocode(city_name)
        if location:
            context.user_data['location'] = f"{location.latitude},{location.longitude}"
        else:
            await update.message.reply_text(tr(me, "could_not_recognize_location"))
            return GET_LOCATION

    await update.message.reply_text(
        tr(me, "availability_prompt"),
        reply_markup=ReplyKeyboardRemove()
    )
    return GET_AVAILABILITY

async def get_availability(update: Update, context: ContextTypes.DEFAULT_TYPE):
    me = str(update.effective_user.id)
    logging.info("[create] get_availability user=%s text=%r", me, update.message.text)
    raw_text = (update.message.text or "").strip()

    # Find all dates like DD/MM/YYYY, DD-MM-YYYY, or DD,MM,YYYY (day-first)
    pat = re.compile(r'(?P<d>\d{1,2})\s*[\./,\-]\s*(?P<m>\d{1,2})\s*[\./,\-]\s*(?P<y>\d{4})')
    matches = list(pat.finditer(raw_text))

    availability: list[str] = []
    if len(matches) >= 2:
        # Pair dates in the order they appear: (1st,2nd), (3rd,4th), ...
        pairs = [ (matches[i], matches[i+1]) for i in range(0, len(matches) - 1, 2) ]
        for a, b in pairs:
            d1 = date(int(a.group("y")), int(a.group("m")), int(a.group("d")))
            d2 = date(int(b.group("y")), int(b.group("m")), int(b.group("d")))
            if d2 < d1:
                await update.message.reply_text(
                    tr(me, "availability_end_before_start", start=a.group(0), end=b.group(0))
                )
                return GET_AVAILABILITY
            cur = d1
            while cur <= d2:
                availability.append(cur.strftime("%Y-%m-%d"))
                cur += timedelta(days=1)

    # Fallback: try the old strict "DD/MM/YYYY - DD/MM/YYYY, ..." format if nothing parsed
    if not availability:
        chunks = raw_text.split(",")
        for period in chunks:
            try:
                start_str, end_str = period.strip().split("-")
                start_date = datetime.strptime(start_str.strip(), "%d/%m/%Y").date()
                end_date   = datetime.strptime(end_str.strip(),   "%d/%m/%Y").date()
                if end_date < start_date:
                    raise ValueError("End date is before start date")
                cur = start_date
                while cur <= end_date:
                    availability.append(cur.strftime("%Y-%m-%d"))
                    cur += timedelta(days=1)
            except Exception:
                pass
    if not availability:
        # supports "DD/MM/YYYY - DD/MM/YYYY" and also with "." or "-"
        pairs = re.split(r'\s*(?:;|,)\s*', raw_text)  # allow comma/semicolon separating ranges
        date_fmts = ["%d/%m/%Y", "%d-%m-%Y", "%d.%m.%Y", "%d,%m,%Y"]
        def _try_parse(s):
            for fmt in date_fmts:
                try:
                    return datetime.strptime(s.strip(), fmt).date()
                except Exception:
                    pass
            return None

        # split on hyphen or en/em dash
        for period in pairs:
            bits = re.split(r'\s*[-–—]\s*', period.strip())
            if len(bits) != 2:
                continue
            d1 = _try_parse(bits[0]); d2 = _try_parse(bits[1])
            if not d1 or not d2:
                continue
            if d2 < d1:
                await update.message.reply_text(tr(me, "availability_end_before_start", start=bits[0], end=bits[1]))
                return GET_AVAILABILITY
            cur = d1
            while cur <= d2:
                availability.append(cur.strftime("%Y-%m-%d"))
                cur += timedelta(days=1)

    if not availability:
        await update.message.reply_text(tr(me, "availability_parse_fail"))
        return GET_AVAILABILITY

    context.user_data['availability'] = availability
    user_id = str(update.effective_user.id)

    # Build embedding as before (unchanged)
    row_like = {
        "item": context.user_data.get("item_title"),
        "brand_model": context.user_data.get("brand_model"),
        "specs": context.user_data.get("specs"),
        "tags": context.user_data.get("tags"),
        "location": context.user_data.get("location"),
        "description": context.user_data.get("description"),
        "category": context.user_data.get("category"),
    }
    embedding_input = build_embedding_input_from_row(row_like)
    logging.info(f"[Embeddings] Create flow → input='{embedding_input[:160]}...'")
    embedding = await generate_embedding(embedding_input)

    # ---- gather all fields from the create flow safely ----
    category    = context.user_data.get("category") or ""
    item_title  = context.user_data.get("item_title") or context.user_data.get("title") or "Untitled"
    brand_model = context.user_data.get("brand_model") or None
    specs       = context.user_data.get("specs") or []
    tags        = context.user_data.get("tags") or []
    location    = context.user_data.get("location") or ""
    description = context.user_data.get("description") or ""
    condition   = context.user_data.get("condition") or ""
    price       = float(context.user_data.get("price_per_day") or 0)
    currency    = (context.user_data.get("currency") or "PLN").upper()
    photos      = (context.user_data.get("photos") or [])[:3]

    # Safety: ensure a users row exists for FK (owner_id -> users.id)
    try:
        me = str(update.effective_user.id)
        rows = supabase.table("users").select("id").eq("id", me).limit(1).execute().data
        if not rows:
            supabase.table("users").insert({
                "id": me,
                "telegram_username": update.effective_user.username or None,
                "display_name": (update.effective_user.full_name or update.effective_user.first_name or "").strip() or None,
                "created_at": datetime.utcnow().isoformat() + "Z",
                # nice to have: store last known location if you have it
                "location": (context.user_data.get("location") or None)
            }).execute()
    except Exception as e:
        logging.warning(f"[create] ensure user row failed: {e}")

    try:
        had_listings = bool(
            supabase.table("listings")
            .select("id")
            .eq("owner_id", me)
            .limit(1)
            .execute()
            .data
        )
    except Exception:
        had_listings = True  # fail-safe: assume not first if query fails

    # build idem
    idem = context.user_data.get("create_idem_key")
    if not idem:
        idem = uuid.uuid4().hex
        context.user_data["create_idem_key"] = idem

    payload = {
        "owner_id": me,
        "category": category,
        "item": item_title,
        "brand_model": brand_model,
        "specs": specs,
        "tags": tags,
        "description": description,
        "condition": condition,
        "price_per_day": price,
        "currency": currency,
        "photos": photos,
        "location": location,
        "availability": availability,
        "booked_days": [],
        "embedding": embedding,
        "idempotency_key": idem,
    }

    # ✅ single idempotent write
    await supa_exec_with_retry(
        lambda: supabase.table("listings")
            .upsert(payload, on_conflict="idempotency_key")
            .execute()
    )
    context.user_data.pop("create_idem_key", None)
    
    # tidy up the create-flow state but leave unrelated user_data (e.g., language) intact
    for k in (
        "category","item_title","brand_model","specs","tags","description","condition",
        "price_per_day","currency","photos","location","availability","_in_listing_creation"
    ):
        context.user_data.pop(k, None)

    await update.message.reply_text(
        tr(me, "listing_created"),
        reply_markup=main_menu_keyboard(me)
    )

    # Optional quick feedback question (safe to ignore if you don't want it)
    if not had_listings:
        try:
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("👍", callback_data="ins_yes"),
                InlineKeyboardButton("👎", callback_data="ins_no")]
            ])
            await update.message.reply_text(tr(me, "insurance_q"), reply_markup=kb)
        except Exception:
            pass

    return ConversationHandler.END

async def handle_insurance_feedback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    me = str(update.effective_user.id)
    if (update.message.text or "").strip().lower() == "yes":
        context.application.create_task(update.message.reply_text(tr(me, "insurance_yes")), update=update)
    else:
        context.application.create_task(update.message.reply_text(tr(me, "insurance_no")), update=update)
    return await go_back(update, context)

async def handle_insurance_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    me = str(q.from_user.id)

    choice = (q.data == "ins_yes")
    context.user_data["insurance_opt_in"] = choice

    # Acknowledge and remove inline buttons, so user can't tap twice
    try:
        await q.answer("Saved ✅")
        await q.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    # ✅ Send a simple reply (no prefix appended to the original question)
    try:
        msg_key = "insurance_yes" if choice else "insurance_no"
        context.application.create_task(q.message.chat.send_message(tr(me, msg_key)), update=update)
    except Exception:
        pass

# ===== Rental flow (Year -> Month -> Range -> Day) =====
async def rent_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    # Clear any in-progress selection for this listing if known
    listing_id = context.user_data.get("rent_listing_id")
    if listing_id:
        _clear_selected_days(context, listing_id)
    # Go back to the current card
    await rent_back_to_listing(update, context)

async def rent_choose_year(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    me = str(update.effective_user.id)
    _, _, listing_id = q.data.partition("rent_year_")

    context.user_data["rent_listing_id"] = listing_id

    # Compute available years dynamically (and stay within 2025/2026)
    listing = supabase.table("listings").select("*").eq("id", listing_id).execute().data[0]
    grouped = _group_available_by_year_month(listing)
    years = sorted(grouped.keys())

    if not years:
        kb = [[InlineKeyboardButton(tr(me, "back_to_listing"), callback_data="rent_back_to_listing")]]
        kb.append([InlineKeyboardButton(tr(me, "cancel"), callback_data="rent_cancel")])
        context.application.create_task(q.message.edit_text(tr(me, "rent_no_days"), reply_markup=InlineKeyboardMarkup(kb)), update=update)
        return

    rows, row = [], []
    for y in years:
        row.append(InlineKeyboardButton(str(y), callback_data=f"rent_month_{listing_id}_{y}"))
        if len(row) == 2:
            rows.append(row); row = []
    if row: rows.append(row)

    rows.append([InlineKeyboardButton(tr(me, "back"), callback_data="rent_year_back"),
                InlineKeyboardButton(tr(me, "cancel"), callback_data="rent_cancel")])

    kb = [[InlineKeyboardButton(tr(me, "back_to_years"), callback_data="rent_year_back")],
        [InlineKeyboardButton(tr(me, "cancel"), callback_data="rent_cancel")]]


    context.application.create_task(q.message.edit_text(tr(me, "choose_year"), reply_markup=InlineKeyboardMarkup(rows)), update=update)

async def rent_choose_month(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    me = str(update.effective_user.id)
    parts = q.data.split("_")  # ["rent","month", listing_id, year]
    listing_id, year = parts[2], int(parts[3])

    context.user_data["rent_listing_id"] = listing_id
    context.user_data["rent_year"] = year

    listing = supabase.table("listings").select("*").eq("id", listing_id).execute().data[0]
    grouped = _group_available_by_year_month(listing)
    months = sorted((grouped.get(year) or {}).keys())

    if not months:
        kb = [[InlineKeyboardButton("⬅️ Back to years", callback_data="rent_year_back")],
              [InlineKeyboardButton(tr(me, "cancel"), callback_data="rent_cancel")]]
        context.application.create_task(q.message.edit_text(tr(me, "no_months", y=year), reply_markup=InlineKeyboardMarkup(kb)), update=update)
        return

    rows, row = [], []
    for m in months:
        row.append(InlineKeyboardButton(_month_name(m), callback_data=f"rent_days_{listing_id}_{year}_{m}"))
        if len(row) == 3:
            rows.append(row); row = []
    if row: rows.append(row)

    rows.append([InlineKeyboardButton(tr(me, "back"), callback_data="rent_year_back"),
                InlineKeyboardButton(tr(me, "cancel"), callback_data="rent_cancel")])

    kb = [[InlineKeyboardButton(tr(me, "back_to_years"), callback_data="rent_year_back")],
        [InlineKeyboardButton(tr(me, "cancel"), callback_data="rent_cancel")]]


    context.application.create_task(q.message.edit_text(tr(me, "year_title", y=year), reply_markup=InlineKeyboardMarkup(rows)), update=update)

async def rent_show_days_month(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # From 'rent_days_<listing_id>_<year>_<month>'
    q = update.callback_query
    await q.answer()
    me = str(update.effective_user.id)
    _, _, listing_id, year, month = q.data.split("_")
    year, month = int(year), int(month)

    context.user_data.update({"rent_listing_id": listing_id, "rent_year": year, "rent_month": month})

    listing = supabase.table("listings").select("*").eq("id", listing_id).execute().data[0]
    grouped = _group_available_by_year_month(listing)
    day_pairs = (grouped.get(year, {}).get(month, []))  # [(day_int, token), ...]

    # Build day buttons only for available days
    buttons, row = [], []
    for day, token in day_pairs:
        row.append(InlineKeyboardButton(str(day), callback_data=f"rent_pick_{listing_id}_{token}"))
        if len(row) == 7:
            buttons.append(row); row = []
    if row: buttons.append(row)

    if not buttons:
        buttons = [[InlineKeyboardButton(tr(me, "no_free_days"), callback_data="noop")]]

    buttons.append([
        InlineKeyboardButton("⬅️ Back", callback_data=f"rent_month_back_{listing_id}_{year}"),
        InlineKeyboardButton(tr(me, "cancel"), callback_data="rent_cancel")
    ])

    context.application.create_task(
        q.message.edit_text(tr(me, "pick_day", month=_month_name(month), year=year),
                                  reply_markup=InlineKeyboardMarkup(buttons)),
        update=update
    )

async def rent_pick_day(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    me = str(update.effective_user.id)
    _, _, listing_id, day_token = q.data.split("_", 3)

    listing = supabase.table("listings").select("*").eq("id", listing_id).execute().data[0]
    available = _collect_available_days(listing)

    if day_token not in available:
        # Figure out a sensible year to go back to
        d = _parse_date_any(day_token)
        year = d.year if d else context.user_data.get('rent_year', 2025)
        context.application.create_task(
            q.message.edit_text(tr(me, "day_just_taken"),
                                      reply_markup=InlineKeyboardMarkup(
                                          [[InlineKeyboardButton(tr(me, "back_to_months"), callback_data=f"rent_month_{listing_id}_{year}")],
                                           [InlineKeyboardButton(tr(me, "cancel_reservation"), callback_data="rent_cancel")]]
                                      )),
            update=update
        )
        return

    sel = _add_selected_day(context, listing_id, day_token)

    preview = ", ".join(sel[:6]) + (" …" if len(sel) > 6 else "")
    txt = tr(me, "day_added", day=day_token, preview=(preview or "—"))

    year = context.user_data.get('rent_year')
    if not year:
        d = _parse_date_any(day_token)
        year = d.year if d else 2025

    kb = [
        [InlineKeyboardButton(tr(me, "select_more_days"), callback_data=f"rent_month_{listing_id}_{year}")],
        [InlineKeyboardButton(tr(me, "finish_selecting"), callback_data=f"rent_finish_{listing_id}")],
        [InlineKeyboardButton(tr(me, "cancel_reservation"), callback_data="rent_cancel")]
    ]
    context.application.create_task(q.message.edit_text(txt, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb)), update=update)

async def rent_back_to_listing(update: Update, context: ContextTypes.DEFAULT_TYPE):

    q = update.callback_query
    if not q:
        return
    await q.answer()

    me = str(q.from_user.id)
    data = (q.data or "").strip()

    # Use the existing browse collection, don't overwrite it
    listings = context.user_data.get("matched_listings", [])

    # If we carried the listing id in the callback, move the cursor to it
    if data.startswith("rent_back_to_listing_"):
        target_id = data.split("rent_back_to_listing_", 1)[1]
        for i, l in enumerate(listings or []):
            if str(l.get("id")) == str(target_id):
                context.user_data["browse_index"] = i
                break

    # If we have a collection, show it
    if listings:
        return await send_browse_listing(update, context)

    # Fallback: no collection in memory (e.g., user came from deep link or fresh session)
    # Send them back to the browse/search entry point
    return await handle_browse(update, context)

async def rent_year_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    me = str(update.effective_user.id)
    listing_id = context.user_data.get("rent_listing_id")
    if not listing_id:
        return await rent_back_to_listing(update, context)

    listing = supabase.table("listings").select("*").eq("id", listing_id).execute().data[0]
    grouped = _group_available_by_year_month(listing)
    years = sorted(grouped.keys())

    rows, row = [], []
    for y in years:
        row.append(InlineKeyboardButton(str(y), callback_data=f"rent_month_{listing_id}_{y}"))
        if len(row) == 2:
            rows.append(row); row = []
    if row: rows.append(row)
    rows.append([InlineKeyboardButton(tr(me, "back"), callback_data="rent_year_back"),
                InlineKeyboardButton(tr(me, "cancel"), callback_data="rent_cancel")])

    kb = [[InlineKeyboardButton(tr(me, "back_to_years"), callback_data="rent_year_back")],
        [InlineKeyboardButton(tr(me, "cancel"), callback_data="rent_cancel")]]


    context.application.create_task(q.message.edit_text(tr(me, "choose_year"), reply_markup=InlineKeyboardMarkup(rows)), update=update)

async def rent_month_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    me = str(update.effective_user.id)

    parts = q.data.split("_")        
    listing_id = parts[3]
    year = int(parts[4])

    listing = supabase.table("listings").select("*").eq("id", listing_id).execute().data[0]
    grouped = _group_available_by_year_month(listing)
    months = sorted((grouped.get(year) or {}).keys())

    rows, row = [], []
    for m in months:
        row.append(InlineKeyboardButton(_month_name(m), callback_data=f"rent_days_{listing_id}_{year}_{m}"))
        if len(row) == 3:
            rows.append(row); row = []
    if row: rows.append(row)
    rows.append([InlineKeyboardButton(tr(me, "back"), callback_data="rent_year_back"),
                InlineKeyboardButton(tr(me, "cancel"), callback_data="rent_cancel")])

    kb = [[InlineKeyboardButton(tr(me, "back_to_years"), callback_data="rent_year_back")],
        [InlineKeyboardButton(tr(me, "cancel"), callback_data="rent_cancel")]]

    context.application.create_task(q.message.edit_text(tr(me, "year_title", y=year), reply_markup=InlineKeyboardMarkup(rows)), update=update)

async def noop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()

async def rent_finish_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    me = str(update.effective_user.id)
    # rent_finish_<listing_id>
    _, _, listing_id = q.data.split("_", 2)

    sel = _get_selected_days(context, listing_id)
    if not sel:
        context.application.create_task(
            q.message.edit_text(tr(me, "keep_selecting_ok"),
                                      reply_markup=InlineKeyboardMarkup(
                                          [[InlineKeyboardButton(tr(me, "back_to_months"), callback_data=f"rent_month_{listing_id}_{context.user_data.get('rent_year','2025')}")],
                                           [InlineKeyboardButton(tr(me, "cancel_reservation"), callback_data="rent_cancel")]]
                                      )),
            update=update
        )
        return

    # Sort for nicer view
    def _key(s): 
        d = _parse_date_any(s)
        return (d or datetime.max.date(), s)
    sel_sorted = sorted(sel, key=_key)

    txt = tr(me, "review_selection_title") + "\n".join(f"• {d}" for d in sel_sorted) + f"\n\n{tr(me,'are_you_sure')}"
    kb = [
        [InlineKeyboardButton(tr(me, "yes_book_them"), callback_data=f"rent_confirm_yes_{listing_id}")],
        [InlineKeyboardButton(tr(me, "no_keep_selecting"), callback_data=f"rent_confirm_no_{listing_id}")],
        [InlineKeyboardButton(tr(me, "cancel_reservation"), callback_data="rent_cancel")]
    ]
    context.application.create_task(q.message.edit_text(txt, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb)), update=update)

async def rent_confirm_yes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    me = str(update.effective_user.id)
    # rent_confirm_yes_<listing_id>
    _, _, _, listing_id = q.data.split("_", 3)

    sel = _get_selected_days(context, listing_id)
    if not sel:
        context.application.create_task(
            q.message.edit_text(
                tr(me, "keep_selecting_ok"),
                parse_mode="MarkdownV2",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton(tr(me, "back_to_months"), callback_data=f"rent_month_{listing_id}_{context.user_data.get('rent_year','2025')}")],
                     [InlineKeyboardButton(tr(me, "cancel_reservation"), callback_data="rent_cancel")]]
                )
            ),
            update=update
        )
        return

    # Normalize to ISO for storage
    iso_days = _to_iso_list(sel)

    # Fetch listing and compute totals
    listing = supabase.table("listings").select(
        "owner_id,item,category,price_per_day,currency"
    ).eq("id", listing_id).execute().data[0]

    lender_id = listing["owner_id"]
    borrower_id = str(update.effective_user.id)
    borrower_username = update.effective_user.username or ""
    price_per_day = float(listing.get("price_per_day") or 0)
    currency = (listing.get("currency") or "PLN").upper()
    day_count = len(iso_days)
    total_price = round(price_per_day * day_count, 2)

    # Insert the rental request (pending)
    supabase.table("rental_requests").insert({
        "listing_id": listing_id,
        "lender_id": lender_id,
        "borrower_id": borrower_id,
        "borrower_username": borrower_username,
        "dates": iso_days,
        "status": "pending",
        "total_price": total_price,
        "currency": currency,
        "message_from_borrower": context.user_data.get("last_search_query", ""),
    }).execute()

    # Immediately refresh lender's main menu keyboard with count badge
    try:
        lender_menu = main_menu_keyboard(lender_id)  # localized + count-aware
        item_title = listing.get("item") or "Item"
        context.application.create_task(
            context.bot.send_message(
                chat_id=int(lender_id),
                text=tr(lender_id, "notify_lender_new_req", item=item_title),
                reply_markup=lender_menu
            ),
            update=update
        )
    except Exception as e:
        logging.warning(f"Failed to notify lender about new request: {e}")

    # Clear selection now that it’s submitted
    _clear_selected_days(context, listing_id)

    # Nice ranges for the message
    nice_dates, _ = _ranges_from_iso(iso_days)

    item_e  = esc_md2(listing.get("item") or "Item")
    cat_e   = esc_md2(listing.get("category") or "")
    dates_e = esc_md2(nice_dates)
    cur_e   = esc_md2(currency)
    total_e = esc_md2(f"{total_price:.2f}")
    footer  = esc_md2(tr(me, "notify_when_accept"))

    # Localized labels (escape only dynamic bits)
    txt = (
        tr(me, "req_sent_title") +
        f"{tr(me, 'lbl_item')} *{item_e}* — {cat_e}\n"
        f"{tr(me, 'lbl_dates')} {dates_e} — {day_count} day{'s' if day_count != 1 else ''}\n"
        f"{tr(me, 'lbl_est_total')} *{total_e} {cur_e}*\n\n"
        f"{footer}"
    )

    context.application.create_task(
        q.message.edit_text(
            txt,
            parse_mode="MarkdownV2",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton(tr(me, "back_to_listing_btn"), callback_data=f"rent_back_to_listing_{listing_id}")]]
            ),
        ),
        update=update
    )

async def rent_confirm_no(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    me = str(update.effective_user.id)
    # rent_confirm_no_<listing_id>
    _, _, _, listing_id = q.data.split("_", 3)
    # Go back to month grid with selection preserved
    year = context.user_data.get("rent_year", "2025")
    context.application.create_task(
        q.message.edit_text(tr(me, "keep_selecting_ok"),
                                  reply_markup=InlineKeyboardMarkup(
                                      [[InlineKeyboardButton(tr(me, "back_to_months"), callback_data=f"rent_month_{listing_id}_{year}")],
                                       [InlineKeyboardButton(tr(me, "cancel_reservation"), callback_data="rent_cancel")]]
                                  )),
        update=update
    )

# === i18n-aware label regex helpers ===
def _labels_for(key: str) -> set[str]:
    # gather all non-empty localized strings for a given LOCALES key
    return {s for s in (LOCALES.get(lang, {}).get(key, "") for lang in LOCALES) if s}

def _re_alt(labels: set[str]) -> str:
    import re as _re
    if not labels:
        return r"^$"  # nothing matches (shouldn't happen)
    return r"^(?:" + "|".join(_re.escape(s) for s in labels) + r")$"

def _re_alt_with_badge(labels: set[str]) -> str:
    import re as _re
    if not labels:
        return r"^$"
    # matches: "Label", "Label (12)", or "Label [12]"
    return r"^(?:" + "|".join(_re.escape(s) for s in labels) + r")(?: (?:\(\d+\)|\[\d+\]))?$"

# Build label sets from LOCALES so they always track your translations
MY_ACCOUNT_LABELS   = _labels_for("btn_my_account")
CREATE_LABELS       = _labels_for("btn_create_listing")
BROWSE_LABELS       = _labels_for("btn_browse")          # <- will include "Пошук"
MY_LISTINGS_LABELS  = _labels_for("btn_my_listings")
BACK_LABELS         = _labels_for("back")

# Compile regexes used by handlers
CREATE_RE       = _re_alt(CREATE_LABELS)
BROWSE_RE       = _re_alt(BROWSE_LABELS)
MY_LISTINGS_RE  = _re_alt(MY_LISTINGS_LABELS)
BACK_RE         = _re_alt(BACK_LABELS)
MY_ACCOUNT_RE   = _re_alt_with_badge(MY_ACCOUNT_LABELS)

# Yes/No are short; keep explicit union (add more locales if you add langs)
YES_LABELS = {"Yes", "Tak", "Так"}
NO_LABELS  = {"No",  "Nie", "Ні"}
YESNO_RE   = r"^(?:" + "|".join((YES_LABELS | NO_LABELS)) + r")$"


# === Conversation Handlers ===
listing_conv = ConversationHandler(
    entry_points=[MessageHandler(filters.Regex(CREATE_RE), start_listing)],
    states={
        GET_CATEGORY:     [MessageHandler(filters.TEXT & ~filters.COMMAND, get_category)],
        GET_ITEM_TITLE:   [MessageHandler(filters.TEXT & ~filters.COMMAND, get_item_title)],
        GET_SPECS:        [MessageHandler(filters.TEXT & ~filters.COMMAND, get_specs)],
        GET_DESCRIPTION:  [MessageHandler(filters.TEXT & ~filters.COMMAND, get_description)],
        GET_CONDITION:    [MessageHandler(filters.TEXT & ~filters.COMMAND, get_condition)],
        GET_PRICE:        [MessageHandler(filters.TEXT & ~filters.COMMAND, get_price)],
        GET_PHOTOS:       [MessageHandler(filters.PHOTO | filters.TEXT, get_photos)],
        GET_LOCATION:     [MessageHandler(filters.LOCATION | filters.TEXT, get_location)],
        GET_AVAILABILITY: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_availability)]
    },
    fallbacks=[
        CommandHandler("stop", stop_listing_creation),
        MessageHandler(filters.Regex(BACK_RE), go_back),
    ]
)

edit_conv = ConversationHandler(
    entry_points=[CallbackQueryHandler(start_editing_listing, pattern=r"^edit_[0-9a-fA-F-]{36}$")],
    states={
        EDIT_CHOICE:            [CallbackQueryHandler(handle_edit_field_choice)],
        AWAIT_NEW_PHOTOS:       [MessageHandler(filters.PHOTO | filters.TEXT, receive_new_photos)],
        AWAIT_NEW_DESCRIPTION:  [MessageHandler(filters.TEXT, receive_new_description)],
        AWAIT_NEW_PRICE:        [MessageHandler(filters.TEXT, receive_new_price)],
        AWAIT_NEW_LOCATION:     [MessageHandler(filters.TEXT, receive_new_location)],
        AWAIT_NEW_CATEGORY:     [MessageHandler(filters.TEXT, receive_new_category)],
        AWAIT_NEW_CONDITION:    [MessageHandler(filters.TEXT, receive_new_condition)],
        AWAIT_NEW_ITEM_TITLE:   [MessageHandler(filters.TEXT, receive_new_item_title)],
        AWAIT_NEW_BRAND_MODEL:  [MessageHandler(filters.TEXT, receive_new_brand_model)],
        AWAIT_NEW_SPECS:        [MessageHandler(filters.TEXT, receive_new_specs)],
        CONFIRM_DELETE:         [CallbackQueryHandler(handle_delete_confirmation)],
    },
    fallbacks=[CallbackQueryHandler(cancel_editing, pattern="^cancel_edit$")],
    allow_reentry=True,           
)

browse_conv = ConversationHandler(
    entry_points=[MessageHandler(filters.Regex(BROWSE_RE), handle_browse)],
    states={
        AWAIT_SEARCH_QUERY: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_natural_search)]
    },
    fallbacks=[MessageHandler(filters.Regex(BACK_RE), go_back)]
)


# === Main Setup ===
if __name__ == '__main__':
    # --- runtime config for Cloud Run / local ---
    MODE = os.getenv("MODE", "polling")             # 'webhook' on Cloud Run
    PORT = int(os.getenv("PORT", "8080"))           # Cloud Run provides this
    HOST = "0.0.0.0"
    WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook")
    WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "set-a-secret")

    # Build app ONCE with your real bot token
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
        logging.exception("Unhandled exception", exc_info=context.error)
    app.add_error_handler(on_error)

    # === register ALL handlers here ===
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(set_language_from_callback, pattern=r"^set_lang_(en|pl|uk)$"))  
    app.add_handler(CommandHandler("language", prompt_language))
    app.add_handler(CommandHandler("stop", stop_listing_creation))


    # My Account (supports badge in () or [])
    app.add_handler(MessageHandler(filters.Regex(MY_ACCOUNT_RE), handle_my_account))

    # === Shop ===
    app.add_handler(CallbackQueryHandler(shop_open,   pattern=r"^shop_open$"))
    app.add_handler(CallbackQueryHandler(shop_buy_2,  pattern=r"^shop_buy_2$"))
    app.add_handler(CallbackQueryHandler(shop_buy_5,  pattern=r"^shop_buy_5$"))
    app.add_handler(CallbackQueryHandler(shop_buy_sub,pattern=r"^shop_buy_sub$"))

    # Payments (Stars)
    app.add_handler(PreCheckoutQueryHandler(payment_precheckout))
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, payment_success))

    # Requests UI
    app.add_handler(CallbackQueryHandler(account_requests, pattern=r"^account_requests$"))
    app.add_handler(CallbackQueryHandler(account_req_next, pattern=r"^account_req_next$"))
    app.add_handler(CallbackQueryHandler(account_req_prev, pattern=r"^account_req_prev$"))

    # Accept / Decline
    app.add_handler(CallbackQueryHandler(handle_request_accept,  pattern=r"^req_accept_[0-9a-fA-F-]{36}$"))
    app.add_handler(CallbackQueryHandler(handle_request_decline, pattern=r"^req_decline_[0-9a-fA-F-]{36}$"))
    app.add_handler(CallbackQueryHandler(show_lending_schedule,   pattern=r"^schedule_[0-9a-fA-F-]{36}$"))
    app.add_handler(CallbackQueryHandler(schedule_back_to_listing, pattern=r"^schedule_back_[0-9a-fA-F-]{36}$"))
    app.add_handler(CallbackQueryHandler(handle_insurance_choice, pattern=r"^ins_(?:yes|no)$"))


    # Conversations/handlers 
    app.add_handler(listing_conv)
    app.add_handler(MessageHandler(filters.Regex(BACK_RE), go_back))
    app.add_handler(MessageHandler(filters.Regex(MY_LISTINGS_RE), view_my_listings))
    app.add_handler(edit_conv)
    app.add_handler(CallbackQueryHandler(browse_next_listing, pattern=r"^next_listing$"))
    app.add_handler(CallbackQueryHandler(browse_prev_listing, pattern=r"^prev_listing$"))
    app.add_handler(CallbackQueryHandler(confirm_delete_listing, pattern=r"^delete_[0-9a-fA-F-]{36}$"))
    app.add_handler(CallbackQueryHandler(handle_delete_confirmation, pattern=r"^confirm_delete_"))
    app.add_handler(browse_conv)
    app.add_handler(CallbackQueryHandler(browse_next_match, pattern=r"^browse_next$"))
    app.add_handler(CallbackQueryHandler(browse_prev_match, pattern=r"^browse_prev$"))
    app.add_handler(CallbackQueryHandler(account_my_borrowings, pattern=r"^my_borrowings$"))
    app.add_handler(CallbackQueryHandler(account_overview,      pattern=r"^account_overview$"))
    app.add_handler(CallbackQueryHandler(edit_menu_back,        pattern=r"^edit_menu_back$"))

    # Rental flow (registered before the debug catch-all so specific patterns take priority)
    app.add_handler(CallbackQueryHandler(rent_choose_year,     pattern=r"^rent_year_[0-9a-fA-F-]{36}$"))
    app.add_handler(CallbackQueryHandler(rent_choose_month,    pattern=r"^rent_month_[0-9a-fA-F-]{36}_(\d{4})$"))
    app.add_handler(CallbackQueryHandler(rent_pick_day,        pattern=r"^rent_pick_[0-9a-fA-F-]{36}_.+$"))
    app.add_handler(CallbackQueryHandler(rent_back_to_listing, pattern=r"^rent_back_to_listing(?:_.+)?$"))
    app.add_handler(CallbackQueryHandler(rent_year_back,       pattern=r"^rent_year_back$"))
    app.add_handler(CallbackQueryHandler(rent_month_back,      pattern=r"^rent_month_back_[0-9a-fA-F-]{36}_(\d{4})$"))
    app.add_handler(CallbackQueryHandler(noop,                 pattern=r"^noop$"))
    app.add_handler(CallbackQueryHandler(rent_finish_prompt,   pattern=r"^rent_finish_[0-9a-fA-F-]{36}$"))
    app.add_handler(CallbackQueryHandler(rent_confirm_yes,     pattern=r"^rent_confirm_yes_[0-9a-fA-F-]{36}$"))
    app.add_handler(CallbackQueryHandler(rent_confirm_no,      pattern=r"^rent_confirm_no_[0-9a-fA-F-]{36}$"))
    app.add_handler(CallbackQueryHandler(rent_show_days_month, pattern=r"^rent_days_[0-9a-fA-F-]{36}_(\d{4})_(1[0-2]|[1-9])$"))
    app.add_handler(CallbackQueryHandler(rent_cancel,          pattern=r"^rent_cancel$"))

    # Debug catch-all: logs any callback not matched by the specific handlers above.
    # Placed last (group=99) so it never intercepts callbacks meant for other handlers.
    app.add_handler(CallbackQueryHandler(_debug_all_callbacks, pattern=r".*"), group=99)
    app.add_handler(MessageHandler(filters.ALL, dbg_update), group=-1)

    # Insurance feedback (Yes/No in all supported languages)
    app.add_handler(MessageHandler(filters.Regex(YESNO_RE), handle_insurance_feedback))

    # === run ===
    import asyncio
    from threading import Thread
    from http.server import HTTPServer, BaseHTTPRequestHandler
    
    class HealthHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path in ['/health', '/']:
                self.send_response(200)
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write(b'OK')
            else:
                self.send_response(404)
                self.end_headers()
        
        def log_message(self, format, *args):
            pass  # Suppress HTTP server logs
    
    def run_health_server():
        server = HTTPServer((HOST, PORT), HealthHandler)
        server.serve_forever()
    
    if MODE == "webhook":
        from aiohttp import web
        from aiohttp.web import Response
        from telegram import Update
        
        print(f"Starting webhook mode on {HOST}:{PORT}")
        print(f"Webhook endpoint: {WEBHOOK_PATH}")
        print(f"Health endpoint: /health")

        async def on_startup(aiohttp_app_instance):
            # Initialize and start the PTB application inside the aiohttp event loop
            # so all async resources (HTTP client, etc.) share the same running loop.
            try:
                await app.initialize()
                await app.start()
                print("[startup] PTB application initialized and started.")
            except Exception as e:
                print(f"[startup] CRITICAL: PTB application failed to start: {e}")
                raise

        async def on_cleanup(aiohttp_app_instance):
            try:
                await app.stop()
                await app.shutdown()
                print("[cleanup] PTB application stopped.")
            except Exception as e:
                print(f"[cleanup] Error stopping PTB application: {e}")

        async def health_handler(request):
            return Response(text='OK', status=200)
        
        async def webhook_handler(request):
            try:
                # Verify secret token
                if request.headers.get('X-Telegram-Bot-Api-Secret-Token') != WEBHOOK_SECRET:
                    return Response(text='Unauthorized', status=401)
                
                # Process update
                json_data = await request.json()
                if json_data:
                    update = Update.de_json(json_data, app.bot)
                    await app.process_update(update)
                
                return Response(text='OK', status=200)
            except Exception as e:
                print(f"Webhook error: {e}")
                return Response(text='Error', status=500)
        
        # Create aiohttp app with lifecycle hooks so PTB is initialized in the
        # same event loop as the aiohttp server, preventing 'Event loop is closed' errors.
        aiohttp_app = web.Application()
        aiohttp_app.on_startup.append(on_startup)
        aiohttp_app.on_cleanup.append(on_cleanup)
        aiohttp_app.router.add_get('/health', health_handler)
        aiohttp_app.router.add_get('/', health_handler)
        aiohttp_app.router.add_post(WEBHOOK_PATH, webhook_handler)
        
        # Run the server
        web.run_app(aiohttp_app, host=HOST, port=PORT)
    else:
        # Start health server in background thread
        health_thread = Thread(target=run_health_server, daemon=True)
        health_thread.start()
        print(f"Health server started on {HOST}:{PORT}")
        
        print("Bot started (polling)...")
        app.run_polling()
