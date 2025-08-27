import logging
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, InputMediaPhoto, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters, ConversationHandler, CallbackQueryHandler
from supabase import create_client, Client
from datetime import datetime, timedelta, date
from openai import OpenAI
from geopy.distance import geodesic
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from calendar import monthrange
import json
import os
from pathlib import Path
from dotenv import load_dotenv

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

# === Initialize clients (with clear error logging) ===
try:
    client = OpenAI(api_key=OPENAI_KEY)
except Exception as e:
    raise RuntimeError(f"[startup] OpenAI client init failed: {e}")

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception as e:
    raise RuntimeError(f"[startup] Supabase client init failed: {e}")

# === Initialize Supabase ===
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# === Logging ===
logging.basicConfig(level=logging.INFO)

# === States ===
GET_CATEGORY, GET_ITEM_TITLE, GET_BRAND_MODEL, GET_SPECS, GET_TAGS, GET_DESCRIPTION, GET_CONDITION, GET_PRICE, GET_PHOTOS, GET_LOCATION, GET_AVAILABILITY, BROWSE_SEARCH = range(12)

EDIT_DESCRIPTION,EDIT_CHOICE, CONFIRM_DELETE, AWAIT_NEW_DESCRIPTION, AWAIT_NEW_PRICE, AWAIT_NEW_LOCATION, AWAIT_NEW_CATEGORY, AWAIT_NEW_CONDITION, AWAIT_NEW_ITEM_TITLE, AWAIT_NEW_BRAND_MODEL, AWAIT_NEW_SPECS, AWAIT_NEW_TAGS = range(100, 112)  # Avoid overlap with existing states

AWAIT_SEARCH_QUERY, SETTINGS_MENU, AWAIT_LOCATION_CHOICE = range(300, 303)

# === Helpers ===
geolocator = Nominatim(user_agent="taxitool_bot", timeout=5)
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1, max_retries=2)
reverse = RateLimiter(geolocator.reverse, min_delay_seconds=1, max_retries=2)

def location_name_from_coords(input_str):
    try:
        # Check if it's in coordinate format
        if any(char.isdigit() for char in input_str) and "," in input_str:
            lat, lon = map(float, input_str.split(","))
        else:
            # Try to geocode the location (forward geocode)
            location = geocode(input_str)
            if not location:
                return input_str  # Fallback to raw string
            lat, lon = location.latitude, location.longitude

        # Reverse geocode to get city and region names
        location = reverse((lat, lon), exactly_one=True)
        if location:
            addr = location.raw.get("address", {})
            city = addr.get("city") or addr.get("town") or addr.get("village") or ""
            region = addr.get("suburb") or addr.get("city_district") or addr.get("state_district") or ""
            if city:
                return f"{city}, {region}" if region else city
        return f"{lat},{lon}"  # fallback to coordinates if nothing found

    except Exception as e:
        print(f"[Geo Error] {e}")
        return input_str  # fallback to original input

def _parse_date_any(fmt_str: str) -> date | None:
    """Accepts either DD/MM/YYYY or YYYY-MM-DD; returns date or None."""
    s = fmt_str.strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    return None

def _fmt_like_source(d: datetime.date, availability_list: list[str]) -> str:
    """Return date string in the same format the listing uses (DD/MM/YYYY or ISO)."""
    uses_slash = any("/" in x for x in (availability_list or []))
    return d.strftime("%d/%m/%Y") if uses_slash else d.strftime("%Y-%m-%d")

def _collect_available_days(listing: dict) -> set[str]:
    """Available days = availability minus booked_days (normalize formats)."""
    avail_raw = listing.get("availability") or []
    booked_raw = listing.get("booked_days") or []  # text[] in DB; create this column
    avail = {_fmt_like_source(_parse_date_any(a), avail_raw) for a in avail_raw if _parse_date_any(a)}
    booked = {_fmt_like_source(_parse_date_any(b), avail_raw) for b in booked_raw if _parse_date_any(b)}
    return {d for d in avail if d not in booked}

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

# === Start Command ===
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user

    existing = supabase.table("users").select("id").eq("id", str(user.id)).execute()
    if not existing.data:
        supabase.table("users").insert({
            "id": str(user.id),
            "telegram_username": user.username,
            "display_name": user.first_name
        }).execute()

    keyboard = [["My Account", "Create a Listing"], ["Browse", "My Listings"], ["Settings"]]
    await update.message.reply_text(
        "Hi! I'm RentoTo bot — your tool-sharing assistant. You can rent tools from others or list your own.",
        reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    )

# === Back to Menu ===
async def go_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [["My Account", "Create a Listing"], ["Browse", "My Listings"], ["Settings"]]
    await update.message.reply_text("Main Menu:", reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True))
    return ConversationHandler.END

# === Settings ===
async def save_location_from_gps(update: Update, context: ContextTypes.DEFAULT_TYPE):
    loc = update.message.location
    user_id = str(update.effective_user.id)
    lat_lon = f"{loc.latitude},{loc.longitude}"
    supabase.table("users").update({"location": lat_lon}).eq("id", user_id).execute()
    await update.message.reply_text("✅ Location saved!", reply_markup=ReplyKeyboardRemove())
    return await go_back(update, context)

async def save_location_from_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_input = update.message.text
    user_id = str(update.effective_user.id)

    geolocator = Nominatim(user_agent="taxitool_bot")
    location = geolocator.geocode(user_input)

    if location:
        lat_lon = f"{location.latitude},{location.longitude}"
        supabase.table("users").update({"location": lat_lon}).eq("id", user_id).execute()
        await update.message.reply_text(f"✅ Location '{location.address}' saved!", reply_markup=ReplyKeyboardRemove())
    else:
        await update.message.reply_text("❗ Could not find that location. Please try again with a city name.")
        return AWAIT_LOCATION_CHOICE

    return await go_back(update, context)

async def prompt_location_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [KeyboardButton("Share Location with Telegram", request_location=True)],
        ["Type in Location Manually"],
        ["Back"]
    ]
    await update.message.reply_text(
        "How would you like to set your location?",
        reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    )
    return AWAIT_LOCATION_CHOICE

async def handle_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [["Change Location"], ["Back"]]
    await update.message.reply_text(
        "Settings:",
        reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    )
    return SETTINGS_MENU

CATEGORY_ALIASES = {
    "musical instruments": "Musical Instrument",
    "musical instrument": "Musical Instrument",
    "instruments": "Musical Instrument",
    "party": "Events & Party",
    "party equipment": "Events & Party",
    "home tools": "Home",
    "home appliances": "Home",
    "electronics": "Electronics",
    "recreation": "Recreation",
    "construction": "Construction",
    "tools": "Construction",
    "garden": "Gardening",
    "gardening": "Gardening"
}

# === Browse ===
def generate_embedding(text):
    response = client.embeddings.create(
        model="text-embedding-3-small",
        input=text,
    )
    return response.data[0].embedding

async def handle_browse(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Please type what you need. For example: “I want to rent a dirt bike in Zabrze”")
    return AWAIT_SEARCH_QUERY

async def handle_natural_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_query = update.message.text
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
        llm_resp = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}]
        )
        raw_output = llm_resp.choices[0].message.content.strip()
        if "```json" in raw_output:
            raw_output = raw_output.split("```json")[1].split("```")[0].strip()
        elif "```" in raw_output:
            raw_output = raw_output.split("```")[1].strip()
        data = json.loads(raw_output)
        print("LLM Parsed:", data)
    except Exception as e:
        print("LLM Output Error:", e)
        await update.message.reply_text("❗ Sorry, I couldn’t understand your request.")
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
        geolocator = Nominatim(user_agent="taxitool_bot")
        target_city = geolocator.geocode(location)
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
    query_embedding = client.embeddings.create(
        model="text-embedding-3-small",
        input=user_query
    ).data[0].embedding

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
        await update.message.reply_text("No listings found in the database.")
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
            await update.message.reply_text("😕 Sorry, nothing found — try a different keyword.")
            return ConversationHandler.END

    await send_browse_listing(update, context)
    return ConversationHandler.END

async def browse_next_match(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["browse_index"] += 1
    await send_browse_listing(update, context)

async def browse_prev_match(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["browse_index"] -= 1
    await send_browse_listing(update, context)

async def send_browse_listing(update_or_query, context):
    listings = context.user_data.get("matched_listings", [])
    index = context.user_data.get("browse_index", 0)
    if not listings:
        return

    listing = listings[index]
    photos = listing.get("photos") or []
    title = listing.get("item") or listing.get("description", "Item")
    brand_model = listing.get("brand_model")
    specs = listing.get("specs") or []
    tags = listing.get("tags") or []

    extra = ""
    if brand_model:
        extra += f"\n🏷 Brand/Model: {brand_model}"
    if specs:
        extra += f"\n⚙️ Specs: {', '.join(specs[:4])}"
    if tags:
        extra += f"\n🏷 Tags: {', '.join(tags[:5])}"

    msg = (
        f"🧰 *{listing['category']}* — *{title}*\n"
        f"📄 {listing['description']}\n"
        f"📦 Condition: {listing.get('condition', 'Not specified')}\n"
        f"💰 {listing['price_per_day']} PLN/day\n"
        f"📍 Location: {location_name_from_coords(listing['location'])}"
        f"{extra}"
    )

    # ✅ define chat first
    chat = update_or_query.effective_chat

    buttons = []
    if index > 0:
        buttons.append(InlineKeyboardButton("⬅️ Previous", callback_data="browse_prev"))
    buttons.append(InlineKeyboardButton("🗓 Rent out", callback_data=f"rent_year_{listing['id']}"))
    if index < len(listings) - 1:
        buttons.append(InlineKeyboardButton("➡️ Next", callback_data="browse_next"))
    reply_markup = InlineKeyboardMarkup([buttons])

    if update_or_query.callback_query:
        query = update_or_query.callback_query
        await query.answer()
        for msg_id in context.user_data.get("browse_media_ids", []):
            try:
                await context.bot.delete_message(chat.id, msg_id)
            except:
                pass
        try:
            await query.message.delete()
        except:
            pass
    else:
        context.user_data["browse_media_ids"] = []

    if photos:
        media_group = await context.bot.send_media_group(
            chat_id=chat.id,
            media=[InputMediaPhoto(p) for p in photos[:3]]
        )
        context.user_data["browse_media_ids"] = [m.message_id for m in media_group]

    await context.bot.send_message(
        chat_id=chat.id,
        text=msg,
        parse_mode="Markdown",
        reply_markup=reply_markup
    )

# === View My Listings ===
async def send_single_listing(update_or_query, context):
    listings = context.user_data.get("my_listings", [])
    index = context.user_data.get("listing_index", 0)

    if not listings:
        return

    listing = listings[index]
    photos = listing.get("photos") or []

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


    msg = (
        f"🧰 *{listing['category']}*\n"
        f"📄 {listing['description']}\n"
        f"📦 Condition: {listing['condition']}\n"
        f"💰 {listing['price_per_day']} PLN/day\n"
        f"📍 Location: {location_name_from_coords(listing['location'])}\n"
        f"📅 Availability:\n{availability_str}"
    )

    buttons = []
    if index > 0:
        buttons.append(InlineKeyboardButton("⬅️ Previous", callback_data="prev_listing"))
    if index < len(listings) - 1:
        buttons.append(InlineKeyboardButton("➡️ Next", callback_data="next_listing"))
    nav_row = buttons

    edit_row = [
        InlineKeyboardButton("✏️ Edit", callback_data=f"edit_{listing['id']}"),
        InlineKeyboardButton("🗑 Delete", callback_data=f"delete_{listing['id']}")
    ]

    reply_markup = InlineKeyboardMarkup([nav_row, edit_row])

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
        await chat.send_message(
            text=msg,
            parse_mode="Markdown",
            reply_markup=reply_markup
        )
        
    # Handle Message (initial case)
    elif update_or_query.message:
        if photos:
            media_messages = await update_or_query.message.reply_media_group([InputMediaPhoto(p) for p in photos[:3]])
            context.user_data["last_media_messages"] = [m.message_id for m in media_messages]
        else:
            context.user_data["last_media_messages"] = []

        await update_or_query.message.reply_text(
            msg,
            parse_mode="Markdown",
            reply_markup=reply_markup
        )

async def view_my_listings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    result = supabase.table("listings").select("*").eq("owner_id", user_id).execute()
    listings = result.data

    if not listings:
        await update.message.reply_text("You don’t have any listings yet.")
        return

    context.user_data["my_listings"] = listings
    context.user_data["listing_index"] = 0

    await send_single_listing(update, context)

async def browse_next_listing(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if "listing_index" not in context.user_data:
        context.user_data["listing_index"] = 0
    context.user_data["listing_index"] += 1

    await send_single_listing(update, context)
    
async def browse_prev_listing(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if "listing_index" not in context.user_data:
        context.user_data["listing_index"] = 0
    context.user_data["listing_index"] -= 1
    await send_single_listing(update, context)

# === Edit or Delete Listing Choice ===
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

    parts = query.data.split("_")
    if len(parts) < 2:
        await query.edit_message_text("❗ Error: Invalid listing ID.")
        return ConversationHandler.END

    listing_id = parts[1]
    context.user_data["edit_listing_id"] = listing_id

    keyboard = [
        [InlineKeyboardButton("📂 Category", callback_data="edit_field_category")],
        [InlineKeyboardButton("📝 Item title", callback_data="edit_field_item")],
        [InlineKeyboardButton("🏷 Brand/Model", callback_data="edit_field_brand_model")],
        [InlineKeyboardButton("⚙️ Specs", callback_data="edit_field_specs")],
        [InlineKeyboardButton("🔖 Tags", callback_data="edit_field_tags")],
        [InlineKeyboardButton("✏️ Description", callback_data="edit_field_description")],
        [InlineKeyboardButton("🛠 Condition", callback_data="edit_field_condition")],
        [InlineKeyboardButton("💰 Price", callback_data="edit_field_price")],
        [InlineKeyboardButton("📍 Location", callback_data="edit_field_location")],
        [InlineKeyboardButton("🔙 Back", callback_data="cancel_edit")]
    ]

    await query.message.reply_text(
        text="What would you like to edit?",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    return EDIT_CHOICE

async def handle_edit_description_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    listing_id = context.user_data.get("edit_listing_id")
    if not listing_id:
        await query.edit_message_text("❗ Error: Listing not found.")
        return ConversationHandler.END

    await query.edit_message_text("Please type in chat to what we will change it:")
    return AWAIT_NEW_DESCRIPTION

async def handle_edit_field_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    listing_id = context.user_data.get("edit_listing_id")
    if not listing_id:
        await query.edit_message_text("Listing ID missing. Please try again.")
        return ConversationHandler.END

    if query.data == "edit_field_description":
        context.user_data["edit_field"] = "description"
        prompt = "Please type in chat to what we will change the *description*:"
        next_state = AWAIT_NEW_DESCRIPTION
    elif query.data == "edit_field_price":
        context.user_data["edit_field"] = "price"
        prompt = "Please type in chat to what we will change the *price*:"
        next_state = AWAIT_NEW_PRICE
    elif query.data == "edit_field_location":
        context.user_data["edit_field"] = "location"
        prompt = "Please type in chat to what we will change the *location*:"
        next_state = AWAIT_NEW_LOCATION
    elif query.data == "edit_field_category":
        context.user_data["edit_field"] = "category"
        keyboard = [
            ["Construction", "Home utilities"],
            ["Electronics", "Recreation"],
            ["Cancel"]
        ]
        await query.message.reply_text(
            "Please select your category or type it in chat:",
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        )
        return AWAIT_NEW_CATEGORY
    elif query.data == "edit_field_condition":
        context.user_data["edit_field"] = "condition"
        prompt = "Please describe the item's current condition or any damages:"
        next_state = AWAIT_NEW_CONDITION
    elif query.data == "edit_field_item":
        context.user_data["edit_field"] = "item"
        prompt = "Please send the new *item title* (e.g., Electric guitar):"
        next_state = AWAIT_NEW_ITEM_TITLE
    elif query.data == "edit_field_brand_model":
        context.user_data["edit_field"] = "brand_model"
        prompt = "Please send the new *brand/model* (e.g., Ibanez RG370), or type 'Skip' to clear:"
        next_state = AWAIT_NEW_BRAND_MODEL
    elif query.data == "edit_field_specs":
        context.user_data["edit_field"] = "specs"
        prompt = "Send 1–6 *specs* comma-separated (e.g., 24 frets, tremolo, HSH):"
        next_state = AWAIT_NEW_SPECS
    elif query.data == "edit_field_tags":
        context.user_data["edit_field"] = "tags"
        prompt = "Send 2–8 *tags* comma-separated (e.g., music, rehearsal, rock):"
        next_state = AWAIT_NEW_TAGS

    else:
        await query.edit_message_text("Unknown edit field.")
        return ConversationHandler.END
    
    cancel_button = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_edit")]])

    # send a completely new message
    await query.message.reply_text(prompt, reply_markup=cancel_button, parse_mode="Markdown")
    return next_state

async def receive_new_item_title(update: Update, context: ContextTypes.DEFAULT_TYPE):
    new_item = update.message.text.strip()
    listing_id = context.user_data.get("edit_listing_id")
    if not listing_id:
        await update.message.reply_text("Something went wrong.")
        return ConversationHandler.END

    listing = supabase.table("listings").select(
        "item,brand_model,specs,tags,location,description,category"
    ).eq("id", listing_id).execute().data[0]

    embedding_input = build_embedding_input_from_row(listing, overrides={"item": new_item})
    embedding = generate_embedding(embedding_input)
    logging.info(f"[Embeddings] Item edit → input='{embedding_input[:120]}...'")

    supabase.table("listings").update({
        "item": new_item,
        "embedding": embedding
    }).eq("id", listing_id).execute()

    await update.message.reply_text("✅ Item title updated and embedding refreshed.")
    return ConversationHandler.END

async def receive_new_brand_model(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    new_brand_model = "" if text.lower() == "skip" else text
    listing_id = context.user_data.get("edit_listing_id")
    if not listing_id:
        await update.message.reply_text("Something went wrong.")
        return ConversationHandler.END

    listing = supabase.table("listings").select(
        "item,brand_model,specs,tags,location,description,category"
    ).eq("id", listing_id).execute().data[0]

    embedding_input = build_embedding_input_from_row(listing, overrides={"brand_model": new_brand_model})
    embedding = generate_embedding(embedding_input)
    logging.info(f"[Embeddings] Brand/Model edit → input='{embedding_input[:120]}...'")

    supabase.table("listings").update({
        "brand_model": new_brand_model if new_brand_model else None,
        "embedding": embedding
    }).eq("id", listing_id).execute()

    await update.message.reply_text("✅ Brand/Model updated and embedding refreshed.")
    return ConversationHandler.END

async def receive_new_specs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw = update.message.text.strip()
    new_specs = [s.strip() for s in raw.split(",") if s.strip()]
    listing_id = context.user_data.get("edit_listing_id")
    if not listing_id:
        await update.message.reply_text("Something went wrong.")
        return ConversationHandler.END

    listing = supabase.table("listings").select(
        "item,brand_model,specs,tags,location,description,category"
    ).eq("id", listing_id).execute().data[0]

    embedding_input = build_embedding_input_from_row(listing, overrides={"specs": new_specs})
    embedding = generate_embedding(embedding_input)
    logging.info(f"[Embeddings] Specs edit → input='{embedding_input[:120]}...'")

    supabase.table("listings").update({
        "specs": new_specs,
        "embedding": embedding
    }).eq("id", listing_id).execute()

    await update.message.reply_text("✅ Specs updated and embedding refreshed.")
    return ConversationHandler.END

async def receive_new_tags(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw = update.message.text.strip()
    new_tags = [t.strip() for t in raw.split(",") if t.strip()]
    listing_id = context.user_data.get("edit_listing_id")
    if not listing_id:
        await update.message.reply_text("Something went wrong.")
        return ConversationHandler.END

    listing = supabase.table("listings").select(
        "item,brand_model,specs,tags,location,description,category"
    ).eq("id", listing_id).execute().data[0]

    embedding_input = build_embedding_input_from_row(listing, overrides={"tags": new_tags})
    embedding = generate_embedding(embedding_input)
    logging.info(f"[Embeddings] Tags edit → input='{embedding_input[:120]}...'")

    supabase.table("listings").update({
        "tags": new_tags,
        "embedding": embedding
    }).eq("id", listing_id).execute()

    await update.message.reply_text("✅ Tags updated and embedding refreshed.")
    return ConversationHandler.END

async def receive_new_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    new_category = update.message.text
    if new_category == "Cancel":
        return await go_back(update, context)

    listing_id = context.user_data.get("edit_listing_id")
    if not listing_id:
        await update.message.reply_text("Something went wrong.")
        return ConversationHandler.END

    # Get current row (only the columns used by the embedding builder)
    listing = supabase.table("listings").select(
        "item,brand_model,specs,tags,location,description,category"
    ).eq("id", listing_id).execute().data[0]

    # Build embedding input with the new category
    embedding_input = build_embedding_input_from_row(listing, overrides={"category": new_category})
    embedding = generate_embedding(embedding_input)
    logging.info(f"[Embeddings] Category edit → input='{embedding_input[:120]}...'")

    supabase.table("listings").update({
        "category": new_category,
        "embedding": embedding
    }).eq("id", listing_id).execute()

    await update.message.reply_text(
        f"✅ Category updated to '{new_category}' and embedding refreshed.",
        reply_markup=ReplyKeyboardMarkup(
            [["My Account", "Create a Listing"], ["Browse", "My Listings"], ["Settings"]],
            resize_keyboard=True
        )
    )
    return ConversationHandler.END

async def receive_new_description(update: Update, context: ContextTypes.DEFAULT_TYPE):
    new_desc = update.message.text
    listing_id = context.user_data.get("edit_listing_id")
    if not listing_id:
        await update.message.reply_text("Something went wrong.")
        return ConversationHandler.END

    listing = supabase.table("listings").select(
        "item,brand_model,specs,tags,location,description,category"
    ).eq("id", listing_id).execute().data[0]

    embedding_input = build_embedding_input_from_row(listing, overrides={"description": new_desc})
    embedding = generate_embedding(embedding_input)
    logging.info(f"[Embeddings] Description edit → input='{embedding_input[:120]}...'")

    supabase.table("listings").update({
        "description": new_desc,
        "embedding": embedding
    }).eq("id", listing_id).execute()

    await update.message.reply_text(
        "✅ Description updated and embedding refreshed.",
        reply_markup=ReplyKeyboardMarkup(
            [["My Account", "Create a Listing"], ["Browse", "My Listings"], ["Settings"]],
            resize_keyboard=True
        )
    )
    return ConversationHandler.END

async def receive_new_condition(update: Update, context: ContextTypes.DEFAULT_TYPE):
    new_condition = update.message.text
    listing_id = context.user_data.get("edit_listing_id")

    if not listing_id:
        await update.message.reply_text("Something went wrong.")
        return ConversationHandler.END

    supabase.table("listings").update({"condition": new_condition}).eq("id", listing_id).execute()
    keyboard = [["My Account", "Create a Listing"], ["Browse", "My Listings"], ["Settings"]]
    await update.message.reply_text("✅ Condition updated.", reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True))

    return ConversationHandler.END

async def receive_new_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    listing_id = context.user_data.get("edit_listing_id")
    try:
        new_price = float(update.message.text)
    except ValueError:
        await update.message.reply_text("❗ Please enter a valid number.")
        return AWAIT_NEW_PRICE

    supabase.table("listings").update({"price_per_day": new_price}).eq("id", listing_id).execute()
    keyboard = [["My Account", "Create a Listing"], ["Browse", "My Listings"], ["Settings"]]
    await update.message.reply_text("✅ Price updated.", reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True))

    return ConversationHandler.END

async def receive_new_location(update: Update, context: ContextTypes.DEFAULT_TYPE):
    new_location = update.message.text
    listing_id = context.user_data.get("edit_listing_id")
    if not listing_id:
        await update.message.reply_text("Something went wrong.")
        return ConversationHandler.END

    listing = supabase.table("listings").select(
        "item,brand_model,specs,tags,location,description,category"
    ).eq("id", listing_id).execute().data[0]

    embedding_input = build_embedding_input_from_row(listing, overrides={"location": new_location})
    embedding = generate_embedding(embedding_input)
    logging.info(f"[Embeddings] Location edit → input='{embedding_input[:120]}...'")

    supabase.table("listings").update({
        "location": new_location,
        "embedding": embedding
    }).eq("id", listing_id).execute()

    await update.message.reply_text(
        "✅ Location updated and embedding refreshed.",
        reply_markup=ReplyKeyboardMarkup(
            [["My Account", "Create a Listing"], ["Browse", "My Listings"], ["Settings"]],
            resize_keyboard=True
        )
    )
    return ConversationHandler.END

async def update_listing_description(update: Update, context: ContextTypes.DEFAULT_TYPE):
    listing_id = context.user_data.get("edit_listing_id")
    new_text = update.message.text
    supabase.table("listings").update({"description": new_text}).eq("id", listing_id).execute()
    keyboard = [["My Account", "Create a Listing"], ["Browse", "My Listings"], ["Settings"]]
    await update.message.reply_text("✅ Description updated.", reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True))

    return ConversationHandler.END

async def cancel_editing(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    return await start_editing_listing(update, context)

async def confirm_delete_listing(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    listing_id = query.data.split("_")[1]
    context.user_data["delete_listing_id"] = listing_id

    chat = query.message.chat

    # 🧹 Delete old media messages
    for msg_id in context.user_data.get("last_media_messages", []):
        try:
            await context.bot.delete_message(chat_id=chat.id, message_id=msg_id)
        except Exception as e:
            print(f"Failed to delete media message {msg_id}: {e}")
    context.user_data["last_media_messages"] = []

    # 🧹 Delete the listing text block
    try:
        await query.message.delete()
    except Exception as e:
        print(f"Failed to delete main listing message: {e}")

    # ✅ Show confirmation message
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Yes, delete", callback_data="confirm_delete_yes")],
        [InlineKeyboardButton("❌ No, cancel", callback_data="confirm_delete_no")]
    ])

    await chat.send_message(
        "Are you sure you want to delete this listing?",
        reply_markup=keyboard
    )

    return CONFIRM_DELETE

async def handle_delete_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    decision = query.data

    if decision == "confirm_delete_yes":
        listing_id = context.user_data.get("delete_listing_id")

        if listing_id:
            supabase.table("listings").delete().eq("id", listing_id).execute()

        # Reload listings
        user_id = str(update.effective_user.id)
        result = supabase.table("listings").select("*").eq("owner_id", user_id).execute()
        listings = result.data

        if not listings:
            await query.message.reply_text("✅ Listing deleted. You don’t have any other listings.")
            return ConversationHandler.END

        context.user_data["my_listings"] = listings
        context.user_data["listing_index"] = 0
        await query.message.reply_text("✅ Listing deleted.")
    elif decision == "confirm_delete_no":
        await query.message.reply_text("Deletion cancelled.")
        return ConversationHandler.END

# === Create Listing Flow ===
async def start_listing(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        ["Recreation", "Electronics"],
        ["Construction", "Home Improvement"],
        ["Events & Party", "Gardening"],
        ["Back"]
    ]
    await update.message.reply_text(
        "Please select a category from the buttons below or type it in chat:",
        reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    )
    return GET_CATEGORY

async def get_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text == "Back": return await go_back(update, context)
    context.user_data['category'] = update.message.text
    await update.message.reply_text("What's the name of the item? (e.g. 'Electric guitar', 'VR headset')", reply_markup=ReplyKeyboardRemove())
    return GET_ITEM_TITLE

async def get_item_title(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text == "Back":
        return await go_back(update, context)
    context.user_data['item_title'] = update.message.text
    await update.message.reply_text("Enter brand and/or model (or type 'Skip'):")
    return GET_BRAND_MODEL

async def get_brand_model(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text == "Back":
        return await go_back(update, context)
    brand_model = update.message.text.strip()
    if brand_model.lower() != "skip":
        context.user_data['brand_model'] = brand_model
    await update.message.reply_text("List 1–4 key specs (comma-separated):")
    return GET_SPECS

async def get_specs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text == "Back":
        return await go_back(update, context)
    specs = [s.strip() for s in update.message.text.split(",") if s.strip()]
    context.user_data['specs'] = specs
    await update.message.reply_text("Add 2–5 tags (comma-separated, like 'music, VR, guitar'):")
    return GET_TAGS

async def get_tags(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text == "Back":
        return await go_back(update, context)
    tags = [t.strip() for t in update.message.text.split(",") if t.strip()]
    context.user_data['tags'] = tags
    await update.message.reply_text("✅ Got it! Now enter a short description:", reply_markup=ReplyKeyboardRemove())
    return GET_DESCRIPTION

async def get_description(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['description'] = update.message.text  # ← You were missing this line
    await update.message.reply_text(
        "Please describe any visible damage or wear. If it's in perfect condition, just say so.",
        reply_markup=ReplyKeyboardRemove()
    )
    return GET_CONDITION

async def get_condition(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['condition'] = update.message.text
    await update.message.reply_text("Enter the price per day (in PLN):", reply_markup=ReplyKeyboardRemove())
    return GET_PRICE

async def get_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['price_per_day'] = float(update.message.text)
    await update.message.reply_text("Send up to 3 photos of your item:")
    context.user_data['photos'] = []
    return GET_PHOTOS

async def get_photos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if 'photos' not in context.user_data:
        context.user_data['photos'] = []

    if update.message.text:
        text = update.message.text.strip().lower()
        if text.startswith("delete"):
            try:
                index = int(text.split()[-1]) - 1
                if 0 <= index < len(context.user_data['photos']):
                    context.user_data['photos'].pop(index)
                    await update.message.reply_text(f"Deleted photo {index + 1}.")
                else:
                    await update.message.reply_text("Invalid photo number to delete.")
            except:
                await update.message.reply_text("Please type Delete followed by a number (e.g., Delete 1).")
        elif text == "cancel listing":
            context.user_data.clear()
            return await go_back(update, context)
        elif text == "continue":
            await update.message.reply_text("Now share your location or type it manually:", reply_markup=ReplyKeyboardMarkup([[KeyboardButton("Send Location", request_location=True), "Cancel Listing"]], resize_keyboard=True))
            return GET_LOCATION
        elif text != "add more":
            await update.message.reply_text("❗ Please send a photo or type 'Delete X', 'Add More', 'Continue', or 'Cancel Listing'.")
            return GET_PHOTOS

    elif update.message.photo:
        file_id = update.message.photo[-1].file_id
        context.user_data['photos'].append(file_id)
    else:
        await update.message.reply_text("❗ Please send a photo or use text commands like 'Delete X', 'Add More', 'Continue', or 'Cancel Listing'.")
        return GET_PHOTOS

    keyboard = ReplyKeyboardMarkup([
        ["Add More", "Continue"],
        ["Delete 1", "Delete 2", "Delete 3"],
        ["Cancel Listing"]
    ], resize_keyboard=True)
    await update.message.reply_text("Thanks! Photos received.", reply_markup=keyboard)
    return GET_PHOTOS

async def get_location(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.location:
        lat, lon = update.message.location.latitude, update.message.location.longitude
        context.user_data['location'] = f"{lat},{lon}"
    else:
        city_name = update.message.text
        from geopy.geocoders import Nominatim
        geolocator = Nominatim(user_agent="taxitool_bot")
        location = geolocator.geocode(city_name)
        if location:
            context.user_data['location'] = f"{location.latitude},{location.longitude}"
        else:
            await update.message.reply_text("❗ Could not recognize that location. Please try again with a valid city.")
            return GET_LOCATION

    await update.message.reply_text("Please type your availability in this format:\nDD/MM/YYYY - DD/MM/YYYY, DD/MM/YYYY - DD/MM/YYYY")
    return GET_AVAILABILITY

async def get_availability(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw_text = update.message.text
    dates = raw_text.split(",")
    availability = []

    # Parse & validate dates safely
    for period in dates:
        try:
            start_str, end_str = period.strip().split("-")
            start_date = datetime.strptime(start_str.strip(), "%d/%m/%Y")
            end_date = datetime.strptime(end_str.strip(), "%d/%m/%Y")
            if end_date < start_date:
                raise ValueError("End date is before start date")
            current = start_date
            while current <= end_date:
                availability.append(current.strftime("%Y-%m-%d"))
                current += timedelta(days=1)
        except Exception:
            await update.message.reply_text(
                f"❗ Error parsing: '{period.strip()}'. Please use DD/MM/YYYY - DD/MM/YYYY (e.g., 05/09/2025 - 12/09/2025). Try again:"
            )
            return GET_AVAILABILITY

    if not availability:
        await update.message.reply_text("❗ No valid dates found. Please enter at least one valid range.")
        return GET_AVAILABILITY

    context.user_data["availability"] = availability
    user_id = str(update.effective_user.id)

    # Build a row-like dict and embed ONCE using the unified helper
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
    embedding = generate_embedding(embedding_input)

    # Insert
    supabase.table("listings").insert({
        "owner_id": user_id,
        "category": context.user_data['category'],
        "description": context.user_data['description'],
        "item": context.user_data.get("item_title"),
        "brand_model": context.user_data.get("brand_model"),
        "specs": context.user_data.get("specs"),
        "tags": context.user_data.get("tags"),
        "condition": context.user_data['condition'],
        "price_per_day": context.user_data['price_per_day'],
        "photos": context.user_data['photos'],
        "location": context.user_data['location'],
        "availability": availability,
        "embedding": embedding
    }).execute()

    keyboard = [["Yes", "No"]]
    await update.message.reply_text(
        "Would you feel safer with insurance for unexpected damage or theft?",
        reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    )
    return ConversationHandler.END

async def handle_insurance_feedback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text.lower() == "yes":
        await update.message.reply_text("Thanks! We're working hard to implement this feature soon.")
    else:
        await update.message.reply_text("Thanks for your feedback!")
    return await go_back(update, context)

# ===== Rental flow (Year -> Month -> Range -> Day) =====
async def rent_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    # Clear any in‑progress selection for this listing if known
    listing_id = context.user_data.get("rent_listing_id")
    if listing_id:
        _clear_selected_days(context, listing_id)
    # Go back to the current card
    await rent_back_to_listing(update, context)

async def rent_choose_year(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Entry: from 'rent_year_<listing_id>'"""
    q = update.callback_query
    await q.answer()
    _, _, listing_id = q.data.partition("rent_year_")

    # Persist listing_id during this rent flow
    context.user_data["rent_listing_id"] = listing_id

    kb = [
        [InlineKeyboardButton("2025", callback_data=f"rent_month_{listing_id}_2025"),
        InlineKeyboardButton("2026", callback_data=f"rent_month_{listing_id}_2026")],
        [InlineKeyboardButton("❌ Cancel Reservation", callback_data="rent_cancel")]
    ]
    await q.message.edit_text("Choose a year:", reply_markup=InlineKeyboardMarkup(kb))

async def rent_choose_month(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """From 'rent_month_<listing_id>_<year>'"""
    q = update.callback_query
    await q.answer()
    parts = q.data.split("_")  # ["rent","month", listing_id, year]
    listing_id, year = parts[2], int(parts[3])
    context.user_data["rent_listing_id"] = listing_id
    context.user_data["rent_year"] = year

    rows = []
    row = []
    for m in range(1, 13):
        row.append(InlineKeyboardButton(_month_name(m), callback_data=f"rent_range_{listing_id}_{year}_{m}"))
        if len(row) == 3:
            rows.append(row); row = []
    if row: rows.append(row)
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="rent_year_back"),
                InlineKeyboardButton("❌ Cancel Reservation", callback_data="rent_cancel")])
    await q.message.edit_text(f"Year: {year}\nChoose a month:", reply_markup=InlineKeyboardMarkup(rows))

async def rent_choose_range(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    _, _, listing_id, year, month = q.data.split("_")
    year, month = int(year), int(month)
    context.user_data.update({"rent_listing_id": listing_id, "rent_year": year, "rent_month": month})

    ranges = _split_week_ranges(year, month)
    kb = []
    kb.append([
        InlineKeyboardButton(f"{ranges[0][0]}-{ranges[0][1]}", callback_data=f"rent_day_{listing_id}_{year}_{month}_{ranges[0][0]}_{ranges[0][1]}"),
        InlineKeyboardButton(f"{ranges[1][0]}-{ranges[1][1]}", callback_data=f"rent_day_{listing_id}_{year}_{month}_{ranges[1][0]}_{ranges[1][1]}")
    ])
    kb.append([
        InlineKeyboardButton(f"{ranges[2][0]}-{ranges[2][1]}", callback_data=f"rent_day_{listing_id}_{year}_{month}_{ranges[2][0]}_{ranges[2][1]}"),
        InlineKeyboardButton(f"{ranges[3][0]}-{ranges[3][1]}", callback_data=f"rent_day_{listing_id}_{year}_{month}_{ranges[3][0]}_{ranges[3][1]}")
    ])
    kb.append([
        InlineKeyboardButton("⬅️ Back", callback_data=f"rent_month_back_{listing_id}_{year}"),
        InlineKeyboardButton("❌ Cancel Reservation", callback_data="rent_cancel")
    ])
    await q.message.edit_text(f"{_month_name(month)} {year}\nChoose a range:", reply_markup=InlineKeyboardMarkup(kb))

async def rent_show_days(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """From 'rent_day_<listing_id>_<year>_<month>_<a>_<b>' -> show only available days as buttons"""
    q = update.callback_query
    await q.answer()
    _, _, listing_id, year, month, a, b = q.data.split("_")
    year, month, a, b = int(year), int(month), int(a), int(b)

    # Fetch fresh listing for availability & bookings
    listing = supabase.table("listings").select("*").eq("id", listing_id).execute().data[0]
    available = _collect_available_days(listing)

    # Build day buttons only for those in 'available' for that month & range
    day_buttons = []
    row = []
    for day in range(a, b+1):
        # Guard against non-existent days (e.g., 31 in a 30-day month)
        if day > monthrange(year, month)[1]:
            continue
        d = datetime(year, month, day).date()
        token = _fmt_like_source(d, listing.get("availability") or [])
        if token in available:
            row.append(InlineKeyboardButton(str(day), callback_data=f"rent_pick_{listing_id}_{token}"))
            if len(row) == 7:
                day_buttons.append(row); row = []
    if row: day_buttons.append(row)

    # Fallback if nothing free in that range
    if not day_buttons:
        day_buttons = [[InlineKeyboardButton("No free days here", callback_data="noop")]]

    day_buttons.append([
        InlineKeyboardButton("⬅️ Back", callback_data=f"rent_range_back_{listing_id}_{year}_{month}"),
        InlineKeyboardButton("❌ Cancel Reservation", callback_data="rent_cancel")
    ])
    await q.message.edit_text(
        f"{_month_name(month)} {year}\nPick a day:",
        reply_markup=InlineKeyboardMarkup(day_buttons)
    )

async def rent_pick_day(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    # rent_pick_<listing_id>_<dateStrInSourceFormat>
    _, _, listing_id, day_token = q.data.split("_", 3)

    # Load listing and check that day is still free
    listing = supabase.table("listings").select("*").eq("id", listing_id).execute().data[0]
    available = _collect_available_days(listing)

    if day_token not in available:
        await q.message.edit_text("😕 Sorry, this day was just taken. Please pick another one.",
                                  reply_markup=InlineKeyboardMarkup(
                                      [[InlineKeyboardButton("Back to months", callback_data=f"rent_month_{listing_id}_2025"),
                                        InlineKeyboardButton("❌ Cancel Reservation", callback_data="rent_cancel")]]
                                  ))
        return

    sel = _add_selected_day(context, listing_id, day_token)

    # Build a short summary of selected days
    preview = ", ".join(sel[:6]) + (" …" if len(sel) > 6 else "")
    txt = f"✅ Added **{day_token}**.\n\nSelected so far: {preview or '—'}\n\nSelect more days?"

    kb = [
        [InlineKeyboardButton("➕ Select more days", callback_data=f"rent_month_{listing_id}_{context.user_data.get('rent_year', '2025')}")],
        [InlineKeyboardButton("✅ Finish selecting", callback_data=f"rent_finish_{listing_id}")],
        [InlineKeyboardButton("❌ Cancel Reservation", callback_data="rent_cancel")]
    ]
    await q.message.edit_text(txt, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))

async def rent_back_to_listing(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Re-render the current listing card
    q = update.callback_query
    await q.answer()
    await send_browse_listing(update, context)

async def rent_year_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    listing_id = context.user_data.get("rent_listing_id")
    if not listing_id:
        return await rent_back_to_listing(update, context)
    kb = [
        [InlineKeyboardButton("2025", callback_data=f"rent_month_{listing_id}_2025"),
         InlineKeyboardButton("2026", callback_data=f"rent_month_{listing_id}_2026")],
        [InlineKeyboardButton("❌ Cancel Reservation", callback_data="rent_cancel")]
    ]
    await q.message.edit_text("Choose a year:", reply_markup=InlineKeyboardMarkup(kb))

async def rent_month_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    # rent_month_back_<listing_id>_<year>
    _, _, listing_id, year = q.data.split("_")
    kb = [
        [InlineKeyboardButton("2025", callback_data=f"rent_month_{listing_id}_2025"),
         InlineKeyboardButton("2026", callback_data=f"rent_month_{listing_id}_2026")],
        [InlineKeyboardButton("⬅️ Back", callback_data="rent_back_to_listing")]
    ]
    await q.message.edit_text("Choose a year:", reply_markup=InlineKeyboardMarkup(kb))

async def rent_range_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    _, _, listing_id, year, month = q.data.split("_")
    year = int(year)
    # Reuse the month grid rendering (inline here for safety)
    rows, row = [], []
    for m in range(1, 13):
        row.append(InlineKeyboardButton(_month_name(m), callback_data=f"rent_range_{listing_id}_{year}_{m}"))
        if len(row) == 3:
            rows.append(row); row = []
    if row: rows.append(row)
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="rent_year_back"),
                 InlineKeyboardButton("❌ Cancel Reservation", callback_data="rent_cancel")])
    await q.message.edit_text(f"Year: {year}\nChoose a month:", reply_markup=InlineKeyboardMarkup(rows))
async def noop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()

async def rent_finish_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    # rent_finish_<listing_id>
    _, _, listing_id = q.data.split("_", 2)

    sel = _get_selected_days(context, listing_id)
    if not sel:
        await q.message.edit_text("You haven’t selected any days yet.",
                                  reply_markup=InlineKeyboardMarkup(
                                      [[InlineKeyboardButton("Back to months", callback_data=f"rent_month_{listing_id}_{context.user_data.get('rent_year','2025')}")],
                                       [InlineKeyboardButton("❌ Cancel Reservation", callback_data="rent_cancel")]]
                                  ))
        return

    # Sort for nicer view (try to parse to date; fallback to string)
    def _key(s): 
        d = _parse_date_any(s)
        return (d or datetime.max.date(), s)
    sel_sorted = sorted(sel, key=_key)

    txt = "🗓 **Review selection**\n\n" + "\n".join(f"• {d}" for d in sel_sorted) + "\n\nAre you sure?"
    kb = [
        [InlineKeyboardButton("✅ Yes, book them", callback_data=f"rent_confirm_yes_{listing_id}")],
        [InlineKeyboardButton("🙅 No, keep selecting", callback_data=f"rent_confirm_no_{listing_id}")],
        [InlineKeyboardButton("❌ Cancel Reservation", callback_data="rent_cancel")]
    ]
    await q.message.edit_text(txt, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))

async def rent_confirm_yes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    # rent_confirm_yes_<listing_id>
    _, _, _, listing_id = q.data.split("_", 3)

    sel = _get_selected_days(context, listing_id)
    if not sel:
        await q.message.edit_text("There are no selected days to book.", reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("Back", callback_data=f"rent_month_{listing_id}_{context.user_data.get('rent_year','2025')}")],
             [InlineKeyboardButton("❌ Cancel Reservation", callback_data="rent_cancel")]]
        ))
        return

    # Re‑fetch listing to avoid race conditions
    listing = supabase.table("listings").select("availability, booked_days").eq("id", listing_id).execute().data[0]
    available = _collect_available_days(listing)
    booked_now = set(listing.get("booked_days") or [])

    # Only keep still‑free days
    commit = [d for d in sel if d in available and d not in booked_now]
    skipped = [d for d in sel if d not in commit]

    if not commit:
        await q.message.edit_text("😕 None of your selected days are still free. Please pick other dates.",
                                  reply_markup=InlineKeyboardMarkup(
                                      [[InlineKeyboardButton("Back to months", callback_data=f"rent_month_{listing_id}_{context.user_data.get('rent_year','2025')}")],
                                       [InlineKeyboardButton("❌ Cancel Reservation", callback_data="rent_cancel")]]
                                  ))
        _clear_selected_days(context, listing_id)
        return

    # Save
    new_booked = list(booked_now.union(commit))
    supabase.table("listings").update({"booked_days": new_booked}).eq("id", listing_id).execute()

    _clear_selected_days(context, listing_id)

    msg = "✅ Booked:\n" + "\n".join(f"• {d}" for d in commit)
    if skipped:
        msg += "\n\n(These were already unavailable and were skipped):\n" + "\n".join(f"• {d}" for d in skipped)

    kb = [[InlineKeyboardButton("⬅️ Back to listing", callback_data="rent_back_to_listing")]]
    await q.message.edit_text(msg, reply_markup=InlineKeyboardMarkup(kb))

async def rent_confirm_no(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    # rent_confirm_no_<listing_id>
    _, _, _, listing_id = q.data.split("_", 3)
    # Go back to month grid with selection preserved
    year = context.user_data.get("rent_year", "2025")
    await q.message.edit_text("Okay, keep selecting.",
                              reply_markup=InlineKeyboardMarkup(
                                  [[InlineKeyboardButton("Back to months", callback_data=f"rent_month_{listing_id}_{year}")],
                                   [InlineKeyboardButton("❌ Cancel Reservation", callback_data="rent_cancel")]]
                              ))

# === Conversation Handlers ===
listing_conv = ConversationHandler(
    entry_points=[MessageHandler(filters.Regex("^Create a Listing$"), start_listing)],
    states={
        GET_CATEGORY: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_category)],
        GET_ITEM_TITLE: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_item_title)],
        GET_BRAND_MODEL: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_brand_model)],
        GET_SPECS: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_specs)],
        GET_TAGS: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_tags)],
        GET_DESCRIPTION: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_description)],
        GET_CONDITION: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_condition)],
        GET_PRICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_price)],
        GET_PHOTOS: [MessageHandler(filters.PHOTO | filters.TEXT, get_photos)],
        GET_LOCATION: [MessageHandler(filters.LOCATION | filters.TEXT, get_location)],
        GET_AVAILABILITY: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_availability)]
    },
    fallbacks=[MessageHandler(filters.Regex("^Back$"), go_back)]
)

edit_conv = ConversationHandler(
    entry_points=[CallbackQueryHandler(start_editing_listing, pattern=r"^edit_[0-9a-fA-F-]{36}$")],
    states={
        EDIT_CHOICE: [CallbackQueryHandler(handle_edit_field_choice)],
        AWAIT_NEW_DESCRIPTION: [MessageHandler(filters.TEXT, receive_new_description)],
        AWAIT_NEW_PRICE: [MessageHandler(filters.TEXT, receive_new_price)],
        AWAIT_NEW_LOCATION: [MessageHandler(filters.TEXT, receive_new_location)],
        AWAIT_NEW_CATEGORY: [MessageHandler(filters.TEXT, receive_new_category)],
        AWAIT_NEW_CONDITION: [MessageHandler(filters.TEXT, receive_new_condition)],
        AWAIT_NEW_ITEM_TITLE: [MessageHandler(filters.TEXT, receive_new_item_title)],
        AWAIT_NEW_BRAND_MODEL: [MessageHandler(filters.TEXT, receive_new_brand_model)],
        AWAIT_NEW_SPECS: [MessageHandler(filters.TEXT, receive_new_specs)],
        AWAIT_NEW_TAGS: [MessageHandler(filters.TEXT, receive_new_tags)],
        CONFIRM_DELETE: [CallbackQueryHandler(handle_delete_confirmation)]
    },
    fallbacks=[CallbackQueryHandler(cancel_editing, pattern="^cancel_edit$")]
)

browse_conv = ConversationHandler(
    entry_points=[MessageHandler(filters.Regex("^Browse$"), handle_browse)],
    states={
        AWAIT_SEARCH_QUERY: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_natural_search)]
    },
    fallbacks=[MessageHandler(filters.Regex("^Back$"), go_back)]
)

settings_conv = ConversationHandler(
    entry_points=[MessageHandler(filters.Regex("^Settings$"), handle_settings)],
    states={
        SETTINGS_MENU: [MessageHandler(filters.Regex("^Change Location$"), prompt_location_choice)],
        AWAIT_LOCATION_CHOICE: [
            MessageHandler(filters.LOCATION, save_location_from_gps),
            MessageHandler(filters.Regex("^Type in Location Manually$"), lambda u, c: AWAIT_LOCATION_CHOICE),
            MessageHandler(filters.TEXT, save_location_from_text)
        ]
    },
    fallbacks=[MessageHandler(filters.Regex("^Back$"), go_back)]
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

    # === register ALL handlers here (your existing ones) ===
    app.add_handler(CommandHandler("start", start))

    # Conversations/handlers you already set up:
    app.add_handler(listing_conv)
    app.add_handler(MessageHandler(filters.Regex("^Back$"), go_back))
    app.add_handler(MessageHandler(filters.Regex("^My Listings$"), view_my_listings))
    app.add_handler(edit_conv)
    app.add_handler(CallbackQueryHandler(browse_next_listing, pattern="^next_listing$"))
    app.add_handler(CallbackQueryHandler(browse_prev_listing, pattern="^prev_listing$"))
    app.add_handler(CallbackQueryHandler(confirm_delete_listing, pattern=r"^delete_[0-9a-fA-F-]{36}$"))
    app.add_handler(CallbackQueryHandler(handle_delete_confirmation, pattern="^confirm_delete_"))
    app.add_handler(browse_conv)
    app.add_handler(CallbackQueryHandler(browse_next_match, pattern="^browse_next$"))
    app.add_handler(CallbackQueryHandler(browse_prev_match, pattern="^browse_prev$"))

    # Rental flow
    app.add_handler(CallbackQueryHandler(rent_choose_year,   pattern=r"^rent_year_[0-9a-fA-F-]{36}$"))
    app.add_handler(CallbackQueryHandler(rent_choose_month,  pattern=r"^rent_month_[0-9a-fA-F-]{36}_(2025|2026)$"))
    app.add_handler(CallbackQueryHandler(rent_choose_range,  pattern=r"^rent_range_[0-9a-fA-F-]{36}_(2025|2026)_(1[0-2]|[1-9])$"))
    app.add_handler(CallbackQueryHandler(rent_show_days,     pattern=r"^rent_day_[0-9a-fA-F-]{36}_(2025|2026)_(1[0-2]|[1-9])_([0-2]?[0-9]|3[01])_([0-2]?[0-9]|3[01])$"))
    app.add_handler(CallbackQueryHandler(rent_pick_day,      pattern=r"^rent_pick_[0-9a-fA-F-]{36}_.+$"))
    app.add_handler(CallbackQueryHandler(rent_back_to_listing, pattern=r"^rent_back_to_listing$"))
    app.add_handler(CallbackQueryHandler(rent_year_back,       pattern=r"^rent_year_back$"))
    app.add_handler(CallbackQueryHandler(rent_month_back,      pattern=r"^rent_month_back_[0-9a-fA-F-]{36}_(2025|2026)$"))
    app.add_handler(CallbackQueryHandler(rent_range_back,      pattern=r"^rent_range_back_[0-9a-fA-F-]{36}_(2025|2026)_(1[0-2]|[1-9])$"))
    app.add_handler(CallbackQueryHandler(noop,                 pattern=r"^noop$"))
    app.add_handler(CallbackQueryHandler(rent_finish_prompt,   pattern=r"^rent_finish_[0-9a-fA-F-]{36}$"))
    app.add_handler(CallbackQueryHandler(rent_confirm_yes,     pattern=r"^rent_confirm_yes_[0-9a-fA-F-]{36}$"))
    app.add_handler(CallbackQueryHandler(rent_confirm_no,      pattern=r"^rent_confirm_no_[0-9a-fA-F-]{36}$"))

    # Insurance feedback (add once)
    app.add_handler(MessageHandler(filters.Regex("^(Yes|No)$"), handle_insurance_feedback))

    # Settings conv
    app.add_handler(settings_conv)

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
        import asyncio
        from aiohttp import web
        from aiohttp.web import Request, Response
        from telegram import Update
        import json as json_lib
        
        print(f"Starting webhook mode on {HOST}:{PORT}")
        print(f"Webhook endpoint: {WEBHOOK_PATH}")
        print(f"Health endpoint: /health")
        
        # Initialize the application
        asyncio.run(app.initialize())
        
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
        
        # Create aiohttp app
        aiohttp_app = web.Application()
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
