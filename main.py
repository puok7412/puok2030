
# =============================
# Imports
# =============================
import os
import logging
import re
import asyncio
import time
import secrets
import html
import json
import traceback
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Set, Any

from fastapi import FastAPI, Request, HTTPException

from telegram import (
    Update,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    InputMediaPhoto,
    InputMediaVideo,
    ChatMemberUpdated,
)
from telegram.constants import ParseMode, ChatType
from telegram.error import BadRequest, RetryAfter, TimedOut, NetworkError
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ChatMemberHandler,
    ContextTypes,
    filters,
)

# =============================
# Logging (Render/Uvicorn friendly)
# =============================
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
logger = logging.getLogger("publisher")
logging.getLogger("httpx").setLevel(logging.WARNING)

# =============================
# Environment on Render
# =============================
TOKEN = os.getenv("TOKEN")
if not TOKEN:
    logger.error("Missing TOKEN environment variable. Set it in Render → Settings → Environment.")
    raise RuntimeError("TOKEN is required")

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "MyStrongSecretKey194525194525")
BASE_URL = os.getenv("RENDER_EXTERNAL_URL") or os.getenv("PUBLIC_URL")
PORT = int(os.getenv("PORT", "10000"))
SETUP_KEY = os.getenv("SETUP_KEY", WEBHOOK_SECRET)

# ==== Concurrency defaults ====
MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "5"))
PER_CHAT_TIMEOUT = int(os.getenv("PER_CHAT_TIMEOUT", "25"))

# قفل + طابع زمني لمنع النداءات المتقاربة
_WEBHOOK_LOCK = asyncio.Lock()
_LAST_SET_TS = 0.0

# =============================
# FastAPI & Telegram
# =============================
app = FastAPI()
application = ApplicationBuilder().token(TOKEN).build()

# =============================
# Time helpers (KSA)
# =============================
KSA_TZ = timezone(timedelta(hours=3))
GRANT_TTL_MINUTES = 30

def ksa_time(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(KSA_TZ).strftime("%Y-%m-%d %H:%M")

PANEL_MAIN = "panel_main"
PANEL_WAIT_REACTION_PROMPT = "panel_wait_reaction_prompt"
PANEL_WAIT_TEMPLATE_NAME_SAVE = "panel_wait_template_name_save"
panel_state: Dict[int, str] = {}

# =============================
# Persistence & Panel constants
# =============================
OK_REGEX = r"(?i)^\s*ok\s*$"   # يطابق OK فقط (مع تجاهل حالة الأحرف)

# مسار الحفظ (قابل للكتابة على Render)
STATE_PATH = os.getenv("STATE_PATH", "/tmp/publisher_state.json")

# باكِتات تستخدم مفاتيح tuple وتحتاج تحويلًا خاصًا وقت الحفظ/التحميل
_TUPLEKEY_BUCKETS = {
    "reactions_counters": True,
    "campaign_base_msg": True,
    "message_to_campaign": True,
    "reaction_style_by_message": True,  # جديد: نمط الإيموجي لكل رسالة مستقلة
}

# القوائم/القواميس التي نحفظها على القرص
_PERSIST_BUCKETS = [
    "global_settings",
    "admin_settings",
    "group_permissions",
    "known_chats",
    "known_chats_admins",
    "sessions",
    "temp_grants",
    "reactions_counters",
    "campaign_messages",
    "campaign_base_msg",
    "message_to_campaign",
    "campaign_prompt_msgs",
    "campaign_counters",
    "active_rebroadcasts",
    "panel_state",
    "start_tokens",               # مهم للتصريح المؤقت عبر /ok
    "campaign_styles",            # جديد: نمط التفاعلات لكل حملة
    "reaction_style_by_message",  # جديد: نمط التفاعلات لكل رسالة (خارج الحملات)
]

# حاوية نمط التفاعلات لكل حملة (thumbs/faces/hearts)
campaign_styles: Dict[int, str] = globals().get("campaign_styles", {})
globals()["campaign_styles"] = campaign_styles  # تأكيد التوفّر عالميًا

# حاوية نمط التفاعلات للرسائل غير المرتبطة بحملة: مفتاحها (chat_id, message_id)
reaction_style_by_message: Dict[Tuple[int, int], str] = globals().get("reaction_style_by_message", {})
globals()["reaction_style_by_message"] = reaction_style_by_message

def _to_jsonable(obj):
    from datetime import datetime, timezone
    from dataclasses import is_dataclass, asdict
    # حوّل أي dataclass (مثل Session) إلى dict أولاً
    if is_dataclass(obj):
        return _walk_to_jsonable(asdict(obj))
    if isinstance(obj, set):
        return {"__set__": True, "items": list(obj)}
    if isinstance(obj, datetime):
        return {"__dt__": True, "iso": obj.replace(tzinfo=timezone.utc).isoformat()}
    return obj

def _from_jsonable(obj):
    from datetime import datetime
    if isinstance(obj, dict) and "__set__" in obj:
        return set(obj["items"])
    if isinstance(obj, dict) and "__dt__" in obj:
        return datetime.fromisoformat(obj["iso"])
    return obj

def _walk_to_jsonable(x):
    if isinstance(x, dict):
        return {k: _walk_to_jsonable(v) for k, v in x.items()}
    if isinstance(x, list):
        return [_walk_to_jsonable(v) for v in x]
    if isinstance(x, tuple):  # ← إضافة اختيارية
        return [_walk_to_jsonable(v) for v in x]
    return _to_jsonable(x)

def _walk_from_jsonable(x):
    if isinstance(x, dict):
        return {k: _walk_from_jsonable(_from_jsonable(v)) for k, v in x.items()}
    if isinstance(x, list):
        return [_walk_from_jsonable(_from_jsonable(v)) for v in x]
    return _from_jsonable(x)

def save_state():
    global STATE_PATH
    try:
        # تحويل الحاويات مع دعم مفاتيح tuple لبعض الباكِتات
        data = {}
        for bucket in _PERSIST_BUCKETS:
            raw = globals().get(bucket, {})
            if bucket in _TUPLEKEY_BUCKETS and isinstance(raw, dict):
                conv = {}
                for k, v in raw.items():
                    if isinstance(k, tuple):
                        conv["::".join(map(str, k))] = _walk_to_jsonable(v)
                    else:
                        conv[str(k)] = _walk_to_jsonable(v)
                data[bucket] = conv
            else:
                data[bucket] = _walk_to_jsonable(raw)

        # إنشاء المجلد لمسار STATE_PATH الحالي
        dirpath = os.path.dirname(STATE_PATH) or "."
        try:
            os.makedirs(dirpath, exist_ok=True)
        except PermissionError:
            # تحوّل تلقائيًا إلى /tmp إن كان المسار غير قابل للكتابة (كما يحدث على Render)
            STATE_PATH = "/tmp/publisher_state.json"
            dirpath = os.path.dirname(STATE_PATH)
            os.makedirs(dirpath, exist_ok=True)
        except Exception:
            # أي خطأ آخر في إنشاء المجلد، نحاول الاستمرار بالمسار كما هو
            pass

        with open(STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        logger.info("State saved → %s", STATE_PATH)

    except Exception:
        logger.exception("save_state failed")

def load_state():
    try:
        if not os.path.exists(STATE_PATH):
            logger.info("No state file yet")
            return
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            raw = json.load(f)
        for bucket in _PERSIST_BUCKETS:
            loaded = _walk_from_jsonable(raw.get(bucket, {}))
            if bucket not in globals() or not isinstance(globals()[bucket], dict):
                globals()[bucket] = {}
            globals()[bucket].clear()
            if isinstance(loaded, dict):
                globals()[bucket].update(loaded)
        logger.info("State loaded ← %s", STATE_PATH)
    except Exception:
        logger.exception("load_state failed")

async def autosave_loop():
    while True:
        try:
            await asyncio.sleep(30)
            save_state()
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("autosave_loop iteration failed")

# =============================
# Global singletons / state
# =============================
global_settings = {
    "scheduling_enabled": True,
    "schedule_locked": False,
    "rebroadcast_interval_seconds": 7200,  # 2h
    "rebroadcast_total": 4,
    "pin_feature_enabled": True,
    "reactions_feature_enabled": True,
    "maintenance_mode": False,
    "default_reactions_enabled": True,
    "hide_links_default": False,
}
admin_settings: Dict[int, Dict[str, Any]] = {}
group_permissions: Dict[int, Dict[str, Any]] = {}
known_chats: Dict[int, Dict[str, Any]] = {}
known_chats_admins: Dict[int, Dict[int, str]] = {}
sessions: Dict[int, "Session"] = {}
temp_grants: Dict[int, Dict[str, Any]] = {}
reactions_counters: Dict[Tuple[int, int], Dict[str, Any]] = {}
campaign_messages: Dict[int, List[Tuple[int, int]]] = {}
campaign_base_msg: Dict[Tuple[int, int], int] = {}
message_to_campaign: Dict[Tuple[int, int], int] = {}
campaign_prompt_msgs: Dict[int, List[Tuple[int, int]]] = {}
campaign_counters: Dict[int, Dict[str, Any]] = {}
active_rebroadcasts: Dict[str, Dict[str, Any]] = {}  # name -> {interval, payload}
panel_state: Dict[int, str] = {}
start_tokens: Dict[str, Dict[str, Any]] = {}

# campaign id generator
_campaign_seq = 0
def new_campaign_id() -> int:
    global _campaign_seq
    _campaign_seq = (_campaign_seq + 1) % 10_000_000
    return _campaign_seq or 1

# =============================
# Text clean & link helpers
# =============================
START_TOKEN_RE = re.compile(r"/start\s+\S+", re.IGNORECASE)
IDSHAT_RE      = re.compile(r"idshat\\S*", re.IGNORECASE)
URL_RE = re.compile(r"(https?://[A-Za-z0-9._~:/?#\[\]@!$&'()*+,;=%-]+)")

def sanitize_text(text: Optional[str]) -> Optional[str]:
    if not text:
        return text
    txt = START_TOKEN_RE.sub('', text)
    txt = IDSHAT_RE.sub('', txt)
    txt = re.sub(r'\\n{3,}', '\\n\\n', txt)
    txt = re.sub(r'[ \\t]{2,}', ' ', txt)
    return txt.strip()

def make_html_with_hidden_links(text: str) -> str:
    parts: List[str] = []
    last = 0
    idx = 1
    for m in URL_RE.finditer(text):
        start, end = m.span()
        url = m.group(1)
        if start > last:
            parts.append(html.escape(text[last:start]))
        label = "اضغط هنا" if idx == 1 else f"اضغط هنا ({idx})"
        idx += 1
        parts.append(f'<a href="{html.escape(url, quote=True)}">{label}</a>')
        last = end
    parts.append(html.escape(text[last:]))
    return "".join(parts)

def hidden_links_or_plain(text: Optional[str], hide: bool) -> Optional[str]:
    if not text:
        return text
    text = sanitize_text(text)
    return make_html_with_hidden_links(text) if hide else text

# =============================
# Session
# =============================
@dataclass
class Session:
    stage: str = "waiting_first_input"  # waiting_first_input | collecting | ready_options | choosing_chats
    text: Optional[str] = None
    media_list: List[Tuple[str, str, Optional[str]]] = field(default_factory=list)  # (type, file_id, caption)
    single_attachment: Optional[Tuple[str, str, Optional[str]]] = None
    use_reactions: bool = True
    pin_enabled: bool = True
    chosen_chats: Set[int] = field(default_factory=set)
    picker_msg_id: Optional[int] = None
    panel_msg_id: Optional[int] = None
    schedule_active: bool = False
    rebroadcast_interval_seconds: int = 7200
    rebroadcast_total: int = 0
    campaign_id: Optional[int] = None
    allowed_chats: Set[int] = field(default_factory=set)
    is_temp_granted: bool = False
    granted_by: Optional[int] = None

# =============================
# Settings & helper getters
# =============================
def get_settings(user_id: int) -> Dict[str, Any]:
    s = admin_settings.setdefault(user_id, {
        "disabled_chats": set(),
        "templates": [],
        "reaction_prompt_text": "قيّم المنشور 👇",
        "permissions_mode": "all",  # or whitelist
        "whitelist": set(),
        "default_reactions_enabled": True,
        "hide_links_default": False,
    })
    return s

async def is_admin_in_chat(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int) -> bool:
    try:
        member = await context.bot.get_chat_member(chat_id, user_id)
        return member.status in ("administrator", "creator")
    except Exception:
        return False

async def chats_where_user_is_admin(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> List[int]:
    res = []
    for cid, admins in known_chats_admins.items():
        if user_id in admins:
            res.append(cid)
    return res

def add_log(user_id: int, text: str) -> None:
    s = get_settings(user_id)
    logs = s.setdefault("logs", [])
    logs.append({"ts": datetime.utcnow().isoformat(), "text": text})
    save_state()

# =============================
# Keyboards
# =============================
def keyboard_collecting() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("✅ تم", callback_data="done")]])

def build_chats_keyboard(chat_ids: List[int], chosen: Set[int], settings: Dict[str, Any]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for cid in chat_ids:
        ch = known_chats.get(cid, {"title": str(cid), "type": "group"})
        title = ch.get("title") or str(cid)
        typ = ch.get("type") or "group"
        badge = "📢" if typ == "channel" else "👥"
        mark  = "✅" if cid in chosen else "🚫"
        rows.append([InlineKeyboardButton(f"{badge} {mark} {title}", callback_data=f"toggle_chat:{cid}")])
    rows.append([
        InlineKeyboardButton("✅ تحديد الكل", callback_data="select_all"),
        InlineKeyboardButton(f"💾 حفظ الاختيار ({len(chosen)})", callback_data="done_chats"),
    ])
    rows.append([InlineKeyboardButton("▶️ متابعة", callback_data="back_main")])
    return InlineKeyboardMarkup(rows)

def _fmt_interval(secs: int) -> str:
    if secs < 60:
        return f"{secs}ث"
    if secs < 3600:
        m = secs // 60
        return f"{m}د"
    h = secs // 3600
    return f"{h}س"

def build_session_schedule_keyboard(sess: "Session") -> InlineKeyboardMarkup:
    # احصل على الإعداد الحالي إن وُجد
    cur_int = getattr(sess, "rebroadcast_interval_seconds", None)
    cur_total = getattr(sess, "rebroadcast_total", None)

    rows: List[List[InlineKeyboardButton]] = []

    # ⏱️ صف الساعات
    rows.append([
        InlineKeyboardButton("2س", callback_data="ssched_int:7200"),
        InlineKeyboardButton("4س", callback_data="ssched_int:14400"),
        InlineKeyboardButton("6س", callback_data="ssched_int:21600"),
        InlineKeyboardButton("12س", callback_data="ssched_int:43200"),
    ])

    # 🔁 عدد مرات الإعادة
    rows.append([
        InlineKeyboardButton("2×",  callback_data="ssched_count:2"),
        InlineKeyboardButton("4×",  callback_data="ssched_count:4"),
        InlineKeyboardButton("8×",  callback_data="ssched_count:8"),
        InlineKeyboardButton("12×", callback_data="ssched_count:12"),
    ])

    # ✅ حفظ / ▶️ متابعة
    rows.append([
        InlineKeyboardButton("💾 حفظ الإعداد", callback_data="sschedule_done"),
        InlineKeyboardButton("▶️ متابعة", callback_data="back_main"),
    ])

    # شريط حالة
    status = "—"
    try:
        parts = []
        if cur_int is not None:
            parts.append(f"الفاصل: {_fmt_interval(int(cur_int))}")
        if cur_total is not None:
            parts.append(f"المرات: {int(cur_total)}×")
        parts.append("الحالة: " + ("مفعّل" if getattr(sess, "schedule_active", False) else "غير مفعّل"))
        status = " | ".join(parts) if parts else status
    except Exception:
        pass
    rows.append([InlineKeyboardButton(f"ℹ️ {status}", callback_data="noop")])

    return InlineKeyboardMarkup(rows)

def _get_reaction_pair(style: str) -> Tuple[str, str]:
    return {
        "thumbs": ("👍", "👎"),
        "faces":  ("😊", "🙁"),
        "hearts": ("❤️", "💔"),
    }.get(style, ("👍", "👎"))

def _reaction_style_for(chat_id: int, message_id: int, campaign_id: Optional[int], user_id: Optional[int] = None) -> str:
    if campaign_id is not None:
        # نمط الحملة أو الافتراضي
        default_style = get_settings(user_id or 0).get("last_reactions_style", "thumbs")
        return campaign_styles.get(campaign_id, default_style)
    # نمط الرسالة الفردية أو الافتراضي
    return reaction_style_by_message.get(
        (chat_id, message_id),
        get_settings(user_id or 0).get("last_reactions_style", "thumbs")
    )

def _get_sess_style(sess: "Session", s_for_user: Optional[Dict[str, Any]] = None) -> str:
    # نقرأ من الجلسة إن وُجد، وإلا من آخر اختيار محفوظ بالمشرف، وإلا thumbs
    style = getattr(sess, "reactions_style", None)
    if style:
        return style
    if s_for_user and s_for_user.get("last_reactions_style"):
        return s_for_user["last_reactions_style"]
    return "thumbs"

def build_reactions_menu_keyboard(sess: "Session", s_for_user: Optional[Dict[str, Any]] = None) -> InlineKeyboardMarkup:
    cur = _get_sess_style(sess, s_for_user)
    pos, neg = _get_reaction_pair(cur)
    enabled = bool(getattr(sess, "use_reactions", True))

    rows: List[List[InlineKeyboardButton]] = []
    rows.append([InlineKeyboardButton(f"الحالي: {pos}/{neg} — {'مفعّلة' if enabled else 'معطلة'}", callback_data="noop")])

    # أنماط جاهزة
    rows.append([
        InlineKeyboardButton("👍/👎", callback_data="reactions_set:thumbs"),
        InlineKeyboardButton("😊/🙁", callback_data="reactions_set:faces"),
        InlineKeyboardButton("❤️/💔", callback_data="reactions_set:hearts"),
    ])

    # تفعيل/تعطيل
    rows.append([
        InlineKeyboardButton(("🔕 تعطيل التفاعلات" if enabled else "🔔 تفعيل التفاعلات"), callback_data="reactions_toggle")
    ])

    # حفظ / رجوع
    rows.append([
        InlineKeyboardButton("💾 حفظ", callback_data="reactions_save"),
        InlineKeyboardButton("⬅️ رجوع", callback_data="back_main"),
    ])
    return InlineKeyboardMarkup(rows)

# 2) تبديل زر التفاعلات في لوحة الخيارات الرئيسية
def keyboard_ready_options(sess: "Session", settings: Dict[str, Any]) -> InlineKeyboardMarkup:
    """
    كيبورد خيارات المنشور (مرحلة ready_options):
    - يحترم التعطيل المركزي من لوحة المسؤول:
      reactions_feature_enabled / scheduling_enabled / pin_feature_enabled
    - يعرض نمط التفاعلات الحالي (pos/neg) عند التفعيل
    """
    gs = global_settings
    btns: List[List[InlineKeyboardButton]] = []

    # 🎭 التفاعلات — تُعرض فقط إذا مفعّلة مركزيًا + مفعّلة للمشرف + مفعّلة داخل الجلسة
    if (gs.get("reactions_feature_enabled", True)
        and settings.get("default_reactions_enabled", True)
        and getattr(sess, "use_reactions", True)):
        cur_style = getattr(sess, "reactions_style", None) or settings.get("last_reactions_style", "thumbs")
        pos, neg = _get_reaction_pair(cur_style)
        btns.append([InlineKeyboardButton(f"🎭 اختيار التفاعلات ({pos}/{neg})", callback_data="reactions_menu")])

    # 🗂️ اختيار الوجهات (يُخفى عند التصريح المؤقت المقيَّد بوجهة واحدة)
    if not (getattr(sess, "is_temp_granted", False) and getattr(sess, "allowed_chats", set())):
        btns.append([InlineKeyboardButton("🗂️ اختيار الوجهات (مجموعات/قنوات)", callback_data="choose_chats")])

    # ⏱️ الجدولة — تُعرض فقط إذا مفعّلة مركزيًا
    if gs.get("scheduling_enabled", True):
        if gs.get("schedule_locked", False):
            hrs   = max(1, int(gs.get("rebroadcast_interval_seconds", 3600))) // 3600
            total = int(gs.get("rebroadcast_total", 0) or 0)
            btns.append([InlineKeyboardButton(f"⏱️ الإعادة (مقفلة): كل {hrs} ساعة × {total}", callback_data="noop")])
        else:
            hrs   = max(1, int(getattr(sess, "rebroadcast_interval_seconds", gs.get("rebroadcast_interval_seconds", 3600)))) // 3600
            total = int(getattr(sess, "rebroadcast_total", gs.get("rebroadcast_total", 0)) or 0)
            label = "⏱️ تفعيل الجدولة" if not getattr(sess, "schedule_active", False) else f"⏱️ الإعادة: كل {hrs} ساعة × {total}"
            btns.append([InlineKeyboardButton(label, callback_data="schedule_menu")])

    # 📌 التثبيت — يُعرض فقط إذا مفعّل مركزيًا
    if gs.get("pin_feature_enabled", True):
        pin_on = bool(getattr(sess, "pin_enabled", True))
        btns.append([InlineKeyboardButton("📌 تعطيل التثبيت" if pin_on else "📌 تفعيل التثبيت", callback_data="toggle_pin")])

    # 🔙 رجوع / 🧽 مسح / ❌ إنهاء / 👁️ معاينة
    btns.append([InlineKeyboardButton("⬅️ الرجوع للتعديل", callback_data="back_to_collect")])
    btns.append([
        InlineKeyboardButton("🧽 مسح", callback_data="clear"),
        InlineKeyboardButton("❌ إنهاء", callback_data="cancel"),
    ])
    btns.append([InlineKeyboardButton("👁️ معاينة", callback_data="preview")])

    return InlineKeyboardMarkup(btns)
# =============================
# Panel helpers
# =============================
def status_text(sess: "Session") -> str:
    gs = global_settings
    attach_map = {"document": "مستند", "audio": "ملف صوتي", "voice": "رسالة صوتية"}
    check, cross = "✅", "❌"
    lines = []

    # 📋 العنوان
    lines.append("*📋 ملخص الجلسة*")

    # 📝 المحتوى
    has_text   = bool(getattr(sess, "text", None))
    media_list = list(getattr(sess, "media_list", []) or [])
    single_att = getattr(sess, "single_attachment", None)
    att_txt    = attach_map.get(single_att[0], single_att[0]) if single_att else cross
    lines.append(f"*📝 المحتوى*: نص {check if has_text else cross} • وسائط {len(media_list)} • مرفق {att_txt}")

    # ⚙️ الخيارات (تحترم التعطيل المركزي)
    use_reacts  = bool(getattr(sess, "use_reactions", False))
    pin_enabled = bool(getattr(sess, "pin_enabled", True))
    chosen_cnt  = len(getattr(sess, "chosen_chats", set()) or [])

    if not gs.get("reactions_feature_enabled", True):
        reacts_part = "تفاعلات معطّلة مركزيًا"
    else:
        reacts_part = f"تفاعلات {check if use_reacts else cross}"

    if not gs.get("pin_feature_enabled", True):
        pin_part = "تثبيت معطّل مركزيًا"
    else:
        pin_part = f"تثبيت {'📌' if pin_enabled else cross}"

    lines.append(f"*⚙️ الخيارات*: {reacts_part} • {pin_part} • وجهات {chosen_cnt}")

    # ⏱️ الجدولة (تحترم التعطيل/القفل المركزي)
    sched_enabled = gs.get("scheduling_enabled", True)
    sched_locked  = gs.get("schedule_locked", False)
    schedule_active = bool(getattr(sess, "schedule_active", False)) and sched_enabled

    if not sched_enabled:
        sched_line = "*⏱️ الجدولة*: معطّلة مركزيًا"
    else:
        if schedule_active:
            total = int(getattr(sess, "rebroadcast_total", 0) or 0)
            secs  = int(getattr(sess, "rebroadcast_interval_seconds", 0) or 0)
            hrs   = (secs + 3599) // 3600 if secs > 0 else 0
            sched_line = f"*⏱️ الجدولة*: مفعّلة • كل {hrs} ساعة × {total}"
        else:
            sched_line = "*⏱️ الجدولة*: غير مفعّلة"
        if sched_locked:
            sched_line += " • مقفلة"
    lines.append(sched_line)

    # 📦 الحملة (بدون شرطة إذا لا يوجد ID)
    campaign_id = getattr(sess, "campaign_id", None)
    if campaign_id is not None:
        lines.append(f"*📦 المنشور*: ID `{campaign_id}` • حفظ تلقائي {check}")
    else:
        lines.append(f"*📦 المنشور*: حفظ تلقائي {check}")

    return "\n".join(lines)

async def push_panel(context: ContextTypes.DEFAULT_TYPE, chat_id: int, sess: Session, header_text: str):
    # احذف لوحة سابقة إن وجدت
    if getattr(sess, "panel_msg_id", None):
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=sess.panel_msg_id)
        except Exception:
            pass

    # اختيار لوحة الأزرار حسب المرحلة
    kb = (
        keyboard_collecting()
        if sess.stage in ("waiting_first_input", "collecting")
        else keyboard_ready_options(sess, get_settings(chat_id))
    )

    # نص منسّق Markdown عادي (بدون \\n)
    text = f"📋 *لوحة المنشور*\n{header_text}\n\n{status_text(sess)}"

    m = await context.bot.send_message(
        chat_id=chat_id,
        text=text,
        parse_mode="Markdown",
        reply_markup=kb
    )
    sess.panel_msg_id = m.message_id

def delete_picker_if_any(context: ContextTypes.DEFAULT_TYPE, user_id: int, sess: Session):
    if sess.picker_msg_id:
        try:
            context.application.create_task(context.bot.delete_message(chat_id=user_id, message_id=sess.picker_msg_id))
        except Exception:
            pass
        sess.picker_msg_id = None

# =============================
# Session start & permissions
# =============================
def _user_has_active_grant(user_id: int) -> bool:
    g = temp_grants.get(user_id)
    if not g or g.get("used"):
        return False
    exp = g.get("expires")
    return (exp is None) or (datetime.utcnow() <= exp)

def _grant_active_for(user_id: int, chat_id: int) -> bool:
    g = temp_grants.get(user_id)
    if not g or g.get("used"):
        return False
    if g.get("chat_id") != chat_id:
        return False
    exp = g.get("expires")
    return (exp is None) or (datetime.utcnow() <= exp)

globals()["_grant_active_for"] = _grant_active_for

def _md_escape(s: str) -> str:
    # تفادي كسر Markdown لأسماء فيها رموز خاصة
    return s.replace("\\", "\\\\").replace("_", "\\_").replace("*", "\\*").replace("`", "\\`")

async def start_publishing_session(user, context: ContextTypes.DEFAULT_TYPE):
    user_id = user.id

    # احترام وضع الصيانة
    if global_settings.get("maintenance_mode", False):
        await context.bot.send_message(chat_id=user_id, text="🛠️ نظام النشر تحت الصيانة والتحديث حاليًا.")
        return

    s = get_settings(user_id)

    sess = Session(
        stage="waiting_first_input",
        use_reactions=s.get("default_reactions_enabled", True),
        rebroadcast_interval_seconds=global_settings["rebroadcast_interval_seconds"],
        rebroadcast_total=global_settings["rebroadcast_total"],
        schedule_active=False
    )

    # تفعيل التصريح المؤقت إن وُجد
    g = temp_grants.get(user_id)
    if g and not g.get("used") and (not g.get("expires") or datetime.utcnow() <= g["expires"]):
        sess.allowed_chats = {g["chat_id"]}
        sess.is_temp_granted = True
        sess.granted_by = g.get("granted_by")
        sess.chosen_chats = set(sess.allowed_chats)

    sessions[user_id] = sess

    name = _md_escape(user.full_name or "")
    sep = "\n────────────\n"  # فاصل نحيف/باهت

    welcome = (
        f"👋 *مرحبًا* _{name}_\n"
        f"{sep}"
        f"*وضع النشر*\n"
        f"- أرسل *نصًا* أو *وسائط* **بأي ترتيب**.\n"
        f"- بعد أول إدخال سيتم *الحفظ تلقائيًا* وتظهر لوحة الخيارات.\n"
        f"{sep}"
        f"*المدخلات المدعومة*\n"
        f"- 📝 نص\n"
        f"- 🖼️ صورة / 🎞️ فيديو / 🗂️ ألبوم\n"
        f"- 📎 مستند\n"
        f"- 🎵 صوت / 🎙️ فويس\n"
    )

    await context.bot.send_message(chat_id=user_id, text=welcome, parse_mode="Markdown")
    save_state()
async def start_publishing_keyword(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != ChatType.PRIVATE:
        return

    user = update.effective_user
    user_id = user.id

    # ⛔ امنع OK أثناء الصيانة (مع صورة)
    if global_settings.get("maintenance_mode", False):
        await send_maintenance_notice(context.bot, user_id)
        return

    if _user_has_active_grant(user_id):
        await start_publishing_session(user, context)
        return
    admin_ids = await chats_where_user_is_admin(context, user_id)
    if admin_ids:
        await start_publishing_session(user, context)
        return
    await update.message.reply_text("🔒 هذا الأمر متاح فقط بتصريح مؤقّت فعّال من مشرف المجموعة.")

async def cmd_temp_ok(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return

    granter = update.effective_user
    if not await is_admin_in_chat(context, chat.id, granter.id):
        try:
            await update.message.reply_text("🚫 هذا الأمر للمشرفين فقط.")
        except Exception:
            pass
        return

    if not update.message.reply_to_message or not update.message.reply_to_message.from_user:
        await update.message.reply_text("⚠️ استخدم /ok بالرد على رسالة العضو الذي تريد منحه إذن نشر مؤقّت.")
        return

    target = update.message.reply_to_message.from_user
    if target.is_bot:
        await update.message.reply_text("⚠️ لا يمكن منح الصلاحية إلى بوت.")
        return

    # إنشاء التصريح المؤقت
    token = secrets.token_urlsafe(16)
    expires = datetime.utcnow() + timedelta(minutes=GRANT_TTL_MINUTES)
    start_tokens[token] = {"user_id": target.id, "chat_id": chat.id, "expires": expires}
    temp_grants[target.id] = {"chat_id": chat.id, "expires": expires, "used": False, "granted_by": granter.id}

    # رابط البدء العميق
    me = await context.bot.get_me()
    deep_link_tg = f"tg://resolve?domain={me.username}&start={token}"
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🚀 ابدأ النشر الآن", url=deep_link_tg)]])

    # اسم المستخدم بأسلوب Markdown عادي
    def _md_escape(s: str) -> str:
        return s.replace("\\", "\\\\").replace("_", "\\_").replace("*", "\\*").replace("`", "\\`").replace("[", "\\[").replace("]", "\\]")

    target_name = _md_escape(target.full_name or "")
    md_user_link = f"[{target_name}](tg://user?id={target.id})"
    md_chat_title = _md_escape(chat.title or str(chat.id))

    # رسالة مختصرة في المجموعة (Markdown عادي)
    group_text = (
        f"- 👤 {md_user_link}\n"
        f"**  تفضل امر نشر مؤقّت**\n"
        f"- ⏳ ينتهي: {ksa_time(expires)}\n"
        f"- ✅ استطلاع تفاعلي ونشر سريع (مؤقّت)\n\n"
        f"اضغط الزر لبدء الجلسة في الخاص."
    )

    # إرسالها كـ رد على رسالة العضو
    try:
        sent = await update.message.reply_to_message.reply_text(group_text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
    except Exception:
        sent = await update.message.reply_text(group_text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)

    # إخطار المستخدم في الخاص (مختصر)
    try:
        dm_text = (
            f"👋 مرحبًا *{target_name}*\n\n"
            f"   تفضل امر نشر مؤقّت : *{md_chat_title}*\n"
            f"⏳ ينتهي: {ksa_time(expires)}\n\n"
            f"اضغط الزر لبدء الجلسة."
        )
        await context.bot.send_message(chat_id=target.id, text=dm_text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
    except Exception:
        pass

    # حذف رسالة المجموعة تلقائيًا عند انتهاء الوقت
    async def _delete_when_expired(ctx: ContextTypes.DEFAULT_TYPE):
        try:
            await ctx.bot.delete_message(chat_id=chat.id, message_id=sent.message_id)
        except Exception:
            pass

    delay = max(0, int((expires - datetime.utcnow()).total_seconds()))
    try:
        context.application.job_queue.run_once(_delete_when_expired, when=delay)
    except Exception:
        # في حال عدم توفر JobQueue لأي سبب، نتجاهل الحذف التلقائي بأمان
        pass

    save_state()

async def start_with_token(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # يُستخدم فقط في الخاص
    if update.effective_chat.type != ChatType.PRIVATE:
        return

    # احترام وضع الصيانة
    if global_settings.get("maintenance_mode", False):
        await update.message.reply_text("🛠️ نظام النشر تحت الصيانة والتحديث حاليًا.")
        return

    user = update.effective_user
    args = context.args or []

    def _md_escape(s: str) -> str:
        return s.replace("\\", "\\\\").replace("_", "\\_").replace("*", "\\*").replace("`", "\\`").replace("[", "\\[").replace("]", "\\]")

    if not args:
        msg = (
            f"*مرحبًا* _{_md_escape(user.full_name or '')}_\n"
            f"- اطلب من المشرف منحك تصريحًا مؤقتًا بالأمر /ok في المجموعة.\n"
            f"- بعد منْح التصريح اضغط زر *ابدأ النشر* لبدء الجلسة."
        )
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
        return

    token = args[0]
    rec = start_tokens.get(token)
    if not rec:
        await update.message.reply_text("⛔ رابط غير صالح أو منتهي.")
        return

    if user.id != rec["user_id"]:
        await update.message.reply_text("⛔ هذا الرابط ليس لك. اطلب من المشرفين منحك تصريح نشر مؤقت.")
        return

    if datetime.utcnow() > rec["expires"]:
        await update.message.reply_text("⌛ انتهت صلاحية الرابط.")
        start_tokens.pop(token, None)
        temp_grants.pop(user.id, None)
        save_state()
        return

    await start_publishing_session(user, context)

    msg_ok = (
        f"*تم تفعيل تصريح النشر المؤقّت*\n"
        f"- 👤 المستخدم: _{_md_escape(user.full_name or '')}_\n"
        f"- ⏳ ينتهي: {ksa_time(rec['expires'])}\n"
        f"- ملاحظة: يُسحب التصريح بعد أول عملية نشر."
    )
    await update.message.reply_text(msg_ok, parse_mode=ParseMode.MARKDOWN)

    start_tokens.pop(token, None)
    save_state()
# =============================
# Input collection
# =============================
def build_next_hint(sess: Session, saved_type: str) -> str:
    has_text = bool(getattr(sess, "text", None) and str(sess.text).strip())
    has_media = bool(getattr(sess, "media_list", []) or [])
    has_attach = bool(getattr(sess, "single_attachment", None))

    can_add = []
    if not has_text:
        can_add.append("📝 نص")
    if not has_media:
        can_add.append("🖼️ صور/فيديو")
    if not has_attach:
        can_add.append("📎 ملف/صوت/فويس")

    return (
        f"*إضافة متاحة*: {' • '.join(can_add)} — ثم اضغط *تم* لبدء اعداد المنشور."
        if can_add else
        "✅ كل شيء جاهز — اضغط *تم* للخيارات."
    )

async def handle_admin_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat.type != ChatType.PRIVATE:
        return

    user_id = update.effective_user.id
    s = get_settings(user_id)
    sess = sessions.get(user_id)
    msg = update.message
    saved_type: Optional[str] = None

    # --- حفظ نص الدعوة من لوحة التحكم (يُسمح حتى أثناء الصيانة) ---
    state = panel_state.get(user_id)
    if state == PANEL_WAIT_REACTION_PROMPT and msg and msg.text:
        txt = msg.text.strip()
        if txt:
            global_settings["reaction_prompt_text"] = txt
            panel_state.pop(user_id, None)
            save_state()
            await update.message.reply_text("💾 تم حفظ نص الدعوة للتفاعلات.")
        else:
            await update.message.reply_text("⛔ النص فارغ. أرسل نصًا صحيحًا.")
        return

    # --- سلوك ok / ok25s في الخاص (ok25s قبل فحص الصيانة) ---
    if msg and msg.text:
        raw = msg.text.strip().lower()
        if raw == "ok25s":
            # افتح لوحة التحكّم حتى أثناء الصيانة
            await cmd_panel(update, context)
            return
        if raw == "ok":
            # تمت معالجتها في start_publishing_keyword (block=True) — نتجاهل هنا لتفادي التكرار
            return
    # --- نهاية سلوك ok / ok25s ---

    # 🛠️ احترام وضع الصيانة لأي إدخالات نشر
    if global_settings.get("maintenance_mode", False):
        await send_maintenance_notice(context.bot, user_id)
        return

    # قيود القائمة البيضاء (إن وُجدت)
    if s.get("permissions_mode") == "whitelist" and user_id not in s.get("whitelist", set()):
        await update.message.reply_text("🔒 النشر متاح لأعضاء القائمة البيضاء فقط.")
        return

    # التحقق من حالة الجلسة
    if not sess or sess.stage not in ("waiting_first_input", "collecting", "ready_options", "choosing_chats"):
        return

    # تجاهل نص فارغ
    if msg.text and not msg.text.strip():
        return

    # --------- استقبال المحتوى ---------
    # نص منفرد (ليس ضمن ألبوم)
    if msg.text and not msg.media_group_id:
        clean = sanitize_text(msg.text)
        if clean.strip():
            sess.text = f"{sess.text}\n{clean}" if sess.text else clean
            saved_type = "نص"

    # ألبوم وسائط (صور/فيديو)
    if msg.media_group_id and (msg.photo or msg.video):
        if msg.photo:
            sess.media_list.append(
                ("photo", msg.photo[-1].file_id, sanitize_text(msg.caption) if msg.caption else None)
            )
            saved_type = "صورة ضمن ألبوم"
        elif msg.video:
            sess.media_list.append(
                ("video", msg.video.file_id, sanitize_text(msg.caption) if msg.caption else None)
            )
            saved_type = "فيديو ضمن ألبوم"

    # صورة/فيديو مفرد
    if msg.photo and not msg.media_group_id:
        sess.media_list.append(
            ("photo", msg.photo[-1].file_id, sanitize_text(msg.caption) if msg.caption else None)
        )
        saved_type = "صورة"
    if msg.video and not msg.media_group_id:
        sess.media_list.append(
            ("video", msg.video.file_id, sanitize_text(msg.caption) if msg.caption else None)
        )
        saved_type = "فيديو"

    # مرفقات مفردة
    if msg.document:
        sess.single_attachment = ("document", msg.document.file_id, sanitize_text(msg.caption) if msg.caption else None)
        saved_type = "مستند"
    if msg.audio:
        sess.single_attachment = ("audio", msg.audio.file_id, sanitize_text(msg.caption) if msg.caption else None)
        saved_type = "ملف صوتي"
    if msg.voice:
        sess.single_attachment = ("voice", msg.voice.file_id, None)
        saved_type = "رسالة صوتية"

    if not saved_type:
        return

    if sess.stage == "waiting_first_input":
        sess.stage = "collecting"

    hint = build_next_hint(sess, saved_type)
    await push_panel(context, user_id, sess, f"✅ تم حفظ *{saved_type}*.\n{hint}")
    save_state()

# =============================
# Campaign buttons & panel
# =============================
# === on_button (merged) ===
async def on_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data

    # تجاهل الأزرار التي لها هاندلرات خاصة
    if data.startswith(("like:", "dislike:", "panel:", "perm:", "show_stats:", "stop_rebroadcast:")):
        return

    # أجب فورًا لتجنب أخطاء "Query is too old/invalid"
    try:
        await query.answer()
    except BadRequest as e:
        msg = str(e).lower()
        if "too old" in msg or "invalid" in msg:
            return
    except Exception:
        pass

    user_id = query.from_user.id
    s = get_settings(user_id)
    sess = sessions.get(user_id)

    # لوج تشخيصي
    try:
        logger.info(
            "CB >> user=%s data=%s stage=%s chosen=%s",
            user_id,
            data,
            (sess.stage if sess else None),
            (len(sess.chosen_chats) if (sess and getattr(sess, "chosen_chats", None) is not None) else None),
        )
    except Exception:
        logger.exception("CB log failed")

    # لا توجد جلسة
    if not sess:
        await query.message.reply_text("⚠️ لا توجد جلسة حالية. أرسل ok لبدء جلسة نشر جديدة.")
        return

    # ====== قائمة اختيار نمط التفاعلات ======
    if data == "reactions_menu":
        # احمِ القائمة إذا كانت التفاعلات مُعطّلة مركزيًا
        if not global_settings.get("reactions_feature_enabled", True):
            await query.answer("❌ التفاعلات مُعطّلة من لوحة المسؤول.", show_alert=True)
            return
        if not hasattr(sess, "reactions_style"):
            sess.reactions_style = s.get("last_reactions_style", "thumbs")
        try:
            await query.message.reply_text("🎭 اختر نمط التفاعلات:", reply_markup=build_reactions_menu_keyboard(sess, s))
        except Exception:
            pass
        return

    if data.startswith("reactions_set:"):
        if not global_settings.get("reactions_feature_enabled", True):
            await query.answer("❌ التفاعلات مُعطّلة من لوحة المسؤول.", show_alert=True)
            return
        style = data.split(":", 1)[1]
        if style not in ("thumbs", "faces", "hearts"):
            await query.answer("نمط غير مدعوم.", show_alert=True)
            return
        sess.reactions_style = style
        s["last_reactions_style"] = style
        save_state()
        try:
            await query.edit_message_reply_markup(reply_markup=build_reactions_menu_keyboard(sess, s))
        except Exception:
            pass
        await query.answer("تم اختيار النمط.")
        return

    if data == "reactions_toggle":
        if not global_settings.get("reactions_feature_enabled", True):
            await query.answer("❌ التفاعلات مُعطّلة من لوحة المسؤول.", show_alert=True)
            return
        sess.use_reactions = not bool(getattr(sess, "use_reactions", True))
        save_state()
        try:
            await query.edit_message_reply_markup(reply_markup=build_reactions_menu_keyboard(sess, s))
        except Exception:
            pass
        await query.answer("تم التبديل.")
        return

    if data == "reactions_save":
        try:
            await context.bot.delete_message(chat_id=user_id, message_id=query.message.message_id)
        except Exception:
            pass
        await push_panel(context, user_id, sess, "✅ تم حفظ إعداد التفاعلات.")
        save_state()
        return
    # ====== نهاية التفاعلات ======

    # زر "تم"
    if data == "done":
        if getattr(sess, "is_temp_granted", False) and getattr(sess, "allowed_chats", set()):
            sess.chosen_chats = set(sess.allowed_chats)
        sess.stage = "ready_options"
        await push_panel(context, user_id, sess, "🎛️ خيارات المنشور")
        save_state()
        return

    # الرجوع لمرحلة التجميع
    if data == "back_to_collect":
        sess.stage = "collecting"
        await push_panel(context, user_id, sess, "✍️ عدّل المحتوى ثم اضغط تم")
        save_state()
        return

    # مسح المدخلات
    if data == "clear":
        sessions[user_id] = Session(
            stage="waiting_first_input",
            use_reactions=s.get('default_reactions_enabled', True),
            rebroadcast_interval_seconds=global_settings['rebroadcast_interval_seconds'],
            rebroadcast_total=global_settings['rebroadcast_total'],
            schedule_active=False
        )
        await query.message.reply_text("🧽 تم مسح المدخلات. ابدأ من جديد بإرسال المحتوى.")
        save_state()
        return

    # إنهاء الجلسة
    if data == "cancel":
        sessions.pop(user_id, None)
        await query.message.reply_text("✅ تم إنهاء جلسة النشر.")
        save_state()
        return

    # تبديل التفاعلات (متروك للتوافق – يبقى محميًا مركزيًا)
    if data == "toggle_reactions":
        if not global_settings.get("reactions_feature_enabled", True):
            await query.answer("❌ التفاعلات مُعطّلة من لوحة المسؤول.", show_alert=True)
            return
        sess.use_reactions = not bool(sess.use_reactions)
        await push_panel(context, user_id, sess, "✅ تم ضبط التفاعلات.")
        save_state()
        return

    # تبديل التثبيت (يحترم التعطيل المركزي)
    if data == "toggle_pin":
        if not global_settings.get("pin_feature_enabled", True):
            await query.answer("📌 خيار التثبيت مُعطّل مركزيًا.", show_alert=True)
            return
        sess.pin_enabled = not bool(getattr(sess, "pin_enabled", True))
        await push_panel(context, user_id, sess, "✅ تم ضبط خيار التثبيت.")
        save_state()
        return

    # قائمة جدولة الإعادة
    if data == "schedule_menu":
        if not global_settings.get("scheduling_enabled", True):
            await query.message.reply_text("⏹️ تم إيقاف الجدولة مركزيًا بواسطة المسؤول.")
        elif global_settings.get("schedule_locked", False):
            await query.message.reply_text(
                f"⏱️ الإعادة *(مقفلة مركزيًا)*: كل {global_settings['rebroadcast_interval_seconds']//3600} ساعة × {global_settings['rebroadcast_total']} مرات.",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ رجوع", callback_data="back_main")]])
            )
        else:
            await query.message.reply_text(
                f"⏱️ الإعداد الحالي: كل {sess.rebroadcast_interval_seconds//3600} ساعة × {sess.rebroadcast_total} مرات.\n"
                f"الحالة: {'مفعّلة' if getattr(sess, 'schedule_active', False) else 'غير مفعّلة'}",
                reply_markup=build_session_schedule_keyboard(sess)
            )
        return

    # تغيير عدد مرات الإعادة
    if data.startswith("ssched_count:"):
        if global_settings.get("schedule_locked", False) or not global_settings.get("scheduling_enabled", True):
            await query.answer("⏱️ الجدولة مقفلة أو معطّلة مركزيًا.", show_alert=True)
            return
        try:
            cnt = int(data.split(":", 1)[1])
        except ValueError:
            await query.answer("قيمة غير صحيحة.", show_alert=True)
            return
        sess.rebroadcast_total = max(0, cnt)
        try:
            hrs = max(1, int(getattr(sess, "rebroadcast_interval_seconds", 3600))) // 3600
            await query.edit_message_text(
                text=f"⏱️ الإعداد الحالي: كل {hrs} ساعة × {sess.rebroadcast_total} مرات.",
                reply_markup=build_session_schedule_keyboard(sess)
            )
        except Exception:
            pass
        save_state()
        return

    # تغيير الفاصل الزمني
    if data.startswith("ssched_int:"):
        if global_settings.get("schedule_locked", False) or not global_settings.get("scheduling_enabled", True):
            await query.answer("⏱️ الجدولة مقفلة أو معطّلة مركزيًا.", show_alert=True)
            return
        try:
            sec = int(data.split(":", 1)[1])
        except ValueError:
            await query.answer("قيمة غير صحيحة.", show_alert=True)
            return
        sess.rebroadcast_interval_seconds = max(1, sec)
        try:
            hrs = max(1, int(sess.rebroadcast_interval_seconds)) // 3600
            await query.edit_message_text(
                text=f"⏱️ الإعداد الحالي: كل {hrs} ساعة × {sess.rebroadcast_total} مرات.",
                reply_markup=build_session_schedule_keyboard(sess)
            )
        except Exception:
            pass
        save_state()
        return

    # تفعيل الجدولة لهذه الجلسة
    if data == "sschedule_done":
        if global_settings.get("schedule_locked", False) or not global_settings.get("scheduling_enabled", True):
            await query.answer("⏱️ الجدولة مقفلة أو معطّلة مركزيًا.", show_alert=True)
            return
        sess.schedule_active = True
        try:
            await context.bot.delete_message(chat_id=user_id, message_id=query.message.message_id)
        except Exception:
            pass
        await push_panel(
            context, user_id, sess,
            f"✅ تم تفعيل الإعادة: كل {sess.rebroadcast_interval_seconds//3600} ساعة × {sess.rebroadcast_total} مرة."
        )
        save_state()
        return

    if data == "noop":
        return

    # اختيار الوجهات
    if data == "choose_chats":
        # في حالة التصريح المؤقت: لا قائمة — تثبيت الوجهة تلقائيًا
        if getattr(sess, "is_temp_granted", False) and getattr(sess, "allowed_chats", set()):
            sess.chosen_chats = set(sess.allowed_chats)
            await push_panel(context, user_id, sess, "🎯 الوجهة محددة تلقائيًا وفق التصريح.")
            save_state()
            return

        admin_chat_ids = await list_authorized_chats(context, user_id)
        if not admin_chat_ids:
            await query.message.reply_text(
                "🚫 لا توجد وجهات متاحة لك.\n\n"
                "• تأكد أن البوت مضاف ومشرف في المجموعة/القناة المطلوب النشر فيها.\n"
                "• أرسل /register داخل كل مجموعة مرة واحدة لتسجيلها.\n"
                "• ثم أعد فتح «🗂️ اختيار الوجهات».",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ رجوع", callback_data="back_main")]])
            )
            return

        # استبعد المُعطّل مركزيًا من إعدادات هذا المسؤول
        s_user = get_settings(user_id)
        active_ids = [cid for cid in admin_chat_ids if cid not in s_user.get("disabled_chats", set())]
        if not active_ids:
            await query.message.reply_text("🚫 كل الوجهات المصرّح بها معطّلة حاليًا من لوحة التحكّم.")
            return

        sess.stage = "choosing_chats"
        m = await query.message.reply_text(
            "🗂️ اختر الوجهات المستهدفة (👥 مجموعة / 📢 قناة):",
            reply_markup=build_chats_keyboard(active_ids, sess.chosen_chats, s_user)
        )
        sess.picker_msg_id = m.message_id
        save_state()
        return

    if data.startswith("toggle_chat:"):
        if sess.stage != "choosing_chats":
            return
        try:
            cid = int(data.split(":", 1)[1])
        except ValueError:
            return

        # منع اختيار غير المجموعة الممنوحة عند وجود منحة مؤقتة
        if getattr(sess, "is_temp_granted", False) and (cid not in getattr(sess, "allowed_chats", set())):
            await query.answer("هذه الصلاحية مقيدة بالوجهات الممنوحة فقط.", show_alert=True)
            return

        if cid in sess.chosen_chats:
            sess.chosen_chats.remove(cid)
        else:
            sess.chosen_chats.add(cid)

        # إعادة البناء من المصدر الصحيح
        if getattr(sess, "is_temp_granted", False):
            source_ids = list(sess.allowed_chats)
        else:
            source_ids = await list_authorized_chats(context, user_id)

        s_user = get_settings(user_id)
        active_ids = [i for i in source_ids if i not in s_user.get("disabled_chats", set())]
        await query.edit_message_reply_markup(reply_markup=build_chats_keyboard(active_ids, sess.chosen_chats, s_user))
        save_state()
        return

    if data == "select_all":
        if sess.stage != "choosing_chats":
            return
        if getattr(sess, "is_temp_granted", False):
            source_ids = list(sess.allowed_chats)
        else:
            source_ids = await list_authorized_chats(context, user_id)

        s_user = get_settings(user_id)
        sess.chosen_chats = set(i for i in source_ids if i not in s_user.get("disabled_chats", set()))
        delete_picker_if_any(context, user_id, sess)
        sess.stage = "ready_options"
        await push_panel(context, user_id, sess, "✅ تم تحديد جميع الوجهات (المسموح بها فقط).")
        save_state()
        return

    if data == "done_chats":
        delete_picker_if_any(context, user_id, sess)
        sess.stage = "ready_options"
        await push_panel(context, user_id, sess, "🎛️ تم حفظ اختيار الوجهات.")
        save_state()
        return

    if data == "back_main":
        # تنظيف رسالة اللائحة إن وُجدت + العودة للوحة الرئيسية
        try:
            if getattr(sess, "panel_msg_id", None) and query.message and (query.message.message_id != sess.panel_msg_id):
                await context.bot.delete_message(chat_id=query.message.chat_id, message_id=query.message.message_id)
        except Exception:
            pass
        delete_picker_if_any(context, user_id, sess)
        sess.stage = "ready_options"
        await push_panel(context, user_id, sess, "🎛️ عدنا للخيارات.")
        save_state()
        return

    # معاينة
    if data == "preview":
        await send_preview(update, context, sess, hide_links=get_settings(user_id).get("hide_links_default", False))
        return

    # ====== النشر ======
    # نشر (عدم الحجب) مع حفظ معرف الحملة وخيارات رد الفعل
    if data == "publish":
        if global_settings.get("maintenance_mode", False):
            await send_maintenance_notice(context.bot, user_id)
            return

    # في حالة التصريح: اجبر الوجهة على الممنوحة
        if getattr(sess, "is_temp_granted", False) and getattr(sess, "allowed_chats", set()):
            sess.chosen_chats = set(sess.allowed_chats)

        if not getattr(sess, "chosen_chats", set()):
            await query.message.reply_text(
                "⚠️ لا توجد وجهة للنشر.\nيرجى اختيار مجموعة أو قناة من «اختيار الوجهات».",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ رجوع", callback_data="back_main")]])
            )
            return

        # ⛳ احترام قرارات لوحة المسؤول: تفاعلات/تثبيت/جدولة
        s_user = get_settings(user_id)

        # تفاعلات
        if not global_settings.get("reactions_feature_enabled", True):
            sess.use_reactions = False
        else:
            sess.use_reactions = bool(sess.use_reactions) and bool(s_user.get("default_reactions_enabled", True))

        # تثبيت
        if not global_settings.get("pin_feature_enabled", True):
            sess.pin_enabled = False

        allow_schedule = bool(global_settings.get("scheduling_enabled", True))

        # 🔐 إذونات المشرفين + الوجهات المعطّلة: صفِّ الوجهات قبل الإرسال
        orig = list(sess.chosen_chats or [])
        allowed: List[int] = []
        disabled_skipped: List[int] = []
        blocked_skipped: List[int] = []
        for cid in orig:
            if cid in s_user.get("disabled_chats", set()):
                disabled_skipped.append(cid)
                continue
            blocked = group_permissions.setdefault(cid, {}).setdefault("blocked_admins", set())
            if user_id in blocked:
                blocked_skipped.append(cid)
                continue
            allowed.append(cid)
        sess.chosen_chats = set(allowed)

        # أخطر بالمستبعد
        try:
            msgs = []
            if disabled_skipped:
                names = ", ".join(known_chats.get(x, {}).get("title", str(x)) for x in disabled_skipped)
                msgs.append(f"⚠️ الوجهات المُعطّلة مركزيًا: {names}")
            if blocked_skipped:
                names = ", ".join(known_chats.get(x, {}).get("title", str(x)) for x in blocked_skipped)
                msgs.append(f"🚫 محظور لك النشر على: {names}")
            if msgs:
                await query.message.reply_text("\n".join(msgs))
        except Exception:
            pass

        if not sess.chosen_chats:
            await query.message.reply_text("⛔ لا توجد وجهات متاحة بعد تطبيق قيود المسؤول.")
            return

        # توليد campaign_id أول مرة
        if sess.campaign_id is None:
            sess.campaign_id = new_campaign_id()
            campaign_messages[sess.campaign_id] = []

        # نمط التفاعلات للحملة (يُحفظ)
        try:
            if "campaign_styles" in globals():
                style = getattr(sess, "reactions_style", s_user.get("last_reactions_style", "thumbs"))
                globals()["campaign_styles"][sess.campaign_id] = style
        except Exception:
            pass

        # استخدم نص الدعوة من لوحة المسؤول إن تم تحديده
        try:
            rp = global_settings.get("reaction_prompt_text")
            if rp:
                s_user["reaction_prompt_text"] = rp
        except Exception:
            pass

        save_state()

        # نشر كـ Task
        asyncio.create_task(
            _publish_and_report(
                context, user_id, sess, allow_schedule, s_user.get("hide_links_default", False)
            )
        )
        await query.message.reply_text("🚀 بدأ النشر… سأرسل لك النتائج قريبًا.")
        return

async def _publish_and_report(
    context: ContextTypes.DEFAULT_TYPE,
    user_id: int,
    sess: "Session",
    allow_schedule: bool,
    hide_links: bool
):
    sent_count = 0
    errors = []

    try:
        # الإرسال الفعلي
        result = await publish_to_chats(
            context, user_id, sess,
            is_rebroadcast=False,
            hide_links=hide_links
        )
        sent_count, errors = result if isinstance(result, tuple) else (0, ["تعذّر تحديد نتيجة النشر."])
    except Exception as e:
        sent_count = 0
        errors = [f"❌ حدث خطأ أثناء النشر: {e}"]

    # ✅ بعد إتمام النشر بنجاح: احذف أزرار (نشر/إلغاء) من رسالة المعاينة فقط
    #    (نترك رسالة المعاينة نفسها كما هي)
    try:
        if sent_count > 0:
            mid = getattr(sess, "preview_action_msg_id", None)
            if mid:
                await context.bot.edit_message_reply_markup(chat_id=user_id, message_id=mid, reply_markup=None)
                sess.preview_action_msg_id = None
                save_state()
    except Exception:
        pass

    # أعد نتيجة موجزة للمالك
    result_text = f"✅ تم النشر في {sent_count} وجهة." if sent_count else "⚠️ لم يتم النشر في أي وجهة."
    if errors:
        result_text += "\n\n" + "\n".join(errors)

    try:
        await context.bot.send_message(chat_id=user_id, text=result_text)
    except Exception:
        pass

    # أرسل لوحة الحملة (إن وُجدت حملة) وربما لمن منح التصريح
    try:
        if sess.campaign_id is not None:
            await send_campaign_panel(context, user_id, sess.campaign_id, also_to=getattr(sess, "granted_by", None))
    except Exception:
        pass

    # جدولة الإعادات إن مطابقة للشروط
    try:
        if sent_count > 0 and allow_schedule and getattr(sess, "schedule_active", False):
            await schedule_rebroadcast(
                context.application, user_id, sess,
                interval_seconds=getattr(sess, "rebroadcast_interval_seconds", 0),
                total_times=getattr(sess, "rebroadcast_total", 0)
            )
    except Exception:
        pass

    # علّم التصريح كمستخدم وأنهِ الجلسة
    try:
        g = temp_grants.get(user_id)
        if g:
            g["used"] = True
    except Exception:
        pass

    sessions.pop(user_id, None)
    save_state()

def build_status_block(sess: Session, *, allow_schedule: bool) -> str:
    parts = [status_text(sess)]
    if allow_schedule and getattr(sess, "schedule_active", False):
        parts.append("سيتم جدولة الإعادات بعد النشر.")
    return "\n".join(parts)

async def send_campaign_panel(context: ContextTypes.DEFAULT_TYPE, user_id: int, campaign_id: Optional[int], also_to: Optional[int] = None):
    if not campaign_id:
        return
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 عرض التقييم", callback_data=f"show_stats:{campaign_id}")],
        [InlineKeyboardButton("⏹️ إيقاف الإعادة", callback_data=f"stop_rebroadcast:{user_id}:{campaign_id}")]
    ])
    try:
        await context.bot.send_message(chat_id=user_id, text=f"📋 لوحة المنشور #{campaign_id}", reply_markup=kb)
    except Exception:
        pass
    if also_to and also_to != user_id:
        try:
            await context.bot.send_message(chat_id=also_to, text=f"📋 لوحة المنشور #{campaign_id} (نسخة للمسئول)", reply_markup=kb)
        except Exception:
            pass

# =============================
# Reactions 👍👎
# =============================

# حذف رسالة لاحقًا عبر JobQueue
async def _delete_msg_job(ctx):
    try:
        data = ctx.job.data
        await ctx.bot.delete_message(chat_id=data["chat_id"], message_id=data["message_id"])
    except Exception:
        pass

async def handle_reactions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    try:
        action, chat_id_str, message_id_str = data.split(":", 2)
        chat_id = int(chat_id_str)
        message_id = int(message_id_str)
    except Exception:
        return

    rec = reactions_counters.setdefault((chat_id, message_id), {"like": 0, "dislike": 0, "voters": {}})
    user = query.from_user
    voters = rec["voters"]

    # ربط الرسالة بحملة (إن وُجد)
    campaign_id = message_to_campaign.get((chat_id, message_id))

    # منع التصويت المكرر
    if campaign_id is not None:
        cc = campaign_counters.setdefault(campaign_id, {"like": 0, "dislike": 0, "voters": {}})
        if user.id in cc["voters"]:
            try:
                await query.answer(f"سجّلنا تفاعلك على هذا المنشور مسبقًا. شكرًا {user.first_name} 💙", show_alert=True)
            except BadRequest:
                pass
            return
    if user.id in voters:
        try:
            await query.answer(f"عزيزي {user.first_name}، لقد سجّلنا تفاعلك على هذا المنشور سابقًا. شكرًا لك 💙", show_alert=True)
        except BadRequest:
            pass
        return

    # تحديث العدادات
    if action == "like":
        rec["like"] += 1
    else:
        rec["dislike"] += 1
    voters[user.id] = action

    # تحديث على مستوى الحملة أو الرسالة
    if campaign_id is not None:
        cc = campaign_counters.setdefault(campaign_id, {"like": 0, "dislike": 0, "voters": {}})
        if action == "like":
            cc["like"] += 1
        else:
            cc["dislike"] += 1
        cc["voters"][user.id] = action

        # مزامنة الكيبوردات لكل رسائل الحملة بنفس الإيموجي المختار
        try:
            await _sync_campaign_keyboards(context, campaign_id)
        except Exception:
            pass

        # نمط الإيموجي للحملة
        style = globals().get("campaign_styles", {}).get(campaign_id, "thumbs")
        pos_emo, neg_emo = _get_reaction_pair(style)
    else:
        # رسالة غير مرتبطة بحملة: استخدم النمط المحفوظ لهذه الرسالة إن وُجد وإلا الافتراضي
        style = reaction_style_by_message.get((chat_id, message_id), "thumbs")
        pos_emo, neg_emo = _get_reaction_pair(style)

        # ✅ احفظ النمط لهذه الرسالة (لأول تفاعل أو عند افتقاد الحفظ السابق)
        reaction_style_by_message[(chat_id, message_id)] = style
        save_state()

        # تحديث كيبورد الرسالة الحالية فقط
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton(f"{pos_emo} {rec['like']}",  callback_data=f"like:{chat_id}:{message_id}"),
            InlineKeyboardButton(f"{neg_emo} {rec['dislike']}", callback_data=f"dislike:{chat_id}:{message_id}"),
        ]])
        try:
            await query.edit_message_reply_markup(reply_markup=kb)
        except Exception:
            pass

    # إشعار تفاعل
    try:
        await query.answer("تم تسجيل تفاعلك. شكرًا لك 🌟", show_alert=False)
    except BadRequest:
        pass

    # رسالة شكر مؤقتة تُحذف بعد 90 ثانية (بنفس الإيموجي المختار)
    try:
        emoji = pos_emo if action == "like" else neg_emo
        m = await context.bot.send_message(
            chat_id=chat_id,
            text=f"🙏 شكرًا {user.mention_html()} على تفاعلك {emoji}!",
            reply_to_message_id=message_id,
            parse_mode="HTML",
        )
        context.application.job_queue.run_once(
            _delete_msg_job,
            when=90,
            data={"chat_id": chat_id, "message_id": m.message_id},
            name=f"thx_{chat_id}_{m.message_id}"
        )
    except Exception:
        pass

    save_state()

async def _sync_campaign_keyboards(context: ContextTypes.DEFAULT_TYPE, campaign_id: int):
    # إجمالي العدّادات على مستوى الحملة
    cc = campaign_counters.setdefault(campaign_id, {"like": 0, "dislike": 0, "voters": {}})
    tot_like = cc["like"]
    tot_dislike = cc["dislike"]

    # 1) حدد نمط الإيموجي للحملة:
    #    - أولاً من campaign_styles
    #    - إن لم يوجد، حاول استنتاجه من reaction_style_by_message لأي رسالة أساسية ضمن الحملة
    #    - وإلا استخدم "thumbs" كافتراضي
    style = campaign_styles.get(campaign_id)
    inferred = False
    if not style:
        try:
            # حاول استنتاج النمط من أي رسالة أساسية معروفة للحملة
            for (cid, _prompt_mid) in list(campaign_prompt_msgs.get(campaign_id, [])):
                base_mid = campaign_base_msg.get((campaign_id, cid))
                if base_mid:
                    st = reaction_style_by_message.get((cid, base_mid))
                    if st:
                        style = st
                        inferred = True
                        break
        except Exception:
            pass
        if not style:
            style = "thumbs"

    # ثبّت النمط إذا كان مستنتجًا أو لم يكن محفوظًا سابقًا
    if campaign_styles.get(campaign_id) != style:
        campaign_styles[campaign_id] = style
        save_state()

    pos_emo, neg_emo = _get_reaction_pair(style)

    # 2) حدّث كيبورد كل رسالة "دعوة التفاعل" التابعة للحملة
    for (cid, prompt_mid) in list(campaign_prompt_msgs.get(campaign_id, [])):
        try:
            base_mid = campaign_base_msg.get((campaign_id, cid))
            if not base_mid:
                continue
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton(f"{pos_emo} {tot_like}",  callback_data=f"like:{cid}:{base_mid}"),
                InlineKeyboardButton(f"{neg_emo} {tot_dislike}", callback_data=f"dislike:{cid}:{base_mid}"),
            ]])
            await context.bot.edit_message_reply_markup(chat_id=cid, message_id=prompt_mid, reply_markup=kb)
        except Exception:
            pass

# =============================
# Preview & publishing
# =============================
async def send_preview(update: Update, context: ContextTypes.DEFAULT_TYPE, sess: Session, *, hide_links: bool):
    query = update.callback_query
    user_id = query.from_user.id
    caption = sess.text

    action_kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀 نشر الآن", callback_data="publish")],
        [InlineKeyboardButton("❌ إلغاء",    callback_data="cancel")],
    ])

    if not getattr(sess, "chosen_chats", None):
        await context.bot.send_message(
            chat_id=user_id,
            text="⚠️ لا توجد وجهة للنشر.\nيرجى اختيار مجموعة أو قناة من «اختيار الوجهات».",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ رجوع", callback_data="back_main")]])
        )

    status_block = build_status_block(sess, allow_schedule=True)

    # مرفق منفرد
    if getattr(sess, "single_attachment", None):
        a_type, file_id, a_caption = sess.single_attachment
        c = caption or a_caption
        c = hidden_links_or_plain(c, hide_links)
        if a_type == "document":
            await context.bot.send_document(chat_id=user_id, document=file_id, caption=c, parse_mode=("HTML" if hide_links else None))
        elif a_type == "audio":
            await context.bot.send_audio(chat_id=user_id, audio=file_id, caption=c, parse_mode=("HTML" if hide_links else None))
        elif a_type == "voice":
            await context.bot.send_voice(chat_id=user_id, voice=file_id, caption=c, parse_mode=("HTML" if hide_links else None))
        m = await context.bot.send_message(chat_id=user_id, text=status_block, reply_markup=action_kb)
        sess.preview_action_msg_id = m.message_id
        save_state()
        return

    # ألبوم/وسائط متعددة أو مفردة
    if getattr(sess, "media_list", None):
        if len(sess.media_list) > 1:
            media_group = []
            for idx, (t, fid, cap) in enumerate(sess.media_list):
                c = caption if idx == 0 else (cap or None)
                c = hidden_links_or_plain(c, hide_links) if c else None
                if t == "photo":
                    media_group.append(InputMediaPhoto(media=fid, caption=c, parse_mode=("HTML" if hide_links else None)))
                else:
                    media_group.append(InputMediaVideo(media=fid, caption=c, parse_mode=("HTML" if hide_links else None)))
            await context.bot.send_media_group(chat_id=user_id, media=media_group)
        else:
            t, fid, cap = sess.media_list[0]
            c = caption or cap
            c = hidden_links_or_plain(c, hide_links)
            if t == "photo":
                await context.bot.send_photo(chat_id=user_id, photo=fid, caption=c, parse_mode=("HTML" if hide_links else None))
            else:
                await context.bot.send_video(chat_id=user_id, video=fid, caption=c, parse_mode=("HTML" if hide_links else None))
        m = await context.bot.send_message(chat_id=user_id, text=status_block, reply_markup=action_kb)
        sess.preview_action_msg_id = m.message_id
        save_state()
        return

    # نص فقط
    if getattr(sess, "text", None):
        await context.bot.send_message(chat_id=user_id, text=hidden_links_or_plain(sess.text, hide_links), parse_mode=("HTML" if hide_links else None))
        m = await context.bot.send_message(chat_id=user_id, text=status_block, reply_markup=action_kb)
        sess.preview_action_msg_id = m.message_id
        save_state()
        return

    await query.message.reply_text("لا يوجد محتوى للمعاينة.")

# ---------- Concurrency-safe single chat send ----------
async def send_post_one_chat(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    sess: Session,
    *,
    is_rebroadcast: bool,
    hide_links: bool,
    reaction_prompt: str
) -> Optional[int]:
    first_message_id: Optional[int] = None
    caption = sess.text

    # --- مرفق مفرد ---
    if sess.single_attachment:
        a_type, file_id, a_caption = sess.single_attachment
        c = caption or a_caption
        c = hidden_links_or_plain(c, hide_links)
        if a_type == "document":
            m = await context.bot.send_document(chat_id=chat_id, document=file_id, caption=c, parse_mode=("HTML" if hide_links else None))
        elif a_type == "audio":
            m = await context.bot.send_audio(chat_id=chat_id, audio=file_id, caption=c, parse_mode=("HTML" if hide_links else None))
        elif a_type == "voice":
            m = await context.bot.send_voice(chat_id=chat_id, voice=file_id, caption=c, parse_mode=("HTML" if hide_links else None))
        else:
            m = None
        if m:
            first_message_id = m.message_id
        caption = None

    # --- وسائط متعددة ---
    if sess.media_list:
        if len(sess.media_list) > 1:
            media_group = []
            for idx, (t, fid, cap) in enumerate(sess.media_list):
                c = caption if idx == 0 else (cap or None)
                c = hidden_links_or_plain(c, hide_links) if c else None
                if t == "photo":
                    media_group.append(InputMediaPhoto(media=fid, caption=c, parse_mode=("HTML" if hide_links else None)))
                else:
                    media_group.append(InputMediaVideo(media=fid, caption=c, parse_mode=("HTML" if hide_links else None)))
            msgs = await context.bot.send_media_group(chat_id=chat_id, media=media_group)
            first_message_id = first_message_id or (msgs[0].message_id if msgs else None)
            caption = None
        else:
            t, fid, cap = sess.media_list[0]
            c = caption or cap
            c = hidden_links_or_plain(c, hide_links)
            if t == "photo":
                m = await context.bot.send_photo(chat_id=chat_id, photo=fid, caption=c, parse_mode=("HTML" if hide_links else None))
            else:
                m = await context.bot.send_video(chat_id=chat_id, video=fid, caption=c, parse_mode=("HTML" if hide_links else None))
            first_message_id = first_message_id or (m.message_id if m else None)
            caption = None

    # --- نص فقط ---
    if caption:
        m = await context.bot.send_message(chat_id=chat_id, text=hidden_links_or_plain(caption, hide_links), parse_mode=("HTML" if hide_links else None))
        first_message_id = first_message_id or m.message_id

    if not first_message_id:
        return None

    # توحيد رسالة الأساس للأزرار ضمن الحملة
    base_id_for_buttons = first_message_id
    if sess.campaign_id:
        key = (sess.campaign_id, chat_id)
        if key not in campaign_base_msg:
            campaign_base_msg[key] = first_message_id
        base_id_for_buttons = campaign_base_msg[key]

    # --- التفاعلات ---
    # احترم التعطيل المركزي + تفعيل الجلسة
    if not global_settings.get("reactions_feature_enabled", True):
        return first_message_id
    if not getattr(sess, "use_reactions", True):
        return first_message_id

    # عدّادات على مستوى الرسالة/الحملة
    rec = reactions_counters.get((chat_id, base_id_for_buttons))
    if rec is None:
        rec = {"like": 0, "dislike": 0, "voters": {}}
        reactions_counters[(chat_id, base_id_for_buttons)] = rec

    like_count = rec["like"]
    dislike_count = rec["dislike"]

    if sess.campaign_id is not None:
        cc = campaign_counters.setdefault(sess.campaign_id, {"like": 0, "dislike": 0, "voters": {}})
        like_count = cc["like"]
        dislike_count = cc["dislike"]

    # تثبيت/استرجاع نمط الإيموجي
    try:
        style = getattr(sess, "reactions_style", None)
        if sess.campaign_id is not None:
            if style is None:
                style = campaign_styles.get(sess.campaign_id, "thumbs")
            campaign_styles[sess.campaign_id] = style
        else:
            if style is None:
                style = reaction_style_by_message.get((chat_id, base_id_for_buttons), "thumbs")
            reaction_style_by_message[(chat_id, base_id_for_buttons)] = style
        save_state()
    except Exception:
        style = "thumbs"

    pos_emo, neg_emo = _get_reaction_pair(style)

    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton(f"{pos_emo} {like_count}",   callback_data=f"like:{chat_id}:{base_id_for_buttons}"),
        InlineKeyboardButton(f"{neg_emo} {dislike_count}", callback_data=f"dislike:{chat_id}:{base_id_for_buttons}"),
    ]])

    # إرسال/تثبيت رسالة الدعوة
    ensure_prompt_ok = True
    if sess.campaign_id is not None and is_rebroadcast:
        # جرّب تحديث الدعوات المسجّلة؛ إذا كانت محذوفة ننظّفها
        lst = campaign_prompt_msgs.setdefault(sess.campaign_id, [])
        still_any_for_chat = False
        new_lst = []
        for (c, mid) in lst:
            if c != chat_id:
                new_lst.append((c, mid))
                continue
            try:
                # مجرد محاولة تحديث لتعقّب صلاحية الرسالة
                await context.bot.edit_message_reply_markup(chat_id=chat_id, message_id=mid, reply_markup=kb)
                still_any_for_chat = True
                new_lst.append((c, mid))
            except Exception:
                # الرسالة قديمة/محذوفة → لا نعيد إضافتها
                pass
        campaign_prompt_msgs[sess.campaign_id] = new_lst
        ensure_prompt_ok = not still_any_for_chat  # لو لا توجد دعوة صالحة سنرسل واحدة جديدة

    if ensure_prompt_ok:
        try:
            prompt_msg = await context.bot.send_message(chat_id=chat_id, text=reaction_prompt, reply_markup=kb)
            if sess.campaign_id is not None:
                message_to_campaign[(chat_id, base_id_for_buttons)] = sess.campaign_id
                lst = campaign_prompt_msgs.setdefault(sess.campaign_id, [])
                if (chat_id, prompt_msg.message_id) not in lst:
                    lst.append((chat_id, prompt_msg.message_id))
            save_state()
        except Exception:
            pass

    return first_message_id

async def _safe_send_one(context, cid, sess, *, is_rebroadcast, hide_links, reaction_prompt, sem):
    async with sem:
        try:
            return await asyncio.wait_for(
                send_post_one_chat(
                    context, cid, sess,
                    is_rebroadcast=is_rebroadcast,
                    hide_links=hide_links,
                    reaction_prompt=reaction_prompt
                ),
                timeout=PER_CHAT_TIMEOUT
            )
        except RetryAfter as e:
            await asyncio.sleep(getattr(e, "retry_after", 3))
            try:
                return await asyncio.wait_for(
                    send_post_one_chat(
                        context, cid, sess,
                        is_rebroadcast=is_rebroadcast,
                        hide_links=hide_links,
                        reaction_prompt=reaction_prompt
                    ),
                    timeout=PER_CHAT_TIMEOUT
                )
            except Exception:
                return None
        except (TimedOut, NetworkError):
            return None
        except Exception:
            return None

async def publish_to_chats(context: ContextTypes.DEFAULT_TYPE, user_id: int, sess: Session, *, is_rebroadcast: bool, hide_links: bool):
    if global_settings.get("maintenance_mode", False):
        try:
            await context.bot.send_message(chat_id=user_id, text="🛠️ نظام النشر تحت الصيانة والتحديث حاليًا.")
        except Exception:
            pass
        return 0, ["الصيانة مفعلة"]

    s = get_settings(user_id)
    disabled = s.setdefault("disabled_chats", set())
    target_chats = [cid for cid in list(getattr(sess, "chosen_chats", [])) if cid not in disabled]

    # فلترة بحسب صلاحية النشر (أدمن أو تصريح) + استبعاد المحظورين
    if not is_rebroadcast:
        checked = []
        skip_msgs = []
        for cid in target_chats:
            # محظور؟
            blocked = group_permissions.setdefault(cid, {}).setdefault("blocked_admins", set())
            if user_id in blocked:
                title = known_chats.get(cid, {}).get("title", str(cid))
                skip_msgs.append(f"🚫 محظور عليك النشر في: {title}")
                continue
            # أدمن أو تصريح؟
            try:
                member = await context.bot.get_chat_member(cid, user_id)
                is_admin = (member.status in ("administrator", "creator"))
            except Exception:
                is_admin = False
            allowed_by_grant = globals().get("_grant_active_for", lambda a,b: False)(user_id, cid)
            if is_admin or allowed_by_grant:
                checked.append(cid)
            else:
                title = known_chats.get(cid, {}).get("title", str(cid))
                skip_msgs.append(f"⚠️ لست مشرفًا/مصرّحًا في: {title}")
        target_chats = checked
    else:
        skip_msgs = []

    # أرسل بالتوازي
    sem = asyncio.Semaphore(max(1, MAX_CONCURRENCY))
    tasks = [
        _safe_send_one(
            context, cid, sess,
            is_rebroadcast=is_rebroadcast,
            hide_links=hide_links,
            # ✅ خذ نص الدعوة من global_settings
            reaction_prompt=global_settings.get("reaction_prompt_text", "قيّم هذا المنشور 👇"),
            sem=sem
        ) for cid in target_chats
    ]
    results = await asyncio.gather(*tasks, return_exceptions=False)

    sent_count = 0
    errors: List[str] = []
    for cid, mid in zip(target_chats, results):
        if mid:
            if (not is_rebroadcast) and global_settings.get("pin_feature_enabled", True) and getattr(sess, "pin_enabled", True):
                try:
                    await context.bot.pin_chat_message(chat_id=cid, message_id=mid, disable_notification=True)
                except Exception:
                    pass
            sent_count += 1
            if sess.campaign_id is not None:
                lst = campaign_messages.setdefault(sess.campaign_id, [])
                if (cid, mid) not in lst:
                    lst.append((cid, mid))
        else:
            errors.append(f"تعذّر الإرسال إلى {known_chats.get(cid, {}).get('title', cid)}")

    # أضِف الرسائل التي تم تخطيها (محظور/ليس أدمن)
    errors.extend(skip_msgs)

    save_state()
    return sent_count, errors

# =============================
# Rebroadcast scheduling
# =============================
async def rebroadcast_job(ctx: ContextTypes.DEFAULT_TYPE):
    data = ctx.job.data
    if not data:
        return

    owner_id = data["owner_id"]
    s = get_settings(owner_id)

    # إعادة تكوين جلسة مؤقتة للإرسال
    tmp = Session(
        text=data["text"],
        media_list=data["media_list"],
        single_attachment=data["single_attachment"],
        use_reactions=data["use_reactions"],
        chosen_chats=set(data["chosen_chats"]),
        campaign_id=data.get("campaign_id"),
        schedule_active=False
    )

    # ✅ ضبط نمط التفاعلات المختار للحملة (thumbs / faces / hearts)
    try:
        if tmp.campaign_id:
            style = globals().get("campaign_styles", {}).get(tmp.campaign_id)
            if style:
                tmp.reactions_style = style
    except Exception:
        pass

    # النشر كإعادة (يحترم إخفاء الروابط من إعدادات المالك)
    try:
        await publish_to_chats(
            ctx,
            owner_id,
            tmp,
            is_rebroadcast=True,
            hide_links=s.get("hide_links_default", False)
        )
    except Exception as e:
        logger.exception("rebroadcast_job publish failed: %s", e)

    # مزامنة لوحات التفاعل (اختياري)
    try:
        if tmp.campaign_id:
            await _sync_campaign_keyboards(ctx, tmp.campaign_id)
    except Exception:
        pass

    # إشعار تقدّم الإعادة — رسالة جديدة كل مرة (بدون اقتباس)
    try:
        total = int(data.get("total", 0) or 0)
        left  = int(data.get("left", 0) or 0)
        done  = (total - left + 1) if total else None
        text  = f"🔁 إعادة النشر {done}/{total}" if done is not None else "🔁 تمت إعادة نشر المنشور."
        for cid in list(data.get("chosen_chats", [])):
            try:
                # لا نمرر reply_to_message_id إطلاقًا ⇒ لا يوجد اقتباس
                await ctx.bot.send_message(chat_id=cid, text=text)
            except Exception:
                pass
    except Exception:
        pass

    # إرسال الإحصاءات للمدير (إن وُجدت الدالة)
    try:
        await send_stats_to_admin(ctx, owner_id, tmp.campaign_id)
    except NameError:
        pass
    except Exception:
        pass

    # تحديث العدّ وإيقاف الجدولة عند الانتهاء
    data["left"] -= 1
    if data["left"] <= 0:
        try:
            ctx.job.schedule_removal()
        except Exception:
            pass
        try:
            del active_rebroadcasts[ctx.job.name]
        except Exception:
            pass

    save_state()

async def schedule_rebroadcast(app_or_ctx, user_id: int, sess: Session, interval_seconds: int = 7200, total_times: int = 12):
    payload = {
        "text": sess.text,
        "media_list": sess.media_list[:],
        "single_attachment": sess.single_attachment,
        "use_reactions": sess.use_reactions,
        "chosen_chats": list(sess.chosen_chats),
        "owner_id": user_id,
        "campaign_id": sess.campaign_id,
        "total": total_times,
        "left": total_times
    }
    name = f"rebroadcast_{user_id}_{sess.campaign_id}"
    jq = getattr(app_or_ctx, "job_queue", None)
    if jq is None and hasattr(app_or_ctx, "application"):
        jq = getattr(app_or_ctx.application, "job_queue", None)
    if jq is not None:
        try:
            jq.run_repeating(rebroadcast_job, interval=interval_seconds, first=interval_seconds, data=payload, name=name)
            active_rebroadcasts[name] = {"interval": int(interval_seconds), "payload": payload}
            save_state()
            return
        except Exception:
            pass
    async def _fallback_loop():
        data = dict(payload)
        while data["left"] > 0:
            tmp = Session(
                text=data["text"],
                media_list=data["media_list"],
                single_attachment=data["single_attachment"],
                use_reactions=data["use_reactions"],
                chosen_chats=set(data["chosen_chats"]),
                campaign_id=data.get("campaign_id"),
                schedule_active=False
            )
            await publish_to_chats(app_or_ctx if isinstance(app_or_ctx, ContextTypes.DEFAULT_TYPE) else app_or_ctx,
                                   data["owner_id"], tmp, is_rebroadcast=True,
                                   hide_links=get_settings(data["owner_id"])["hide_links_default"])
            done = data["total"] - data["left"] + 1
            try:
                bot = getattr(app_or_ctx, "bot", None)
                if bot is None and hasattr(app_or_ctx, "application"):
                    bot = app_or_ctx.application.bot
                if bot is not None:
                    for cid in data["chosen_chats"]:
                        await bot.send_message(chat_id=cid, text=f"🔁 إعادة النشر {done}/{data['total']}")
            except Exception:
                pass
            await send_stats_to_admin(app_or_ctx if isinstance(app_or_ctx, ContextTypes.DEFAULT_TYPE) else app_or_ctx,
                                      data["owner_id"], data.get("campaign_id"))
            data["left"] -= 1
            save_state()
            if data["left"] <= 0:
                break
            await asyncio.sleep(interval_seconds)
        try:
            del active_rebroadcasts[name]
        except Exception:
            pass
        save_state()
    asyncio.create_task(_fallback_loop())
    active_rebroadcasts[name] = {"interval": int(interval_seconds), "payload": payload}
    save_state()

# =============================
# Stats & control buttons
# =============================
async def handle_campaign_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    if data.startswith("show_stats:"):
        try:
            campaign_id = int(data.split(":", 1)[1])
        except Exception:
            try: await query.answer("خطأ في المعرف.", show_alert=True)
            except Exception: pass
            return
        cc = campaign_counters.get(campaign_id)
        if not cc:
            text = f"📊 إحصاءات المنشورات #{campaign_id}\\n——————————————\\nلا توجد تفاعلات حتى الآن."
            try: await query.message.reply_text(text)
            except Exception: pass
            try: await query.answer()
            except Exception: pass
            return
        tot_like = cc.get("like", 0); tot_dislike = cc.get("dislike", 0)
        voters_count = len(cc.get("voters", {}))
        per_chat_lines = []
        for (cmp_id, cid), base_mid in campaign_base_msg.items():
            if cmp_id != campaign_id: continue
            rec = reactions_counters.get((cid, base_mid), {"like": 0, "dislike": 0})
            title = known_chats.get(cid, {}).get("title", str(cid))
            per_chat_lines.append(f"• {title} — 👍 {rec.get('like', 0)} | 👎 {rec.get('dislike', 0)}")
        text = (
            f"📊 إحصاءات المنشورات #{campaign_id}\\n"
            f"——————————————\\n"
            f"👍 الإجمالي: {tot_like}\\n"
            f"👎 الإجمالي: {tot_dislike}\\n"
            f"👥 المصوّتون الفريدون: {voters_count}\\n"
        )
        if per_chat_lines: text += "\\n" + "\\n".join(per_chat_lines)
        try: await query.message.reply_text(text)
        except Exception: pass
        try: await query.answer()
        except Exception: pass
        return
    if data.startswith("stop_rebroadcast:"):
        try:
            _, user_id, campaign_id = data.split(":", 2)
            user_id = int(user_id); campaign_id = int(campaign_id)
        except Exception:
            try: await query.answer("خطأ في المعرف.", show_alert=True)
            except Exception: pass
            return
        name = f"rebroadcast_{user_id}_{campaign_id}"
        jobs = context.application.job_queue.get_jobs_by_name(name)
        if jobs:
            for j in jobs:
                try: j.schedule_removal()
                except Exception: pass
            try: await query.message.reply_text("⏹️ تم إيقاف إعادة البث لهذا المنشور.")
            except Exception: pass
            try: del active_rebroadcasts[name]
            except Exception: pass
            save_state()
        else:
            try: await query.message.reply_text("لا توجد إعادة بث نشطة لهذا المنشور.")
            except Exception: pass
        try: await query.answer()
        except Exception: pass
        return

# =============================
# Optional admin commands (register/mychats/panel/ping)
# Implementations kept minimal to preserve behavior; feel free to expand as per your original.
# =============================
async def cmd_register(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP, ChatType.CHANNEL):
        await update.message.reply_text("استخدم هذا الأمر داخل المجموعة/القناة لتسجيلها.")
        return
    meta = known_chats.setdefault(chat.id, {"title": chat.title or str(chat.id), "type": "group" if chat.type!=ChatType.CHANNEL else "channel"})
    admins = known_chats_admins.setdefault(chat.id, {})
    try:
        members = await context.bot.get_chat_administrators(chat.id)
        for m in members:
            admins[m.user.id] = m.user.full_name
    except Exception:
        pass
    await update.message.reply_text("✅ تم تسجيل هذه الوجهة في النظام.")
    save_state()

async def cmd_mychats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    ids = await list_authorized_chats(context, user.id)
    if not ids:
        await update.message.reply_text("لا توجد وجهات مرتبطة بك.")
        return
    lines = []
    for cid in ids:
        meta = known_chats.get(cid, {})
        title = meta.get("title", str(cid))
        typ = meta.get("type", "group")
        lines.append(f"• {title} — {'قناة' if typ=='channel' else 'مجموعة'}")
    await update.message.reply_text("وجهاتك:\n" + "\n".join(lines))

# =============================
# Webhook helpers & routes (single copy)
# =============================
def build_webhook_url() -> Optional[str]:
    if not BASE_URL or not WEBHOOK_SECRET:
        return None
    return f"{BASE_URL.rstrip('/')}/webhook/{WEBHOOK_SECRET}"

@app.get("/setup-webhook")
async def setup_webhook(request: Request):
    # حماية المفتاح
    key = request.query_params.get("key")
    if key != SETUP_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

    # بناء الرابط
    url = build_webhook_url()
    if not url:
        raise HTTPException(status_code=503, detail="RENDER_EXTERNAL_URL غير متاح حالياً")

    global _LAST_SET_TS
    async with _WEBHOOK_LOCK:
        # Debounce: لا تعاود خلال 5 ثواني
        now = time.monotonic()
        if now - _LAST_SET_TS < 5:
            logger.info("setup-webhook: debounced")
            return {"ok": True, "skipped": "debounced", "url": url}

        # إذا كان مضبوطًا أصلًا على نفس الرابط، لا تعيد الضبط
        try:
            info = await application.bot.get_webhook_info()
            if getattr(info, "url", "") == url:
                _LAST_SET_TS = now
                logger.info("setup-webhook: already set")
                return {"ok": True, "skipped": "already set", "url": url}
        except Exception as e:
            logger.warning("get_webhook_info failed: %s", e)

        # جرّب set_webhook + معالجة RetryAfter بإعادة واحدة
        try:
            ok = await application.bot.set_webhook(
                url=url,
                allowed_updates=None,
                drop_pending_updates=True,
            )
        except RetryAfter as e:
            await asyncio.sleep(getattr(e, "retry_after", 1) or 1)
            ok = await application.bot.set_webhook(
                url=url,
                allowed_updates=None,
                drop_pending_updates=True,
            )
        except (TimedOut, NetworkError) as e:
            raise HTTPException(status_code=503, detail=f"network: {e}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"unexpected: {e}")

        _LAST_SET_TS = time.monotonic()
        logger.info("Manual set_webhook %s -> %s", url, ok)
        return {"ok": ok, "url": url}

@app.get("/unset-webhook")
async def unset_webhook(request: Request):
    key = request.query_params.get("key")
    if key != SETUP_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")
    ok = await application.bot.delete_webhook(drop_pending_updates=False)
    logger.info("Manual delete_webhook -> %s", ok)
    return {"ok": ok}

@app.api_route("/", methods=["GET", "HEAD"])
async def root():
    return {"status": "ok", "service": "puok-bot"}

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/uptime")
async def uptime():
    return {"ok": True, "ts": datetime.utcnow().isoformat()}

ALLOWED_UPDATES = None

@app.post("/webhook/{secret}")
async def webhook_handler(secret: str, request: Request):
    if secret != WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")
    data = await request.json()
    update = Update.de_json(data, application.bot)
    await application.process_update(update)
    return {"ok": True}

# ========= Chat member tracking & control-panel button handlers (definitions come BEFORE registration) =========

async def handle_chat_member_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        chat = update.chat_member.chat
        _ = known_chats.setdefault(
            chat.id,
            {"title": chat.title or str(chat.id), "type": "group" if chat.type != ChatType.CHANNEL else "channel"}
        )
        admins = known_chats_admins.setdefault(chat.id, {})
        members = await context.bot.get_chat_administrators(chat.id)
        for m in members:
            admins[m.user.id] = m.user.full_name
    except Exception:
        pass
    save_state()

async def handle_my_member_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Keep for completeness; update caches if needed
    await handle_chat_member_update(update, context)

# ========= Auto-register chats on any group/channel message =========
async def _refresh_chat_admins(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    admins = known_chats_admins.setdefault(chat_id, {})
    try:
        members = await context.bot.get_chat_administrators(chat_id)
        for m in members:
            admins[m.user.id] = m.user.full_name
    except Exception:
        pass

async def auto_register_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    chat = update.effective_chat
    if not msg or not chat:
        return
    if chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP, ChatType.CHANNEL):
        return

    # خزّن الميتاداتا الأساسية (العنوان + النوع)
    meta = known_chats.setdefault(
        chat.id,
        {"title": chat.title or str(chat.id),
         "type": "channel" if chat.type == ChatType.CHANNEL else "group"}
    )
    # لو تغيّر العنوان لاحقًا، حدّثه
    if chat.title and meta.get("title") != chat.title:
        meta["title"] = chat.title

    # حدّث قائمة المدراء (إن توفرت صلاحيات البوت)
    await _refresh_chat_admins(context, chat.id)

    save_state()
    
# --- لوحات فرعية ---
def build_panel_chats_keyboard(admin_chat_ids: List[int], settings: Dict[str, Any]) -> InlineKeyboardMarkup:
    rows = []
    disabled: Set[int] = settings.setdefault("disabled_chats", set())
    for cid in admin_chat_ids:
        info = known_chats.get(cid, {})
        title = info.get("title", str(cid))
        typ = info.get("type", "group")
        label = "📢 قناة" if typ == "channel" else "👥 مجموعة"
        is_disabled = cid in disabled
        prefix = "🚫 " if is_disabled else "✅ "
        rows.append([InlineKeyboardButton(f"{prefix}{title} — {label}", callback_data=f"panel:toggle_chat:{cid}")])
    if admin_chat_ids:
        rows.append([InlineKeyboardButton("💾 حفظ الإعداد", callback_data="panel:dest_save")])
    rows.append([InlineKeyboardButton("⬅️ رجوع", callback_data="panel:back")])
    return InlineKeyboardMarkup(rows)

def permissions_root_keyboard() -> InlineKeyboardMarkup:
    rows = []
    for cid, info in known_chats.items():
        title = info.get("title", str(cid))
        badge = "📢" if info.get("type") == "channel" else "👥"
        rows.append([InlineKeyboardButton(f"{badge} {title}", callback_data=f"perm:chat:{cid}")])
    rows.append([InlineKeyboardButton("⬅️ رجوع", callback_data="panel:back")])
    return InlineKeyboardMarkup(rows)

def permissions_admins_keyboard(chat_id: int) -> InlineKeyboardMarkup:
    rows = []
    blocked = group_permissions.setdefault(chat_id, {}).setdefault("blocked_admins", set())
    admins = known_chats_admins.get(chat_id, {})
    for uid, name in admins.items():
        mark = "🚫" if uid in blocked else "✅"
        rows.append([InlineKeyboardButton(f"{mark} {name}", callback_data=f"perm:toggle:{chat_id}:{uid}")])
    rows.append([InlineKeyboardButton("💾 حفظ الإعداد", callback_data=f"perm:save:{chat_id}")])
    rows.append([InlineKeyboardButton("⬅️ رجوع", callback_data="panel:permissions")])
    return InlineKeyboardMarkup(rows)

# --- مساعدات إدارة الوجهات/المشرفين (مطابقة للمنطق القديم) ---
async def list_authorized_chats(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> List[int]:
    admin_chats: List[int] = []
    for cid in known_chats.keys():
        try:
            member = await context.bot.get_chat_member(cid, user_id)
            if member.status in ("administrator", "creator"):
                # استبعد المحظورين لهذه الوجهة
                blocked = group_permissions.setdefault(cid, {}).setdefault("blocked_admins", set())
                if user_id in blocked:
                    continue
                admin_chats.append(cid)
        except Exception:
            continue
    admin_chats.sort()
    return admin_chats

async def refresh_admins_for_chat(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    admins: Dict[int, str] = {}
    try:
        members = await context.bot.get_chat_administrators(chat_id=chat_id)
        for m in members:
            if m.user and not m.user.is_bot:
                admins[m.user.id] = m.user.full_name
    except Exception:
        pass
    known_chats_admins[chat_id] = admins
    return [{"id": uid, "name": name} for uid, name in sorted(admins.items(), key=lambda x: x[1]) ]

# --------------- المعالج المركزي لأزرار اللوحة ---------------
def panel_main_keyboard() -> InlineKeyboardMarkup:
    gs = global_settings
    rows = [
        [InlineKeyboardButton("📊 الإحصاءات", callback_data="panel:stats")],
        [InlineKeyboardButton("🗂️ الوجهات", callback_data="panel:destinations")],

        # ⏱️ الإعادة (قائمة فرعية فيها تشغيل/إيقاف + إيقاف الجاري + رجوع)
        [InlineKeyboardButton(
            f"⏱️ الإعادة: {'مفعّلة' if gs.get('scheduling_enabled', True) else 'معطّلة'}",
            callback_data="panel:schedule"
        )],

        # 👍 التفاعلات (قائمة فرعية فيها تشغيل/إيقاف + تعديل نص الدعوة + رجوع)
        [InlineKeyboardButton(
            f"👍 التفاعلات: {'مفعّلة' if gs.get('reactions_feature_enabled', True) else 'معطّلة'}",
            callback_data="panel:reactions"
        )],

        # 📌 التثبيت (قائمة فرعية فيها تشغيل/إيقاف + رجوع)
        [InlineKeyboardButton(
            f"📌 التثبيت: {'مفعّل' if gs.get('pin_feature_enabled', True) else 'معطّل'}",
            callback_data="panel:pin"
        )],

        [InlineKeyboardButton("🛡️ الأذونات (المشرفون)", callback_data="panel:permissions")],

        # 🛠️ وضع الصيانة (قائمة فرعية فيها تشغيل/إيقاف + رجوع)
        [InlineKeyboardButton(
            f"🛠️ وضع الصيانة: {'مفعل' if gs.get('maintenance_mode', False) else 'معطل'}",
            callback_data="panel:maintenance"
        )],
  
        [InlineKeyboardButton("🚪 خروج", callback_data="panel:exit")],
    ]
    return InlineKeyboardMarkup(rows)

async def cmd_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat.type != ChatType.PRIVATE:
        try:
            await update.message.reply_text("افتح لوحة التحكّم في الخاص.")
        except Exception:
            pass
        return
    panel_state[chat.id] = PANEL_MAIN
    try:
        await context.bot.send_message(
            chat_id=chat.id,
            text="🛠️ لوحة التحكّم الرئيسية",
            reply_markup=panel_main_keyboard()
        )
    except Exception:
        pass

async def _panel_replace(query, text: str, *, reply_markup=None, parse_mode=None):
    """
    يستبدل شاشة اللوحة الحالية (edit) بدل الرد برسالة جديدة.
    إن تعذّر التعديل، يرسل رسالة جديدة ويحذف القديمة.
    """
    # جرّب التعديل أولاً
    try:
        await query.message.edit_text(text=text, reply_markup=reply_markup, parse_mode=parse_mode)
        return
    except BadRequest as e:
        msg = str(e).lower()
        # لو النص نفسه، حاول تحديث الأزرار فقط
        if "message is not modified" in msg:
            try:
                await query.edit_message_reply_markup(reply_markup=reply_markup)
                return
            except Exception:
                pass
        # لو فشل التعديل لأي سبب، احذف القديمة وأرسل جديدة
    except Exception:
        pass

    # Fallback: احذف القديمة وأرسل جديدة
    try:
        await query.message.delete()
    except Exception:
        pass
    try:
        await query.message.reply_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
    except Exception:
        pass

async def handle_control_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query

    # أجب فورًا لتجنّب أخطاء "too old/invalid"
    try:
        await query.answer()
    except BadRequest as e:
        msg = str(e).lower()
        if "too old" in msg or "invalid" in msg:
            return
        raise
    except Exception:
        pass

    user_id = query.from_user.id
    data = query.data
    s = get_settings(user_id)

    # ====== الإحصاءات (قائمة الحملات + تفاصيل الحملة حسب الوجهة) ======
    if data == "panel:stats":
        ids = sorted(campaign_messages.keys())
        if not ids:
            try:
                await _panel_replace(query, "لا توجد حملات بعد.", reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🏠 القائمة الرئيسية", callback_data="panel:back")]
                ]))
            except Exception:
                pass
            return

        last_ids = list(reversed(ids))[:10]
        rows = [[InlineKeyboardButton(f"📦 الحملة #{cid}", callback_data=f"panel:stats:camp:{cid}")]
                for cid in last_ids]
        rows.append([InlineKeyboardButton("⬅️ رجوع", callback_data="panel:back")])
        try:
            await _panel_replace(
                query,
                "اختر حملة لعرض الإحصاءات حسب الوجهة:",
                reply_markup=InlineKeyboardMarkup(rows)
            )
        except Exception:
            pass
        return

    if data.startswith("panel:stats:camp:"):
        try:
            camp_id = int(data.split(":")[3])
        except Exception:
            return

        pairs = campaign_messages.get(camp_id, [])
        if not pairs:
            try:
                await _panel_replace(query, "لا توجد رسائل مسجّلة لهذه الحملة.",
                                     reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ رجوع للحملات", callback_data="panel:stats")],
                                                                         [InlineKeyboardButton("🏠 القائمة الرئيسية", callback_data="panel:back")]]))
            except Exception:
                pass
            return

        lines = [f"📊 إحصاءات الحملة #{camp_id} حسب الوجهة:"]
        used = set()
        for cid, _mid in pairs:
            if cid in used:
                continue
            used.add(cid)
            base_mid = campaign_base_msg.get((camp_id, cid))
            if not base_mid:
                like = dislike = 0
            else:
                rec = reactions_counters.get((cid, base_mid), {"like": 0, "dislike": 0})
                like = rec.get("like", 0)
                dislike = rec.get("dislike", 0)
            title = known_chats.get(cid, {}).get("title", str(cid))
            lines.append(f"• {title} — 👍 {like} / 👎 {dislike}")

        try:
            await _panel_replace(
                query,
                "\n".join(lines),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⬅️ رجوع للحملات", callback_data="panel:stats")],
                    [InlineKeyboardButton("🏠 القائمة الرئيسية", callback_data="panel:back")]
                ])
            )
        except Exception:
            pass
        return

    # ====== الوجهات ======
    if data == "panel:destinations":
        admin_chat_ids = await list_authorized_chats(context, user_id)
        if not admin_chat_ids:
            try:
                await _panel_replace(query, "لا توجد وجهات ظاهر أنك مشرف عليها. أضف البوت كمشرف ثم أعد المحاولة.",
                                     reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ رجوع", callback_data="panel:back")]]))
            except Exception:
                pass
            return
        kb = build_panel_chats_keyboard(admin_chat_ids, s)
        try:
            await _panel_replace(query, "🗂️ الوجهات — فعّل/عطّل لكل وجهة ثم احفظ:", reply_markup=kb)
        except Exception:
            pass
        return

    if data.startswith("panel:toggle_chat:"):
        try:
            cid = int(data.split(":", 2)[2])
        except Exception:
            return
        disabled = s.setdefault("disabled_chats", set())
        if cid in disabled:
            disabled.remove(cid)
        else:
            disabled.add(cid)
        save_state()
        admin_chat_ids = await list_authorized_chats(context, user_id)
        kb = build_panel_chats_keyboard(admin_chat_ids, s)
        try:
            await query.edit_message_reply_markup(reply_markup=kb)
            await query.answer("تم التبديل.")
        except Exception:
            pass
        return

    if data == "panel:dest_save":
        add_log(user_id, "حفظ إعداد الوجهات")
        save_state()
        try:
            await context.bot.send_message(chat_id=user_id, text="💾 تم حفظ إعداد الوجهات.")
        except Exception:
            pass
        return

    # ====== الإعادة (تشغيل/إيقاف + إيقاف الجاري/لكل وجهة) ======
    if data == "panel:schedule":
        gs = global_settings
        on = gs.get("scheduling_enabled", True)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"{'⏹️ إيقاف' if on else '▶️ تشغيل'} الإعادة", callback_data="panel:sched_toggle")],
            [InlineKeyboardButton("🟥 إيقاف الإعادات الجارية", callback_data="panel:sched_stop_menu")],
            [InlineKeyboardButton("⬅️ رجوع", callback_data="panel:back")],
        ])
        try:
            await _panel_replace(query, f"⏱️ الإعادة: *{'مفعّلة' if on else 'معطّلة'}*",
                                 reply_markup=kb, parse_mode="Markdown")
        except Exception:
            pass
        return

    if data == "panel:sched_toggle":
        gs = global_settings
        now = not gs.get("scheduling_enabled", True)
        gs["scheduling_enabled"] = now
        add_log(user_id, f"{'تشغيل' if now else 'إيقاف'} خدمة الجدولة مركزيًا")
        save_state()
        # أعِد فتح نفس القائمة بحالتها الجديدة
        on = gs.get("scheduling_enabled", True)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"{'⏹️ إيقاف' if on else '▶️ تشغيل'} الإعادة", callback_data="panel:sched_toggle")],
            [InlineKeyboardButton("🟥 إيقاف الإعادات الجارية", callback_data="panel:sched_stop_menu")],
            [InlineKeyboardButton("⬅️ رجوع", callback_data="panel:back")],
        ])
        try:
            await _panel_replace(query, f"✅ تم {'تشغيل' if now else 'إيقاف'} خدمة الجدولة.\n\n⏱️ الإعادة: *{'مفعّلة' if on else 'معطّلة'}*",
                                 reply_markup=kb, parse_mode="Markdown")
        except Exception:
            pass
        return

    if data == "panel:sched_stop_menu":
        uniq_cids = set()
        try:
            for job in context.application.job_queue.jobs():
                if job.name and job.name.startswith("rebroadcast_") and job.data:
                    uniq_cids.update(job.data.get("chosen_chats", []))
        except Exception:
            pass
        rows = []
        for cid in sorted(uniq_cids):
            title = known_chats.get(cid, {}).get("title", str(cid))
            rows.append([InlineKeyboardButton(f"⛔ إيقاف لِـ {title}", callback_data=f"panel:sched_stop:{cid}")])
        if rows:
            rows.insert(0, [InlineKeyboardButton("⛔ إيقاف جميع الإعادات", callback_data="panel:sched_stop:all")])
        else:
            rows = [[InlineKeyboardButton("لا توجد إعادات جارية", callback_data="panel:back")]]
        rows.append([InlineKeyboardButton("⬅️ رجوع", callback_data="panel:schedule")])
        try:
            await _panel_replace(query, "اختر وجهة لإيقاف الإعادات الجارية:", reply_markup=InlineKeyboardMarkup(rows))
        except Exception:
            pass
        return

    if data.startswith("panel:sched_stop:"):
        _, _, target = data.split(":")
        removed = 0
        try:
            for job in context.application.job_queue.jobs():
                if not (job.name and job.name.startswith("rebroadcast_") and job.data):
                    continue
                if target == "all" or int(target) in set(job.data.get("chosen_chats", [])):
                    try:
                        job.schedule_removal()
                        removed += 1
                    except Exception:
                        pass
            for name in list(active_rebroadcasts.keys()):
                if name.startswith("rebroadcast_"):
                    try:
                        del active_rebroadcasts[name]
                    except Exception:
                        pass
        except Exception:
            pass
        save_state()
        txt = "⛔ لا توجد إعادات لإيقافها." if removed == 0 else f"⛔ تم إيقاف {removed} مهمة إعادة."
        # أعِد فتح قائمة الإعادة مع إشعار
        gs = global_settings
        on = gs.get("scheduling_enabled", True)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"{'⏹️ إيقاف' if on else '▶️ تشغيل'} الإعادة", callback_data="panel:sched_toggle")],
            [InlineKeyboardButton("🟥 إيقاف الإعادات الجارية", callback_data="panel:sched_stop_menu")],
            [InlineKeyboardButton("⬅️ رجوع", callback_data="panel:back")],
        ])
        try:
            await _panel_replace(query, f"{txt}\n\n⏱️ الإعادة: *{'مفعّلة' if on else 'معطّلة'}*",
                                 reply_markup=kb, parse_mode="Markdown")
        except Exception:
            pass
        return

    # ====== التفاعلات ======
    if data == "panel:reactions":
        on = global_settings.get("reactions_feature_enabled", True)
        rows = [
            [InlineKeyboardButton(f"{'⏹️ إيقاف' if on else '▶️ تشغيل'} التفاعلات", callback_data="panel:react_toggle")],
            [InlineKeyboardButton("✏️ تعديل نص الدعوة", callback_data="panel:reactions:edit")],
            [InlineKeyboardButton("⬅️ رجوع", callback_data="panel:back")],
        ]
        try:
            await _panel_replace(query,
                                 f"👍 حالة التفاعلات: *{'مفعّلة' if on else 'معطّلة'}*",
                                 reply_markup=InlineKeyboardMarkup(rows),
                                 parse_mode="Markdown")
        except Exception:
            pass
        return

    if data == "panel:react_toggle":
        now = not global_settings.get("reactions_feature_enabled", True)
        global_settings["reactions_feature_enabled"] = now
        add_log(user_id, f"{'تشغيل' if now else 'إيقاف'} خدمة التفاعلات مركزيًا")
        save_state()
        on = global_settings.get("reactions_feature_enabled", True)
        rows = [
            [InlineKeyboardButton(f"{'⏹️ إيقاف' if on else '▶️ تشغيل'} التفاعلات", callback_data="panel:react_toggle")],
            [InlineKeyboardButton("✏️ تعديل نص الدعوة", callback_data="panel:reactions:edit")],
            [InlineKeyboardButton("⬅️ رجوع", callback_data="panel:back")],
        ]
        try:
            await _panel_replace(query,
                                 f"✅ تم {'تشغيل' if now else 'إيقاف'} التفاعلات.\n\n👍 الحالة: *{'مفعّلة' if on else 'معطّلة'}*",
                                 reply_markup=InlineKeyboardMarkup(rows),
                                 parse_mode="Markdown")
        except Exception:
            pass
        return

    if data == "panel:reactions:edit":
        panel_state[user_id] = PANEL_WAIT_REACTION_PROMPT
        save_state()
        try:
            await context.bot.send_message(chat_id=user_id, text="أرسل الآن نص دعوة التفاعل الجديد.")
        except Exception:
            pass
        return

    # ====== التثبيت ======
    if data == "panel:pin":
        on = global_settings.get("pin_feature_enabled", True)
        rows = [
            [InlineKeyboardButton(f"{'⏹️ إيقاف' if on else '▶️ تشغيل'} التثبيت", callback_data="panel:pin_toggle")],
            [InlineKeyboardButton("⬅️ رجوع", callback_data="panel:back")],
        ]
        try:
            await _panel_replace(query,
                                 f"📌 حالة التثبيت: *{'مفعّل' if on else 'معطّل'}*",
                                 reply_markup=InlineKeyboardMarkup(rows),
                                 parse_mode="Markdown")
        except Exception:
            pass
        return

    if data == "panel:pin_toggle":
        now = not global_settings.get("pin_feature_enabled", True)
        global_settings["pin_feature_enabled"] = now
        add_log(user_id, f"{'تشغيل' if now else 'إيقاف'} خدمة التثبيت مركزيًا")
        save_state()
        on = global_settings.get("pin_feature_enabled", True)
        rows = [
            [InlineKeyboardButton(f"{'⏹️ إيقاف' if on else '▶️ تشغيل'} التثبيت", callback_data="panel:pin_toggle")],
            [InlineKeyboardButton("⬅️ رجوع", callback_data="panel:back")],
        ]
        try:
            await _panel_replace(query,
                                 f"✅ تم {'تشغيل' if now else 'إيقاف'} التثبيت.\n\n📌 الحالة: *{'مفعّل' if on else 'معطّل'}*",
                                 reply_markup=InlineKeyboardMarkup(rows),
                                 parse_mode="Markdown")
        except Exception:
            pass
        return

    # ====== الأذونات ======
    if data == "panel:permissions":
        if not known_chats:
            try:
                await _panel_replace(query, "لا توجد وجهات معروفة بعد.",
                                     reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ رجوع", callback_data="panel:back")]]))
            except Exception:
                pass
            return
        try:
            await _panel_replace(query, "اختر وجهة لإدارة مشرفيها:", reply_markup=permissions_root_keyboard())
        except Exception:
            pass
        return

    if data.startswith("perm:chat:"):
        cid = int(data.split(":", 2)[2])
        admins = await refresh_admins_for_chat(context, cid)
        blocked = group_permissions.setdefault(cid, {}).setdefault("blocked_admins", set())
        rows = []
        for adm in admins:
            uid = adm["id"]; name = adm["name"]
            is_blocked = (uid in blocked)
            label = f"{'🚫' if is_blocked else '✅'} {name}"
            rows.append([InlineKeyboardButton(label, callback_data=f"perm:toggle:{cid}:{uid}")])
        rows.append([InlineKeyboardButton(f"💾 حفظ ({known_chats.get(cid, {}).get('title', cid)})", callback_data=f"perm:save:{cid}")])
        rows.append([InlineKeyboardButton("⬅️ رجوع", callback_data="panel:permissions")])
        try:
            await _panel_replace(query, "👤 مشرفو الوجهة المحددة:\n(فعّل/عطّل ثم احفظ)", reply_markup=InlineKeyboardMarkup(rows))
        except Exception:
            pass
        return

    if data.startswith("perm:toggle:"):
        _, _, chat_id, uid = data.split(":")
        chat_id = int(chat_id); uid = int(uid)
        blocked = group_permissions.setdefault(chat_id, {}).setdefault("blocked_admins", set())
        now_blocked = None
        if uid in blocked:
            blocked.remove(uid)
            now_blocked = False
        else:
            blocked.add(uid)
            now_blocked = True
        save_state()

        # إعادة بناء الكيبورد فورًا
        admins = await refresh_admins_for_chat(context, chat_id)
        rows = []
        for adm in admins:
            a_uid = adm["id"]; name = adm["name"]
            is_blocked = (a_uid in blocked)
            label = f"{'🚫' if is_blocked else '✅'} {name}"
            rows.append([InlineKeyboardButton(label, callback_data=f"perm:toggle:{chat_id}:{a_uid}")])
        rows.append([InlineKeyboardButton(f"💾 حفظ ({known_chats.get(chat_id, {}).get('title', chat_id)})", callback_data=f"perm:save:{chat_id}")])
        rows.append([InlineKeyboardButton("⬅️ رجوع", callback_data="panel:permissions")])
        try:
            await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(rows))
        except Exception:
            pass

        # إشعار باسم المشرف والمجموعة
        try:
            name = next((adm["name"] for adm in admins if adm["id"] == uid), str(uid))
        except Exception:
            name = str(uid)
        title = known_chats.get(chat_id, {}).get("title", str(chat_id))
        msg = f"✅ تم {'إيقاف' if now_blocked else 'تفعيل'} صلاحية {name} بمجموعة {title}"
        try:
            await context.bot.send_message(chat_id=user_id, text=msg)
        except Exception:
            pass
        return

    if data.startswith("perm:save:"):
        _, _, chat_id = data.split(":")
        chat_id = int(chat_id)
        add_log(user_id, "حفظ إعداد أذونات مشرفي وجهة")
        save_state()
        title = known_chats.get(chat_id, {}).get("title", str(chat_id))

        blocked = sorted(group_permissions.setdefault(chat_id, {}).setdefault("blocked_admins", set()))
        names = []
        try:
            admins = await refresh_admins_for_chat(context, chat_id)
            m = {a["id"]: a["name"] for a in admins}
            names = [m.get(uid, str(uid)) for uid in blocked]
        except Exception:
            names = [str(uid) for uid in blocked]

        summary = f"💾 تم حفظ أذونات المشرفين للوجهة: {title}"
        if names:
            summary += "\n🚫 الموقوفون: " + ", ".join(names)
        try:
            await context.bot.send_message(chat_id=user_id, text=summary)
        except Exception:
            pass
        return

    # ====== وضع الصيانة ======
    if data == "panel:maintenance":
        on = global_settings.get("maintenance_mode", False)
        rows = [
            [InlineKeyboardButton(f"{'⏹️ إيقاف' if on else '▶️ تشغيل'} وضع الصيانة", callback_data="panel:maint_toggle")],
            [InlineKeyboardButton("⬅️ رجوع", callback_data="panel:back")],
        ]
        try:
            await _panel_replace(query, f"🛠️ حالة الوضع: *{'مفعل' if on else 'معطل'}*",
                                 reply_markup=InlineKeyboardMarkup(rows), parse_mode="Markdown")
        except Exception:
            pass
        return

    if data == "panel:maint_toggle":
        global_settings["maintenance_mode"] = not global_settings.get("maintenance_mode", False)
        on = global_settings["maintenance_mode"]
        add_log(user_id, f"{'تفعيل' if on else 'إلغاء'} وضع الصيانة")
        note = "🛠️ نظام النشر تحت الصيانة والتحديث حاليًا." if on else "✅ تم إلغاء وضع الصيانة. عاد النظام للعمل."
        for uid in list(sessions.keys()):
            try: await context.bot.send_message(chat_id=uid, text=note)
            except Exception: pass
        for uid, rec in list(temp_grants.items()):
            if rec and not rec.get("used"):
                try: await context.bot.send_message(chat_id=uid, text=note)
                except Exception: pass
        save_state()
        rows = [
            [InlineKeyboardButton(f"{'⏹️ إيقاف' if on else '▶️ تشغيل'} وضع الصيانة", callback_data="panel:maint_toggle")],
            [InlineKeyboardButton("⬅️ رجوع", callback_data="panel:back")],
        ]
        try:
            await _panel_replace(query, f"تم {'تفعيل' if on else 'إيقاف'} وضع الصيانة.",
                                 reply_markup=InlineKeyboardMarkup(rows))
        except Exception:
            pass
        return

    # ====== خروج نهائي من اللوحة ======
    if data == "panel:exit":
        try:
            panel_state.pop(user_id, None)
        except Exception:
            pass
        # حذف رسالة اللوحة الحالية أو إزالة أزرارها
        try:
            await context.bot.delete_message(chat_id=query.message.chat_id, message_id=query.message.message_id)
        except Exception:
            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass
        try:
            await context.bot.send_message(chat_id=user_id, text="👋 تم الخروج من لوحة التحكّم.")
        except Exception:
            pass
        return

    # ====== رجوع إلى القائمة الرئيسية ======
    if data == "panel:back":
        try:
            await _panel_replace(query, "🛠️ لوحة التحكّم الرئيسية", reply_markup=panel_main_keyboard())
        except Exception:
            pass
        return

async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await update.message.reply_text("pong")
    except Exception:
        pass

async def send_maintenance_notice(bot, chat_id: int):
    caption = "🛠️ نظام النشر تحت الصيانة والتحديث حاليًا."
    for p in ("/mnt/data/nook.jpg", "nook.jpg"):
        try:
            with open(p, "rb") as f:
                await bot.send_photo(chat_id=chat_id, photo=f, caption=caption)
                return
        except Exception:
            continue
    # fallback بدون صورة
    try:
        await bot.send_message(chat_id=chat_id, text=caption)
    except Exception:
        pass

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    # يسجّل الاستثناء مع الستاك
    logger.exception("Unhandled exception", exc_info=context.error)

    # (اختياري) تنبيه خفيف للمستخدم إن كان في خاص
    try:
        chat = getattr(update, "effective_chat", None)
        if chat and chat.type == ChatType.PRIVATE:
            await context.bot.send_message(chat_id=chat.id, text="⚠️ حدث خطأ غير متوقع. تم تسجيله.")
    except Exception:
        pass    
# =============================
# Handlers registration (ordered AFTER definitions)
# =============================
def register_handlers():
    # ========= أوامر أساسية =========
    application.add_handler(CommandHandler("start", start_with_token))
    application.add_handler(CommandHandler("ok", cmd_temp_ok))
    application.add_handler(CommandHandler("register", cmd_register))
    application.add_handler(CommandHandler("mychats", cmd_mychats))
    application.add_handler(CommandHandler("panel", cmd_panel))
    application.add_handler(CommandHandler("ping", cmd_ping))

    # ========= تحديثات العضوية =========
    application.add_handler(ChatMemberHandler(handle_chat_member_update, ChatMemberHandler.CHAT_MEMBER))
    application.add_handler(ChatMemberHandler(handle_my_member_update, ChatMemberHandler.MY_CHAT_MEMBER))

    # ========= Auto-register لأي رسالة في المجموعات/القنوات =========
    application.add_handler(
        MessageHandler(
            (filters.ChatType.GROUPS | filters.ChatType.CHANNEL) & ~filters.COMMAND,
            auto_register_chat
        ),
        group=0,
    )

    # ========= أزرار Callback (الأكثر تحديدًا أولًا) =========
    # 1) أزرار التفاعلات
    application.add_handler(
        CallbackQueryHandler(handle_reactions, pattern=r"^(like|dislike):", block=True),
        group=0,
    )
    # 2) أزرار لوحة الحملة (إحصاءات/إيقاف إعادة)
    application.add_handler(
        CallbackQueryHandler(handle_campaign_buttons, pattern=r"^(show_stats|stop_rebroadcast):", block=True),
        group=0,
    )
    # 3) لوحة التحكّم + أذونات المشرفين
    application.add_handler(
        CallbackQueryHandler(handle_control_buttons, pattern=r"^(panel:|perm:)", block=True),
        group=1,
    )
    # 4) أي أزرار أخرى (catch-all) — آخر شيء
    application.add_handler(
        CallbackQueryHandler(on_button, block=True),
        group=99,
    )

    # ========= نص "ok" في الخاص — يجب أن يكون قبل هاندلر الخاص العام =========
    application.add_handler(
        MessageHandler(
            filters.ChatType.PRIVATE & filters.TEXT & filters.Regex(OK_REGEX),
            start_publishing_keyword,
            block=True,   # يمنع تكرار المعالجة ويوقف تمرير نفس التحديث
        ),
        group=0,
    )

    # ========= استقبال محتوى/نصوص في الخاص (آخر واحد) =========
    application.add_handler(
        MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND, handle_admin_input),
        group=50,
    )

    # ========= Error handler =========
    application.add_error_handler(error_handler)

# =============================
# Lifecycle
# =============================
@app.on_event("startup")
async def on_startup():
    load_state()
    register_handlers()
    await application.initialize()
    try:
        await application.bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        pass
    url = build_webhook_url()
    if url:
        ok = await application.bot.set_webhook(url=url, allowed_updates=None, drop_pending_updates=True)
        logger.info("Webhook set to: %s -> %s", url, ok)
    else:
        logger.warning("RENDER_EXTERNAL_URL غير متاح… لن يتم ضبط الويبهوك الآن.")
    await application.start()

    # Restore rebroadcast jobs
    try:
        jq = application.job_queue
        for name, rec in active_rebroadcasts.items():
            interval = int(rec.get("interval", 7200))
            payload = rec.get("payload", {})
            try:
                jq.run_repeating(rebroadcast_job, interval=interval, first=max(30, interval), data=payload, name=name)
            except Exception:
                logger.exception("Failed to restore job %s", name)
    except Exception:
        logger.exception("restore jobs failed")

    # Autosave
    try:
        asyncio.create_task(autosave_loop())
    except Exception:
        logger.exception("failed to start autosave_loop")

@app.on_event("shutdown")
async def on_shutdown():
    try:
        save_state()
    except Exception:
        pass
    try:
        await application.bot.delete_webhook(drop_pending_updates=False)
    except Exception:
        pass
    await application.stop()
    await application.shutdown()
