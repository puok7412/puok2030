
import os
import logging
import re
import asyncio
import secrets
import html
import json
from fastapi import FastAPI, Request, HTTPException
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Set, Any
from telegram.error import BadRequest, RetryAfter, TimedOut, NetworkError

from telegram import (
    Update,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    InputMediaPhoto,
    InputMediaVideo,
    ChatMemberUpdated,
)
from telegram.constants import ChatType
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

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", secrets.token_urlsafe(16))
BASE_URL = os.getenv("RENDER_EXTERNAL_URL")  # may be None on first deploy
PORT = int(os.getenv("PORT", "10000"))
SETUP_KEY = os.getenv("SETUP_KEY", WEBHOOK_SECRET)

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

# أوامر الكلمات المفتاحية في الخاص
OK_REGEX   = re.compile(r'(?i)^\s*ok\s*$')        # بدء جلسة النشر
# مسار الحفظ (قابل للكتابة على Render)
STATE_PATH = os.getenv("STATE_PATH", "/tmp/publisher_state.json")

# باكِتات تستخدم مفاتيح tuple وتحتاج تحويلًا خاصًا وقت الحفظ/التحميل
_TUPLEKEY_BUCKETS = {
    "reactions_counters": True,
    "campaign_base_msg": True,
    "message_to_campaign": True,
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
    "start_tokens",  # مهم للتصريح المؤقت عبر /ok
]

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

    # 🧪 صف الدقائق للاختبار
    rows.append([
        InlineKeyboardButton("🧪 1د", callback_data="ssched_int:60"),
        InlineKeyboardButton("2د",   callback_data="ssched_int:120"),
        InlineKeyboardButton("5د",   callback_data="ssched_int:300"),
    ])

    # ⏱️ صف الساعات
    rows.append([
        InlineKeyboardButton("2س", callback_data="ssched_int:7200"),
        InlineKeyboardButton("4س", callback_data="ssched_int:14400"),
        InlineKeyboardButton("6س", callback_data="ssched_int:21600"),
        InlineKeyboardButton("12س", callback_data="ssched_int:43200"),
    ])

    # 🔁 عدد مرات الإعادة
    rows.append([
        InlineKeyboardButton("1×", callback_data="ssched_total:1"),
        InlineKeyboardButton("2×", callback_data="ssched_total:2"),
        InlineKeyboardButton("4×", callback_data="ssched_total:4"),
        InlineKeyboardButton("8×", callback_data="ssched_total:8"),
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

def keyboard_ready_options(sess: Session, settings: Dict[str, Any]) -> InlineKeyboardMarkup:
    btns: List[List[InlineKeyboardButton]] = []
    if settings.get("default_reactions_enabled", True):
        btns.append([
            InlineKeyboardButton(
                "👍 إضافة تفاعلات" if not sess.use_reactions else "🧹 إزالة التفاعلات",
                callback_data="toggle_reactions"
            )
        ])
    if not (sess.is_temp_granted and sess.allowed_chats):
        btns.append([InlineKeyboardButton("🗂️ اختيار الوجهات (مجموعات/قنوات)", callback_data="choose_chats")])
    if global_settings.get("scheduling_enabled", True):
        if global_settings.get("schedule_locked", False):
            btns.append([InlineKeyboardButton(
                f"⏱️ الإعادة (مقفلة): كل {global_settings['rebroadcast_interval_seconds']//3600} ساعة × {global_settings['rebroadcast_total']}",
                callback_data="noop"
            )])
        else:
            label = "⏱️ تفعيل الجدولة" if not getattr(sess, "schedule_active", False) else "⏱️ الإعادة: مفعّلة"
            btns.append([InlineKeyboardButton(label, callback_data="schedule_menu")])
    btns.append([InlineKeyboardButton(
        "📌 تفعيل التثبيت" if not getattr(sess, "pin_enabled", True) else "📌 تعطيل التثبيت",
        callback_data="toggle_pin"
    )])
    btns.append([InlineKeyboardButton("⬅️ الرجوع للتعديل", callback_data="back_to_collect")])
    btns.append([InlineKeyboardButton("🧽 مسح", callback_data="clear"),
                 InlineKeyboardButton("❌ إنهاء", callback_data="cancel")])
    btns.append([InlineKeyboardButton("👁️ معاينة", callback_data="preview")])
    return InlineKeyboardMarkup(btns)

# =============================
# Panel helpers
# =============================
def status_text(sess: Session) -> str:
    parts: List[str] = []
    if sess.text:
        parts.append("📝 نص موجود")
    if sess.media_list:
        parts.append(f"🖼️ وسائط: {len(sess.media_list)}")
    if sess.single_attachment:
        t = sess.single_attachment[0]
        mapping = {"document": "مستند", "audio": "ملف صوتي", "voice": "رسالة صوتية"}
        parts.append(f"📎 {mapping.get(t, t)}")
    parts.append(f"{'✅' if sess.use_reactions else '🚫'} التفاعلات")
    parts.append(f"{'📌 مفعّل' if getattr(sess, 'pin_enabled', True) else '📌 معطّل'} التثبيت")
    chosen_cnt = len(getattr(sess, "chosen_chats", set()))
    parts.append(f"🎯 الوجهات: {chosen_cnt}")
    if getattr(sess, "schedule_active", False):
        secs = int(getattr(sess, "rebroadcast_interval_seconds", 0) or 0)
        hrs = (secs + 3599) // 3600 if secs > 0 else 0
        parts.append(f"⏱️ الإعادة: كل {hrs} ساعة × {getattr(sess, 'rebroadcast_total', 0)}")
    else:
        parts.append("⏱️ الإعادة: غير مفعّلة")
    return " • ".join(parts)

async def push_panel(context: ContextTypes.DEFAULT_TYPE, chat_id: int, sess: Session, header_text: str):
    if sess.panel_msg_id:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=sess.panel_msg_id)
        except Exception:
            pass
    kb = keyboard_collecting() if sess.stage in ("waiting_first_input","collecting") else keyboard_ready_options(sess, get_settings(chat_id))
    text = f"📋 *لوحة المنشور*\\n{header_text}\\n\\n{status_text(sess)}"
    m = await context.bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown", reply_markup=kb)
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

async def start_publishing_session(user, context: ContextTypes.DEFAULT_TYPE):
    user_id = user.id
    s = get_settings(user_id)
    sess = Session(
        stage="waiting_first_input",
        use_reactions=s.get('default_reactions_enabled', True),
        rebroadcast_interval_seconds=global_settings['rebroadcast_interval_seconds'],
        rebroadcast_total=global_settings['rebroadcast_total'],
        schedule_active=False
    )
    g = temp_grants.get(user_id)
    if g and not g.get("used") and (not g.get("expires") or datetime.utcnow() <= g["expires"]):
        sess.allowed_chats = {g["chat_id"]}
        sess.is_temp_granted = True
        sess.granted_by = g.get("granted_by")
        sess.chosen_chats = set(sess.allowed_chats)
    sessions[user_id] = sess
    welcome = (
        f"👋 أهلاً *{user.full_name}*\\n\\n"
        "أنت الآن في *وضع النشر*.\\n"
        "1) اكتب نص المنشور أو أرسل صورة/فيديو/ألبوم/ملف/صوت/فويس — *أي ترتيب*.\\n"
        "2) بعد أول إدخال سأؤكد الحفظ، وستظهر لوحة الخيارات في الأسفل.\\n"
    )
    await context.bot.send_message(chat_id=user_id, text=welcome, parse_mode="Markdown")
    save_state()

async def start_publishing_keyword(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != ChatType.PRIVATE:
        return
    user = update.effective_user
    user_id = user.id
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
    token = secrets.token_urlsafe(16)
    expires = datetime.utcnow() + timedelta(minutes=GRANT_TTL_MINUTES)
    start_tokens[token] = {"user_id": target.id, "chat_id": chat.id, "expires": expires}
    temp_grants[target.id] = {"chat_id": chat.id, "expires": expires, "used": False, "granted_by": granter.id}
    me = await context.bot.get_me()
    deep_link_tg = f"tg://resolve?domain={me.username}&start={token}"
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🚀 ابدأ النشر الآن", url=deep_link_tg)]])
    text_html = (
        f"✅ تم منح تصريح نشر مؤقّت لـ {target.mention_html()} في هذه المجموعة.\\n"
        f"⏳ صالح حتى {ksa_time(expires)}.\\n\\n"
        f"✨ المزايا:\\n"
        f"• نظام نشر تفاعلي مع إحصاءات مباشرة.\\n"
        f"• إمكانية التثبيت طوال فترة الإعادة.\\n\\n"
        f"اضغط الزر لبدء الجلسة في الخاص."
    )
    try:
        await update.message.reply_to_message.reply_html(text_html, reply_markup=kb)
    except Exception:
        await update.message.reply_html(text_html, reply_markup=kb)
    try:
        await context.bot.send_message(
            chat_id=target.id,
            text=(
                f"👋 أهلاً {html.escape(target.full_name)}\\n\\n"
                f"تم منحك تصريح نشر مؤقّت في مجموعة: {html.escape(chat.title or str(chat.id))}\\n"
                f"⏳ التصريح صالح حتى {ksa_time(expires)}.\\n\\n"
                f"اضغط الزر بالأسفل لبدء الجلسة الآن."
            ),
            parse_mode="HTML",
            reply_markup=kb
        )
    except Exception:
        pass
    save_state()

async def start_with_token(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != ChatType.PRIVATE:
        return
    user = update.effective_user
    full_name = user.full_name
    args = context.args or []
    if not args:
        await update.message.reply_text(
            f"👋 أهلاً بك {full_name}!\\nبنظام النشر التفاعلي.\\n\\n"
            "• في المجموعات: اطلب من المشرف استخدام /ok بالردّ على رسالتك لمنحك تصريح مؤقّت.\\n"
            "• في الخاص: اكتب المحتوى ثم اضغط «تم» من اللوحة عندما تبدأ الجلسة.\\n\\n"
            "ادارة النشر ( ok )"
        )
        return
    token = args[0]
    rec = start_tokens.get(token)
    if not rec:
        await update.message.reply_text("⛔ رابط غير صالح أو منتهي.")
        return
    if user.id != rec["user_id"]:
        await update.message.reply_text("⛔ هذا الرابط ليس لك اطلب من المشرفين منحك صلاحية نشر مؤقت.")
        return
    if datetime.utcnow() > rec["expires"]:
        await update.message.reply_text("⌛ انتهت صلاحية الرابط.")
        start_tokens.pop(token, None)
        temp_grants.pop(user.id, None)
        save_state()
        return
    await start_publishing_session(user, context)
    await update.message.reply_text(
        f"🎯 أهلاً بك {full_name}!\\n"
        "ملاحظة: إذن النشر مؤقت ومقيد بهذه المجموعة فقط. سيُسحب بعد أول نشر."
    )

# =============================
# Input collection
# =============================
def build_next_hint(sess: Session, saved_type: str) -> str:
    has_text = bool(sess.text and sess.text.strip())
    has_media = bool(sess.media_list)
    has_attach = bool(sess.single_attachment)
    can_add = []
    if not has_text: can_add.append("نص")
    if not has_media: can_add.append("صور/فيديو (ألبوم أو مفرد)")
    if not has_attach: can_add.append("ملف/صوت/فويس")
    return f"يمكنك إضافة: *{', '.join(can_add)}* — أو اضغط *تم* للانتقال للخيارات." if can_add else "كل شيء جاهز — اضغط *تم* للانتقال للخيارات."

async def handle_admin_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat.type != ChatType.PRIVATE:
        return

    user_id = update.effective_user.id
    s = get_settings(user_id)
    if s["permissions_mode"] == "whitelist" and user_id not in s["whitelist"]:
        await update.message.reply_text("🔒 النشر متاح لأعضاء القائمة البيضاء فقط.")
        return

    sess = sessions.get(user_id)
    msg = update.message
    saved_type: Optional[str] = None

    # --- سلوك ok / ok25s في الخاص ---
    if msg.text:
        raw = msg.text.strip().lower()
        if raw == "ok":
            # يمر إلى الهاندلر المخصص لبدء الجلسة (start_publishing_keyword)
            return
        if raw == "ok25s":
            # افتح لوحة التحكّم المركزية فقط، بدون بدء جلسة نشر
            await cmd_panel(update, context)
            return
    # --- نهاية سلوك ok / ok25s ---

    if not sess or sess.stage not in ("waiting_first_input", "collecting", "ready_options", "choosing_chats"):
        return

    if msg.text and not msg.text.strip():
        return

    if msg.text and not msg.media_group_id:
        clean = sanitize_text(msg.text)
        if clean.strip():
            sess.text = f"{sess.text}\n{clean}" if sess.text else clean
            saved_type = "النص"

    if msg.media_group_id and (msg.photo or msg.video):
        if msg.photo:
            sess.media_list.append(("photo", msg.photo[-1].file_id, sanitize_text(msg.caption) if msg.caption else None))
            saved_type = "صورة ضمن ألبوم"
        elif msg.video:
            sess.media_list.append(("video", msg.video.file_id, sanitize_text(msg.caption) if msg.caption else None))
            saved_type = "فيديو ضمن ألبوم"

    if msg.photo and not msg.media_group_id:
        sess.media_list.append(("photo", msg.photo[-1].file_id, sanitize_text(msg.caption) if msg.caption else None))
        saved_type = "صورة"

    if msg.video and not msg.media_group_id:
        sess.media_list.append(("video", msg.video.file_id, sanitize_text(msg.caption) if msg.caption else None))
        saved_type = "فيديو"

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

    # تجاهل الأزرار التي تملك هاندلراتها الخاصة
    if data.startswith(("like:", "dislike:", "panel:", "perm:", "show_stats:", "stop_rebroadcast:")):
        return

    # أجب فورًا لتجنّب أخطاء "Query is too old/invalid"
    try:
        await query.answer()
    except BadRequest as e:
        msg = str(e).lower()
        if "too old" in msg or "invalid" in msg:
            return
        # غير ذلك: دعها تمرّ لتبان باللوج
        pass
    except Exception:
        pass

    user_id = query.from_user.id
    s = get_settings(user_id)
    sess = sessions.get(user_id)

    # ==== تشخيص: سجل كل ضغط زر في اللوج (من النسخة القديمة) ====
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

    # لا جلسة
    if not sess:
        await query.message.reply_text("⚠️ لا توجد جلسة حالية. أرسل ok لبدء جلسة نشر جديدة.")
        return

    # زر تم
    if data == "done":
        if getattr(sess, "is_temp_granted", False) and getattr(sess, "allowed_chats", set()):
            sess.chosen_chats = set(sess.allowed_chats)
        sess.stage = "ready_options"
        await push_panel(context, user_id, sess, "🎛️ خيارات المنشور")
        save_state()
        return

    # رجوع لمرحلة التجميع
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

    # تبديل التفاعلات
    if data == "toggle_reactions":
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
            await query.message.reply_text("⏹️ تم إيقاف الجدولة مركزيًا بواسطة المسئول.")
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
        # في حالة التصريح المؤقت: لا قائمة — نثبت الوجهة تلقائيًا
        if sess.is_temp_granted and sess.allowed_chats:
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

        # استبعد المُعطّل مركزيًا من لوحة المسؤول لهذا المستخدم
        s = get_settings(user_id)
        active_ids = [cid for cid in admin_chat_ids if cid not in s["disabled_chats"]]
        if not active_ids:
            await query.message.reply_text("🚫 كل الوجهات المصرّح بها معطّلة حاليًا من لوحة التحكّم.")
            return

        sess.stage = "choosing_chats"
        # استخدام الباني القياسي من النسخة القديمة لثبات الـ UX
        m = await query.message.reply_text(
            "🗂️ اختر الوجهات المستهدفة (يُظهر النوع: 👥 مجموعة / 📢 قناة):",
            reply_markup=build_chats_keyboard(active_ids, sess.chosen_chats, s)
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
        if sess.is_temp_granted and (cid not in sess.allowed_chats):
            await query.answer("هذه الصلاحية مقيدة بالوجهات الممنوحة فقط.", show_alert=True)
            return

        if cid in sess.chosen_chats:
            sess.chosen_chats.remove(cid)
        else:
            sess.chosen_chats.add(cid)

        # إعادة البناء من المصدر الصحيح
        if sess.is_temp_granted:
            source_ids = list(sess.allowed_chats)
        else:
            source_ids = await list_authorized_chats(context, user_id)

        s = get_settings(user_id)
        active_ids = [i for i in source_ids if i not in s["disabled_chats"]]
        await query.edit_message_reply_markup(reply_markup=build_chats_keyboard(active_ids, sess.chosen_chats, s))
        save_state()
        return

    if data == "select_all":
        if sess.stage != "choosing_chats":
            return
        if sess.is_temp_granted:
            source_ids = list(sess.allowed_chats)
        else:
            source_ids = await list_authorized_chats(context, user_id)

        s = get_settings(user_id)
        sess.chosen_chats = set(i for i in source_ids if i not in s["disabled_chats"])
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

    # نشر (لا يحجب – أسلوب النسخة الجديدة) مع الحفاظ على توليد campaign_id
    if data == "publish":
        # في حالة التصريح: اجبر الوجهة على المجموعة الممنوحة
        if sess.is_temp_granted and sess.allowed_chats:
            sess.chosen_chats = set(sess.allowed_chats)

        if not sess.chosen_chats:
            await query.message.reply_text(
                "⚠️ لا توجد وجهة للنشر.\nيرجى اختيار مجموعة أو قناة من «اختيار الوجهات».",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ رجوع", callback_data="back_main")]])
            )
            return

        # احترام الضبط المركزي للتفاعلات والجدولة
        s = get_settings(user_id)
        sess.use_reactions = bool(sess.use_reactions) and bool(s.get("default_reactions_enabled", True))
        allow_schedule = bool(global_settings.get("scheduling_enabled", True))

        # توليد معرف الحملة أول مرة
        if sess.campaign_id is None:
            sess.campaign_id = new_campaign_id()
            campaign_messages[sess.campaign_id] = []
        save_state()

        # تشغيل النشر كـ Task وعدم الحجب
        asyncio.create_task(_publish_and_report(context, user_id, sess, allow_schedule, s.get("hide_links_default", False)))
        await query.message.reply_text("🚀 بدأ النشر… سأرسل لك النتائج قريبًا.")
        return

async def _publish_and_report(context, user_id, sess, allow_schedule, hide_links):
    result = await publish_to_chats(context, user_id, sess, is_rebroadcast=False, hide_links=hide_links)
    sent_count, errors = result if isinstance(result, tuple) else (0, ["تعذّر تحديد نتيجة النشر."])
    result_text = f"✅ تم النشر في {sent_count} وجهة." if sent_count else "⚠️ لم يتم النشر في أي وجهة."
    if errors: result_text += "\\n\\n" + "\\n".join(errors)
    await context.bot.send_message(chat_id=user_id, text=result_text + "\\n\\n" + build_status_block(sess, allow_schedule=allow_schedule))
    await send_campaign_panel(context, user_id, sess.campaign_id, also_to=sess.granted_by)
    if sent_count > 0 and allow_schedule and getattr(sess, 'schedule_active', False):
        await schedule_rebroadcast(context.application, user_id, sess, interval_seconds=sess.rebroadcast_interval_seconds, total_times=sess.rebroadcast_total)
    g = temp_grants.get(user_id)
    if g: g["used"] = True
    sessions.pop(user_id, None)
    save_state()

def build_status_block(sess: Session, *, allow_schedule: bool) -> str:
    parts = [status_text(sess)]
    if allow_schedule and getattr(sess, "schedule_active", False):
        parts.append("سيتم جدولة الإعادات بعد النشر.")
    return "\\n".join(parts)

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
        chat_id = int(chat_id_str); message_id = int(message_id_str)
    except Exception:
        return
    rec = reactions_counters.setdefault((chat_id, message_id), {"like": 0, "dislike": 0, "voters": {}})
    user = query.from_user
    voters = rec["voters"]
    campaign_id = message_to_campaign.get((chat_id, message_id))
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
    if action == "like": rec["like"] += 1
    else: rec["dislike"] += 1
    voters[user.id] = action
    if campaign_id is not None:
        if action == "like": campaign_counters[campaign_id]["like"] += 1
        else: campaign_counters[campaign_id]["dislike"] += 1
        campaign_counters[campaign_id]["voters"][user.id] = action
        try:
            await _sync_campaign_keyboards(context, campaign_id)
        except Exception:
            pass
    else:
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton(f"👍 {rec['like']}", callback_data=f"like:{chat_id}:{message_id}"),
            InlineKeyboardButton(f"👎 {rec['dislike']}", callback_data=f"dislike:{chat_id}:{message_id}"),
        ]])
        try:
            await query.edit_message_reply_markup(reply_markup=kb)
        except Exception:
            pass
    try:
        await query.answer("تم تسجيل تفاعلك. شكرًا لك 🌟", show_alert=False)
    except BadRequest:
        pass

    # رسالة شكر مؤقتة تُحذف بعد 90 ثانية (بدون إرسال للخاص)
    try:
        emoji = "👍" if action == "like" else "👎"
        m = await context.bot.send_message(
            chat_id=chat_id,
            text=f"🙏 شكرًا {user.mention_html()} على تفاعلك {emoji}!",
            reply_to_message_id=message_id,
            parse_mode="HTML",
        )
        # جدولة حذف الرسالة بعد 90 ثانية
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
    cc = campaign_counters.setdefault(campaign_id, {"like": 0, "dislike": 0, "voters": {}})
    tot_like = cc["like"]; tot_dislike = cc["dislike"]
    for (cid, prompt_mid) in list(campaign_prompt_msgs.get(campaign_id, [])):
        try:
            base_mid = campaign_base_msg.get((campaign_id, cid))
            if not base_mid:
                continue
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton(f"👍 {tot_like}",  callback_data=f"like:{cid}:{base_mid}"),
                InlineKeyboardButton(f"👎 {tot_dislike}", callback_data=f"dislike:{cid}:{base_mid}"),
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
    action_kb = InlineKeyboardMarkup([[InlineKeyboardButton("🚀 نشر الآن", callback_data="publish")],
                                      [InlineKeyboardButton("❌ إلغاء", callback_data="cancel")]])
    if not getattr(sess, "chosen_chats", None):
        await context.bot.send_message(
            chat_id=user_id,
            text="⚠️ لا توجد وجهة للنشر.\\nيرجى اختيار مجموعة أو قناة من «اختيار الوجهات».",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ رجوع", callback_data="back_main")]])
        )
    status_block = build_status_block(sess, allow_schedule=True)
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
        await context.bot.send_message(chat_id=user_id, text=status_block, reply_markup=action_kb)
        return
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
        await context.bot.send_message(chat_id=user_id, text=status_block, reply_markup=action_kb)
        return
    if getattr(sess, "text", None):
        await context.bot.send_message(chat_id=user_id, text=hidden_links_or_plain(sess.text, hide_links), parse_mode=("HTML" if hide_links else None))
        await context.bot.send_message(chat_id=user_id, text=status_block, reply_markup=action_kb)
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
        if m: first_message_id = m.message_id
        caption = None
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
            first_message_id = first_message_id or m.message_id
            caption = None
    if caption:
        m = await context.bot.send_message(chat_id=chat_id, text=hidden_links_or_plain(caption, hide_links), parse_mode=("HTML" if hide_links else None))
        first_message_id = first_message_id or m.message_id
    if not first_message_id:
        return None
    base_id_for_buttons = first_message_id
    if sess.campaign_id:
        key = (sess.campaign_id, chat_id)
        if key not in campaign_base_msg:
            campaign_base_msg[key] = first_message_id
        base_id_for_buttons = campaign_base_msg[key]
    if sess.use_reactions:
        rec = reactions_counters.get((chat_id, base_id_for_buttons))
        if rec is None:
            rec = {"like": 0, "dislike": 0, "voters": {}}
            reactions_counters[(chat_id, base_id_for_buttons)] = rec
        like_count = rec["like"]; dislike_count = rec["dislike"]
        if sess.campaign_id is not None:
            cc = campaign_counters.setdefault(sess.campaign_id, {"like": 0, "dislike": 0, "voters": {}})
            like_count = cc["like"]; dislike_count = cc["dislike"]
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton(f"👍 {like_count}", callback_data=f"like:{chat_id}:{base_id_for_buttons}"),
            InlineKeyboardButton(f"👎 {dislike_count}", callback_data=f"dislike:{chat_id}:{base_id_for_buttons}"),
        ]])
        try:
            prompt_msg = await context.bot.send_message(chat_id=chat_id, text=reaction_prompt, reply_markup=kb)
            if sess.campaign_id is not None:
                message_to_campaign[(chat_id, base_id_for_buttons)] = sess.campaign_id
                lst = campaign_prompt_msgs.setdefault(sess.campaign_id, [])
                if (chat_id, prompt_msg.message_id) not in lst:
                    lst.append((chat_id, prompt_msg.message_id))
        except Exception:
            pass
    return first_message_id

MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "5"))
PER_CHAT_TIMEOUT = int(os.getenv("PER_CHAT_TIMEOUT", "25"))

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
    if not is_rebroadcast:
        checked = []
        for cid in target_chats:
            try:
                member = await context.bot.get_chat_member(cid, user_id)
                is_admin = (member.status in ("administrator", "creator"))
            except Exception:
                is_admin = False
            allowed_by_grant = globals().get("_grant_active_for", lambda a,b: False)(user_id, cid)
            if is_admin or allowed_by_grant:
                checked.append(cid)
        target_chats = checked
    sem = asyncio.Semaphore(max(1, MAX_CONCURRENCY))
    tasks = [
        _safe_send_one(
            context, cid, sess,
            is_rebroadcast=is_rebroadcast,
            hide_links=hide_links,
            reaction_prompt=s.get("reaction_prompt_text", "قيّم المنشور 👇"),
            sem=sem
        ) for cid in target_chats
    ]
    results = await asyncio.gather(*tasks, return_exceptions=False)
    sent_count = 0
    errors: List[str] = []
    for cid, mid in zip(target_chats, results):
        if mid:
            if (not is_rebroadcast) and global_settings.get("pin_feature_enabled", True) and getattr(sess, "pin_enabled", True):
                try: await context.bot.pin_chat_message(chat_id=cid, message_id=mid, disable_notification=True)
                except Exception: pass
            sent_count += 1
            if sess.campaign_id is not None:
                lst = campaign_messages.setdefault(sess.campaign_id, [])
                if (cid, mid) not in lst:
                    lst.append((cid, mid))
        else:
            errors.append(f"تعذّر الإرسال إلى {known_chats.get(cid, {}).get('title', cid)}")
    save_state()
    return sent_count, errors

# =============================
# Rebroadcast scheduling
# =============================
async def rebroadcast_job(ctx: ContextTypes.DEFAULT_TYPE):
    data = ctx.job.data
    if not data:
        return
    tmp = Session(
        text=data["text"],
        media_list=data["media_list"],
        single_attachment=data["single_attachment"],
        use_reactions=data["use_reactions"],
        chosen_chats=set(data["chosen_chats"]),
        campaign_id=data.get("campaign_id"),
        schedule_active=False
    )
    await publish_to_chats(ctx, data["owner_id"], tmp, is_rebroadcast=True, hide_links=get_settings(data["owner_id"])["hide_links_default"])
    done = data["total"] - data["left"] + 1
    for cid in data["chosen_chats"]:
        try:
            await ctx.bot.send_message(chat_id=cid, text=f"🔁 إعادة النشر {done}/{data['total']}")
        except Exception:
            pass
    await send_stats_to_admin(ctx, data["owner_id"], data.get("campaign_id"))
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
    key = request.query_params.get("key")
    if key != SETUP_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")
    url = build_webhook_url()
    if not url:
        raise HTTPException(status_code=503, detail="RENDER_EXTERNAL_URL غير متاح حالياً")
    try:
        await application.bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        pass
    ok = await application.bot.set_webhook(url=url, allowed_updates=None, drop_pending_updates=True)
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
    
def panel_main_keyboard() -> InlineKeyboardMarkup:
    gs = global_settings
    rows = [
        [InlineKeyboardButton("📊 الإحصاءات", callback_data="panel:stats")],
        [InlineKeyboardButton("🗂️ الوجهات", callback_data="panel:destinations")],
        [InlineKeyboardButton("⏱️ إعدادات الإعادة", callback_data="panel:schedule")],
        [InlineKeyboardButton("👍 التفاعلات", callback_data="panel:reactions")],
        [InlineKeyboardButton("🧩 القوالب", callback_data="panel:templates")],
        [InlineKeyboardButton("🛡️ الأذونات (المشرفون)", callback_data="panel:permissions")],
        [InlineKeyboardButton("🧾 السجل", callback_data="panel:logs")],
        [InlineKeyboardButton("⚙️ الإعدادات العامة", callback_data="panel:settings")],
        [InlineKeyboardButton(
            f"🛠️ وضع الصيانة: {'مفعل' if gs.get('maintenance_mode', False) else 'معطل'}",
            callback_data=f"panel:maintenance:{'off' if gs.get('maintenance_mode', False) else 'on'}"
        )],
    ]
    return InlineKeyboardMarkup(rows)

def panel_settings_keyboard() -> InlineKeyboardMarkup:
    gs = global_settings
    rows = [
        [InlineKeyboardButton(
            f"📌 التثبيت: {'مفعل' if gs.get('pin_feature_enabled', True) else 'معطل'}",
            callback_data=f"panel:set:pin:{'off' if gs.get('pin_feature_enabled', True) else 'on'}"
        )],
        [InlineKeyboardButton(
            f"👍 التفاعلات: {'مفعلة' if gs.get('reactions_feature_enabled', True) else 'معطلة'}",
            callback_data=f"panel:set:react:{'off' if gs.get('reactions_feature_enabled', True) else 'on'}"
        )],
        [InlineKeyboardButton(
            f"⏱️ الجدولة: {'مفعلة' if gs.get('scheduling_enabled', True) else 'معطلة'}",
            callback_data=f"panel:set:schedule:{'off' if gs.get('scheduling_enabled', True) else 'on'}"
        )],
        [InlineKeyboardButton(
            f"🔒 قفل إعدادات الجدولة: {'مفعل' if gs.get('schedule_locked', False) else 'معطل'}",
            callback_data=f"panel:set:schedule_lock:{'off' if gs.get('schedule_locked', False) else 'on'}"
        )],
        [InlineKeyboardButton(
            f"🛠️ وضع الصيانة: {'مفعل' if gs.get('maintenance_mode', False) else 'معطل'}",
            callback_data=f"panel:set:maintenance:{'off' if gs.get('maintenance_mode', False) else 'on'}"
        )],
        [InlineKeyboardButton("🟥 إيقاف الإعادات الجارية", callback_data="panel:schedule:stop_now")],
        [InlineKeyboardButton("⬅️ رجوع", callback_data="panel:back")],
    ]
    return InlineKeyboardMarkup(rows)

# --- فتح اللوحة في الخاص (توحيد الاسم إلى cmd_panel) ---
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
async def handle_control_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query

    # أجب فوراً لتجنب "تحميل" وخطأ Query is too old...
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

    # ====== الإحصاءات ======
    if data == "panel:stats":
        total_like = total_dislike = 0
        for rec in reactions_counters.values():
            total_like += rec.get("like", 0)
            total_dislike += rec.get("dislike", 0)
        await query.message.reply_text(
            f"📊 ملخص عام:\nإجمالي 👍 {total_like} / 👎 {total_dislike}\n\n"
            "لإحصاءات منشور معيّن: افتح معاينة/إرسال وستظهر لك أزرار مخصصة.",
        )
        try: await query.answer()
        except Exception: pass
        return

    # ====== الوجهات ======
    if data == "panel:destinations":
        admin_chat_ids = await list_authorized_chats(context, user_id)
        if not admin_chat_ids:
            await query.message.reply_text("لا توجد وجهات ظاهر أنك مشرف عليها. أضف البوت كمشرف ثم أعد المحاولة.")
            try: await query.answer()
            except Exception: pass
            return
        kb = build_panel_chats_keyboard(admin_chat_ids, s)
        await query.message.reply_text("🗂️ الوجهات — فعّل/عطّل لكل وجهة ثم احفظ:", reply_markup=kb)
        try: await query.answer()
        except Exception: pass
        return

    if data.startswith("panel:toggle_chat:"):
        try:
            cid = int(data.split(":", 2)[2])
        except Exception:
            try: await query.answer()
            except Exception: pass
            return
        disabled = s.setdefault("disabled_chats", set())
        if cid in disabled: disabled.remove(cid)
        else: disabled.add(cid)
        save_state()
        admin_chat_ids = await list_authorized_chats(context, user_id)
        kb = build_panel_chats_keyboard(admin_chat_ids, s)
        try:
            await query.edit_message_reply_markup(reply_markup=kb)
        except Exception:
            pass
        try: await query.answer("تم التبديل.")
        except Exception: pass
        return

    if data == "panel:dest_save":
        add_log(user_id, "حفظ إعداد الوجهات")
        save_state()
        await query.message.reply_text("💾 تم حفظ إعداد الوجهات.")
        try: await query.answer()
        except Exception: pass
        return

    # ====== إعدادات الإعادة ======
    if data == "panel:schedule":
        gs = global_settings
        await query.message.reply_text(
            f"⏱️ حالة الجدولة: {'مفعلة' if gs.get('scheduling_enabled', True) else 'معطلة'}\n"
            f"🔒 القفل: {'مفعل' if gs.get('schedule_locked', False) else 'معطل'}\n"
            f"الفاصل الافتراضي: {gs.get('rebroadcast_interval_seconds',0)//3600} ساعة\n"
            f"عدد الإعادات الافتراضي: {gs.get('rebroadcast_total',0)}\n\n"
            "يمكنك تغيير الحالة من «الإعدادات العامة».",
        )
        try: await query.answer()
        except Exception: pass
        return

    if data == "panel:schedule:stop_now":
        try:
            jobs = context.application.job_queue.jobs()
            for job in jobs:
                if job.name and job.name.startswith("rebroadcast_"):
                    job.schedule_removal()
        except Exception:
            pass
        try:
            await query.message.reply_text("⛔ تم إيقاف أي إعادات مجدولة حالياً.")
        except Exception:
            pass
        try: await query.answer()
        except Exception: pass
        save_state()
        return

    # ====== التفاعلات ======
    if data == "panel:reactions":
        await query.message.reply_text(
            f"حالة التفاعلات حاليًا: {'مفعلة' if global_settings.get('reactions_feature_enabled', True) else 'معطلة'}\n"
            "يمكنك تشغيل/إيقاف الخدمة من «الإعدادات العامة»، وتعديل نص الدعوة هنا.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✏️ تعديل نص الدعوة", callback_data="panel:reactions:edit")],
                [InlineKeyboardButton("⬅️ رجوع", callback_data="panel:back")]
            ])
        )
        try: await query.answer()
        except Exception: pass
        return

    if data == "panel:reactions:edit":
        panel_state[user_id] = PANEL_WAIT_REACTION_PROMPT
        save_state()
        try:
            await query.message.reply_text("أرسل الآن نص دعوة التفاعل الجديد.")
        except Exception:
            pass
        try: await query.answer()
        except Exception: pass
        return

    # ====== القوالب ======
    if data == "panel:templates":
        tpl = s.get("templates", [])
        if not tpl:
            await query.message.reply_text("لا توجد قوالب محفوظة بعد.")
        else:
            names = "\n".join(f"• {t['name']}" for t in tpl)
            await query.message.reply_text("📘 القوالب:\n" + names)
        try: await query.answer()
        except Exception: pass
        return

    # ====== الأذونات ======
    if data == "panel:permissions":
        if not known_chats:
            await query.message.reply_text("لا توجد وجهات معروفة بعد.")
            try: await query.answer()
            except Exception: pass
            return
        await query.message.reply_text("اختر وجهة لإدارة مشرفيها:", reply_markup=permissions_root_keyboard())
        try: await query.answer()
        except Exception: pass
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
        await query.message.reply_text("👤 مشرفو الوجهة المحددة:\n(فعّل/عطّل ثم احفظ)", reply_markup=InlineKeyboardMarkup(rows))
        try: await query.answer()
        except Exception: pass
        return

    if data.startswith("perm:toggle:"):
        _, _, chat_id, uid = data.split(":")
        chat_id = int(chat_id); uid = int(uid)
        blocked = group_permissions.setdefault(chat_id, {}).setdefault("blocked_admins", set())
        if uid in blocked: blocked.remove(uid)
        else: blocked.add(uid)
        save_state()
        try: await query.answer("تم التبديل.")
        except Exception: pass
        return

    if data.startswith("perm:save:"):
        add_log(user_id, "حفظ إعداد أذونات مشرفي وجهة")
        save_state()
        try:
            await query.message.reply_text("💾 تم حفظ أذونات المشرفين للوجهة.")
        except Exception:
            pass
        try: await query.answer()
        except Exception: pass
        return

    # ====== السجل ======
    if data == "panel:logs":
        logs = s.get("logs", [])
        text = "🧾 آخر العمليات:\n" + ("\n".join(f"• {x['ts']} — {x['text']}" for x in logs[-20:]) if logs else "لا يوجد سجل بعد.")
        await query.message.reply_text(text)
        try: await query.answer()
        except Exception: pass
        return

    # ====== الإعدادات العامة ======
    if data == "panel:settings":
        await query.message.reply_text("⚙️ الإعدادات العامة:", reply_markup=panel_settings_keyboard())
        try: await query.answer()
        except Exception: pass
        return

    if data.startswith("panel:set:"):
        _, _, key, val = data.split(":")
        if key == "pin":
            global_settings["pin_feature_enabled"] = (val == "on")
            add_log(user_id, f"{'تشغيل' if val=='on' else 'إيقاف'} خدمة التثبيت مركزيًا")
        elif key == "react":
            global_settings["reactions_feature_enabled"] = (val == "on")
            add_log(user_id, f"{'تشغيل' if val=='on' else 'إيقاف'} خدمة التفاعلات مركزيًا")
        elif key == "schedule":
            global_settings["scheduling_enabled"] = (val == "on")
            add_log(user_id, f"{'تشغيل' if val=='on' else 'إيقاف'} خدمة الجدولة مركزيًا")
        elif key == "schedule_lock":
            global_settings["schedule_locked"] = (val == "on")
            add_log(user_id, f"{'تشغيل' if val=='on' else 'إيقاف'} قفل إعدادات الجدولة")
        elif key == "maintenance":
            global_settings["maintenance_mode"] = (val == "on")
            add_log(user_id, f"{'تفعيل' if val=='on' else 'إلغاء'} وضع الصيانة")
            note = "🛠️ نظام النشر تحت الصيانة والتحديث حاليًا." if global_settings["maintenance_mode"] else "✅ تم إلغاء وضع الصيانة. عاد النظام للعمل."
            # إخطار الجلسات الجارية وحاملي التصريح غير المستخدم
            for uid in list(sessions.keys()):
                try: await context.bot.send_message(chat_id=uid, text=note)
                except Exception: pass
            for uid, rec in list(temp_grants.items()):
                if rec and not rec.get("used"):
                    try: await context.bot.send_message(chat_id=uid, text=note)
                    except Exception: pass
        save_state()
        try:
            await query.edit_message_reply_markup(reply_markup=panel_settings_keyboard())
        except Exception:
            pass
        try: await query.answer("تم الحفظ.")
        except Exception: pass
        return

    if data.startswith("panel:maintenance:"):
        val = data.split(":")[2]
        global_settings["maintenance_mode"] = (val == "on")
        add_log(user_id, f"{'تفعيل' if val=='on' else 'إلغاء'} وضع الصيانة")
        note = "🛠️ نظام النشر تحت الصيانة والتحديث حاليًا." if global_settings["maintenance_mode"] else "✅ تم إلغاء وضع الصيانة. عاد النظام للعمل."
        for uid in list(sessions.keys()):
            try: await context.bot.send_message(chat_id=uid, text=note)
            except Exception: pass
        for uid, rec in list(temp_grants.items()):
            if rec and not rec.get("used"):
                try: await context.bot.send_message(chat_id=uid, text=note)
                except Exception: pass
        save_state()
        try:
            await query.edit_message_reply_markup(reply_markup=panel_main_keyboard())
        except Exception:
            pass
        try: await query.answer("تم التنفيذ.")
        except Exception: pass
        return

    if data == "panel:back":
        try:
            await query.message.reply_text("🛠️ لوحة التحكّم الرئيسية", reply_markup=panel_main_keyboard())
        except Exception:
            pass
        try: await query.answer()
        except Exception: pass
        return

async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await update.message.reply_text("pong")
    except Exception:
        pass
    
# =============================
# Handlers registration (ordered AFTER definitions)
# =============================
def register_handlers():
    # Critical commands first
    application.add_handler(CommandHandler("start", start_with_token))
    application.add_handler(CommandHandler("ok", cmd_temp_ok))
    application.add_handler(CommandHandler("register", cmd_register))
    application.add_handler(CommandHandler("mychats", cmd_mychats))
    application.add_handler(CommandHandler("panel", cmd_panel))
    application.add_handler(CommandHandler("ping", cmd_ping))

    # Membership updates
    application.add_handler(ChatMemberHandler(handle_chat_member_update, ChatMemberHandler.CHAT_MEMBER))
    application.add_handler(ChatMemberHandler(handle_my_member_update, ChatMemberHandler.MY_CHAT_MEMBER))

    # 🔹 Auto-register any group/channel on any message (no /register needed)
    application.add_handler(
        MessageHandler(
            (filters.ChatType.GROUPS | filters.ChatType.CHANNEL) & ~filters.COMMAND,
            auto_register_chat
        )
    )

    # Callback buttons (specific first)
    application.add_handler(CallbackQueryHandler(handle_reactions, pattern=r"^(like|dislike):"))
    application.add_handler(CallbackQueryHandler(handle_campaign_buttons, pattern=r"^(show_stats|stop_rebroadcast):"))

# 3) لوحة التحكّم panel:/perm: — لا تحجب الباقي
    application.add_handler(
        CallbackQueryHandler(handle_control_buttons, pattern=r"^(panel:|perm:)", block=False),
        group=12
    )
    application.add_handler(CallbackQueryHandler(on_button))  # catch-all

    # ok in private (must be before general private handler)
    application.add_handler(
        MessageHandler(
            filters.ChatType.PRIVATE & filters.TEXT & filters.Regex(OK_REGEX),
            start_publishing_keyword
        )
    )

    # Direct private messages (content intake) — keep last
    application.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND, handle_admin_input))

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
