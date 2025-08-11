import os
import logging
import re
import asyncio
import secrets
import html
from fastapi import FastAPI, Request, HTTPException
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Set, Any

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
# تهيئة اللوجز (متوافقة مع Render/Uvicorn)
# =============================
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
logger = logging.getLogger("publisher")

# =============================
# إعدادات البيئة لرندر
# =============================
# لا تضع التوكن داخل الكود — يُمرر من لوحة Render → Environment
TOKEN = os.getenv("TOKEN")
if not TOKEN:
    logger.error("Missing TOKEN environment variable. Set it in Render → Settings → Environment.")
    raise RuntimeError("TOKEN is required")

# سر مسار الويبهوك (ضَع قيمة قوية من Environment؛ نضع قيمة عشوائية للتطوير المحلي فقط)
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", secrets.token_urlsafe(16))

# Render يعرّف العنوان الخارجي بعد النشر (مثلاً https://your-app.onrender.com)
BASE_URL = os.getenv("RENDER_EXTERNAL_URL")  # قد تكون None في أول Deploy
PORT = int(os.getenv("PORT", "10000"))       # Render يحدد المنفذ تلقائياً

# =============================
# إنشاء تطبيق FastAPI و Telegram
# =============================
app = FastAPI()
application = ApplicationBuilder().token(TOKEN).build()

# مدة صلاحية الإذن المؤقت (بالدقائق)
GRANT_TTL_MINUTES = 30

# خرائط الإذن المؤقت
# token -> {"user_id": int, "chat_id": int, "expires": datetime}
start_tokens: Dict[str, Dict[str, Any]] = {}
# user_id -> {"chat_id": int, "expires": datetime, "used": bool, "granted_by": Optional[int]}
temp_grants: Dict[int, Dict[str, Any]] = {}

# =============================
# أداة الوقت بتوقيت السعودية
# =============================
KSA_TZ = timezone(timedelta(hours=3))

def ksa_time(dt: datetime) -> str:
    # نحول datetime إلى واعٍ بالتوقيت (UTC) ثم إلى توقيت السعودية
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(KSA_TZ).strftime("%Y-%m-%d %H:%M")

# =============================
# إعدادات مركزية للمسئول (تنطبق على الجميع)
# =============================
global_settings = {
    "scheduling_enabled": True,            # تشغيل/إيقاف الجدولة مركزيًا
    "schedule_locked": False,              # قفل الإعدادات بحيث لا يقدر المشرف تغييرها
    "rebroadcast_interval_seconds": 7200,  # الافتراضي: كل 2 ساعة
    "rebroadcast_total": 4,                # الافتراضي: 4 مرات
}

# =============================
# الحالة وذاكرة العمل
# =============================
@dataclass
class Session:
    # مراحل: waiting_first_input -> collecting -> ready_options -> choosing_chats
    stage: str = "waiting_first_input"
    text: Optional[str] = None
    media_list: List[Tuple[str, str, Optional[str]]] = field(default_factory=list)  # (type, file_id, caption)
    single_attachment: Optional[Tuple[str, str, Optional[str]]] = None            # (type, file_id, caption)
    use_reactions: Optional[bool] = None
    chosen_chats: Set[int] = field(default_factory=set)

    # لوحة التحكم في الخاص (لتبقى دائمًا بالأسفل)
    panel_msg_id: Optional[int] = None
    # رسالة اختيار الوجهات (نحذفها بعد الانتهاء لتقليل الزحام)
    picker_msg_id: Optional[int] = None

    # معرف المنشور/الحملة
    campaign_id: Optional[int] = None

    # إعدادات إعادة البث للجلسة
    rebroadcast_interval_seconds: int = global_settings.get('rebroadcast_interval_seconds', 7200)
    rebroadcast_total: int = global_settings.get('rebroadcast_total', 4)
    schedule_active: bool = False  # تبدأ الجلسة بدون تفعيل الجدولة

    # ===== إذن مؤقّت =====
    allowed_chats: Set[int] = field(default_factory=set)   # الوجهات المصرّح بها للعضو
    is_temp_granted: bool = False                          # هل الجلسة بإذن مؤقّت؟
    granted_by: Optional[int] = None                       # من منح الإذن (مشرف)

# جلسات النشر لكل مشرف/عضو (بالخاص)
sessions: Dict[int, Session] = {}

# إعدادات لوحة التحكم لكل مشرف (في الذاكرة)
admin_settings: Dict[int, Dict[str, Any]] = {}  # يُملأ عند أول ok/ok25s

def get_settings(user_id: int) -> Dict[str, Any]:
    s = admin_settings.get(user_id)
    if not s:
        s = {
            "default_rebroadcast_interval_seconds": 60,
            "default_rebroadcast_total": 12,
            "default_reactions_enabled": True,
            "reaction_prompt_text": "💬 رأيكم يهمّنا! فضلاً التفاعل مع المنشور:",
            "disabled_chats": set(),   # chat_ids المعطّلة مركزيًا
            "templates": [],           # [{"name": str, "text":..., "media_list":[...], "single_attachment":...}]
            "permissions_mode": "all", # "all" | "whitelist"
            "whitelist": set(),        # user_ids
            "hide_links_default": True,
            "scheduling_enabled": True,  # تشغيل/إيقاف الجدولة مركزيًا (لوضع التوافق القديم)
            "logs": [],                # آخر عمليات اللوحة
        }
        admin_settings[user_id] = s
    return s

def add_log(user_id: int, text_msg: str) -> None:
    s = get_settings(user_id)
    s.setdefault("logs", [])
    s["logs"].append(text_msg)
    if len(s["logs"]) > 200:
        s["logs"] = s["logs"][-200:]

# ذاكرة الوجهات التي عرفها البوت
# chat_id -> {"title": str, "username": Optional[str], "type": "group"|"channel"}
known_chats: Dict[int, Dict[str, Optional[str]]] = {}

# أذونات كل مجموعة: من يُمنع من النشر فيها
# chat_id -> {"blocked_admins": set(user_ids)}
group_permissions: Dict[int, Dict[str, Set[int]]] = {}

# أسماء المشرفين لكل مجموعة (نحدّثها عند فتح شاشة الأذونات)
# chat_id -> {user_id: name}
known_chats_admins: Dict[int, Dict[int, str]] = {}

# تفاعلات المنشورات:
# (chat_id, base_message_id) -> {"like": int, "dislike": int, "voters": {user_id: "like"|"dislike"}}
reactions_counters: Dict[Tuple[int, int], Dict[str, Any]] = {}

# رسائل كل منشور: campaign_id -> [(chat_id, base_message_id), ...]
campaign_messages: Dict[int, List[Tuple[int, int]]] = {}
campaign_base_msg: Dict[Tuple[int, int], int] = {}  # (campaign_id, chat_id) -> base_message_id
_next_campaign_id = 1

def new_campaign_id() -> int:
    global _next_campaign_id
    cid = _next_campaign_id
    _next_campaign_id += 1
    return cid

# =============================
# أدوات مساعدة
# =============================
def cache_chat(chat) -> None:
    if not chat:
        return
    if chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
        known_chats[chat.id] = {
            "title": chat.title or str(chat.id),
            "username": getattr(chat, "username", None),
            "type": "group",
        }
    elif chat.type == ChatType.CHANNEL:
        known_chats[chat.id] = {
            "title": chat.title or str(chat.id),
            "username": getattr(chat, "username", None),
            "type": "channel",
        }

async def cache_chat_from_update(update: Update):
    chat = update.effective_chat
    if chat:
        cache_chat(chat)

async def is_admin_in_chat(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int) -> bool:
    try:
        admins = await context.bot.get_chat_administrators(chat_id)
        return any(a.user.id == user_id for a in admins)
    except Exception:
        return False

async def chats_where_user_is_admin(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> List[int]:
    ids = []
    for cid in list(known_chats.keys()):
        if await is_admin_in_chat(context, cid, user_id):
            ids.append(cid)
    return ids

# --- منحة النشر المؤقتة
def _grant_active_for(user_id: int, chat_id: int) -> bool:
    g = temp_grants.get(user_id)
    if not g: return False
    if g.get("used"): return False
    if g.get("chat_id") != chat_id: return False
    if g.get("expires") and datetime.utcnow() > g["expires"]: return False
    return True

def _user_has_active_grant(user_id: int) -> bool:
    g = temp_grants.get(user_id)
    return bool(g and (not g.get("used")) and (not g.get("expires") or datetime.utcnow() <= g["expires"]))

def authorized_chats_list_sync(user_id: int) -> List[int]:
    """ترجع قائمة الوجهات المسموحة: المنحة المؤقتة فقط (بدون فحص أدمن)."""
    allowed = set()
    g = temp_grants.get(user_id)
    if g and not g.get("used") and (not g.get("expires") or datetime.utcnow() <= g["expires"]):
        allowed.add(g["chat_id"])
    return list(allowed)

async def list_authorized_chats(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> List[int]:
    """المزج بين صلاحية المشرف + المنحة المؤقتة"""
    ids = await chats_where_user_is_admin(context, user_id)
    g = temp_grants.get(user_id)
    if g and not g.get("used") and (not g.get("expires") or datetime.utcnow() <= g["expires"]):
        ids = list(set(ids) | {g["chat_id"]})
    return ids

async def refresh_admins_for_chat(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    """تجلب قائمة المدراء الحقيقيين وتحدّث known_chats_admins."""
    try:
        admins = await context.bot.get_chat_administrators(chat_id)
        mapping = {}
        for a in admins:
            user = a.user
            name = (user.full_name or user.username or str(user.id))
            mapping[user.id] = name
        if mapping:
            known_chats_admins[chat_id] = mapping
    except Exception as e:
        logging.warning("Failed to fetch admins for %s: %s", chat_id, e)

# ====== إخفاء الروابط + تنظيف النص (إزالة /start و idshat) ======
URL_RE = re.compile(r'(https?://\S+)', re.IGNORECASE)
START_TOKEN_RE = re.compile(r'/start\s+\S+', re.IGNORECASE)
IDSHAT_RE = re.compile(r'\bidshat\w*', re.IGNORECASE)

def sanitize_text(text: Optional[str]) -> Optional[str]:
    if not text:
        return text
    # احذف /start {token} وأي سلاسل idshat...
    txt = START_TOKEN_RE.sub('', text)
    txt = IDSHAT_RE.sub('', txt)
    # تنظيف فراغات زائدة
    txt = re.sub(r'\n{3,}', '\n\n', txt)
    txt = re.sub(r'[ \t]{2,}', ' ', txt)
    return txt.strip()

def make_html_with_hidden_links(text: str) -> str:
    """يُخفي كل الروابط بوضع <a href="...">اضغط هنا</a> مع هروب آمن لباقي النص."""
    parts: List[str] = []
    last = 0
    idx = 1
    for m in URL_RE.finditer(text):
        start, end = m.span()
        url = m.group(1)
        # أضف الجزء السابق بعد هروبه
        if start > last:
            parts.append(html.escape(text[last:start]))
        label = "اضغط هنا" if idx == 1 else f"اضغط هنا ({idx})"
        idx += 1
        parts.append(f'<a href="{html.escape(url, quote=True)}">{label}</a>')
        last = end
    # الذيل
    parts.append(html.escape(text[last:]))
    return "".join(parts)

def hidden_links_or_plain(text: Optional[str], hide: bool) -> Optional[str]:
    if not text:
        return text
    text = sanitize_text(text)  # ← تنظيف قبل الإخفاء
    if not hide:
        return text
    return make_html_with_hidden_links(text)

def chat_type_badge(cid: int) -> str:
    typ = known_chats.get(cid, {}).get("type", "group")
    return "📢 قناة" if typ == "channel" else "👥 مجموعة"

def status_text(sess: Session) -> str:
    parts = []
    if sess.text:
        parts.append("📝 نص موجود")
    if sess.media_list:
        parts.append(f"🖼️ وسائط: {len(sess.media_list)}")
    if sess.single_attachment:
        t = sess.single_attachment[0]
        mapping = {"document":"مستند","audio":"ملف صوتي","voice":"رسالة صوتية"}
        parts.append(f"📎 {mapping.get(t,t)}")
    parts.append(f"{'✅' if sess.use_reactions else '🚫'} تفاعلات")
    parts.append(f"🎯 وجهات محددة: {len(sess.chosen_chats)}")

    if getattr(sess, "schedule_active", False):
        parts.append(f"⏱️ الإعادة: كل {sess.rebroadcast_interval_seconds//60} دقيقة × {sess.rebroadcast_total}")
    else:
        parts.append("⏱️ الإعادة: كل 0 دقيقة × 0")

    return " • ".join(parts)

# =============================
# لوحات المفاتيح
# =============================
def keyboard_collecting() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("✅ تم", callback_data="done")]])

def _single_destination_row(sess: Session) -> List[List[InlineKeyboardButton]]:
    """سطر معلومات الوجهة الوحيدة عند التصريح المؤقت."""
    rows: List[List[InlineKeyboardButton]] = []
    if sess.is_temp_granted and sess.allowed_chats:
        cid = next(iter(sess.allowed_chats))
        title = known_chats.get(cid, {}).get("title", str(cid))
        rows.append([InlineKeyboardButton(f"🎯 الوجهة: {title}", callback_data="noop")])
    return rows

def keyboard_ready_options(sess: Session, settings: Dict[str, Any]) -> InlineKeyboardMarkup:
    btns: List[List[InlineKeyboardButton]] = []

    # عند التصريح المؤقت: أظهر الوجهة فقط ولا تعرض اختيار الوجهات
    btns.extend(_single_destination_row(sess))

    if settings.get("default_reactions_enabled", True):
        btns.append([
            InlineKeyboardButton(
                "👍 إضافة تفاعلات" if not sess.use_reactions else "🧹 إزالة التفاعلات",
                callback_data="toggle_reactions"
            )
        ])

    if not (sess.is_temp_granted and sess.allowed_chats):
        # فقط إن لم تكن جلسة تصريح، أظهر اختيار الوجهات
        btns.append([InlineKeyboardButton("🗂️ اختيار الوجهات (مجموعات/قنوات)", callback_data="choose_chats")])

    if global_settings.get("scheduling_enabled", True):
        if global_settings.get("schedule_locked", False):
            btns.append([
                InlineKeyboardButton(
                    f"⏱️ الإعادة (مقفلة): كل {global_settings['rebroadcast_interval_seconds']//3600} ساعة × {global_settings['rebroadcast_total']}",
                    callback_data="noop"
                )
            ])
        else:
            label = "⏱️ تفعيل الجدولة" if not getattr(sess, "schedule_active", False) else "⏱️ الإعادة: مفعّلة"
            btns.append([InlineKeyboardButton(label, callback_data="schedule_menu")])

    btns.append([InlineKeyboardButton("⬅️ الرجوع للتعديل", callback_data="back_to_collect")])
    btns.append([
        InlineKeyboardButton("🧽 مسح", callback_data="clear"),
        InlineKeyboardButton("❌ إنهاء", callback_data="cancel")
    ])
    btns.append([InlineKeyboardButton("👁️ معاينة", callback_data="preview")])

    return InlineKeyboardMarkup(btns)

def build_session_schedule_keyboard(sess) -> InlineKeyboardMarkup:
    counts = [2, 4, 6, 12]
    hours = [2, 4, 6, 12]
    rows = []
    rows.append([InlineKeyboardButton(f"🔁 عدد الإعادات (الحالي {getattr(sess, 'rebroadcast_total', 0)})", callback_data="noop")])
    rows.append([InlineKeyboardButton(str(c), callback_data=f"ssched_count:{c}") for c in counts])
    rows.append([InlineKeyboardButton(f"⏲️ الفاصل بالساعات (الحالي {getattr(sess, 'rebroadcast_interval_seconds', 0)//3600})", callback_data="noop")])
    rows.append([InlineKeyboardButton(f"{h}h", callback_data=f"ssched_int:{h*3600}") for h in hours])
    rows.append([InlineKeyboardButton("💾 حفظ الإعداد", callback_data="sschedule_done")])
    rows.append([InlineKeyboardButton("⬅️ رجوع", callback_data="back_main")])
    return InlineKeyboardMarkup(rows)

def build_panel_chats_keyboard(admin_chat_ids: List[int], settings: Dict[str, Any]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    disabled: Set[int] = settings.get("disabled_chats", set())
    for cid in admin_chat_ids:
        ch = known_chats.get(cid, {"title": str(cid), "type": "group"})
        title = ch.get("title") or str(cid)
        typ = ch.get("type") or "group"
        label = "📢 قناة" if typ == "channel" else "👥 مجموعة"
        is_disabled = (cid in disabled)
        prefix = "🚫 " if is_disabled else "✅ "
        rows.append([InlineKeyboardButton(f"{prefix}{title} — {label}", callback_data=f"panel:toggle_chat:{cid}")])
    if admin_chat_ids:
        rows.append([InlineKeyboardButton("💾 حفظ الإعداد", callback_data="panel:dest_save")])
    rows.append([InlineKeyboardButton("⬅️ رجوع", callback_data="panel:back")])
    return InlineKeyboardMarkup(rows)

def permissions_root_keyboard() -> InlineKeyboardMarkup:
    rows = []
    for cid, meta in known_chats.items():
        title = meta.get("title", str(cid))
        badge = "📢" if meta.get("type") == "channel" else "👥"
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

# =============================
# حالة لوحة التحكم
# =============================
PANEL_MAIN = "panel_main"
PANEL_WAIT_REACTION_PROMPT = "panel_wait_reaction_prompt"
PANEL_WAIT_TEMPLATE_NAME_SAVE = "panel_wait_template_name_save"
panel_state: Dict[int, str] = {}

def panel_main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 الإحصاءات", callback_data="panel:stats")],
        [InlineKeyboardButton("🗂️ الوجهات", callback_data="panel:destinations")],
        [InlineKeyboardButton("⏱️ إعدادات الإعادة", callback_data="panel:schedule")],
        [InlineKeyboardButton("👍 التفاعلات", callback_data="panel:reactions")],
        [InlineKeyboardButton("🧰 القوالب", callback_data="panel:templates")],
        [InlineKeyboardButton("🔒 الأذونات", callback_data="panel:permissions")],
        [InlineKeyboardButton("🧾 السجلّ", callback_data="panel:logs")],
        [InlineKeyboardButton("⚙️ إعدادات عامة", callback_data="panel:settings")],
    ])

# =============================
# دفع واجهة اللوحة/لوحة المنشور
# =============================
async def push_panel(context: ContextTypes.DEFAULT_TYPE, chat_id: int, sess: Session, header_text: str):
    if sess.panel_msg_id:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=sess.panel_msg_id)
        except:
            pass
    s = get_settings(chat_id)
    kb = keyboard_collecting() if sess.stage in ("waiting_first_input","collecting") else keyboard_ready_options(sess, s)
    text = f"📋 *لوحة المنشور*\n{header_text}\n\n{status_text(sess)}"
    m = await context.bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown", reply_markup=kb)
    sess.panel_msg_id = m.message_id

def delete_picker_if_any(context: ContextTypes.DEFAULT_TYPE, user_id: int, sess: Session):
    if sess.picker_msg_id:
        try:
            context.application.create_task(
                context.bot.delete_message(chat_id=user_id, message_id=sess.picker_msg_id)
            )
        except:
            pass
        sess.picker_msg_id = None

# =============================
# لوحة التحكّم — فتح ومعالجة
# =============================
async def open_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != ChatType.PRIVATE:
        return
    user_id = update.effective_user.id
    get_settings(user_id)  # تأكد من الإنشاء
    panel_state[user_id] = PANEL_MAIN
    await context.bot.send_message(
        chat_id=user_id,
        text="🛠️ *لوحة التحكّم الرئيسية*",
        parse_mode="Markdown",
        reply_markup=panel_main_keyboard()
    )

# =============================
# استقبال نصوص لوحة التحكم (تحرير الدعوة/اسم القالب)
# =============================
async def handle_panel_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != ChatType.PRIVATE:
        return
    user_id = update.effective_user.id
    st = panel_state.get(user_id)
    if not st:
        return
    s = get_settings(user_id)
    txt = update.message.text or ""

    if st == PANEL_WAIT_REACTION_PROMPT:
        s["reaction_prompt_text"] = txt
        panel_state[user_id] = PANEL_MAIN
        add_log(user_id, "تعديل نص دعوة التفاعل")
        await update.message.reply_text("✅ تم تحديث نص الدعوة للتفاعل.", reply_markup=panel_main_keyboard())
        return

    if st == PANEL_WAIT_TEMPLATE_NAME_SAVE:
        sess = sessions.get(user_id)
        if not sess or (not sess.text and not sess.media_list and not sess.single_attachment):
            await update.message.reply_text("⚠️ لا توجد جلسة نشر بمحتوى لحفظها كقالب.", reply_markup=panel_main_keyboard())
            panel_state[user_id] = PANEL_MAIN
            return
        s["templates"].append({
            "name": txt.strip() or f"قالب {len(s['templates'])+1}",
            "text": sess.text,
            "media_list": sess.media_list[:],
            "single_attachment": sess.single_attachment,
        })
        panel_state[user_id] = PANEL_MAIN
        await update.message.reply_text("💾 تم حفظ القالب بنجاح.", reply_markup=panel_main_keyboard())
        return

# =============================
# تشغيل الجلسة تلقائيًا (نستخدمها أيضًا مع /start {token})
# =============================
async def start_publishing_session(user, context: ContextTypes.DEFAULT_TYPE):
    user_id = user.id
    s = get_settings(user_id)

    # حضّر جلسة
    sess = Session(
        stage="waiting_first_input",
        use_reactions=s.get('default_reactions_enabled', True),
        rebroadcast_interval_seconds=global_settings['rebroadcast_interval_seconds'],
        rebroadcast_total=global_settings['rebroadcast_total'],
        schedule_active=False
    )

    # لو عنده منحة مؤقتة فعّالة، اربطها بالجلسة
    g = temp_grants.get(user_id)
    if g and not g.get("used") and (not g.get("expires") or datetime.utcnow() <= g["expires"]):
        sess.allowed_chats = {g["chat_id"]}
        sess.is_temp_granted = True
        sess.granted_by = g.get("granted_by")
        # اختر الوجهة تلقائيًا
        sess.chosen_chats = set(sess.allowed_chats)

    sessions[user_id] = sess

    welcome = (
        f"👋 أهلاً *{user.full_name}*\n\n"
        "أنت الآن في *وضع النشر*.\n"
        "1) اكتب نص المنشور أو أرسل صورة/فيديو/ألبوم/ملف/صوت/فويس — *أي ترتيب*.\n"
        "2) بعد أول إدخال سأؤكد الحفظ، وستظهر لوحة الخيارات في الأسفل.\n"
    )
    await context.bot.send_message(chat_id=user_id, text=welcome, parse_mode="Markdown")

# للنسخ القديمة (إبقاء ok في الخاص إن رغبت) — الآن مقيد بالتصريح فقط
# للنسخ القديمة (إبقاء ok في الخاص إن رغبت) — اسمح للمشرفين دائمًا، ولغير المشرفين عند وجود تصريح
async def start_publishing_keyword(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != ChatType.PRIVATE:
        return

    user = update.effective_user
    user_id = user.id

    # 1) لو عنده تصريح مؤقّت فعّال → ابدأ فورًا
    if _user_has_active_grant(user_id):
        await start_publishing_session(user, context)
        return

    # 2) لو المستخدم مشرف في أي وجهة معروفة → ابدأ كالعادة (سلوك المشرفين القديم)
    admin_ids = await chats_where_user_is_admin(context, user_id)
    if admin_ids:
        await start_publishing_session(user, context)
        return

    # 3) غير ذلك → ارفض
    await update.message.reply_text("🔒 هذا الأمر متاح فقط بتصريح مؤقّت فعّال من مشرف المجموعة.")

async def cmd_temp_ok(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return

    granter = update.effective_user

    # يجب أن يكون مُرسِل الأمر مشرفًا
    if not await is_admin_in_chat(context, chat.id, granter.id):
        try:
            await update.message.reply_text("🚫 هذا الأمر للمشرفين فقط.")
        except:
            pass
        return

    # يجب استخدام الأمر كـ (رد) على رسالة العضو
    if not update.message.reply_to_message or not update.message.reply_to_message.from_user:
        await update.message.reply_text("⚠️ استخدم /ok بالرد على رسالة العضو الذي تريد منحه إذن نشر مؤقّت.")
        return

    target = update.message.reply_to_message.from_user
    if target.is_bot:
        await update.message.reply_text("⚠️ لا يمكن منح الصلاحية إلى بوت.")
        return

    # إنشاء توكن وربطه
    token = secrets.token_urlsafe(16)
    expires = datetime.utcnow() + timedelta(minutes=GRANT_TTL_MINUTES)
    start_tokens[token] = {"user_id": target.id, "chat_id": chat.id, "expires": expires}

    # حفظ منحة مبدئية (مع من منح الإذن)
    temp_grants[target.id] = {
        "chat_id": chat.id,
        "expires": expires,
        "used": False,
        "granted_by": granter.id
    }

    # رابط داخل تطبيق تيليجرام فقط (بدون متصفح احتياطي)
    me = await context.bot.get_me()
    deep_link_tg = f"tg://resolve?domain={me.username}&start={token}"

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀 ابدأ النشر الآن", url=deep_link_tg)],
    ])

    # ✅ إشعار في المجموعة (كردّ على رسالة العضو)
    text_html = (
        f"✅ تم منح تصريح نشر مؤقّت لـ {target.mention_html()} في هذه المجموعة.\n"
        f"⏳ صالح حتى {ksa_time(expires)}.\n\n"
        f"✨ المزايا:\n"
        f"• نظام نشر تفاعلي مع إحصاءات مباشرة لقياس الأداء.\n"
        f"• إمكانية تثبيت المنشور طوال فترة الإعادة التي تختارها أثناء التجهيز.\n\n"
        f"اضغط الزر أدناه لبدء الجلسة في الخاص.\n\n"
        f"📣 ترغب بالنشر؟ اطلب تصريحًا مؤقتًا من أحد المشرفين."
    )
    try:
        await update.message.reply_to_message.reply_html(text_html, reply_markup=kb)
    except:
        # احتياطيًا لو تعذّر الرد على الرسالة الأصلية
        await update.message.reply_html(text_html, reply_markup=kb)

    # إخطار العضو على الخاص (قد يفشل إن لم يبدأ محادثة مع البوت)
    try:
        await context.bot.send_message(
            chat_id=target.id,
            text=(
                f"👋 أهلاً {html.escape(target.full_name)}\n\n"
                f"تم منحك تصريح نشر مؤقّت في مجموعة: {html.escape(chat.title or str(chat.id))}\n"
                f"⏳ التصريح صالح حتى {ksa_time(expires)}.\n\n"
                f"✨ المزايا:\n"
                f"• نظام نشر تفاعلي مع إحصاءات مباشرة لقياس الأداء.\n"
                f"• إمكانية تثبيت المنشور طوال فترة الإعادة التي تختارها أثناء التجهيز.\n\n"
                f"اضغط الزر بالأسفل لبدء الجلسة الآن."
            ),
            parse_mode="HTML",
            reply_markup=kb
        )
    except:
        pass

# التقاط /start {token} في الخاص لتفعيل الجلسة تلقائيًا
async def start_with_token(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != ChatType.PRIVATE:
        return

    user = update.effective_user
    full_name = user.full_name  # استخراج الاسم الكامل للمستخدم

    args = context.args or []
    # لو ما فيه توكن → رسالة ترحيب بدل التجاهل
    if not args:
        await update.message.reply_text(
            f"👋 أهلاً بك {full_name}!\n"
            "بنظام النشر التفاعلي.\n\n"
            "• في المجموعات: اطلب من المشرف استخدام /ok@ بالردّ على رسالتك لمنحك تصريح مؤقّت.\n"
            "• في الخاص: اكتب المحتوى ثم اضغط «تم» من اللوحة عندما تبدأ الجلسة.\n\n"
            "ادارة النشر ( ok )"
        )
        return

    # ----- الفرع القديم: /start {token} -----
    token = args[0]
    rec = start_tokens.get(token)
    if not rec:
        await update.message.reply_text("⛔ رابط غير صالح أو منتهي.")
        return

    if user.id != rec["user_id"]:
        await update.message.reply_text("⛔ هذا الرابط ليس لك.")
        return
    if datetime.utcnow() > rec["expires"]:
        await update.message.reply_text("⌛ انتهت صلاحية الرابط.")
        start_tokens.pop(token, None)
        temp_grants.pop(user.id, None)
        return

    await start_publishing_session(user, context)
    await update.message.reply_text(
        f"🎯 أهلاً بك {full_name}!\n"
        "ملاحظة: إذن النشر مؤقت ومقيد بهذه المجموعة فقط. سيُسحب بعد أول نشر."
    )

# =============================
# تجميع الإدخال الحر (نص/وسائط/ألبوم/ملفات)
# =============================
def build_next_hint(sess: Session, saved_type: str) -> str:
    has_text = bool(sess.text and sess.text.strip())
    has_media = bool(sess.media_list)
    has_attach = bool(sess.single_attachment)
    can_add = []
    if not has_text:
        can_add.append("نص")
    if not has_media:
        can_add.append("صور/فيديو (ألبوم أو مفرد)")
    if not has_attach:
        can_add.append("ملف/صوت/فويس")
    if can_add:
        extra = "، ".join(can_add)
        return f"يمكنك إضافة: *{extra}* — أو اضغط *تم* للانتقال للخيارات."
    else:
        return "كل شيء جاهز — اضغط *تم* للانتقال للخيارات."

async def handle_admin_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await cache_chat_from_update(update)
    chat = update.effective_chat
    if chat.type != ChatType.PRIVATE:
        return

    user_id = update.effective_user.id

    # فحص الأذونات العامة
    s = get_settings(user_id)
    if s["permissions_mode"] == "whitelist" and user_id not in s["whitelist"]:
        await update.message.reply_text("🔒 النشر متاح لأعضاء القائمة البيضاء فقط.")
        return

    sess = sessions.get(user_id)
    if not sess or sess.stage not in ("waiting_first_input", "collecting", "ready_options", "choosing_chats"):
        return

    msg = update.message
    saved_type: Optional[str] = None

    # لا نعتبر "ok" أو "ok25s" محتوى
    if msg.text and re.fullmatch(r"(?i)\s*(ok|ok25s)\s*", msg.text.strip()):
        return
    if msg.text and not msg.text.strip():
        return

    # نص
    if msg.text and not msg.media_group_id:
        clean = sanitize_text(msg.text)
        if clean.strip():                 # ⬅️ مهم: سجل فقط إذا فيه نص فعلي
            sess.text = f"{sess.text}\n{clean}" if sess.text else clean
            saved_type = "النص"

    # ألبوم
    if msg.media_group_id and (msg.photo or msg.video):
        if msg.photo:
            sess.media_list.append(("photo", msg.photo[-1].file_id, sanitize_text(msg.caption) if msg.caption else None))
            saved_type = "صورة ضمن ألبوم"
        elif msg.video:
            sess.media_list.append(("video", msg.video.file_id, sanitize_text(msg.caption) if msg.caption else None))
            saved_type = "فيديو ضمن ألبوم"

    # مفرد
    if msg.photo and not msg.media_group_id:
        sess.media_list.append(("photo", msg.photo[-1].file_id, sanitize_text(msg.caption) if msg.caption else None))
        saved_type = "صورة"
    if msg.video and not msg.media_group_id:
        sess.media_list.append(("video", msg.video.file_id, sanitize_text(msg.caption) if msg.caption else None))
        saved_type = "فيديو"

    # ملفات/صوت/فويس
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

# =============================
# أزرار التحكم داخل الجلسة + الحملة
# =============================
async def handle_campaign_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data

    if data.startswith("show_stats:"):
        try:
            campaign_id = int(data.split(":",1)[1])
        except:
            await query.answer("خطأ في المعرف.", show_alert=True)
            return
        await send_stats_to_admin(context, query.from_user.id, campaign_id)
        await query.answer("تم إرسال الإحصاءات.", show_alert=False)
        return

    if data.startswith("stop_rebroadcast:"):
        try:
            _, user_id, campaign_id = data.split(":", 2)
            user_id = int(user_id); campaign_id = int(campaign_id)
        except:
            await query.answer("خطأ في المعرف.", show_alert=True)
            return
        name = f"rebroadcast_{user_id}_{campaign_id}"
        jobs = context.application.job_queue.get_jobs_by_name(name)
        if jobs:
            for j in jobs:
                try:
                    j.schedule_removal()
                except:
                    pass
            await query.message.reply_text("⏹️ تم إيقاف إعادة البث لهذا المنشور.")
        else:
            await query.message.reply_text("لا توجد إعادة بث نشطة لهذا المنشور.")
        await query.answer()
        return

async def on_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id
    s = get_settings(user_id)
    sess = sessions.get(user_id)
    data = query.data

    # أزرار ما بعد النشر — تعمل بدون جلسة
    if data.startswith("show_stats:") or data.startswith("stop_rebroadcast:"):
        await handle_campaign_buttons(update, context)
        return

    # من هنا: أزرار الجلسة فقط
    if not sess:
        await query.message.reply_text("⚠️ لا توجد جلسة حالية. أرسل ok لبدء جلسة نشر جديدة.")
        return

    if data == "done":
        # إن كان تصريحًا مؤقتًا: تأكد من الوجهة الوحيدة
        if sess.is_temp_granted and sess.allowed_chats:
            sess.chosen_chats = set(sess.allowed_chats)
        sess.stage = "ready_options"
        await push_panel(context, user_id, sess, "🎛️ خيارات المنشور")
        return

    if data == "back_to_collect":
        sess.stage = "collecting"
        await push_panel(context, user_id, sess, "✍️ عدّل المحتوى ثم اضغط تم")
        return

    if data == "clear":
        sessions[user_id] = Session(
            stage="waiting_first_input",
            use_reactions=s.get('default_reactions_enabled', True),
            rebroadcast_interval_seconds=global_settings['rebroadcast_interval_seconds'],
            rebroadcast_total=global_settings['rebroadcast_total'],
            schedule_active=False
        )
        await query.message.reply_text("🧽 تم مسح المدخلات. ابدأ من جديد بإرسال المحتوى.")
        return

    if data == "cancel":
        sessions.pop(user_id, None)
        await query.message.reply_text("✅ تم إنهاء جلسة النشر.")
        return

    if data == "toggle_reactions":
        sess.use_reactions = not bool(sess.use_reactions)
        await push_panel(context, user_id, sess, "✅ تم ضبط التفاعلات.")
        return

    # ---- قائمة جدولة الإعادة (خاصة بهذه الجلسة)
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

    if data.startswith("ssched_count:"):
        cnt = int(data.split(":", 1)[1])
        sess.rebroadcast_total = cnt
        try:
            await query.edit_message_text(
                text=f"⏱️ الإعداد الحالي: كل {sess.rebroadcast_interval_seconds//3600} ساعة × {sess.rebroadcast_total} مرات.",
                reply_markup=build_session_schedule_keyboard(sess)
            )
        except:
            pass
        return

    if data.startswith("ssched_int:"):
        sec = int(data.split(":", 1)[1])
        sess.rebroadcast_interval_seconds = sec
        try:
            await query.edit_message_text(
                text=f"⏱️ الإعداد الحالي: كل {sess.rebroadcast_interval_seconds//3600} ساعة × {sess.rebroadcast_total} مرات.",
                reply_markup=build_session_schedule_keyboard(sess)
            )
        except:
            pass
        return

    if data == "sschedule_done":
        sess.schedule_active = True
        try:
            await context.bot.delete_message(chat_id=user_id, message_id=query.message.message_id)
        except:
            pass
        await push_panel(
            context, user_id, sess,
            f"✅ تم تفعيل الإعادة لهذه الجلسة: كل {sess.rebroadcast_interval_seconds//3600} ساعة × {sess.rebroadcast_total} مرة."
        )
        return

    if data == "noop":
        return

    # ---- اختيار الوجهات
    if data == "choose_chats":
        # في حالة التصريح المؤقت: لا قائمة — نثبت الوجهة تلقائيًا
        if sess.is_temp_granted and sess.allowed_chats:
            sess.chosen_chats = set(sess.allowed_chats)
            await push_panel(context, user_id, sess, "🎯 الوجهة محددة تلقائيًا وفق التصريح.")
            return

        # المصدر: فحص الأدمن أو المنحة
        admin_chat_ids = await list_authorized_chats(context, user_id)
        if not admin_chat_ids:
            await query.message.reply_text("🚫 لا توجد وجهات متاحة لك.")
            return
        # استبعد المُعطّل مركزيًا
        s = get_settings(user_id)
        active_ids = [cid for cid in admin_chat_ids if cid not in s["disabled_chats"]]
        if not active_ids:
            await query.message.reply_text("🚫 كل الوجهات المصرّح بها معطّلة حاليًا من لوحة التحكّم.")
            return
        sess.stage = "choosing_chats"
        m = await query.message.reply_text(
            "🗂️ اختر الوجهات المستهدفة (يُظهر النوع: 👥 مجموعة / 📢 قناة):",
            reply_markup=build_chats_keyboard(active_ids, sess.chosen_chats, s)
        )
        sess.picker_msg_id = m.message_id
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

        # إعادة بناء القائمة من المصدر
        if sess.is_temp_granted:
            source_ids = list(sess.allowed_chats)
        else:
            source_ids = await list_authorized_chats(context, user_id)

        s = get_settings(user_id)
        active_ids = [i for i in source_ids if i not in s["disabled_chats"]]
        await query.edit_message_reply_markup(reply_markup=build_chats_keyboard(active_ids, sess.chosen_chats, s))
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
        return

    if data == "done_chats":
        delete_picker_if_any(context, user_id, sess)
        sess.stage = "ready_options"
        await push_panel(context, user_id, sess, "🎛️ تم حفظ اختيار الوجهات.")
        return

    if data == "back_main":
        delete_picker_if_any(context, user_id, sess)
        sess.stage = "ready_options"
        await push_panel(context, user_id, sess, "🎛️ عدنا للخيارات.")
        return

    # ---- معاينة ونشر
    if data == "preview":
        await send_preview(update, context, sess, hide_links=get_settings(user_id)["hide_links_default"])
        return

    if data == "publish":
        # في حالة التصريح: اجبر الوجهة على المجموعة الممنوحة
        if sess.is_temp_granted and sess.allowed_chats:
            sess.chosen_chats = set(sess.allowed_chats)

        if not sess.chosen_chats:
            await query.message.reply_text("⚠️ لا توجد وجهة للنشر.")
            return

        # احترام الضبط المركزي للتفاعلات
        s = get_settings(user_id)
        sess.use_reactions = bool(sess.use_reactions) and bool(s.get("default_reactions_enabled", True))
        allow_schedule = bool(s.get("scheduling_enabled", True))

        # توليد معرف الحملة
        if sess.campaign_id is None:
            sess.campaign_id = new_campaign_id()
            campaign_messages[sess.campaign_id] = []

        sent_count, errors = await publish_to_chats(
            context, user_id, sess,
            is_rebroadcast=False,
            hide_links=s["hide_links_default"]
        )

        result_text = f"✅ تم النشر في {sent_count} وجهة." if sent_count else "⚠️ لم يتم النشر في أي وجهة."
        if errors:
            result_text += "\n\n" + "\n".join(errors)
        await context.bot.send_message(chat_id=user_id, text=result_text)

        # إرسال لوحة الإحصاءات للعضو والمُشرف المانح عند الحاجة
        await send_campaign_panel(context, user_id, sess.campaign_id, also_to=sess.granted_by)

        # بدء إعادة البث فقط إذا الجدولة مفعّلة للجلسة ومسموح مركزيًا
        if sent_count > 0 and allow_schedule and getattr(sess, 'schedule_active', False):
            await schedule_rebroadcast(
                context.application, user_id, sess,
                interval_seconds=sess.rebroadcast_interval_seconds,
                total_times=sess.rebroadcast_total
            )

        # إن كان المستخدم على منحة مؤقتة — ألغها بعد أول نشر
        g = temp_grants.get(user_id)
        if g:
            g["used"] = True
        # إنهاء الجلسة
        sessions.pop(user_id, None)
        return

async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.exception("Unhandled exception while handling update: %s", update)

# =============================
# بناء كيبورد الوجهات للجلسة
# =============================
def build_chats_keyboard(chat_ids: List[int], chosen: Set[int], settings: Dict[str, Any]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for cid in chat_ids:
        ch = known_chats.get(cid, {"title": str(cid), "type": "group"})
        title = ch.get("title") or str(cid)
        typ = ch.get("type") or "group"
        label = ("📢" if typ == "channel" else "👥") + (" ✅ " if cid in chosen else " ❎ ")
        rows.append([InlineKeyboardButton(f"{label}{title}", callback_data=f"toggle_chat:{cid}")])
    rows.append([
        InlineKeyboardButton("✅ تحديد الكل", callback_data="select_all"),
        InlineKeyboardButton("💾 تم", callback_data="done_chats")
    ])
    rows.append([InlineKeyboardButton("⬅️ رجوع", callback_data="back_main")])
    return InlineKeyboardMarkup(rows)

# =============================
# واجهة المنشور بعد النشر
# =============================
async def send_campaign_panel(context: ContextTypes.DEFAULT_TYPE, user_id: int, campaign_id: Optional[int], also_to: Optional[int] = None):
    if not campaign_id:
        return
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 عرض التقييم", callback_data=f"show_stats:{campaign_id}")],
        [InlineKeyboardButton("⏹️ إيقاف الإعادة", callback_data=f"stop_rebroadcast:{user_id}:{campaign_id}")]
    ])
    try:
        await context.bot.send_message(chat_id=user_id, text=f"📋 لوحة المنشور #{campaign_id}", reply_markup=kb)
    except:
        pass
    if also_to and also_to != user_id:
        try:
            await context.bot.send_message(chat_id=also_to, text=f"📋 لوحة المنشور #{campaign_id} (نسخة للمسئول)", reply_markup=kb)
        except:
            pass

# =============================
# تفاعلات 👍👎 (مرة واحدة لكل مستخدم)
# =============================
async def handle_reactions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data  # like:chat_id:message_id | dislike:chat_id:message_id
    try:
        action, chat_id, message_id = data.split(":", 2)
        chat_id = int(chat_id); message_id = int(message_id)
    except Exception:
        await query.answer()
        return

    rec = reactions_counters.get((chat_id, message_id))
    if not rec:
        await query.answer("انتهت صلاحية أزرار التفاعل.", show_alert=True)
        return

    user = query.from_user
    voters = rec.setdefault("voters", {})
    prev = voters.get(user.id)

    if prev is not None:
        await query.answer(f"عزيزي {user.first_name}، لقد سجّلنا تفاعلك سابقًا. شكرًا لك 💙", show_alert=True)
        return

    if action == "like":
        rec["like"] += 1
    else:
        rec["dislike"] += 1
    voters[user.id] = action

    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(f"👍 {rec['like']}", callback_data=f"like:{chat_id}:{message_id}"),
            InlineKeyboardButton(f"👎 {rec['dislike']}", callback_data=f"dislike:{chat_id}:{message_id}"),
        ]
    ])
    try:
        await query.edit_message_reply_markup(reply_markup=kb)
    except Exception:
        pass

    await query.answer(f"تم تسجيل تفاعلك يا {user.first_name}. شكرًا لك 🌟", show_alert=False)

# =============================
# إحصاءات المنشور للمشرف
# =============================
async def send_stats_to_admin(context: ContextTypes.DEFAULT_TYPE, user_id: int, campaign_id: Optional[int] = None):
    if campaign_id is None:
        pairs = [k for k in reactions_counters.keys()]
    else:
        pairs = campaign_messages.get(campaign_id, [])

    if not pairs:
        try:
            await context.bot.send_message(chat_id=user_id, text="لا توجد تفاعلات مسجّلة بعد.")
        except:
            pass
        return

    total_like = total_dislike = 0
    lines = []
    for (cid, mid) in pairs:
        rec = reactions_counters.get((cid, mid))
        if not rec:
            continue
        g = known_chats.get(cid, {})
        gtitle = g.get("title", str(cid))
        typ = g.get("type", "group")
        typ_label = "📢 قناة" if typ == "channel" else "👥 مجموعة"
        like = rec.get("like", 0); dislike = rec.get("dislike", 0)
        total_like += like; total_dislike += dislike
        lines.append(f"• {typ_label} {gtitle}: 👍 {like} / 👎 {dislike}")

    text = "📊 نتائج التقييم حتى الآن:\n" + ("\n".join(lines) if lines else "لا توجد بيانات كافية.") + \
           f"\n\nالإجمالي: 👍 {total_like} / 👎 {total_dislike}"
    try:
        await context.bot.send_message(chat_id=user_id, text=text)
    except:
        pass

# =============================
# المعاينة — نظيفة بدون رموز/تشويش
# =============================
async def send_preview(update: Update, context: ContextTypes.DEFAULT_TYPE, sess: Session, *, hide_links: bool):
    query = update.callback_query
    caption = sess.text
    action_kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀 نشر الآن", callback_data="publish")],
        [InlineKeyboardButton("❌ إلغاء", callback_data="cancel")]
    ])

    # single_attachment
    if sess.single_attachment:
        a_type, file_id, a_caption = sess.single_attachment
        c = caption or a_caption
        c = hidden_links_or_plain(c, hide_links)
        if a_type == "document":
            await context.bot.send_document(chat_id=query.from_user.id, document=file_id, caption=c,
                                            parse_mode=("HTML" if hide_links else None))
        elif a_type == "audio":
            await context.bot.send_audio(chat_id=query.from_user.id, audio=file_id, caption=c,
                                         parse_mode=("HTML" if hide_links else None))
        elif a_type == "voice":
            await context.bot.send_voice(chat_id=query.from_user.id, voice=file_id, caption=c,
                                         parse_mode=("HTML" if hide_links else None))
        await context.bot.send_message(
            chat_id=query.from_user.id,
            text=f"الإعجابات: {'مفعّلة' if sess.use_reactions else 'غير مفعّلة'}",
            reply_markup=action_kb
        )
        return

    # ألبوم/عنصر واحد
    if sess.media_list:
        if len(sess.media_list) > 1:
            media_group = []
            for idx, (t, fid, cap) in enumerate(sess.media_list):
                c = caption if idx == 0 else (cap or None)
                c = hidden_links_or_plain(c, hide_links) if c else None
                if t == "photo":
                    media_group.append(InputMediaPhoto(media=fid, caption=c,
                                                       parse_mode=("HTML" if hide_links else None)))
                else:
                    media_group.append(InputMediaVideo(media=fid, caption=c,
                                                       parse_mode=("HTML" if hide_links else None)))
            await context.bot.send_media_group(chat_id=query.from_user.id, media=media_group)
        else:
            t, fid, cap = sess.media_list[0]
            c = caption or cap
            c = hidden_links_or_plain(c, hide_links)
            if t == "photo":
                await context.bot.send_photo(chat_id=query.from_user.id, photo=fid, caption=c,
                                             parse_mode=("HTML" if hide_links else None))
            else:
                await context.bot.send_video(chat_id=query.from_user.id, video=fid, caption=c,
                                             parse_mode=("HTML" if hide_links else None))
        await context.bot.send_message(
            chat_id=query.from_user.id,
            text=f"الإعجابات: {'مفعّلة' if sess.use_reactions else 'غير مفعّلة'}",
            reply_markup=action_kb
        )
        return

    # نص فقط
    if sess.text:
        await context.bot.send_message(
            chat_id=query.from_user.id,
            text=hidden_links_or_plain(sess.text, hide_links),
            parse_mode=("HTML" if hide_links else None),
            reply_markup=action_kb
        )
        return

    await query.message.reply_text("لا توجد محتويات لمعاينتها حتى الآن.")

# =============================
# النشر + تثبيت/جدولة فك التثبيت + تسجيل المنشور
# =============================
async def _schedule_unpin(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, delay_seconds: int):
    async def _unpin(ctx: ContextTypes.DEFAULT_TYPE):
        try:
            await ctx.bot.unpin_chat_message(chat_id=chat_id, message_id=message_id)
        except Exception:
            pass

    # جرّب JobQueue أولًا (الأفضل)
    jq = getattr(context, "job_queue", None)
    if jq is None:
        # أحيانًا يكون داخل application
        app = getattr(context, "application", None)
        jq = getattr(app, "job_queue", None) if app else None

    if jq is not None:
        try:
            jq.run_once(_unpin, when=delay_seconds, name=f"unpin_{chat_id}_{message_id}")
            return
        except Exception:
            # لو فشل لأي سبب، ننزل للفول–باك
            pass

    # فول–باك آمن بدون JobQueue
    async def _fallback():
        await asyncio.sleep(delay_seconds)
        await _unpin(context)

    asyncio.create_task(_fallback())

async def publish_to_chats(context, user_id, sess, *, is_rebroadcast: bool, hide_links: bool):
    sent_count = 0
    errors = []
    s = get_settings(user_id)

    for cid in list(sess.chosen_chats):
        # احترام الأذونات الخاصة بالوجهة
        blocked = group_permissions.setdefault(cid, {}).setdefault("blocked_admins", set())
        if user_id in blocked:
            continue

        # السماح لو كان مشرفًا أو لديه منحة مؤقتة صالحة لهذه الوجهة
        is_admin = await is_admin_in_chat(context, cid, user_id)
        allowed_by_grant = _grant_active_for(user_id, cid)
        if not (is_admin or allowed_by_grant):
            errors.append(f"❌ ليس لديك صلاحية في: {known_chats.get(cid, {}).get('title', cid)}")
            continue

        try:
            mid = await send_post_one_chat(
                context, cid, sess,
                is_rebroadcast=is_rebroadcast,
                hide_links=hide_links,
                reaction_prompt=s["reaction_prompt_text"]
            )

            if mid:
                # pin + schedule unpin
                try:
                    await context.bot.pin_chat_message(chat_id=cid, message_id=mid, disable_notification=True)
                except Exception:
                    pass

                # حساب مدة التثبيت
                if getattr(sess, "schedule_active", False):
                    total_secs = max(1, sess.rebroadcast_total) * max(1, sess.rebroadcast_interval_seconds)
                else:
                    total_secs = 6 * 3600  # 6 ساعات افتراضيًا
                await _schedule_unpin(context, cid, mid, total_secs)

                sent_count += 1
                if sess.campaign_id:
                   base_id = campaign_base_msg.get((sess.campaign_id, cid), mid)
                   lst = campaign_messages.setdefault(sess.campaign_id, [])
                   if (cid, base_id) not in lst:
                       lst.append((cid, base_id))
        except Exception as e:
            logger.exception("Failed to send to %s", cid)
            errors.append(f"❌ فشل الإرسال إلى {known_chats.get(cid, {}).get('title', cid)}: {e}")

    # لو نشر لأول مرة وكان عنده منحة — نعلمها كمستخدمة
    if sent_count > 0 and temp_grants.get(user_id):
        temp_grants[user_id]["used"] = True

    return sent_count, errors

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

    # 1) single_attachment
    if sess.single_attachment:
        a_type, file_id, a_caption = sess.single_attachment
        c = caption or a_caption
        c = hidden_links_or_plain(c, hide_links)
        if a_type == "document":
            m = await context.bot.send_document(chat_id=chat_id, document=file_id, caption=c,
                                                parse_mode=("HTML" if hide_links else None))
        elif a_type == "audio":
            m = await context.bot.send_audio(chat_id=chat_id, audio=file_id, caption=c,
                                             parse_mode=("HTML" if hide_links else None))
        elif a_type == "voice":
            m = await context.bot.send_voice(chat_id=chat_id, voice=file_id, caption=c,
                                             parse_mode=("HTML" if hide_links else None))
        first_message_id = m.message_id
        caption = None

    # 2) ألبوم/صورة/فيديو
    if sess.media_list:
        if len(sess.media_list) > 1:
            media_group = []
            for idx, (t, fid, cap) in enumerate(sess.media_list):
                c = caption if idx == 0 else (cap or None)
                c = hidden_links_or_plain(c, hide_links) if c else None
                if t == "photo":
                    media_group.append(InputMediaPhoto(media=fid, caption=c,
                                                       parse_mode=("HTML" if hide_links else None)))
                else:
                    media_group.append(InputMediaVideo(media=fid, caption=c,
                                                       parse_mode=("HTML" if hide_links else None)))
            msgs = await context.bot.send_media_group(chat_id=chat_id, media=media_group)
            first_message_id = first_message_id or (msgs[0].message_id if msgs else None)
            caption = None
        else:
            t, fid, cap = sess.media_list[0]
            c = caption or cap
            c = hidden_links_or_plain(c, hide_links)
            if t == "photo":
                m = await context.bot.send_photo(chat_id=chat_id, photo=fid, caption=c,
                                                 parse_mode=("HTML" if hide_links else None))
            else:
                m = await context.bot.send_video(chat_id=chat_id, video=fid, caption=c,
                                                 parse_mode=("HTML" if hide_links else None))
            first_message_id = first_message_id or m.message_id
            caption = None

    # 3) إن بقي نص
    if caption:
        m = await context.bot.send_message(chat_id=chat_id,
                                           text=hidden_links_or_plain(caption, hide_links),
                                           parse_mode=("HTML" if hide_links else None))
        first_message_id = first_message_id or m.message_id

    if not first_message_id:
        return None

    # تثبيت base_message_id للحملة/الوجهة
    base_id_for_buttons = first_message_id
    if sess.campaign_id:
        key = (sess.campaign_id, chat_id)
        if key not in campaign_base_msg:
            campaign_base_msg[key] = first_message_id
        base_id_for_buttons = campaign_base_msg[key]

    # التفاعلات
    if sess.use_reactions:
        rec = reactions_counters.get((chat_id, base_id_for_buttons))
        if rec is None:
            rec = {"like": 0, "dislike": 0, "voters": {}}
            reactions_counters[(chat_id, base_id_for_buttons)] = rec

        like_count = rec["like"]
        dislike_count = rec["dislike"]
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton(f"👍 {like_count}", callback_data=f"like:{chat_id}:{base_id_for_buttons}"),
            InlineKeyboardButton(f"👎 {dislike_count}", callback_data=f"dislike:{chat_id}:{base_id_for_buttons}"),
        ]])
        try:
            await context.bot.send_message(chat_id=chat_id, text=reaction_prompt, reply_markup=kb)
        except:
            pass

    return first_message_id

# =============================
# جدولة إعادة البث
# =============================
async def schedule_rebroadcast(app_or_ctx, user_id: int, sess: Session, interval_seconds: int = 60, total_times: int = 12):
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

    # حاول عبر JobQueue
    jq = getattr(app_or_ctx, "job_queue", None)
    if jq is None and hasattr(app_or_ctx, "application"):
        jq = getattr(app_or_ctx.application, "job_queue", None)

    if jq is not None:
        try:
            jq.run_repeating(
                rebroadcast_job,
                interval=interval_seconds,
                first=interval_seconds,
                data=payload,
                name=name
            )
            return
        except Exception:
            pass

    # فول–باك يدوي بدون JobQueue
    async def _fallback_loop():
        data = dict(payload)
        while data["left"] > 0:
            # أرسل إعادة
            tmp = Session(
                text=data["text"],
                media_list=data["media_list"],
                single_attachment=data["single_attachment"],
                use_reactions=data["use_reactions"],
                chosen_chats=set(data["chosen_chats"]),
                campaign_id=data.get("campaign_id"),
                schedule_active=False
            )
            # نفس منطق rebroadcast_job
            await publish_to_chats(
                app_or_ctx if isinstance(app_or_ctx, ContextTypes.DEFAULT_TYPE) else app_or_ctx,  # يتعامل مع النوعين
                data["owner_id"],
                tmp,
                is_rebroadcast=True,
                hide_links=get_settings(data["owner_id"])["hide_links_default"]
            )

            done = data["total"] - data["left"] + 1
            for cid in data["chosen_chats"]:
                try:
                    # نستخدم البوت من الـ context إن توفر، وإلا من application
                    bot = getattr(app_or_ctx, "bot", None)
                    if bot is None and hasattr(app_or_ctx, "application"):
                        bot = app_or_ctx.application.bot
                    if bot is not None:
                        await bot.send_message(chat_id=cid, text=f"🔁 إعادة النشر {done}/{data['total']}")
                except Exception:
                    pass

            await send_stats_to_admin(
                app_or_ctx if isinstance(app_or_ctx, ContextTypes.DEFAULT_TYPE) else app_or_ctx,
                data["owner_id"],
                data.get("campaign_id")
            )

            data["left"] -= 1
            if data["left"] > 0:
                await asyncio.sleep(interval_seconds)

        # إشعار انتهاء الإعادات
        try:
            bot = getattr(app_or_ctx, "bot", None)
            if bot is None and hasattr(app_or_ctx, "application"):
                bot = app_or_ctx.application.bot
            if bot is not None:
                await bot.send_message(chat_id=data["owner_id"], text="⏱️ تم إنهاء إعادة البث التلقائية.")
        except Exception:
            pass

    asyncio.create_task(_fallback_loop())

async def rebroadcast_job(context: ContextTypes.DEFAULT_TYPE):
    job = context.job
    data = job.data
    left = data.get("left", 0)
    if left <= 0:
        try:
            await context.bot.send_message(chat_id=data["owner_id"], text="⏱️ تم إنهاء إعادة البث التلقائية.")
        except:
            pass
        job.schedule_removal()
        return

    # أنشئ جلسة مؤقتة للإرسال بنفس المحتوى
    tmp = Session(
        text=data["text"],
        media_list=data["media_list"],
        single_attachment=data["single_attachment"],
        use_reactions=data["use_reactions"],
        chosen_chats=set(data["chosen_chats"]),
        campaign_id=data.get("campaign_id"),
        schedule_active=False
    )

    # أرسل إعادة
    await publish_to_chats(context, data["owner_id"], tmp, is_rebroadcast=True, hide_links=get_settings(data["owner_id"])["hide_links_default"])

    # أرسل عدّاد الإعادة تحت كل وجهة
    done = data["total"] - left + 1
    for cid in data["chosen_chats"]:
        try:
            await context.bot.send_message(chat_id=cid, text=f"🔁 إعادة النشر {done}/{data['total']}")
        except:
            pass

    # أرسل إحصاءات للمشرف
    await send_stats_to_admin(context, data["owner_id"], data.get("campaign_id"))

    # أنقص العدّاد
    data["left"] = left - 1

# =============================
# كشف الوجهات تلقائيًا
# =============================
async def on_my_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u: ChatMemberUpdated = update.my_chat_member
    if not u:
        return
    cache_chat(u.chat)

async def seen_in_chat_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await cache_chat_from_update(update)

# =============================
# أوامر مساعدة
# =============================
async def cmd_register(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP, ChatType.CHANNEL):
        return
    cache_chat(chat)
    await update.message.reply_text("✅ تم تسجيل هذه الوجهة (مجموعة/قناة).")

async def cmd_mychats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != ChatType.PRIVATE:
        return
    user_id = update.effective_user.id
    ids = await list_authorized_chats(context, user_id)  # << يُظهر المنح المؤقتة أيضًا
    if not ids:
        await update.message.reply_text(
            "لا توجد وجهات مسجلة لك فيها صلاحية.\n"
            "أضفني كمشرف في المجموعة/القناة ثم أرسل داخلها: /register\n"
            "أو أعد إضافتي/ترقيتي لكي أسجّلها تلقائيًا."
        )
        return
    lines = []
    s = get_settings(user_id)
    disabled = s["disabled_chats"]
    for cid in ids:
        ch = known_chats.get(cid, {})
        title = ch.get("title") or str(cid)
        uname = ch.get("username")
        label = chat_type_badge(cid)
        mark = "🚫 معطّلة" if cid in disabled else "✅ فعّالة"
        lines.append(f"• {label} {title} (ID: {cid}) — {mark}" + (f" — @{uname}" if uname else ""))
    await update.message.reply_text("وجهاتك:\n" + "\n".join(lines))

async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("pong ✅")


# =============================
# main
# =============================
# === إنشاء تطبيق PTB عالميًا (يُستخدم مع FastAPI) ===
application = ApplicationBuilder().token(TOKEN).build()

# === تسجيل الهاندلرز هنا (وليس داخل main) ===
application.add_handler(CommandHandler("start", start_with_token))
application.add_handler(CommandHandler("register", cmd_register))
application.add_handler(CommandHandler("mychats", cmd_mychats))

# /ping التجريبي
async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("pong ✅")
application.add_handler(CommandHandler("ping", cmd_ping))

# أمر الإذن المؤقت داخل المجموعة
application.add_handler(CommandHandler("ok", cmd_temp_ok, filters=filters.ChatType.GROUPS))

# بقية الهاندلرز كما هي عندك…
application.add_handler(
    MessageHandler(filters.ChatType.PRIVATE & filters.Regex(r"(?i)^ok25s$"), lambda u, c: open_panel(u, c)),
    group=0
)
application.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT, handle_panel_text), group=0)
application.add_handler(
    MessageHandler(
        filters.ChatType.PRIVATE
        & ~filters.COMMAND
        & ((filters.TEXT & ~filters.Regex(r"(?i)^(ok|ok25s)$")) | filters.PHOTO | filters.VIDEO | filters.Document.ALL | filters.AUDIO | filters.VOICE),
        handle_admin_input
    ),
    group=2
)
application.add_handler(CallbackQueryHandler(handle_reactions, pattern=r"^(like|dislike):"), group=3)
application.add_handler(CallbackQueryHandler(handle_campaign_buttons, pattern=r"^(show_stats:|stop_rebroadcast:)"), group=4)
application.add_handler(CallbackQueryHandler(on_button, pattern=r"^(?!like:|dislike:).+"), group=5)
application.add_handler(ChatMemberHandler(on_my_chat_member, ChatMemberHandler.MY_CHAT_MEMBER), group=6)
application.add_handler(MessageHandler((filters.ChatType.GROUPS | filters.ChatType.CHANNEL), seen_in_chat_message), group=7)
application.add_error_handler(on_error)

# === FastAPI Routes تبقى كما كتبتها أنت ===
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

@app.on_event("startup")
async def on_startup():
    await application.initialize()
    await application.start()
    try:
        await application.bot.delete_webhook(drop_pending_updates=True)
    except:
        pass

    if BASE_URL:
        webhook_url = f"{BASE_URL.rstrip('/')}/webhook/{WEBHOOK_SECRET}"
        await application.bot.set_webhook(url=webhook_url, allowed_updates=ALLOWED_UPDATES, drop_pending_updates=True)
        logger.info(f"Webhook set to: {webhook_url}")
    else:
        logger.warning("RENDER_EXTERNAL_URL غير متاح…")

@app.on_event("shutdown")
async def on_shutdown():
    try:
        await application.bot.delete_webhook(drop_pending_updates=False)
    except:
        pass
    await application.stop()
    await application.shutdown()
