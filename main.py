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

# =============================
# أداة الوقت بتوقيت السعودية
# =============================
KSA_TZ = timezone(timedelta(hours=3))

# مدة صلاحية الإذن المؤقت (بالدقائق)
GRANT_TTL_MINUTES = 30

campaign_messages: Dict[int, List[Tuple[int, int]]] = {}
campaign_base_msg: Dict[Tuple[int, int], int] = {}

# خرائط الإذن المؤقت
# token -> {"user_id": int, "chat_id": int, "expires": datetime}
start_tokens: Dict[str, Dict[str, Any]] = {}
# user_id -> {"chat_id": int, "expires": datetime, "used": bool, "granted_by": Optional[int]}
temp_grants: Dict[int, Dict[str, Any]] = {}


# ===== [FIX] Regexات التنظيف + تعريف Session + أدوات المنح المؤقت =====
START_TOKEN_RE = re.compile(r"/start\s+\S+", re.IGNORECASE)
IDSHAT_RE      = re.compile(r"idshat\S*", re.IGNORECASE)
URL_RE         = re.compile(r"(https?://[\w\-._~:/?#\[\]@!$&'()*+,;=%]+)")

@dataclass
class Session:
    # حالة الجمع/التجهيز
    stage: str = "waiting_first_input"  # waiting_first_input | collecting | ready_options | choosing_chats
    text: Optional[str] = None
    media_list: List[Tuple[str, str, Optional[str]]] = field(default_factory=list)  # [(type, file_id, caption)]
    single_attachment: Optional[Tuple[str, str, Optional[str]]] = None             # ("document"/"audio"/"voice", id, cap)

    # خيارات المنشور
    use_reactions: bool = True
    pin_enabled: bool = True

    # الوجهات واللوحات
    chosen_chats: Set[int] = field(default_factory=set)
    picker_msg_id: Optional[int] = None
    panel_msg_id: Optional[int] = None

    # إعدادات الإعادة (لكل جلسة)
    schedule_active: bool = False
    rebroadcast_interval_seconds: int = 7200  # افتراضي 2 ساعة
    rebroadcast_total: int = 0                # عدد الإعادات

    # الحملة والتتبع
    campaign_id: Optional[int] = None

    # تصريح مؤقت
    allowed_chats: Set[int] = field(default_factory=set)  # وجهات محددة بالتصريح فقط
    is_temp_granted: bool = False
    granted_by: Optional[int] = None

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

# === Campaign ID generator (بسيط وآمن) ===
_campaign_seq = 0
def new_campaign_id() -> int:
    global _campaign_seq
    _campaign_seq = (_campaign_seq + 1) % 10_000_000
    return _campaign_seq or 1

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
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Set, Any

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
    parts.append(f"{'📌 مفعّل' if getattr(sess, 'pin_enabled', True) else '📌 معطّل'} تثبيت")
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

    # زر التفاعلات
    if settings.get("default_reactions_enabled", True):
        btns.append([
            InlineKeyboardButton(
                "👍 إضافة تفاعلات" if not sess.use_reactions else "🧹 إزالة التفاعلات",
                callback_data="toggle_reactions"
            )
        ])

    # اختيار الوجهات (إن لم تكن جلسة تصريح بوجهة واحدة)
    if not (sess.is_temp_granted and sess.allowed_chats):
        btns.append([InlineKeyboardButton("🗂️ اختيار الوجهات (مجموعات/قنوات)", callback_data="choose_chats")])

    # زر الجدولة/الإعادة
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

    # ✅ زر التثبيت — هنا بالضبط بعد الإعادة وقبل الرجوع
    btns.append([
        InlineKeyboardButton(
            "📌 تفعيل التثبيت" if not getattr(sess, "pin_enabled", True) else "📌 تعطيل التثبيت",
            callback_data="toggle_pin"
        )
    ])

    # رجوع للتعديل
    btns.append([InlineKeyboardButton("⬅️ الرجوع للتعديل", callback_data="back_to_collect")])

    # مسح / إنهاء
    btns.append([
        InlineKeyboardButton("🧽 مسح", callback_data="clear"),
        InlineKeyboardButton("❌ إنهاء", callback_data="cancel")
    ])

    # معاينة
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
    data = query.data

    # ⬅️ مهم: لا تتعامل هنا مع أزرار التفاعل حتى لا تُرسل "لا توجد جلسة"
    if data.startswith(("like:", "dislike:")):
        return

    await query.answer()
    
    user_id = query.from_user.id
    s = get_settings(user_id)
    sess = sessions.get(user_id)
    data = query.data

    # ==== تشخيص: سجل كل ضغط زر في اللوج ====
    try:
        logger.info(
            "CB >> user=%s data=%s stage=%s chosen=%s",
            user_id,
            data,
            (sessions.get(user_id).stage if sessions.get(user_id) else None),
            (len(sessions.get(user_id).chosen_chats) if sessions.get(user_id) else None),
        )
    except Exception:
        logger.exception("CB log failed")
    # =========================================

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

    # زر التثبيت (يحترم التعطيل المركزي)
    if data == "toggle_pin":
        if not global_settings.get("pin_feature_enabled", True):
            await query.answer("📌 خيار التثبيت مُعطّل مركزيًا.", show_alert=True)
            return
        sess.pin_enabled = not bool(getattr(sess, "pin_enabled", True))
        await push_panel(context, user_id, sess, "✅ تم ضبط خيار التثبيت.")
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

    # تغيير عدد مرات الإعادة (يحترم القفل/التعطيل)
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
        return

    # تغيير الفاصل الزمني للإعادة (يحترم القفل/التعطيل)
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
        return

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
            await query.message.reply_text(
                "⚠️ لا توجد وجهة للنشر.\nيرجى اختيار مجموعة أو قناة من «اختيار الوجهات».",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ رجوع", callback_data="back_main")]])
            )
            return

        # احترام الضبط المركزي للتفاعلات
        s = get_settings(user_id)
        sess.use_reactions = bool(sess.use_reactions) and bool(s.get("default_reactions_enabled", True))
        # احترام الحالة المركزية للجدولة (وليس من إعدادات المستخدم)
        allow_schedule = bool(global_settings.get("scheduling_enabled", True))

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

        # ⬇️ عرض بلوك الحالة بعد النشر (إعجابات + جدولة)
        status_block = build_status_block(sess, allow_schedule=allow_schedule)
        await context.bot.send_message(chat_id=user_id, text=result_text + "\n\n" + status_block)

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
        action, chat_id_str, message_id_str = data.split(":", 2)
        chat_id = int(chat_id_str)
        message_id = int(message_id_str)
    except Exception:
        await query.answer()
        return

    rec = reactions_counters.get((chat_id, message_id))
    if not rec:
        await query.answer("انتهت صلاحية أزرار التفاعل.", show_alert=True)
        return

    user = query.from_user
    voters = rec.setdefault("voters", {})
    if user.id in voters:
        await query.answer(f"يا {user.first_name}، تفاعلك مسجّل مسبقًا. شكرًا 💙", show_alert=True)
        return

    # تحديث العدّاد وتسجيل التصويت
    if action == "like":
        rec["like"] = rec.get("like", 0) + 1
    else:
        rec["dislike"] = rec.get("dislike", 0) + 1
    voters[user.id] = action

    # تحديث أزرار العدّادات
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton(f"👍 {rec['like']}", callback_data=f"like:{chat_id}:{message_id}"),
        InlineKeyboardButton(f"👎 {rec['dislike']}", callback_data=f"dislike:{chat_id}:{message_id}"),
    ]])
    try:
        await query.edit_message_reply_markup(reply_markup=kb)
    except Exception:
        pass

    # رسالة شكر مهذّبة بالـ mention + رد على المنشور (بدون إعادة نشره)
    try:
        emoji = "👍" if action == "like" else "👎"
        thank_text = (
            f"🙏 شكرًا {user.mention_html()} على تفاعلك {emoji}!\n"
            "آراؤكم تهمّنا — إذا ما صوّتّ بعد، استخدم الأزرار أسفل المنشور. 💬"
        )
        await context.bot.send_message(
            chat_id=chat_id,
            text=thank_text,
            reply_to_message_id=message_id,  # ← رد على المنشور الأصلي
            parse_mode="HTML"
        )
    except Exception:
        pass

    await query.answer("تم تسجيل تفاعلك. شكرًا لك 🌟", show_alert=False)

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
# ضعها في قسم الدوال المساعدة (مرة واحدة)
def build_status_block(sess, *, allow_schedule: bool | None = None) -> str:
    likes_line = f"الإعجابات: {'👍👎 مفعّلة' if bool(getattr(sess, 'use_reactions', False)) else '❌ غير مفعّلة'}"
    pin_line   = f"التثبيت: {'📌 مفعّل' if bool(getattr(sess, 'pin_enabled', True)) else '📌 معطّل'}"

    if allow_schedule is None:
        allow_schedule = True
    schedule_is_on = bool(allow_schedule) and bool(getattr(sess, "schedule_active", False))

    def _fmt_interval(sec):
        if sec is None:
            return None
        try:
            sec = int(sec)
        except Exception:
            return None
        if sec < 60:
            return f"كل {sec} ثانية"
        mins = sec // 60
        if mins < 60:
            return f"كل {mins} دقيقة"
        hrs = mins // 60
        rem = mins % 60
        return f"كل {hrs} ساعة" if rem == 0 else f"كل {hrs} ساعة و{rem} دقيقة"

    if schedule_is_on:
        parts = ["⏱️ مفعّلة"]
        iv = _fmt_interval(getattr(sess, "rebroadcast_interval_seconds", None))
        if iv:
            parts.append(iv)
        total_times = getattr(sess, "rebroadcast_total", None)
        if isinstance(total_times, int) and total_times > 0:
            parts.append(f"× {total_times} مرّة")
        schedule_line = "الجدولة: " + " — ".join(parts)
    else:
        schedule_line = "الجدولة: ❌ غير مفعّلة"

    return f"{likes_line}\n{pin_line}\n{schedule_line}"

# =============================
# المعاينة — نظيفة بدون رموز/تشويش
# =============================
# استبدل دالة send_preview بالكامل بهذه النسخة
async def send_preview(update: Update, context: ContextTypes.DEFAULT_TYPE, sess: Session, *, hide_links: bool):
    query = update.callback_query
    user_id = query.from_user.id
    caption = sess.text

    action_kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀 نشر الآن", callback_data="publish")],
        [InlineKeyboardButton("❌ إلغاء", callback_data="cancel")]
    ])

    # تنبيه مبكّر إذا لا توجد وجهات مختارة + زر رجوع
    if not getattr(sess, "chosen_chats", None):
        await context.bot.send_message(
            chat_id=user_id,
            text="⚠️ لا توجد وجهة للنشر.\nيرجى اختيار مجموعة أو قناة من «اختيار الوجهات».",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ رجوع", callback_data="back_main")]])
        )
        # نكمّل عرض المعاينة بعدها بشكل طبيعي

    # ===== بلوك الحالة =====
    status_block = build_status_block(sess, allow_schedule=True)

    # single_attachment
    if getattr(sess, "single_attachment", None):
        a_type, file_id, a_caption = sess.single_attachment
        c = caption or a_caption
        c = hidden_links_or_plain(c, hide_links)
        if a_type == "document":
            await context.bot.send_document(chat_id=user_id, document=file_id, caption=c,
                                            parse_mode=("HTML" if hide_links else None))
        elif a_type == "audio":
            await context.bot.send_audio(chat_id=user_id, audio=file_id, caption=c,
                                         parse_mode=("HTML" if hide_links else None))
        elif a_type == "voice":
            await context.bot.send_voice(chat_id=user_id, voice=file_id, caption=c,
                                         parse_mode=("HTML" if hide_links else None))
        await context.bot.send_message(chat_id=user_id, text=status_block, reply_markup=action_kb)
        return

    # ألبوم/عنصر واحد
    if getattr(sess, "media_list", None):
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
            await context.bot.send_media_group(chat_id=user_id, media=media_group)
        else:
            t, fid, cap = sess.media_list[0]
            c = caption or cap
            c = hidden_links_or_plain(c, hide_links)
            if t == "photo":
                await context.bot.send_photo(chat_id=user_id, photo=fid, caption=c,
                                             parse_mode=("HTML" if hide_links else None))
            else:
                await context.bot.send_video(chat_id=user_id, video=fid, caption=c,
                                             parse_mode=("HTML" if hide_links else None))
        await context.bot.send_message(chat_id=user_id, text=status_block, reply_markup=action_kb)
        return

    # نص فقط
    if getattr(sess, "text", None):
        await context.bot.send_message(
            chat_id=user_id,
            text=hidden_links_or_plain(sess.text, hide_links),
            parse_mode=("HTML" if hide_links else None),
        )
        await context.bot.send_message(chat_id=user_id, text=status_block, reply_markup=action_kb)
        return

    await query.message.reply_text("لا يوجد محتوى للمعاينة.")

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
                app_or_ctx if isinstance(app_or_ctx, ContextTypes.DEFAULT_TYPE) else app_or_ctx,
                data["owner_id"],
                tmp,
                is_rebroadcast=True,
                hide_links=get_settings(data["owner_id"])["hide_links_default"]
            )

            done = data["total"] - data["left"] + 1
            for cid in data["chosen_chats"]:
                try:
                    bot = getattr(app_or_ctx, "bot", None)
                    if bot is None and hasattr(app_or_ctx, "application"):
                        bot = app_or_ctx.application.bot
                    if bot is not None:
                        await bot.send_message(chat_id=cid, text=f"🔁 إعادة النشر {done}/{data['total']}")
                except Exception:
                    pass

            # 📊 إرسال التقرير تلقائيًا لصاحب التصريح/الناشر
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
    await publish_to_chats(
        context,
        data["owner_id"],
        tmp,
        is_rebroadcast=True,
        hide_links=get_settings(data["owner_id"])["hide_links_default"]
    )

    # أرسل عدّاد الإعادة تحت كل وجهة
    done = data["total"] - left + 1
    for cid in data["chosen_chats"]:
        try:
            await context.bot.send_message(chat_id=cid, text=f"🔁 إعادة النشر {done}/{data['total']}")
        except:
            pass

    # 📊 إرسال التقرير تلقائيًا لصاحب التصريح/الناشر
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


# ===== [FIX] Helpers: تخزين معلومات المحادثات + صلاحيات الأدمن =====
if 'known_chats' not in globals():
    known_chats = {}
if 'known_chats_admins' not in globals():
    known_chats_admins = {}

async def cache_chat(chat):
    """Cache minimal chat info safely (no API calls)."""
    try:
        known_chats[chat.id] = {
            "title": getattr(chat, "title", None),
            "username": getattr(chat, "username", None),
            "type": getattr(chat, "type", "group"),
        }
    except Exception:
        pass

async def cache_chat_from_update(update):
    chat = getattr(update, "effective_chat", None)
    if chat:
        await cache_chat(chat)

async def is_admin_in_chat(context, chat_id: int, user_id: int) -> bool:
    try:
        mem = await context.bot.get_chat_member(chat_id, user_id)
        return mem.status in ("administrator", "creator")
    except Exception:
        return False

async def chats_where_user_is_admin(context, user_id: int):
    result = set()
    # try cached chats
    for cid in list(known_chats.keys()):
        try:
            mem = await context.bot.get_chat_member(cid, user_id)
            if mem.status in ("administrator", "creator"):
                result.add(cid)
        except Exception:
            pass
    return list(result)

async def list_authorized_chats(context, user_id: int):
    ids = set()
    try:
        g = temp_grants.get(user_id)
        if g and not g.get("used"):
            exp = g.get("expires")
            if (exp is None) or (datetime.utcnow() <= exp):
                ids.add(g.get("chat_id"))
    except Exception:
        pass
    try:
        ids.update(await chats_where_user_is_admin(context, user_id))
    except Exception:
        pass
    return list(ids)


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


# ===== Error handler (Telegram) يجب تعريفه قبل التسجيل =====
async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.exception("Unhandled exception", exc_info=context.error)

# سجّله مرة واحدة فقط
application.add_error_handler(on_error)

# === تسجيل الهاندلرز هنا (وليس داخل main) ===
# =========================
# Admin Control Panel (Full)
# =========================

# --- Safe globals / fallbacks ---
try:
    global_settings
except NameError:
    global_settings = {}
global_settings.setdefault("scheduling_enabled", True)
global_settings.setdefault("schedule_locked", False)
global_settings.setdefault("rebroadcast_interval_seconds", 7200)
global_settings.setdefault("rebroadcast_total", 4)
global_settings.setdefault("pin_feature_enabled", True)
global_settings.setdefault("reactions_feature_enabled", True)
global_settings.setdefault("maintenance_mode", False)

try:
    admin_settings
except NameError:
    admin_settings = {}
try:
    group_permissions
except NameError:
    group_permissions = {}
try:
    known_chats
except NameError:
    known_chats = {}
try:
    sessions
except NameError:
    sessions = {}
try:
    temp_grants
except NameError:
    temp_grants = {}
try:
    reactions_counters
except NameError:
    reactions_counters = {}
try:
    campaign_messages
except NameError:
    campaign_messages = {}
try:
    campaign_base_msg
except NameError:
    campaign_base_msg = {}

# store admins per chat between views
known_chats_admins = {}

PANEL_WAIT_REACTION_PROMPT = "panel_wait_reaction_prompt"

def get_settings(user_id: int):
    s = admin_settings.setdefault(user_id, {})
    s.setdefault("disabled_chats", set())
    s.setdefault("templates", [])
    s.setdefault("logs", [])
    s.setdefault("reaction_prompt_text", "قيّم المنشور من فضلك 👍👎")
    # 👇 مهمّة لمنع KeyError لاحقًا
    s.setdefault("permissions_mode", "all")     # أو "whitelist" لو تبي
    s.setdefault("whitelist", set())
    s.setdefault("hide_links_default", False)   # غطّي الروابط أو لا (براحتك)
    s.setdefault("default_reactions_enabled", True)
    return s

def add_log(user_id: int, text: str):
    s = get_settings(user_id)
    s["logs"].append(text)

# --------------- UI helpers ---------------
from telegram import InlineKeyboardMarkup, InlineKeyboardButton, Update, InputMediaPhoto, InputMediaVideo
from telegram.ext import CallbackQueryHandler, CommandHandler, ContextTypes
from telegram.constants import ChatType

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
        [InlineKeyboardButton(f"🛠️ وضع الصيانة: {'مفعل' if gs.get('maintenance_mode', False) else 'معطل'}",
                              callback_data=f"panel:maintenance:{'off' if gs.get('maintenance_mode', False) else 'on'}")],
    ]
    return InlineKeyboardMarkup(rows)

def panel_settings_keyboard() -> InlineKeyboardMarkup:
    gs = global_settings
    rows = [
        [InlineKeyboardButton(f"📌 التثبيت: {'مفعل' if gs.get('pin_feature_enabled', True) else 'معطل'}",
                              callback_data=f"panel:set:pin:{'off' if gs.get('pin_feature_enabled', True) else 'on'}")],
        [InlineKeyboardButton(f"👍 التفاعلات: {'مفعلة' if gs.get('reactions_feature_enabled', True) else 'معطلة'}",
                              callback_data=f"panel:set:react:{'off' if gs.get('reactions_feature_enabled', True) else 'on'}")],
        [InlineKeyboardButton(f"⏱️ الجدولة: {'مفعلة' if gs.get('scheduling_enabled', True) else 'معطلة'}",
                              callback_data=f"panel:set:schedule:{'off' if gs.get('scheduling_enabled', True) else 'on'}")],
        [InlineKeyboardButton(f"🔒 قفل إعدادات الجدولة: {'مفعل' if gs.get('schedule_locked', False) else 'معطل'}",
                              callback_data=f"panel:set:schedule_lock:{'off' if gs.get('schedule_locked', False) else 'on'}")],
        [InlineKeyboardButton(f"🛠️ وضع الصيانة: {'مفعل' if gs.get('maintenance_mode', False) else 'معطل'}",
                              callback_data=f"panel:set:maintenance:{'off' if gs.get('maintenance_mode', False) else 'on'}")],
        [InlineKeyboardButton("🟥 إيقاف الإعادات الجارية", callback_data="panel:schedule:stop_now")],
        [InlineKeyboardButton("⬅️ رجوع", callback_data="panel:back")],
    ]
    return InlineKeyboardMarkup(rows)

async def cmd_open_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != ChatType.PRIVATE:
        await update.message.reply_text("افتح لوحة التحكّم في الخاص.")
        return
    await update.message.reply_text("🛠️ لوحة التحكّم الرئيسية", reply_markup=panel_main_keyboard())

def build_panel_chats_keyboard(admin_chat_ids, settings) -> InlineKeyboardMarkup:
    rows = []
    disabled = settings.setdefault("disabled_chats", set())
    for cid in admin_chat_ids:
        info = known_chats.get(cid, {})
        title = info.get("title", str(cid))
        typ = info.get("type", "group")
        label = ("📢 قناة" if typ == "channel" else "👥 مجموعة")
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

async def list_authorized_chats(context: ContextTypes.DEFAULT_TYPE, user_id: int):
    admin_chats = []
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
    admins = {}
    try:
        members = await context.bot.get_chat_administrators(chat_id=chat_id)
        for m in members:
            if m.user and not m.user.is_bot:
                admins[m.user.id] = m.user.full_name
    except Exception:
        pass
    known_chats_admins[chat_id] = admins
    return [{"id": uid, "name": name} for uid, name in sorted(admins.items(), key=lambda x: x[1]) ]

# --------------- Core panel handler ---------------
async def handle_control_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
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
        await query.answer()
        return

    # ====== الوجهات ======
    if data == "panel:destinations":
        admin_chat_ids = await list_authorized_chats(context, user_id)
        if not admin_chat_ids:
            await query.message.reply_text("لا توجد وجهات ظاهر أنك مشرف عليها. أضف البوت كمشرف ثم أعد المحاولة.")
            await query.answer()
            return
        kb = build_panel_chats_keyboard(admin_chat_ids, s)
        await query.message.reply_text("🗂️ الوجهات — فعّل/عطّل لكل وجهة ثم احفظ:", reply_markup=kb)
        await query.answer()
        return

    if data.startswith("panel:toggle_chat:"):
        try:
            cid = int(data.split(":", 2)[2])
        except:
            await query.answer()
            return
        disabled = s.setdefault("disabled_chats", set())
        if cid in disabled: disabled.remove(cid)
        else: disabled.add(cid)
        admin_chat_ids = await list_authorized_chats(context, user_id)
        kb = build_panel_chats_keyboard(admin_chat_ids, s)
        try:
            await query.edit_message_reply_markup(reply_markup=kb)
        except:
            pass
        await query.answer("تم التبديل.")
        return

    if data == "panel:dest_save":
        add_log(user_id, "حفظ إعداد الوجهات")
        await query.message.reply_text("💾 تم حفظ إعداد الوجهات.")
        await query.answer()
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
        await query.answer()
        return

    if data == "panel:schedule:stop_now":
        try:
            jobs = context.application.job_queue.jobs()
            for job in jobs:
                if job.name and job.name.startswith("rebroadcast_"):
                    job.schedule_removal()
        except Exception:
            pass
        await query.message.reply_text("⛔ تم إيقاف أي إعادات مجدولة حالياً.")
        await query.answer()
        return

    # ====== التفاعلات ======
    if data == "panel:reactions":
        await query.message.reply_text(
            f"حالة التفاعلات حاليًا: {'مفعلة' if global_settings.get('reactions_feature_enabled', True) else 'معطلة'}\n"
            "يمكنك تشغيل/إيقاف الخدمة من «الإعدادات العامة»، وتعديل نص الدعوة هنا.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✏️ تعديل نص الدعوة", callback_data="panel:reactions:edit")],
                                               [InlineKeyboardButton("⬅️ رجوع", callback_data="panel:back")]])
        )
        await query.answer()
        return
    if data == "panel:reactions:edit":
        panel_state = globals().setdefault("panel_state", {})
        panel_state[user_id] = PANEL_WAIT_REACTION_PROMPT
        await query.message.reply_text("أرسل الآن نص دعوة التفاعل الجديد.")
        await query.answer()
        return

    # ====== القوالب ======
    if data == "panel:templates":
        tpl = s.get("templates", [])
        if not tpl:
            await query.message.reply_text("لا توجد قوالب محفوظة بعد.")
        else:
            names = "\n".join(f"• {t['name']}" for t in tpl)
            await query.message.reply_text("📘 القوالب:\n" + names)
        await query.answer()
        return

    # ====== الأذونات ======
    if data == "panel:permissions":
        if not known_chats:
            await query.message.reply_text("لا توجد وجهات معروفة بعد.")
            await query.answer()
            return
        await query.message.reply_text("اختر وجهة لإدارة مشرفيها:", reply_markup=permissions_root_keyboard())
        await query.answer()
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
        await query.answer()
        return

    if data.startswith("perm:toggle:"):
        _, _, chat_id, uid = data.split(":")
        chat_id = int(chat_id); uid = int(uid)
        blocked = group_permissions.setdefault(chat_id, {}).setdefault("blocked_admins", set())
        if uid in blocked: blocked.remove(uid)
        else: blocked.add(uid)
        await query.answer("تم التبديل.")
        return

    if data.startswith("perm:save:"):
        add_log(user_id, "حفظ إعداد أذونات مشرفي وجهة")
        await query.message.reply_text("💾 تم حفظ أذونات المشرفين للوجهة.")
        await query.answer()
        return

    # ====== السجل ======
    if data == "panel:logs":
        logs = s.get("logs", [])
        text = "🧾 آخر العمليات:\n" + ("\n".join(f"• {x}" for x in logs[-20:]) if logs else "لا يوجد سجل بعد.")
        await query.message.reply_text(text)
        await query.answer()
        return

    # ====== الإعدادات العامة ======
    if data == "panel:settings":
        await query.message.reply_text("⚙️ الإعدادات العامة:", reply_markup=panel_settings_keyboard())
        await query.answer()
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
            for uid in list(sessions.keys()):
                try:
                    await context.bot.send_message(chat_id=uid, text=note)
                except: pass
            for uid, rec in list(temp_grants.items()):
                if rec and not rec.get("used"):
                    try:
                        await context.bot.send_message(chat_id=uid, text=note)
                    except: pass

        try:
            await query.edit_message_reply_markup(reply_markup=panel_settings_keyboard())
        except:
            pass
        await query.answer("تم الحفظ.")
        return

    if data.startswith("panel:maintenance:"):
        val = data.split(":")[2]
        global_settings["maintenance_mode"] = (val == "on")
        add_log(user_id, f"{'تفعيل' if val=='on' else 'إلغاء'} وضع الصيانة")
        note = "🛠️ نظام النشر تحت الصيانة والتحديث حاليًا." if global_settings["maintenance_mode"] else "✅ تم إلغاء وضع الصيانة. عاد النظام للعمل."
        for uid in list(sessions.keys()):
            try:
                await context.bot.send_message(chat_id=uid, text=note)
            except: pass
        for uid, rec in list(temp_grants.items()):
            if rec and not rec.get("used"):
                try:
                    await context.bot.send_message(chat_id=uid, text=note)
                except: pass
        try:
            await query.edit_message_reply_markup(reply_markup=panel_main_keyboard())
        except:
            pass
        await query.answer("تم التنفيذ.")
        return

    if data == "panel:back":
        await query.message.reply_text("🛠️ لوحة التحكّم الرئيسية", reply_markup=panel_main_keyboard())
        await query.answer()
        return

# --------------- Feature-aware UI overrides ---------------
# Save originals (if exist)
orig_status_text = globals().get("status_text")
orig_build_status_block = globals().get("build_status_block")
orig_keyboard_ready_options = globals().get("keyboard_ready_options")
orig_publish_to_chats = globals().get("publish_to_chats")

def status_text(sess: 'Session') -> str:
    gs = global_settings
    parts = []
    if getattr(sess, "text", None):
        parts.append("📝 نص موجود")
    if getattr(sess, "media_list", None):
        parts.append(f"🖼️ وسائط: {len(sess.media_list)}")
    if getattr(sess, "single_attachment", None):
        t = sess.single_attachment[0]
        mapping = {"document":"مستند","audio":"ملف صوتي","voice":"رسالة صوتية"}
        parts.append(f"📎 {mapping.get(t,t)}")
    if gs.get("reactions_feature_enabled", True):
        parts.append(f"{'✅' if getattr(sess, 'use_reactions', False) else '🚫'} تفاعلات")
    if gs.get("pin_feature_enabled", True):
        parts.append(f"{'📌 مفعّل' if getattr(sess, 'pin_enabled', True) else '📌 معطّل'} تثبيت")
    parts.append(f"🎯 وجهات محددة: {len(getattr(sess, 'chosen_chats', []))}")
    if gs.get("scheduling_enabled", True) and getattr(sess, "schedule_active", False):
        parts.append(f"⏱️ الإعادة: كل {getattr(sess,'rebroadcast_interval_seconds',0)//60} دقيقة × {getattr(sess,'rebroadcast_total',0)}")
    else:
        parts.append("⏱️ الإعادة: كل 0 دقيقة × 0")
    return " • ".join(parts)

def build_status_block(sess, *, allow_schedule: bool | None = None) -> str:
    gs = global_settings
    likes_line = None
    if gs.get("reactions_feature_enabled", True):
        likes_line = f"الإعجابات: {'👍👎 مفعّلة' if bool(getattr(sess, 'use_reactions', False)) else '❌ غير مفعّلة'}"
    pin_line = None
    if gs.get("pin_feature_enabled", True):
        pin_line = f"التثبيت: {'📌 مفعّل' if bool(getattr(sess, 'pin_enabled', True)) else '📌 معطّل'}"
    if allow_schedule is None:
        allow_schedule = True
    schedule_is_on = bool(allow_schedule) and bool(getattr(sess, "schedule_active", False)) and gs.get("scheduling_enabled", True)

    def _fmt_interval(sec):
        if not isinstance(sec, int):
            try:
                sec = int(sec)
            except Exception:
                return None
        if sec < 60:
            return f"كل {sec} ثانية"
        mins = sec // 60
        if mins < 60:
            return f"كل {mins} دقيقة"
        hrs = mins // 60
        rem = mins % 60
        return f"كل {hrs} ساعة" if rem == 0 else f"كل {hrs} ساعة و{rem} دقيقة"

    if schedule_is_on:
        parts = ["⏱️ مفعّلة"]
        iv = _fmt_interval(getattr(sess, "rebroadcast_interval_seconds", 0))
        if iv:
            parts.append(iv)
        total_times = getattr(sess, "rebroadcast_total", 0)
        if isinstance(total_times, int) and total_times > 0:
            parts.append(f"× {total_times} مرّة")
        schedule_line = "الجدولة: " + " — ".join(parts)
    else:
        schedule_line = "الجدولة: ❌ غير مفعّلة"

    lines = []
    if likes_line:
        lines.append(likes_line)
    if pin_line:
        lines.append(pin_line)
    lines.append(schedule_line)

    return "\n".join(lines)

def keyboard_ready_options(sess: 'Session', settings: dict) -> InlineKeyboardMarkup:
    gs = global_settings
    btns = []

    # عند التصريح المؤقت: أظهر الوجهة فقط ولا تعرض اختيار الوجهات
    if "_single_destination_row" in globals():
        btns.extend(_single_destination_row(sess))

    # زر التفاعلات
    if gs.get("reactions_feature_enabled", True) and settings.get("default_reactions_enabled", True):
        btns.append([InlineKeyboardButton("👍 إضافة تفاعلات" if not getattr(sess, "use_reactions", False) else "🧹 إزالة التفاعلات",
                                          callback_data="toggle_reactions")])

    # اختيار الوجهات (إن لم تكن جلسة تصريح بوجهة واحدة)
    if not (getattr(sess, "is_temp_granted", False) and getattr(sess, "allowed_chats", set())):
        btns.append([InlineKeyboardButton("🗂️ اختيار الوجهات (مجموعات/قنوات)", callback_data="choose_chats")])

    # زر الجدولة/الإعادة
    if gs.get("scheduling_enabled", True):
        if gs.get("schedule_locked", False):
            btns.append([InlineKeyboardButton(
                f"⏱️ الإعادة (مقفلة): كل {global_settings['rebroadcast_interval_seconds']//3600} ساعة × {global_settings['rebroadcast_total']}",
                callback_data="noop")])
        else:
            label = "⏱️ تفعيل الجدولة" if not getattr(sess, "schedule_active", False) else "⏱️ الإعادة: مفعّلة"
            btns.append([InlineKeyboardButton(label, callback_data="schedule_menu")])

    # زر التثبيت
    if gs.get("pin_feature_enabled", True):
        btns.append([InlineKeyboardButton("📌 تفعيل التثبيت" if not getattr(sess, "pin_enabled", True) else "📌 تعطيل التثبيت",
                                          callback_data="toggle_pin")])

    # رجوع/مسح/إنهاء/معاينة
    btns.append([InlineKeyboardButton("⬅️ الرجوع للتعديل", callback_data="back_to_collect")])
    btns.append([InlineKeyboardButton("🧽 مسح", callback_data="clear"),
                 InlineKeyboardButton("❌ إنهاء", callback_data="cancel")])
    btns.append([InlineKeyboardButton("👁️ معاينة", callback_data="preview")])
    return InlineKeyboardMarkup(btns)

async def publish_to_chats(
    context: ContextTypes.DEFAULT_TYPE,
    user_id: int,
    sess: Session,
    *,
    is_rebroadcast: bool,
    hide_links: bool
):
    """إرسال المنشور إلى جميع الوجهات المختارة مع احترام الصلاحيات، التثبيت، والجدولة."""
    # وضع الصيانة
    if global_settings.get("maintenance_mode", False):
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text="🛠️ نظام النشر تحت الصيانة والتحديث حاليًا. الرجاء المحاولة لاحقًا."
            )
        except Exception:
            pass
        return 0, ["الصيانة مفعلة"]

    sent_count: int = 0
    errors: List[str] = []

    s = get_settings(user_id)
    gs = global_settings

    # استبعاد الوجهات المعطلة لهذا المسئول
    disabled = s.setdefault("disabled_chats", set())
    target_chats: List[int] = [cid for cid in list(getattr(sess, "chosen_chats", [])) if cid not in disabled]

    for cid in target_chats:
        # احترام أذونات الوجهة (حظر مُشرف محدد)
        blocked = group_permissions.setdefault(cid, {}).setdefault("blocked_admins", set())
        if user_id in blocked:
            errors.append(f"🚫 صلاحياتك معطّلة في: {known_chats.get(cid, {}).get('title', cid)}")
            continue

        # التحقق من الصلاحية (نتجاوزها في إعادة البث فقط)
        if not is_rebroadcast:
            try:
                member = await context.bot.get_chat_member(cid, user_id)
                is_admin = member.status in ("administrator", "creator")
            except Exception:
                is_admin = False

            allowed_by_grant = globals().get("_grant_active_for", lambda a, b: False)(user_id, cid)
            if not (is_admin or allowed_by_grant):
                errors.append(f"❌ ليس لديك صلاحية في: {known_chats.get(cid, {}).get('title', cid)}")
                continue

        try:
            # الإرسال الفعلي
            mid = await send_post_one_chat(
                context, cid, sess,
                is_rebroadcast=is_rebroadcast,
                hide_links=hide_links,
                reaction_prompt=s.get("reaction_prompt_text", "قيّم المنشور من فضلك 👍👎")
            )
            if not mid:
                continue

            sent_count += 1

            # تتبّع الحملة (استخدم الرسالة الأساسية للوجهة)
            if getattr(sess, "campaign_id", None):
                base_id = campaign_base_msg.get((sess.campaign_id, cid), mid)
                lst = campaign_messages.setdefault(sess.campaign_id, [])
                if (cid, base_id) not in lst:
                    lst.append((cid, base_id))

            # التثبيت (مرة واحدة في النشر الأول فقط، ليس في الإعادات)
            if (
                not is_rebroadcast
                and gs.get("pin_feature_enabled", True)
                and getattr(sess, "pin_enabled", True)
            ):
                try:
                    await context.bot.pin_chat_message(chat_id=cid, message_id=mid, disable_notification=True)
                except Exception as e:
                    # عدم إيقاف النشر في حال فشل التثبيت (صلاحيات/قناة تمنع التثبيت…)
                    logger.info("Pin failed chat=%s msg=%s err=%s", cid, mid, e)

                # جدولة فك التثبيت إن كانت الإعادات مفعّلة، وإلا افتراضي 6 ساعات
                try:
                    if gs.get("scheduling_enabled", True) and getattr(sess, "schedule_active", False):
                        times = int(getattr(sess, "rebroadcast_total", 0))
                        interval = int(getattr(sess, "rebroadcast_interval_seconds", 0))
                        total_secs = max(1, times * interval) + 900  # هامش 15 دقيقة بعد آخر إعادة
                    else:
                        total_secs = 6 * 3600
                    if "_schedule_unpin" in globals() and total_secs > 0:
                        await _schedule_unpin(context, cid, mid, total_secs)
                except Exception:
                    pass

        except Exception as e:
            try:
                logger.exception("Failed to send to chat %s", cid)
            except Exception:
                pass
            errors.append(f"❌ فشل الإرسال إلى {known_chats.get(cid, {}).get('title', cid)}: {e}")

    # إبطال التصريح المؤقت بعد أول نشر ناجح
    if sent_count > 0 and temp_grants.get(user_id):
        try:
            temp_grants[user_id]["used"] = True
        except Exception:
            pass
    
# Guard preview/start during maintenance (light-touch to avoid changing original handlers)
async def cmd_open_panel_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if global_settings.get("maintenance_mode", False) and update.effective_chat.type == ChatType.PRIVATE:
        await update.message.reply_text("🛠️ نظام النشر تحت الصيانة والتحديث حاليًا. الرجاء المحاولة لاحقًا.")
        return
    await cmd_open_panel(update, context)

application = ApplicationBuilder().token(TOKEN).build()

# =========================
# Handlers Registration (ثبتها مرة واحدة فقط وفي آخر الملف)
# =========================

# أوامر عامة
application.add_handler(CommandHandler("start", start_with_token))
application.add_handler(CommandHandler("ok", cmd_temp_ok, filters=filters.ChatType.GROUPS))  # داخل المجموعات فقط
application.add_handler(CommandHandler("register", cmd_register))
application.add_handler(CommandHandler("mychats", cmd_mychats))
application.add_handler(CommandHandler("ping", cmd_ping))
application.add_handler(CommandHandler("panel", cmd_open_panel))  # يفتح لوحة التحكّم في الخاص

# اختصارات في الخاص (افصل بين ok و ok25s منعًا للتداخل)
application.add_handler(
    MessageHandler(
        filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND & filters.Regex(r"(?i)^\s*ok25s\s*$"),
        open_panel
    ),
    group=0
)
application.add_handler(
    MessageHandler(
        filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND & filters.Regex(r"(?i)^\s*ok\s*$"),
        start_publishing_keyword
    ),
    group=0
)

# إدخال المحتوى في الخاص (نص/وسائط/ملفات) — مع استثناء ok/ok25s والأوامر
application.add_handler(
    MessageHandler(
        filters.ChatType.PRIVATE
        & ~filters.COMMAND
        & (
            (filters.TEXT & ~filters.Regex(r"(?i)^\s*(ok|ok25s)\s*$"))
            | filters.PHOTO
            | filters.VIDEO
            | filters.Document.ALL
            | filters.AUDIO
            | filters.VOICE
        ),
        handle_admin_input
    ),
    group=2
)

# نصوص لوحة التحكّم في الخاص (مثل تعديل نص الدعوة، حفظ اسم القالب… إلخ)
application.add_handler(
    MessageHandler(
        filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND,
        handle_panel_text
    ),
    group=3
)

# ---- أزرار الكولباك (ترتيب الأولوية مهم) ----
# تفاعلات 👍👎
application.add_handler(CallbackQueryHandler(handle_reactions, pattern=r"^(like|dislike):"), group=10)

# أزرار ما بعد النشر (إحصاءات / إيقاف الإعادة)
application.add_handler(CallbackQueryHandler(handle_campaign_buttons, pattern=r"^(show_stats:|stop_rebroadcast:)"), group=11)

# لوحة التحكّم (panel:* / perm:*)
application.add_handler(CallbackQueryHandler(handle_control_buttons, pattern=r"^(panel:|perm:)"), group=12)

# باقي أزرار واجهة النشر (choose_chats / select_all / done_chats / back_main / preview / publish / schedule_menu / ssched_* / toggle_pin ...)
application.add_handler(CallbackQueryHandler(on_button, pattern=r"^(?!like:|dislike:).+"), group=99)

# تتبّع وجود البوت في المحادثات (إضافة/ترقية) + التقاط رسائل المجموعات/القنوات لتخزين بيانات الشات
application.add_handler(ChatMemberHandler(on_my_chat_member, ChatMemberHandler.MY_CHAT_MEMBER), group=20)
application.add_handler(MessageHandler((filters.ChatType.GROUPS | filters.ChatType.CHANNEL), seen_in_chat_message), group=21)


# =========================
# Webhook helpers & routes (مرّة واحدة فقط)
# =========================

# مفتاح الإعداد اليدوي يأتي من البيئة، وإن لم يوجد نرجع للسر نفسه (آمن للنشر)
SETUP_KEY = os.getenv("SETUP_KEY", WEBHOOK_SECRET)

def build_webhook_url() -> Optional[str]:
    if not BASE_URL or not WEBHOOK_SECRET:
        return None
    return f"{BASE_URL.rstrip('/')}/webhook/{WEBHOOK_SECRET}"

# إعداد الويبهوك يدويًا (مفيد لو Render تأخر في تمرير RENDER_EXTERNAL_URL عند الإقلاع الأول)
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

# تعطيل الويبهوك يدويًا (اختياري)
@app.get("/unset-webhook")
async def unset_webhook(request: Request):
    key = request.query_params.get("key")
    if key != SETUP_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")
    ok = await application.bot.delete_webhook(drop_pending_updates=False)
    logger.info("Manual delete_webhook -> %s", ok)
    return {"ok": ok}

# فحوصات بسيطة
@app.api_route("/", methods=["GET", "HEAD"])
async def root():
    return {"status": "ok", "service": "puok-bot"}

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/uptime")
async def uptime():
    return {"ok": True, "ts": datetime.utcnow().isoformat()}

# مسار الويبهوك (لازم يطابق build_webhook_url)
ALLOWED_UPDATES = None  # None = السماح بكل أنواع التحديثات
@app.post("/webhook/{secret}")
async def webhook_handler(secret: str, request: Request):
    if secret != WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")
    data = await request.json()
    update = Update.de_json(data, application.bot)
    await application.process_update(update)
    return {"ok": True}

# دورة الحياة: تهيئة PTB وضبط الويبهوك عند الإقلاع، إيقاف نظيف عند الإطفاء
@app.on_event("startup")
async def on_startup():
    await application.initialize()

    try:
        await application.bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        pass

    url = build_webhook_url()
    if url:
        ok = await application.bot.set_webhook(
            url=url,
            allowed_updates=ALLOWED_UPDATES,
            drop_pending_updates=True
        )
        logger.info("Webhook set to: %s -> %s", url, ok)
    else:
        logger.warning("RENDER_EXTERNAL_URL غير متاح… لن يتم ضبط الويبهوك الآن.")

    await application.start()

@app.on_event("shutdown")
async def on_shutdown():
    try:
        await application.bot.delete_webhook(drop_pending_updates=False)
    except Exception:
        pass
    await application.stop()
    await application.shutdown()
