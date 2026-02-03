"""
Telegram Support Bot v2.0 - Production Ready
=============================================
- Outbox Pattern (keine Nachrichten verlieren)
- copy_message (native Experience)
- SQLite WAL Mode (stabil)
- Inline Buttons (schneller Workflow)
- Smart Follow-ups (VIP 12h, normal 24h)
"""

import asyncio
import logging
import sqlite3
import os
import sys
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any
import html
import json

from telegram import Update, Bot, Message, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters
from telegram.constants import ParseMode, ChatAction
from telegram.error import TelegramError, NetworkError, TimedOut, BadRequest

# ============================================================
# KONFIGURATION
# ============================================================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "8443190094:AAEzVvqKbavZKHmEjsGu2WObrfB43qNfas0")
SUPPORT_GROUP_ID = int(os.environ.get("SUPPORT_GROUP_ID", "-1003870321136"))
ADMIN_IDS = [int(x.strip()) for x in os.environ.get("ADMIN_IDS", "2089427192,6696982829").split(",") if x.strip()]

# Timing
FOLLOWUP_HOURS_NORMAL = 24
FOLLOWUP_HOURS_VIP = 12
FOLLOWUP_MORNING_HOUR = 9
ARCHIVE_AFTER_DAYS = 14
OUTBOX_INTERVAL_SECONDS = 30

# Messages
WELCOME_MESSAGE = os.environ.get("WELCOME_MESSAGE", """Hey! 👋

Schreib mir einfach deine Frage – ich melde mich so schnell wie möglich.

Sprachnachrichten, Bilder, alles kein Problem.""")

STATUS = {"unread": "🔴", "read": "⚪", "answered": "🟢", "closed": "⚫", "followup": "💛"}
PRIORITY = {"normal": "", "vip": "⭐", "urgent": "🚨"}

TEMPLATES = {
    "hi": "Hey! 👋 Wie kann ich dir helfen?",
    "danke": "Gerne! Bei Fragen melde dich einfach 😊",
    "moment": "Einen Moment, ich schau mir das an! 🔍",
    "screenshot": "Kannst du mir einen Screenshot schicken? 📸",
    "erledigt": "Super, freut mich! ✅ Bei Fragen melde dich.",
}

# ============================================================
# DATABASE (SQLite WAL Mode)
# ============================================================

DATA_DIR = Path(os.environ.get("DATA_DIR", "/app/data"))
if not DATA_DIR.exists():
    DATA_DIR = Path(__file__).parent
DB_PATH = DATA_DIR / "support.db"

def get_db():
    """Get database connection with WAL mode and busy timeout"""
    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn

def init_db():
    """Initialize all database tables"""
    conn = get_db()
    c = conn.cursor()
    
    # Main chats table
    c.execute("""
        CREATE TABLE IF NOT EXISTS chats (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            topic_id INTEGER,
            status TEXT DEFAULT 'unread',
            priority TEXT DEFAULT 'normal',
            unread_count INTEGER DEFAULT 0,
            last_message_preview TEXT,
            last_message_type TEXT,
            last_message_at TIMESTAMP,
            last_reply_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_archived INTEGER DEFAULT 0,
            snoozed_until TIMESTAMP,
            followup_stage INTEGER DEFAULT 0,
            followup_done INTEGER DEFAULT 0
        )
    """)
    
    # Messages log
    c.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            direction TEXT,
            msg_type TEXT,
            content TEXT,
            file_id TEXT,
            telegram_msg_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Outbox for reliable delivery
    c.execute("""
        CREATE TABLE IF NOT EXISTS outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            direction TEXT,
            from_chat_id INTEGER,
            to_chat_id INTEGER,
            message_id INTEGER,
            topic_id INTEGER,
            retry_count INTEGER DEFAULT 0,
            next_retry_at TIMESTAMP,
            status TEXT DEFAULT 'pending',
            error TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Message mapping for deletion
    c.execute("""
        CREATE TABLE IF NOT EXISTS sent_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            topic_msg_id INTEGER,
            user_msg_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Sequences (multi-message templates)
    c.execute("""
        CREATE TABLE IF NOT EXISTS sequences (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            position INTEGER,
            msg_type TEXT,
            content TEXT,
            file_id TEXT,
            original_chat_id INTEGER,
            original_msg_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Notes
    c.execute("""
        CREATE TABLE IF NOT EXISTS notes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            note TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Topic name cache (for warm start)
    c.execute("""
        CREATE TABLE IF NOT EXISTS topic_cache (
            topic_id INTEGER PRIMARY KEY,
            topic_name TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Create indexes
    c.execute("CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox(status, next_retry_at)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_chats_status ON chats(status, is_archived)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_sent_messages ON sent_messages(user_id, topic_msg_id)")
    
    conn.commit()
    conn.close()
    logging.info(f"Database initialized: {DB_PATH}")

# ============================================================
# CHAT MANAGER
# ============================================================

class Chat:
    @staticmethod
    def get(user_id: int) -> Optional[Dict]:
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT * FROM chats WHERE user_id=?", (user_id,))
        row = c.fetchone()
        conn.close()
        return dict(row) if row else None

    @staticmethod
    def get_by_topic(topic_id: int) -> Optional[Dict]:
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT * FROM chats WHERE topic_id=?", (topic_id,))
        row = c.fetchone()
        conn.close()
        return dict(row) if row else None

    @staticmethod
    def create(user_id: int, username: str, first_name: str, last_name: str, topic_id: int):
        conn = get_db()
        c = conn.cursor()
        c.execute("""
            INSERT INTO chats (user_id, username, first_name, last_name, topic_id, last_message_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username=excluded.username, first_name=excluded.first_name, last_name=excluded.last_name,
                topic_id=excluded.topic_id, is_archived=0, status='unread', unread_count=1
        """, (user_id, username, first_name, last_name, topic_id, datetime.now()))
        conn.commit()
        conn.close()

    @staticmethod
    def new_message(user_id: int, preview: str, msg_type: str):
        conn = get_db()
        c = conn.cursor()
        c.execute("""
            UPDATE chats SET 
                status='unread', 
                unread_count=unread_count+1,
                last_message_preview=?, 
                last_message_type=?, 
                last_message_at=?,
                followup_stage=0, 
                followup_done=0,
                snoozed_until=NULL
            WHERE user_id=?
        """, (preview[:100], msg_type, datetime.now(), user_id))
        conn.commit()
        conn.close()

    @staticmethod
    def mark_answered(user_id: int):
        conn = get_db()
        c = conn.cursor()
        c.execute("""
            UPDATE chats SET 
                status='answered', 
                unread_count=0, 
                last_reply_at=?,
                followup_stage=0
            WHERE user_id=?
        """, (datetime.now(), user_id))
        conn.commit()
        conn.close()

    @staticmethod
    def mark_read(user_id: int):
        conn = get_db()
        c = conn.cursor()
        c.execute("""
            UPDATE chats SET 
                status=CASE WHEN status='unread' THEN 'read' ELSE status END, 
                unread_count=0 
            WHERE user_id=?
        """, (user_id,))
        conn.commit()
        conn.close()

    @staticmethod
    def mark_unread(user_id: int):
        conn = get_db()
        c = conn.cursor()
        c.execute("""
            UPDATE chats SET 
                status='unread', 
                unread_count=CASE WHEN unread_count=0 THEN 1 ELSE unread_count END 
            WHERE user_id=?
        """, (user_id,))
        conn.commit()
        conn.close()

    @staticmethod
    def set_priority(user_id: int, priority: str):
        conn = get_db()
        c = conn.cursor()
        c.execute("UPDATE chats SET priority=? WHERE user_id=?", (priority, user_id))
        conn.commit()
        conn.close()

    @staticmethod
    def archive(user_id: int):
        conn = get_db()
        c = conn.cursor()
        c.execute("UPDATE chats SET is_archived=1, status='closed' WHERE user_id=?", (user_id,))
        conn.commit()
        conn.close()

    @staticmethod
    def snooze(user_id: int, hours: int):
        conn = get_db()
        c = conn.cursor()
        until = datetime.now() + timedelta(hours=hours)
        c.execute("UPDATE chats SET snoozed_until=? WHERE user_id=?", (until, user_id))
        conn.commit()
        conn.close()

    @staticmethod
    def done_followup(user_id: int):
        conn = get_db()
        c = conn.cursor()
        c.execute("UPDATE chats SET followup_done=1 WHERE user_id=?", (user_id,))
        conn.commit()
        conn.close()

    @staticmethod
    def get_unread() -> List[Dict]:
        conn = get_db()
        c = conn.cursor()
        c.execute("""
            SELECT * FROM chats 
            WHERE status='unread' AND is_archived=0 
                AND (snoozed_until IS NULL OR snoozed_until < ?)
            ORDER BY 
                CASE priority WHEN 'urgent' THEN 0 WHEN 'vip' THEN 1 ELSE 2 END,
                last_message_at DESC
        """, (datetime.now(),))
        rows = c.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    @staticmethod
    def get_all_active() -> List[Dict]:
        conn = get_db()
        c = conn.cursor()
        c.execute("""
            SELECT * FROM chats WHERE is_archived=0 
            ORDER BY last_message_at DESC
        """)
        rows = c.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    @staticmethod
    def get_followups_due() -> Dict[str, List[Dict]]:
        """Get follow-ups grouped by urgency"""
        conn = get_db()
        c = conn.cursor()
        now = datetime.now()
        
        results = {'due': [], 'urgent': [], 'overdue': []}
        
        c.execute("""
            SELECT * FROM chats 
            WHERE status='answered' AND is_archived=0 AND followup_done=0
                AND last_reply_at IS NOT NULL
            ORDER BY last_reply_at ASC
        """)
        
        for row in c.fetchall():
            chat = dict(row)
            hours_since = (now - chat['last_reply_at']).total_seconds() / 3600
            threshold = FOLLOWUP_HOURS_VIP if chat['priority'] == 'vip' else FOLLOWUP_HOURS_NORMAL
            
            if hours_since >= threshold * 3:  # 3x threshold = overdue
                results['overdue'].append(chat)
            elif hours_since >= threshold * 1.5:  # 1.5x = urgent
                results['urgent'].append(chat)
            elif hours_since >= threshold:  # 1x = due
                results['due'].append(chat)
        
        conn.close()
        return results

# ============================================================
# OUTBOX (Reliable Delivery)
# ============================================================

class Outbox:
    @staticmethod
    def add(direction: str, from_chat_id: int, to_chat_id: int, message_id: int, topic_id: int = None):
        """Add message to outbox for reliable delivery"""
        conn = get_db()
        c = conn.cursor()
        c.execute("""
            INSERT INTO outbox (direction, from_chat_id, to_chat_id, message_id, topic_id, next_retry_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (direction, from_chat_id, to_chat_id, message_id, topic_id, datetime.now()))
        conn.commit()
        conn.close()

    @staticmethod
    def get_pending() -> List[Dict]:
        """Get messages ready for retry"""
        conn = get_db()
        c = conn.cursor()
        c.execute("""
            SELECT * FROM outbox 
            WHERE status='pending' AND next_retry_at <= ?
            ORDER BY created_at ASC
            LIMIT 20
        """, (datetime.now(),))
        rows = c.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    @staticmethod
    def mark_sent(outbox_id: int):
        conn = get_db()
        c = conn.cursor()
        c.execute("UPDATE outbox SET status='sent' WHERE id=?", (outbox_id,))
        conn.commit()
        conn.close()

    @staticmethod
    def mark_failed(outbox_id: int, error: str):
        """Mark as failed with exponential backoff"""
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT retry_count FROM outbox WHERE id=?", (outbox_id,))
        row = c.fetchone()
        retry_count = row[0] + 1 if row else 1
        
        # Exponential backoff: 5s, 15s, 45s, 2m, 5m, 15m, max 1h
        delays = [5, 15, 45, 120, 300, 900, 3600]
        delay = delays[min(retry_count - 1, len(delays) - 1)]
        next_retry = datetime.now() + timedelta(seconds=delay)
        
        if retry_count >= 10:
            c.execute("UPDATE outbox SET status='failed', error=?, retry_count=? WHERE id=?",
                      (error, retry_count, outbox_id))
        else:
            c.execute("UPDATE outbox SET retry_count=?, next_retry_at=?, error=? WHERE id=?",
                      (retry_count, next_retry, error, outbox_id))
        
        conn.commit()
        conn.close()

# ============================================================
# TOPIC CACHE (Warm Start)
# ============================================================

TOPIC_NAME_CACHE = {}

def load_topic_cache():
    """Load topic names from DB on startup"""
    global TOPIC_NAME_CACHE
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT topic_id, topic_name FROM topic_cache")
        for row in c.fetchall():
            TOPIC_NAME_CACHE[row[0]] = row[1]
        conn.close()
        logging.info(f"Loaded {len(TOPIC_NAME_CACHE)} topics from cache")
    except:
        pass

def save_topic_cache(topic_id: int, topic_name: str):
    """Save topic name to cache"""
    TOPIC_NAME_CACHE[topic_id] = topic_name
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("""
            INSERT INTO topic_cache (topic_id, topic_name, updated_at) VALUES (?, ?, ?)
            ON CONFLICT(topic_id) DO UPDATE SET topic_name=excluded.topic_name, updated_at=excluded.updated_at
        """, (topic_id, topic_name, datetime.now()))
        conn.commit()
        conn.close()
    except:
        pass

# ============================================================
# HELPERS
# ============================================================

def get_name(chat: Dict) -> str:
    parts = [chat.get('first_name', ''), chat.get('last_name', '')]
    parts = [p for p in parts if p]
    if parts:
        return " ".join(parts)
    if chat.get('username'):
        return f"@{chat['username']}"
    return f"User {chat.get('user_id', '?')}"

def get_topic_name(chat: Dict) -> str:
    name = get_name(chat)
    s = STATUS.get(chat.get('status', ''), "")
    p = PRIORITY.get(chat.get('priority', ''), "")
    parts = [x for x in [p, s, name] if x]
    return " ".join(parts)[:128]

def time_ago(dt) -> str:
    if not dt:
        return ""
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt)
    delta = datetime.now() - dt
    if delta.days > 0:
        return f"vor {delta.days}d"
    hours = delta.seconds // 3600
    if hours > 0:
        return f"vor {hours}h"
    minutes = delta.seconds // 60
    if minutes > 0:
        return f"vor {minutes}min"
    return "gerade"

def log_msg(user_id: int, direction: str, msg_type: str, content: str = "", file_id: str = "", telegram_msg_id: int = None):
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("""
            INSERT INTO messages (user_id, direction, msg_type, content, file_id, telegram_msg_id)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (user_id, direction, msg_type, content[:500] if content else "", file_id, telegram_msg_id))
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"Failed to log message: {e}")

# ============================================================
# TOPIC MANAGEMENT
# ============================================================

async def update_topic(bot: Bot, chat: Dict):
    """Update topic name with status"""
    if not chat:
        return
    
    try:
        topic_name = get_topic_name(chat)
        topic_id = chat['topic_id']
        
        # Skip if unchanged
        if TOPIC_NAME_CACHE.get(topic_id) == topic_name:
            return
        
        await bot.edit_forum_topic(
            chat_id=SUPPORT_GROUP_ID,
            message_thread_id=topic_id,
            name=topic_name
        )
        save_topic_cache(topic_id, topic_name)
        
    except BadRequest as e:
        if "not modified" not in str(e).lower():
            logging.warning(f"Topic update failed: {e}")
    except Exception as e:
        logging.warning(f"Topic update error: {e}")

async def create_topic(bot: Bot, user) -> int:
    """Create new topic for user"""
    name = get_name({
        'first_name': user.first_name,
        'last_name': user.last_name,
        'username': user.username
    })
    topic_name = f"🔴 {name}"[:128]
    
    topic = await bot.create_forum_topic(
        chat_id=SUPPORT_GROUP_ID,
        name=topic_name
    )
    
    Chat.create(
        user.id,
        user.username or "",
        user.first_name or "",
        user.last_name or "",
        topic.message_thread_id
    )
    
    save_topic_cache(topic.message_thread_id, topic_name)
    return topic.message_thread_id

async def delete_service_messages(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Delete 'topic renamed' service messages"""
    try:
        if update.message and update.message.forum_topic_edited:
            await update.message.delete()
    except:
        pass

# ============================================================
# MESSAGE FORWARDING (copy_message for native experience)
# ============================================================

async def forward_to_topic(bot: Bot, msg: Message, topic_id: int, user_id: int) -> bool:
    """Forward user message to topic using copy_message"""
    try:
        sent = await bot.copy_message(
            chat_id=SUPPORT_GROUP_ID,
            from_chat_id=msg.chat_id,
            message_id=msg.message_id,
            message_thread_id=topic_id
        )
        
        # Log
        preview = msg.text or msg.caption or f"[{msg.content_type}]"
        log_msg(user_id, "in", msg.content_type or "unknown", preview[:100], telegram_msg_id=sent.message_id)
        Chat.new_message(user_id, preview[:100], msg.content_type or "unknown")
        
        return True
    except Exception as e:
        logging.error(f"Forward to topic failed: {e}")
        # Add to outbox for retry
        Outbox.add("to_topic", msg.chat_id, SUPPORT_GROUP_ID, msg.message_id, topic_id)
        return False

async def forward_to_user(bot: Bot, msg: Message, user_id: int, topic_id: int) -> Optional[int]:
    """Forward admin message to user using copy_message"""
    try:
        sent = await bot.copy_message(
            chat_id=user_id,
            from_chat_id=msg.chat_id,
            message_id=msg.message_id
        )
        
        # Save mapping for deletion
        save_message_mapping(user_id, msg.message_id, sent.message_id)
        
        # Log
        preview = msg.text or msg.caption or f"[{msg.content_type}]"
        log_msg(user_id, "out", msg.content_type or "unknown", preview[:100], telegram_msg_id=sent.message_id)
        Chat.mark_answered(user_id)
        
        return sent.message_id
    except Exception as e:
        logging.error(f"Forward to user failed: {e}")
        # Add to outbox for retry
        Outbox.add("to_user", msg.chat_id, user_id, msg.message_id, topic_id)
        return None

def save_message_mapping(user_id: int, topic_msg_id: int, user_msg_id: int):
    """Save mapping for message deletion"""
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("""
            INSERT INTO sent_messages (user_id, topic_msg_id, user_msg_id)
            VALUES (?, ?, ?)
        """, (user_id, topic_msg_id, user_msg_id))
        # Keep only last 100 per user
        c.execute("""
            DELETE FROM sent_messages WHERE user_id=? AND id NOT IN 
            (SELECT id FROM sent_messages WHERE user_id=? ORDER BY id DESC LIMIT 100)
        """, (user_id, user_id))
        conn.commit()
        conn.close()
    except:
        pass

def get_user_msg_id(user_id: int, topic_msg_id: int) -> Optional[int]:
    """Get user message ID from topic message ID"""
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT user_msg_id FROM sent_messages WHERE user_id=? AND topic_msg_id=?",
                  (user_id, topic_msg_id))
        row = c.fetchone()
        conn.close()
        return row[0] if row else None
    except:
        return None

# ============================================================
# HANDLERS
# ============================================================

# Track sequence recording
PENDING_SEQUENCE = {}

async def handle_user(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle incoming user messages"""
    if not update.effective_chat or update.effective_chat.type != "private":
        return
    
    user = update.effective_user
    msg = update.message
    if not user or not msg:
        return
    
    # Check if recording sequence
    if user.id in PENDING_SEQUENCE:
        await handle_sequence_record(update, ctx)
        return
    
    # Get or create chat
    chat = Chat.get(user.id)
    
    if not chat or chat['is_archived']:
        topic_id = await create_topic(ctx.bot, user)
        if WELCOME_MESSAGE:
            await msg.reply_text(WELCOME_MESSAGE)
        chat = Chat.get(user.id)
    
    # Forward to topic
    success = await forward_to_topic(ctx.bot, msg, chat['topic_id'], user.id)
    
    if not success:
        # Topic might not exist, try to create new one
        topic_id = await create_topic(ctx.bot, user)
        chat = Chat.get(user.id)
        await forward_to_topic(ctx.bot, msg, chat['topic_id'], user.id)
    
    # Update topic status
    chat = Chat.get(user.id)
    await update_topic(ctx.bot, chat)

async def handle_admin(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle admin messages in support group"""
    msg = update.message
    if not msg or update.effective_chat.id != SUPPORT_GROUP_ID:
        return
    
    # Ignore bot's own messages
    if msg.from_user and msg.from_user.is_bot:
        return
    
    # Ignore non-topic messages
    topic_id = msg.message_thread_id
    if not topic_id:
        return
    
    # Check if it's a bot command
    BOT_COMMANDS = [
        'inbox', 'all', 'unread', 'read', 'info', 'vip', 'urgent', 'close',
        'note', 't', 'q', 'save', 'del', 'qdel', 'undo', 'search', 'help',
        'followup', 'done', 'skip', 'snooze', 'next', 'last',
        'bc', 'broadcast', 'confirm', 'cancel', 'start'
    ]
    
    if msg.text:
        first_word = msg.text.split()[0].lower() if msg.text.split() else ""
        if first_word.startswith('/'):
            cmd = first_word[1:].split('@')[0]
            if cmd in BOT_COMMANDS:
                return
    
    # Get chat for this topic
    chat = Chat.get_by_topic(topic_id)
    if not chat:
        return
    
    # Forward to user
    sent_id = await forward_to_user(ctx.bot, msg, chat['user_id'], topic_id)
    
    if sent_id:
        # Update topic
        chat = Chat.get(chat['user_id'])
        await update_topic(ctx.bot, chat)

async def handle_sequence_record(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle messages during sequence recording"""
    user_id = update.effective_user.id
    msg = update.message
    
    if msg.text and msg.text.startswith('/'):
        return  # Commands handled separately
    
    seq = PENDING_SEQUENCE[user_id]
    seq['messages'].append({
        'chat_id': msg.chat_id,
        'message_id': msg.message_id
    })
    
    count = len(seq['messages'])
    await msg.reply_text(f"📝 #{count} hinzugefügt\n\nWeiter senden oder /done")

# ============================================================
# COMMANDS - INBOX WITH BUTTONS
# ============================================================

async def cmd_inbox(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Show inbox with inline buttons"""
    if update.effective_chat.id != SUPPORT_GROUP_ID:
        return
    
    unread = Chat.get_unread()
    
    if not unread:
        await update.message.reply_text("✅ Inbox leer!")
        return
    
    lines = [f"📬 <b>{len(unread)} ungelesen</b>\n"]
    
    for i, chat in enumerate(unread[:10], 1):
        name = get_name(chat)
        preview = chat.get('last_message_preview', '')[:30] or ''
        time = time_ago(chat.get('last_message_at'))
        p = PRIORITY.get(chat.get('priority', ''), '')
        
        lines.append(f"{i}. {p}{html.escape(name)}")
        lines.append(f"   <i>{html.escape(preview)}...</i> • {time}")
    
    if len(unread) > 10:
        lines.append(f"\n... +{len(unread) - 10} weitere")
    
    # Inline buttons for first 5
    buttons = []
    for i, chat in enumerate(unread[:5], 1):
        buttons.append([
            InlineKeyboardButton(f"#{i} öffnen", url=f"https://t.me/c/{str(SUPPORT_GROUP_ID)[4:]}/{chat['topic_id']}"),
            InlineKeyboardButton("✓", callback_data=f"read:{chat['user_id']}"),
            InlineKeyboardButton("⭐", callback_data=f"vip:{chat['user_id']}"),
            InlineKeyboardButton("🚨", callback_data=f"urgent:{chat['user_id']}")
        ])
    
    buttons.append([
        InlineKeyboardButton("🔄 Refresh", callback_data="inbox:refresh"),
        InlineKeyboardButton("✓ Alle gelesen", callback_data="inbox:readall")
    ])
    
    await update.message.reply_text(
        "\n".join(lines),
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(buttons)
    )

async def cmd_followup(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Show follow-ups with buttons"""
    if update.effective_chat.id != SUPPORT_GROUP_ID:
        return
    
    followups = Chat.get_followups_due()
    total = len(followups['due']) + len(followups['urgent']) + len(followups['overdue'])
    
    if total == 0:
        await update.message.reply_text("✅ Keine Follow-ups fällig!")
        return
    
    lines = [f"📋 <b>{total} Follow-ups</b>\n"]
    buttons = []
    
    for stage, emoji, label in [('overdue', '🔴', 'Überfällig'), ('urgent', '🟠', 'Dringend'), ('due', '💛', 'Fällig')]:
        chats = followups[stage]
        if not chats:
            continue
        
        lines.append(f"\n<b>{emoji} {label} ({len(chats)})</b>")
        
        for chat in chats[:3]:
            name = get_name(chat)
            time = time_ago(chat.get('last_reply_at'))
            p = PRIORITY.get(chat.get('priority', ''), '')
            lines.append(f"• {p}{html.escape(name)} – {time}")
            
            buttons.append([
                InlineKeyboardButton(f"📝 {name[:15]}", url=f"https://t.me/c/{str(SUPPORT_GROUP_ID)[4:]}/{chat['topic_id']}"),
                InlineKeyboardButton("✓ Done", callback_data=f"fudone:{chat['user_id']}"),
                InlineKeyboardButton("⏭ Skip", callback_data=f"fuskip:{chat['user_id']}")
            ])
    
    await update.message.reply_text(
        "\n".join(lines),
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(buttons) if buttons else None
    )

async def cmd_all(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Show all active chats"""
    if update.effective_chat.id != SUPPORT_GROUP_ID:
        return
    
    chats = Chat.get_all_active()
    
    if not chats:
        await update.message.reply_text("Keine aktiven Chats")
        return
    
    lines = [f"📋 <b>{len(chats)} aktive Chats</b>\n"]
    
    for chat in chats[:15]:
        name = get_name(chat)
        s = STATUS.get(chat.get('status', ''), '')
        p = PRIORITY.get(chat.get('priority', ''), '')
        time = time_ago(chat.get('last_message_at'))
        lines.append(f"{p}{s} {html.escape(name)} • {time}")
    
    if len(chats) > 15:
        lines.append(f"\n... +{len(chats) - 15} weitere")
    
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)

# ============================================================
# COMMANDS - QUICK ACTIONS
# ============================================================

async def cmd_next(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Jump to next unread chat"""
    if update.effective_chat.id != SUPPORT_GROUP_ID:
        return
    
    unread = Chat.get_unread()
    if not unread:
        await update.message.reply_text("✅ Keine ungelesenen Chats!")
        return
    
    chat = unread[0]
    name = get_name(chat)
    link = f"https://t.me/c/{str(SUPPORT_GROUP_ID)[4:]}/{chat['topic_id']}"
    
    await update.message.reply_text(
        f"➡️ <b>{html.escape(name)}</b>\n\n<a href='{link}'>Zum Chat</a>",
        parse_mode=ParseMode.HTML
    )

async def cmd_last(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Jump to last active chat"""
    if update.effective_chat.id != SUPPORT_GROUP_ID:
        return
    
    chats = Chat.get_all_active()
    if not chats:
        await update.message.reply_text("Keine aktiven Chats")
        return
    
    chat = chats[0]
    name = get_name(chat)
    link = f"https://t.me/c/{str(SUPPORT_GROUP_ID)[4:]}/{chat['topic_id']}"
    
    await update.message.reply_text(
        f"🔙 <b>{html.escape(name)}</b>\n\n<a href='{link}'>Zum Chat</a>",
        parse_mode=ParseMode.HTML
    )

async def cmd_snooze(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Snooze current chat"""
    if update.effective_chat.id != SUPPORT_GROUP_ID:
        return
    
    topic_id = update.message.message_thread_id
    if not topic_id:
        await update.message.reply_text("Im Topic nutzen")
        return
    
    chat = Chat.get_by_topic(topic_id)
    if not chat:
        return
    
    hours = 3
    if ctx.args:
        try:
            hours = int(ctx.args[0])
        except:
            pass
    
    Chat.snooze(chat['user_id'], hours)
    await update.message.reply_text(f"😴 Für {hours}h ausgeblendet")

# ============================================================
# COMMANDS - TOPIC ACTIONS
# ============================================================

async def cmd_unread(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Mark as unread"""
    if update.effective_chat.id != SUPPORT_GROUP_ID:
        return
    
    topic_id = update.message.message_thread_id
    if topic_id:
        chat = Chat.get_by_topic(topic_id)
        if chat:
            Chat.mark_unread(chat['user_id'])
            await update_topic(ctx.bot, Chat.get(chat['user_id']))
            await update.message.reply_text("🔴 Ungelesen")
            return
    
    if ctx.args:
        for c in Chat.get_all_active():
            if ctx.args[0].lower() in get_name(c).lower():
                Chat.mark_unread(c['user_id'])
                await update_topic(ctx.bot, Chat.get(c['user_id']))
                await update.message.reply_text(f"🔴 {get_name(c)} – Ungelesen")
                return
        await update.message.reply_text("Nicht gefunden")

async def cmd_read(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Mark as read"""
    if update.effective_chat.id != SUPPORT_GROUP_ID:
        return
    
    topic_id = update.message.message_thread_id
    if topic_id:
        chat = Chat.get_by_topic(topic_id)
        if chat:
            Chat.mark_read(chat['user_id'])
            await update_topic(ctx.bot, Chat.get(chat['user_id']))
            await update.message.reply_text("⚪ Gelesen")

async def cmd_vip(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Toggle VIP status"""
    if update.effective_chat.id != SUPPORT_GROUP_ID:
        return
    
    topic_id = update.message.message_thread_id
    if not topic_id:
        return
    
    chat = Chat.get_by_topic(topic_id)
    if not chat:
        return
    
    new_priority = "normal" if chat['priority'] == 'vip' else 'vip'
    Chat.set_priority(chat['user_id'], new_priority)
    chat = Chat.get(chat['user_id'])
    await update_topic(ctx.bot, chat)
    await update.message.reply_text("⭐ VIP" if new_priority == 'vip' else "VIP entfernt")

async def cmd_urgent(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Toggle urgent status"""
    if update.effective_chat.id != SUPPORT_GROUP_ID:
        return
    
    topic_id = update.message.message_thread_id
    if not topic_id:
        return
    
    chat = Chat.get_by_topic(topic_id)
    if not chat:
        return
    
    new_priority = "normal" if chat['priority'] == 'urgent' else 'urgent'
    Chat.set_priority(chat['user_id'], new_priority)
    chat = Chat.get(chat['user_id'])
    await update_topic(ctx.bot, chat)
    await update.message.reply_text("🚨 Dringend" if new_priority == 'urgent' else "Dringend entfernt")

async def cmd_close(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Archive chat"""
    if update.effective_chat.id != SUPPORT_GROUP_ID:
        return
    
    topic_id = update.message.message_thread_id
    if not topic_id:
        return
    
    chat = Chat.get_by_topic(topic_id)
    if not chat:
        return
    
    Chat.archive(chat['user_id'])
    
    try:
        await ctx.bot.close_forum_topic(chat_id=SUPPORT_GROUP_ID, message_thread_id=topic_id)
    except:
        pass
    
    await update.message.reply_text("⚫ Archiviert")

async def cmd_info(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Show user info"""
    if update.effective_chat.id != SUPPORT_GROUP_ID:
        return
    
    topic_id = update.message.message_thread_id
    if not topic_id:
        return
    
    chat = Chat.get_by_topic(topic_id)
    if not chat:
        return
    
    name = get_name(chat)
    lines = [
        f"👤 <b>{html.escape(name)}</b>",
        f"",
        f"🆔 <code>{chat['user_id']}</code>",
        f"📊 Status: {STATUS.get(chat['status'], '')} {chat['status']}",
        f"⭐ Priorität: {chat['priority']}",
        f"📨 Ungelesen: {chat['unread_count']}",
        f"",
        f"📅 Erstellt: {chat.get('created_at', '?')}",
        f"💬 Letzte Nachricht: {time_ago(chat.get('last_message_at'))}",
        f"↩️ Letzte Antwort: {time_ago(chat.get('last_reply_at'))}"
    ]
    
    # Get notes
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT note, created_at FROM notes WHERE user_id=? ORDER BY created_at DESC LIMIT 3", (chat['user_id'],))
    notes = c.fetchall()
    conn.close()
    
    if notes:
        lines.append("\n📝 <b>Notizen:</b>")
        for note, created in notes:
            lines.append(f"• {html.escape(note)}")
    
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)

async def cmd_note(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Add note"""
    if update.effective_chat.id != SUPPORT_GROUP_ID:
        return
    
    topic_id = update.message.message_thread_id
    if not topic_id:
        return
    
    chat = Chat.get_by_topic(topic_id)
    if not chat:
        return
    
    if not ctx.args:
        await update.message.reply_text("/note <text>")
        return
    
    note = " ".join(ctx.args)
    conn = get_db()
    c = conn.cursor()
    c.execute("INSERT INTO notes (user_id, note) VALUES (?, ?)", (chat['user_id'], note))
    conn.commit()
    conn.close()
    
    await update.message.reply_text(f"📝 Notiz gespeichert")

# ============================================================
# COMMANDS - TEMPLATES
# ============================================================

async def cmd_t(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Send text template with buttons"""
    if update.effective_chat.id != SUPPORT_GROUP_ID:
        return
    
    topic_id = update.message.message_thread_id
    chat = Chat.get_by_topic(topic_id) if topic_id else None
    
    if not ctx.args:
        # Show templates with buttons
        buttons = []
        for name in TEMPLATES:
            if chat:
                buttons.append([InlineKeyboardButton(f"📤 {name}", callback_data=f"tmpl:{name}:{chat['user_id']}")])
            else:
                buttons.append([InlineKeyboardButton(f"📝 {name}", callback_data=f"tmplshow:{name}")])
        
        lines = ["📝 <b>Templates</b>\n"]
        for name, text in TEMPLATES.items():
            lines.append(f"• <b>{name}</b>: {html.escape(text[:40])}...")
        
        await update.message.reply_text(
            "\n".join(lines),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons) if buttons else None
        )
        return
    
    if not topic_id:
        await update.message.reply_text("Im Topic nutzen")
        return
    
    if not chat:
        return
    
    tmpl = TEMPLATES.get(ctx.args[0].lower())
    if not tmpl:
        await update.message.reply_text("Nicht gefunden")
        return
    
    try:
        await ctx.bot.send_message(chat_id=chat['user_id'], text=tmpl)
        log_msg(chat['user_id'], "out", "text", tmpl)
        Chat.mark_answered(chat['user_id'])
        await update_topic(ctx.bot, Chat.get(chat['user_id']))
        await update.message.reply_text(f"📤 Gesendet")
    except Exception as e:
        await update.message.reply_text(f"⚠️ {e}")

# ============================================================
# COMMANDS - SEQUENCES
# ============================================================

async def cmd_save(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Start recording sequence"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        return
    
    if not ctx.args:
        # List sequences
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT DISTINCT name FROM sequences ORDER BY name")
        names = [row[0] for row in c.fetchall()]
        conn.close()
        
        if names:
            lines = ["📦 <b>Kurzbefehle</b>\n"]
            for name in names:
                conn = get_db()
                c = conn.cursor()
                c.execute("SELECT COUNT(*) FROM sequences WHERE name=?", (name,))
                count = c.fetchone()[0]
                conn.close()
                lines.append(f"• /q {name} ({count} Nachrichten)")
            lines.append("\n/save name → neu erstellen")
            await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)
        else:
            await update.message.reply_text("Keine Kurzbefehle.\n\n/save name → dann Nachrichten senden → /done")
        return
    
    name = ctx.args[0].lower()
    
    # Delete old
    conn = get_db()
    c = conn.cursor()
    c.execute("DELETE FROM sequences WHERE name=?", (name,))
    conn.commit()
    conn.close()
    
    PENDING_SEQUENCE[user_id] = {'name': name, 'messages': []}
    await update.message.reply_text(f"📦 <b>Kurzbefehl '{name}'</b>\n\nSende jetzt Nachrichten...\n/done wenn fertig", parse_mode=ParseMode.HTML)

async def cmd_done(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Finish recording or mark follow-up done"""
    user_id = update.effective_user.id
    
    # Check if recording sequence
    if user_id in PENDING_SEQUENCE:
        seq = PENDING_SEQUENCE.pop(user_id)
        name = seq['name']
        messages = seq['messages']
        
        if not messages:
            await update.message.reply_text("❌ Keine Nachrichten aufgenommen")
            return
        
        # Save to DB
        conn = get_db()
        c = conn.cursor()
        for i, msg_data in enumerate(messages):
            c.execute("""
                INSERT INTO sequences (name, position, original_chat_id, original_msg_id)
                VALUES (?, ?, ?, ?)
            """, (name, i, msg_data['chat_id'], msg_data['message_id']))
        conn.commit()
        conn.close()
        
        await update.message.reply_text(f"✅ <b>{name}</b> gespeichert ({len(messages)} Nachrichten)\n\n/q {name} zum Senden", parse_mode=ParseMode.HTML)
        return
    
    # Mark follow-up done
    topic_id = update.message.message_thread_id
    if topic_id:
        chat = Chat.get_by_topic(topic_id)
        if chat:
            Chat.done_followup(chat['user_id'])
            await update.message.reply_text("✅ Follow-up erledigt")
            return
    
    await update.message.reply_text("Im Topic nutzen oder nach /save")

async def cmd_q(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Send sequence"""
    if update.effective_chat.id != SUPPORT_GROUP_ID:
        return
    
    if not ctx.args:
        await cmd_save(update, ctx)
        return
    
    topic_id = update.message.message_thread_id
    if not topic_id:
        await update.message.reply_text("Im Topic nutzen")
        return
    
    chat = Chat.get_by_topic(topic_id)
    if not chat:
        return
    
    name = ctx.args[0].lower()
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT original_chat_id, original_msg_id FROM sequences WHERE name=? ORDER BY position", (name,))
    messages = c.fetchall()
    conn.close()
    
    if not messages:
        await update.message.reply_text(f"❌ '{name}' nicht gefunden")
        return
    
    sent_count = 0
    for orig_chat_id, orig_msg_id in messages:
        try:
            await ctx.bot.copy_message(
                chat_id=chat['user_id'],
                from_chat_id=orig_chat_id,
                message_id=orig_msg_id
            )
            sent_count += 1
            await asyncio.sleep(0.3)
        except Exception as e:
            logging.warning(f"Failed to send sequence message: {e}")
    
    if sent_count > 0:
        Chat.mark_answered(chat['user_id'])
        await update_topic(ctx.bot, Chat.get(chat['user_id']))
    
    await update.message.reply_text(f"✅ {sent_count}/{len(messages)} Nachrichten gesendet")

async def cmd_qdel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Delete sequence"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        return
    
    if not ctx.args:
        await update.message.reply_text("/qdel name")
        return
    
    name = ctx.args[0].lower()
    conn = get_db()
    c = conn.cursor()
    c.execute("DELETE FROM sequences WHERE name=?", (name,))
    deleted = c.rowcount
    conn.commit()
    conn.close()
    
    if deleted:
        await update.message.reply_text(f"🗑 <b>{name}</b> gelöscht", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(f"❌ '{name}' nicht gefunden")

# ============================================================
# COMMANDS - DELETE & UNDO
# ============================================================

async def cmd_del(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Delete message by replying"""
    if update.effective_chat.id != SUPPORT_GROUP_ID:
        return
    
    msg = update.message
    
    if msg.reply_to_message:
        topic_id = msg.message_thread_id
        if not topic_id:
            return
        
        chat = Chat.get_by_topic(topic_id)
        if not chat:
            return
        
        user_msg_id = get_user_msg_id(chat['user_id'], msg.reply_to_message.message_id)
        
        if not user_msg_id:
            await msg.reply_text("⚠️ Nachricht nicht gefunden")
            return
        
        try:
            await ctx.bot.delete_message(chat_id=chat['user_id'], message_id=user_msg_id)
            await msg.reply_text("🗑 Gelöscht")
        except Exception as e:
            await msg.reply_text(f"⚠️ {e}")
        return
    
    if ctx.args:
        await cmd_qdel(update, ctx)
    else:
        await msg.reply_text("Antworte auf eine Nachricht + /del\noder /del name für Kurzbefehl")

async def cmd_undo(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Undo last messages"""
    if update.effective_chat.id != SUPPORT_GROUP_ID:
        return
    
    topic_id = update.message.message_thread_id
    if not topic_id:
        await update.message.reply_text("Im Topic nutzen")
        return
    
    chat = Chat.get_by_topic(topic_id)
    if not chat:
        return
    
    count = 1
    if ctx.args:
        try:
            count = int(ctx.args[0])
        except:
            pass
    
    # Get last messages from DB
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        SELECT user_msg_id FROM sent_messages 
        WHERE user_id=? 
        ORDER BY id DESC LIMIT ?
    """, (chat['user_id'], count))
    messages = c.fetchall()
    conn.close()
    
    if not messages:
        await update.message.reply_text("❌ Keine Nachrichten zum Löschen")
        return
    
    deleted = 0
    for (msg_id,) in messages:
        try:
            await ctx.bot.delete_message(chat_id=chat['user_id'], message_id=msg_id)
            deleted += 1
        except:
            pass
    
    await update.message.reply_text(f"🗑 {deleted} gelöscht")

# ============================================================
# COMMANDS - BROADCAST
# ============================================================

PENDING_BROADCAST = {}

async def cmd_broadcast(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Broadcast message"""
    if update.effective_chat.id != SUPPORT_GROUP_ID:
        return
    
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        return
    
    if not ctx.args:
        await update.message.reply_text("""📢 <b>Broadcast</b>

/bc followup [text] – An alle Follow-ups
/bc all [text] – An alle aktiven
/bc vip [text] – An alle VIPs""", parse_mode=ParseMode.HTML)
        return
    
    target = ctx.args[0].lower()
    message = " ".join(ctx.args[1:]) if len(ctx.args) > 1 else ""
    
    if target == "followup":
        followups = Chat.get_followups_due()
        recipients = followups['due'] + followups['urgent'] + followups['overdue']
    elif target == "all":
        recipients = Chat.get_all_active()
    elif target == "vip":
        recipients = [c for c in Chat.get_all_active() if c['priority'] == 'vip']
    else:
        await update.message.reply_text("Nutze: followup, all, vip")
        return
    
    if not recipients:
        await update.message.reply_text("Keine Empfänger")
        return
    
    if not message:
        await update.message.reply_text("Keine Nachricht angegeben")
        return
    
    PENDING_BROADCAST[user_id] = {'recipients': recipients, 'message': message}
    
    names = [get_name(r)[:20] for r in recipients[:5]]
    await update.message.reply_text(
        f"📢 <b>Broadcast an {len(recipients)} Empfänger</b>\n\n"
        f"{', '.join(names)}{'...' if len(recipients) > 5 else ''}\n\n"
        f"<i>{html.escape(message[:100])}</i>\n\n"
        f"/confirm zum Senden",
        parse_mode=ParseMode.HTML
    )

async def cmd_confirm(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Confirm broadcast"""
    user_id = update.effective_user.id
    
    if user_id not in PENDING_BROADCAST:
        await update.message.reply_text("Nichts zu bestätigen")
        return
    
    bc = PENDING_BROADCAST.pop(user_id)
    recipients = bc['recipients']
    message = bc['message']
    
    sent = 0
    for r in recipients:
        try:
            await ctx.bot.send_message(chat_id=r['user_id'], text=message)
            Chat.mark_answered(r['user_id'])
            sent += 1
            await asyncio.sleep(0.1)
        except:
            pass
    
    await update.message.reply_text(f"✅ {sent}/{len(recipients)} gesendet")

async def cmd_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Cancel broadcast"""
    user_id = update.effective_user.id
    if user_id in PENDING_BROADCAST:
        del PENDING_BROADCAST[user_id]
    await update.message.reply_text("❌ Abgebrochen")

# ============================================================
# COMMANDS - OTHER
# ============================================================

async def cmd_search(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Search messages"""
    if update.effective_chat.id != SUPPORT_GROUP_ID:
        return
    
    q = " ".join(ctx.args) if ctx.args else ""
    if not q:
        await update.message.reply_text("/search <text>")
        return
    
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        SELECT m.content, m.direction, c.first_name, c.topic_id
        FROM messages m 
        JOIN chats c ON m.user_id=c.user_id 
        WHERE m.content LIKE ? 
        ORDER BY m.created_at DESC LIMIT 10
    """, (f"%{q}%",))
    results = c.fetchall()
    conn.close()
    
    if not results:
        await update.message.reply_text("Nichts gefunden")
        return
    
    lines = [f"🔍 <b>'{html.escape(q)}'</b>\n"]
    for content, direction, name, topic_id in results:
        arrow = "↗️" if direction == "out" else "↙️"
        lines.append(f"{arrow} <b>{html.escape(name or '?')}</b>: {html.escape(content[:40])}")
    
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)

async def cmd_skip(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Skip follow-up"""
    if update.effective_chat.id != SUPPORT_GROUP_ID:
        return
    
    topic_id = update.message.message_thread_id
    if not topic_id:
        await update.message.reply_text("Im Topic nutzen")
        return
    
    chat = Chat.get_by_topic(topic_id)
    if not chat:
        return
    
    days = 3
    if ctx.args:
        try:
            days = int(ctx.args[0])
        except:
            pass
    
    Chat.snooze(chat['user_id'], days * 24)
    await update.message.reply_text(f"⏭ Für {days} Tage übersprungen")

async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Show help"""
    await update.message.reply_text("""<b>📖 Befehle</b>

<b>Inbox</b>
/inbox – Ungelesene (mit Buttons)
/all – Alle Chats
/next – Nächster ungelesener
/last – Letzter Chat
/search – Suchen

<b>Follow-Up</b>
/followup – Fällige (mit Buttons)
/done – Erledigt
/skip – Übersprungen
/snooze – Ausblenden

<b>Im Topic</b>
/unread /read – Status
/vip /urgent – Priorität
/close – Archivieren
/info /note – Details
/del – Nachricht löschen (Reply)
/undo – Letzte(s) löschen

<b>Templates</b>
/t – Text-Templates
/q – Kurzbefehle
/save – Kurzbefehl erstellen

<b>Broadcast</b>
/bc followup/all/vip [text]""", parse_mode=ParseMode.HTML)

# ============================================================
# CALLBACK HANDLERS
# ============================================================

async def handle_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle inline button callbacks"""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    
    # Inbox actions
    if data == "inbox:refresh":
        # Re-send inbox
        unread = Chat.get_unread()
        if not unread:
            await query.edit_message_text("✅ Inbox leer!")
            return
        # Same logic as cmd_inbox...
        await query.edit_message_text(f"📬 {len(unread)} ungelesen - /inbox für Details")
    
    elif data == "inbox:readall":
        for chat in Chat.get_unread():
            Chat.mark_read(chat['user_id'])
        await query.edit_message_text("✅ Alle als gelesen markiert")
    
    elif data.startswith("read:"):
        user_id = int(data.split(":")[1])
        Chat.mark_read(user_id)
        await query.answer("✓ Gelesen")
    
    elif data.startswith("vip:"):
        user_id = int(data.split(":")[1])
        chat = Chat.get(user_id)
        new_p = "normal" if chat and chat['priority'] == 'vip' else 'vip'
        Chat.set_priority(user_id, new_p)
        await query.answer("⭐ VIP" if new_p == 'vip' else "VIP entfernt")
    
    elif data.startswith("urgent:"):
        user_id = int(data.split(":")[1])
        chat = Chat.get(user_id)
        new_p = "normal" if chat and chat['priority'] == 'urgent' else 'urgent'
        Chat.set_priority(user_id, new_p)
        await query.answer("🚨 Dringend" if new_p == 'urgent' else "Entfernt")
    
    elif data.startswith("fudone:"):
        user_id = int(data.split(":")[1])
        Chat.done_followup(user_id)
        await query.answer("✅ Erledigt")
    
    elif data.startswith("fuskip:"):
        user_id = int(data.split(":")[1])
        Chat.snooze(user_id, 72)
        await query.answer("⏭ Übersprungen")
    
    elif data.startswith("tmpl:"):
        parts = data.split(":")
        name = parts[1]
        user_id = int(parts[2])
        tmpl = TEMPLATES.get(name)
        if tmpl:
            try:
                await ctx.bot.send_message(chat_id=user_id, text=tmpl)
                Chat.mark_answered(user_id)
                await query.answer("📤 Gesendet")
            except Exception as e:
                await query.answer(f"Fehler: {e}")

# ============================================================
# JOBS
# ============================================================

async def job_process_outbox(ctx: ContextTypes.DEFAULT_TYPE):
    """Process pending outbox messages"""
    pending = Outbox.get_pending()
    
    for item in pending:
        try:
            if item['direction'] == 'to_topic':
                await ctx.bot.copy_message(
                    chat_id=item['to_chat_id'],
                    from_chat_id=item['from_chat_id'],
                    message_id=item['message_id'],
                    message_thread_id=item['topic_id']
                )
            elif item['direction'] == 'to_user':
                await ctx.bot.copy_message(
                    chat_id=item['to_chat_id'],
                    from_chat_id=item['from_chat_id'],
                    message_id=item['message_id']
                )
            
            Outbox.mark_sent(item['id'])
            logging.info(f"Outbox message {item['id']} sent successfully")
            
        except Exception as e:
            Outbox.mark_failed(item['id'], str(e))
            logging.warning(f"Outbox message {item['id']} failed: {e}")

async def job_followup_morning(ctx: ContextTypes.DEFAULT_TYPE):
    """Morning follow-up report"""
    followups = Chat.get_followups_due()
    total = len(followups['due']) + len(followups['urgent']) + len(followups['overdue'])
    
    if total == 0:
        return
    
    lines = [
        "━━━━━━━━━━━━━━━━━━━━",
        "☀️ <b>GUTEN MORGEN!</b>",
        "━━━━━━━━━━━━━━━━━━━━\n",
        f"📋 <b>{total} Follow-ups fällig</b>\n"
    ]
    
    for stage, emoji in [('overdue', '🔴'), ('urgent', '🟠'), ('due', '💛')]:
        for chat in followups[stage][:3]:
            name = get_name(chat)
            p = PRIORITY.get(chat.get('priority', ''), '')
            lines.append(f"{emoji} {p}{html.escape(name)}")
    
    lines.append("\n/followup für Details")
    
    await ctx.bot.send_message(
        chat_id=SUPPORT_GROUP_ID,
        text="\n".join(lines),
        parse_mode=ParseMode.HTML
    )

async def job_archive(ctx: ContextTypes.DEFAULT_TYPE):
    """Auto-archive old chats"""
    conn = get_db()
    c = conn.cursor()
    cutoff = datetime.now() - timedelta(days=ARCHIVE_AFTER_DAYS)
    c.execute("SELECT user_id, topic_id FROM chats WHERE is_archived=0 AND last_message_at<?", (cutoff,))
    
    for user_id, topic_id in c.fetchall():
        try:
            await ctx.bot.close_forum_topic(chat_id=SUPPORT_GROUP_ID, message_thread_id=topic_id)
        except:
            pass
        c.execute("UPDATE chats SET is_archived=1, status='closed' WHERE user_id=?", (user_id,))
    
    conn.commit()
    conn.close()

# ============================================================
# ERROR HANDLER
# ============================================================

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Handle errors gracefully"""
    logging.error(f"Exception: {context.error}")
    
    if isinstance(context.error, (NetworkError, TimedOut)):
        logging.info("Network error - will retry")
        return
    
    tb = "".join(traceback.format_exception(type(context.error), context.error, context.error.__traceback__))
    logging.error(f"Traceback:\n{tb}")

# ============================================================
# MAIN
# ============================================================

def main():
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s - %(message)s',
        level=logging.INFO,
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    
    logging.info(f"Starting bot...")
    logging.info(f"Database: {DB_PATH}")
    logging.info(f"Support Group: {SUPPORT_GROUP_ID}")
    logging.info(f"Admins: {ADMIN_IDS}")
    
    init_db()
    load_topic_cache()
    
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_error_handler(error_handler)
    
    # Service message handler
    app.add_handler(MessageHandler(
        filters.Chat(SUPPORT_GROUP_ID) & filters.StatusUpdate.FORUM_TOPIC_EDITED,
        delete_service_messages
    ), group=0)
    
    # User messages
    app.add_handler(MessageHandler(
        filters.ChatType.PRIVATE & ~filters.COMMAND,
        handle_user
    ))
    
    # Admin messages
    app.add_handler(MessageHandler(
        filters.Chat(SUPPORT_GROUP_ID),
        handle_admin
    ), group=1)
    
    # Callback handler
    app.add_handler(CallbackQueryHandler(handle_callback))
    
    # Commands
    commands = [
        ("inbox", cmd_inbox), ("all", cmd_all), ("next", cmd_next), ("last", cmd_last),
        ("unread", cmd_unread), ("read", cmd_read), ("vip", cmd_vip), ("urgent", cmd_urgent),
        ("close", cmd_close), ("info", cmd_info), ("note", cmd_note), ("snooze", cmd_snooze),
        ("t", cmd_t), ("q", cmd_q), ("save", cmd_save), ("done", cmd_done),
        ("del", cmd_del), ("qdel", cmd_qdel), ("undo", cmd_undo),
        ("search", cmd_search), ("help", cmd_help),
        ("followup", cmd_followup), ("skip", cmd_skip),
        ("bc", cmd_broadcast), ("broadcast", cmd_broadcast),
        ("confirm", cmd_confirm), ("cancel", cmd_cancel)
    ]
    
    for cmd, fn in commands:
        app.add_handler(CommandHandler(cmd, fn))
    
    # Jobs
    app.job_queue.run_repeating(job_process_outbox, interval=OUTBOX_INTERVAL_SECONDS, first=10)
    app.job_queue.run_repeating(job_archive, interval=3600, first=60)
    
    from datetime import time as dt_time
    app.job_queue.run_daily(job_followup_morning, time=dt_time(hour=FOLLOWUP_MORNING_HOUR, minute=0))
    
    print("🚀 Support Bot v2.0 gestartet")
    print(f"📁 Datenbank: {DB_PATH}")
    
    app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=False
    )

if __name__ == "__main__":
    while True:
        try:
            main()
        except KeyboardInterrupt:
            print("\n👋 Bot gestoppt")
            break
        except Exception as e:
            logging.error(f"Bot crashed: {e}")
            logging.info("Neustart in 5 Sekunden...")
            import time
            time.sleep(5)


