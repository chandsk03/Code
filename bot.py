import os
import asyncio
import logging
import random
import time
import re
import json
from typing import Optional, Dict, List, Tuple, Union
from dataclasses import dataclass
from datetime import datetime, timedelta
import pytz

from aiogram import Bot, Dispatcher, Router, types, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode, ContentType
from aiogram.types import (
    InlineKeyboardMarkup, 
    InlineKeyboardButton, 
    Message,
    CallbackQuery,
    FSInputFile
)
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.markdown import hbold, hcode, hlink
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from telethon import TelegramClient, functions, types as telethon_types
from telethon.errors import (
    PeerFloodError, FloodWaitError, UserPrivacyRestrictedError,
    UserIsBlockedError, SessionPasswordNeededError, PhoneNumberInvalidError,
    SessionRevokedError, AuthKeyError, ChannelPrivateError,
    ChatWriteForbiddenError, UsernameNotOccupiedError,
    ChatAdminRequiredError, InputUserDeactivatedError
)

# --- Configuration ---
API_ID = 25781839
API_HASH = '20a3f2f168739259a180dcdd642e196c'
BOT_TOKEN = '7857537951:AAG71zV2gsW3Eg97x2c2cVgoXzFCDZ3iHpU'
OWNER_ID = 7584086775
SESSIONS_FOLDER = "sessions"
SCHEDULES_FILE = "schedules.json"
SPAM_REPORTS_FILE = "spam_reports.json"
MEDIA_FOLDER = "media"
BATCH_SIZE = 5
MAX_RETRIES = 3
MIN_DELAY = 2  # seconds between messages
MAX_DELAY = 4  # seconds between messages
MAX_MESSAGE_LENGTH = 4096  # Telegram message limit
JOIN_DELAY = 5  # seconds between join attempts
MAX_SESSIONS = 100  # Maximum number of sessions to handle

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot_errors.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Data Structures ---
@dataclass
class SessionInfo:
    name: str
    phone: str = ""
    valid: bool = False
    last_used: datetime = None
    sent_count: int = 0
    errors: int = 0
    is_premium: bool = False
    is_banned: bool = False
    last_error: str = ""
    join_success: int = 0
    join_failed: int = 0

class ScheduledJob:
    def __init__(
        self, 
        job_id: str, 
        targets: List[str], 
        message: str, 
        media: Optional[str] = None, 
        interval: int = 0, 
        next_run: datetime = None,
        total_sent: int = 0,
        total_failed: int = 0
    ):
        self.job_id = job_id
        self.targets = targets
        self.message = message
        self.media = media
        self.interval = interval
        self.next_run = next_run
        self.total_sent = total_sent
        self.total_failed = total_failed

class BotStates(StatesGroup):
    setting_target = State()
    setting_message = State()
    adding_session = State()
    removing_session = State()
    scheduling_message = State()
    setting_interval = State()
    managing_schedules = State()
    editing_job_targets = State()
    editing_job_message = State()
    editing_job_media = State()
    editing_job_interval = State()
    setting_media = State()
    checking_spam = State()
    clearing_media = State()

# --- Bot Setup ---
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)

# --- Global State ---
active_sessions: Dict[str, SessionInfo] = {}
scheduled_jobs: Dict[str, ScheduledJob] = {}
spam_reports: Dict[str, Dict] = {}
active_messages: Dict[str, Message] = {}  # For live updates
campaign_stats = {
    "total_sent": 0,
    "last_run": None,
    "target": "",
    "message": "",
    "media": None
}

# Initialize scheduler
scheduler = AsyncIOScheduler(timezone="UTC")

# --- Helper Functions ---
def get_current_time() -> datetime:
    return datetime.now(pytz.utc)

def format_time(dt: datetime) -> str:
    return dt.astimezone(pytz.utc).strftime('%Y-%m-%d %H:%M:%S UTC')

def format_timedelta(td: timedelta) -> str:
    hours, remainder = divmod(td.seconds, 3600)
    minutes, _ = divmod(remainder, 60)
    return f"{hours}h {minutes}m" if hours else f"{minutes}m"

def split_long_message(text: str) -> List[str]:
    return [text[i:i+MAX_MESSAGE_LENGTH] for i in range(0, len(text), MAX_MESSAGE_LENGTH)]

def is_valid_phone(phone: str) -> bool:
    return bool(re.match(r'^\+?[1-9]\d{7,14}$', phone))

def extract_entity(target: str) -> str:
    """Extract username or invite link from URL"""
    if target.startswith('https://t.me/'):
        target = target.replace('https://t.me/', '')
        if target.startswith('+'):
            return target
        elif target.startswith('joinchat/'):
            return target.replace('joinchat/', '')
        elif '/' in target:
            return target.split('/')[0]
        return target
    return target

async def save_schedules():
    """Save scheduled jobs to file"""
    data = {
        job_id: {
            "targets": job.targets,
            "message": job.message,
            "media": job.media,
            "interval": job.interval,
            "next_run": job.next_run.isoformat() if job.next_run else None,
            "total_sent": job.total_sent,
            "total_failed": job.total_failed
        }
        for job_id, job in scheduled_jobs.items()
    }
    with open(SCHEDULES_FILE, 'w') as f:
        json.dump(data, f, indent=2)

async def load_schedules():
    """Load scheduled jobs from file"""
    if not os.path.exists(SCHEDULES_FILE):
        return
    
    try:
        with open(SCHEDULES_FILE, 'r') as f:
            data = json.load(f)
        
        for job_id, job_data in data.items():
            next_run = datetime.fromisoformat(job_data["next_run"]) if job_data["next_run"] else None
            if not next_run or next_run > get_current_time():
                schedule_job(
                    job_id,
                    job_data["targets"],
                    job_data["message"],
                    job_data.get("media"),
                    job_data["interval"],
                    next_run,
                    job_data.get("total_sent", 0),
                    job_data.get("total_failed", 0)
                )
    except Exception as e:
        logger.error(f"Error loading schedules: {e}")

async def save_spam_reports():
    """Save spam reports to file"""
    with open(SPAM_REPORTS_FILE, 'w') as f:
        json.dump(spam_reports, f, indent=2)

async def load_spam_reports():
    """Load spam reports from file"""
    if not os.path.exists(SPAM_REPORTS_FILE):
        return
    
    try:
        with open(SPAM_REPORTS_FILE, 'r') as f:
            spam_reports.update(json.load(f))
    except Exception as e:
        logger.error(f"Error loading spam reports: {e}")

def schedule_job(
    job_id: str, 
    targets: List[str], 
    message: str, 
    media: Optional[str] = None, 
    interval: int = 0, 
    next_run: datetime = None,
    total_sent: int = 0,
    total_failed: int = 0
):
    """Create or update a scheduled job"""
    if not next_run and interval > 0:
        next_run = get_current_time() + timedelta(minutes=interval)
    
    # Remove existing job if it exists
    if job_id in scheduled_jobs:
        try:
            scheduler.remove_job(job_id)
        except Exception as e:
            logger.error(f"Error removing job {job_id}: {e}")
    
    job = ScheduledJob(job_id, targets, message, media, interval, next_run, total_sent, total_failed)
    scheduled_jobs[job_id] = job
    
    if interval > 0:
        scheduler.add_job(
            send_scheduled_messages,
            trigger=IntervalTrigger(minutes=interval),
            args=[job_id, targets, message, media],
            id=job_id,
            next_run_time=next_run
        )
    
    asyncio.create_task(save_schedules())

def delete_job(job_id: str):
    """Remove a scheduled job"""
    if job_id in scheduled_jobs:
        try:
            scheduler.remove_job(job_id)
            del scheduled_jobs[job_id]
            asyncio.create_task(save_schedules())
            return True
        except Exception as e:
            logger.error(f"Error deleting job {job_id}: {e}")
            return False
    return False

async def update_active_message(job_id: str, text: str, reply_markup: Optional[InlineKeyboardMarkup] = None):
    """Update the active message with progress"""
    if job_id in active_messages:
        try:
            await active_messages[job_id].edit_text(text, reply_markup=reply_markup)
        except Exception as e:
            logger.error(f"Error updating active message: {e}")

async def join_group(client: TelegramClient, target: str) -> bool:
    """Attempt to join a group or channel"""
    try:
        target_entity = extract_entity(target)
        
        # Handle invite links
        if target_entity.startswith('+') or len(target_entity) > 32:
            try:
                await client(functions.messages.ImportChatInviteRequest(target_entity))
                await asyncio.sleep(JOIN_DELAY)
                return True
            except Exception as e:
                logger.error(f"Error joining with invite link {target}: {e}")
                return False
        
        # Handle usernames
        entity = await client.get_entity(target_entity)
        
        if isinstance(entity, (telethon_types.Channel, telethon_types.Chat)):
            if entity.megagroup or entity.broadcast:
                await client(functions.channels.JoinChannelRequest(entity))
                await asyncio.sleep(JOIN_DELAY)
                return True
            elif hasattr(entity, 'username') and entity.username:
                await client(functions.channels.JoinChannelRequest(entity))
                await asyncio.sleep(JOIN_DELAY)
                return True
        
        return True
    except (ChannelPrivateError, ChatWriteForbiddenError):
        logger.error(f"Can't join private group/channel: {target}")
        return False
    except UsernameNotOccupiedError:
        logger.error(f"Username not occupied: {target}")
        return False
    except Exception as e:
        logger.error(f"Error joining {target}: {e}")
        return False

async def send_scheduled_messages(job_id: str, targets: List[str], message: str, media: Optional[str] = None):
    """Send messages to multiple groups on schedule with live updates"""
    valid_sessions = [s.name for s in active_sessions.values() if s.valid and not s.is_banned]
    if not valid_sessions:
        logger.error("No valid sessions for scheduled messages")
        await update_active_message(job_id, "❌ No valid sessions available")
        return
    
    # Create or update active message
    if job_id not in active_messages:
        try:
            msg = await bot.send_message(
                OWNER_ID,
                f"⏳ Starting scheduled job {job_id[:6]}...\n"
                f"Targets: {len(targets)}\n"
                f"Sessions: {len(valid_sessions)}\n"
                f"Status: Preparing...",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🛑 Cancel Job", callback_data=f"cancel_job_{job_id}")]
                ])
            )
            active_messages[job_id] = msg
        except Exception as e:
            logger.error(f"Error creating active message: {e}")
    
    total_sent = 0
    total_failed = 0
    results = []
    cancelled = False
    
    for i, target in enumerate(targets, 1):
        if cancelled:
            break
            
        # Update status
        status = (
            f"🚀 Running job {job_id[:6]}...\n"
            f"Target {i}/{len(targets)}: {target[:20]}...\n"
            f"Sent: {total_sent} | Failed: {total_failed}\n"
            f"Active sessions: {len(valid_sessions)}"
        )
        await update_active_message(
            job_id, 
            status,
            InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🛑 Cancel Job", callback_data=f"cancel_job_{job_id}")]
            ])
        )
        
        result = await send_bulk_messages(target, message, media, job_id)
        results.append(result)
        total_sent += result['sent']
        total_failed += result['failed']
        
        # Update job stats
        if job_id in scheduled_jobs:
            scheduled_jobs[job_id].total_sent = total_sent
            scheduled_jobs[job_id].total_failed = total_failed
            await save_schedules()
        
        logger.info(f"Scheduled send to {target}: {result['sent']}/{result['total']} successful")
    
    # Final update if not cancelled
    if not cancelled:
        final_status = (
            f"✅ Job {job_id[:6]}... completed\n"
            f"Targets: {len(targets)}\n"
            f"Successful: {total_sent}\n"
            f"Failed: {total_failed}\n"
            f"Total sent: {total_sent + total_failed}"
        )
        await update_active_message(job_id, final_status)
    
    # Clean up
    if job_id in active_messages:
        del active_messages[job_id]
    
    # Update next run time if not cancelled
    if not cancelled and job_id in scheduled_jobs and scheduled_jobs[job_id].interval > 0:
        scheduled_jobs[job_id].next_run = get_current_time() + timedelta(
            minutes=scheduled_jobs[job_id].interval
        )
        await save_schedules()
    
    # Notify owner if any failures
    if not cancelled and any(r['failed'] > 0 for r in results):
        await bot.send_message(
            OWNER_ID,
            f"⚠️ Scheduled job had failures\n"
            f"Job ID: {job_id[:8]}...\n"
            f"Total targets: {len(targets)}\n"
            f"Success: {total_sent}\n"
            f"Failures: {total_failed}"
        )

async def send_bulk_messages(
    target: str, 
    message: str, 
    media: Optional[str] = None,
    job_id: Optional[str] = None
) -> Dict[str, int]:
    """Send messages using all valid sessions with batch processing"""
    valid_sessions = [s.name for s in active_sessions.values() if s.valid and not s.is_banned]
    total_sessions = len(valid_sessions)
    
    if not valid_sessions:
        return {"sent": 0, "total": 0, "failed": 0}
    
    sent_count = 0
    failed_count = 0
    target_entity = extract_entity(target)
    
    # Process in batches to avoid overwhelming
    for i in range(0, total_sessions, BATCH_SIZE):
        batch = valid_sessions[i:i+BATCH_SIZE]
        
        # Update status if we have a job_id
        if job_id:
            status = (
                f"⏳ Processing batch {i//BATCH_SIZE + 1}/{(total_sessions//BATCH_SIZE)+1}\n"
                f"Target: {target_entity[:20]}...\n"
                f"Sent: {sent_count}\n"
                f"Failed: {failed_count}"
            )
            await update_active_message(
                job_id, 
                status,
                InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🛑 Cancel Job", callback_data=f"cancel_job_{job_id}")]
                ])
            )
        
        results = await asyncio.gather(
            *(send_from_session(s, target_entity, message, media, job_id) for s in batch),
            return_exceptions=True
        )
        
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Error in batch send: {result}")
                failed_count += 1
            elif result:
                sent_count += 1
            else:
                failed_count += 1
    
    # Update campaign stats
    campaign_stats["total_sent"] += sent_count
    campaign_stats["last_run"] = get_current_time()
    campaign_stats["target"] = target
    campaign_stats["message"] = message
    campaign_stats["media"] = media
    
    return {
        "sent": sent_count,
        "total": total_sessions,
        "failed": failed_count
    }

# --- Keyboard Menus ---
def welcome_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚀 BULK MESSAGE", callback_data="show_full_menu")]
    ])

def main_menu() -> InlineKeyboardMarkup:
    buttons = [
        [
            InlineKeyboardButton(text="📍 Set Target", callback_data="set_target"),
            InlineKeyboardButton(text="💬 Set Message", callback_data="set_message")
        ],
        [
            InlineKeyboardButton(text="🖼 Set Media", callback_data="set_media"),
            InlineKeyboardButton(text="📊 Statistics", callback_data="show_stats")
        ],
        [
            InlineKeyboardButton(text="🚀 Send Now", callback_data="send_messages"),
            InlineKeyboardButton(text="⏰ Schedules", callback_data="manage_schedules")
        ],
        [
            InlineKeyboardButton(text="➕ Add Session", callback_data="add_session"),
            InlineKeyboardButton(text="🗑 Remove Session", callback_data="remove_session")
        ],
        [
            InlineKeyboardButton(text="🔄 Refresh", callback_data="refresh_sessions"),
            InlineKeyboardButton(text="⚠️ Check Spam", callback_data="check_spam")
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def cancel_button() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Cancel", callback_data="cancel_action")]
    ])

def confirm_button() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Confirm", callback_data="confirm_action"),
            InlineKeyboardButton(text="❌ Cancel", callback_data="cancel_action")
        ]
    ])

def send_options() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🚀 Send Now", callback_data="send_now"),
            InlineKeyboardButton(text="⏰ Schedule", callback_data="schedule_current")
        ],
        [InlineKeyboardButton(text="⬅️ Back", callback_data="main_menu")]
    ])

def schedule_keyboard(schedules: List[ScheduledJob]) -> InlineKeyboardMarkup:
    buttons = []
    for job in schedules:
        time_left = format_timedelta(job.next_run - get_current_time()) if job.next_run else "Now"
        has_media = "📎" if job.media else ""
        buttons.append([InlineKeyboardButton(
            text=f"⏰ {job.job_id[:6]}... | Targets: {len(job.targets)} | Every {job.interval}m | Next: {time_left} {has_media}",
            callback_data=f"manage_job_{job.job_id}"
        )])
    buttons.append([InlineKeyboardButton(text="➕ New Schedule", callback_data="schedule_messages")])
    buttons.append([InlineKeyboardButton(text="⬅️ Back", callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def job_management_keyboard(job_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔄 Run Now", callback_data=f"run_job_{job_id}"),
            InlineKeyboardButton(text="✏️ Edit", callback_data=f"edit_job_{job_id}")
        ],
        [
            InlineKeyboardButton(text="⏱ Change Interval", callback_data=f"change_interval_{job_id}"),
            InlineKeyboardButton(text="🗑 Delete", callback_data=f"delete_job_{job_id}")
        ],
        [InlineKeyboardButton(text="⬅️ Back", callback_data="manage_schedules")]
    ])

def edit_job_keyboard(job_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📌 Edit Targets", callback_data=f"edit_targets_{job_id}"),
            InlineKeyboardButton(text="💬 Edit Message", callback_data=f"edit_message_{job_id}")
        ],
        [
            InlineKeyboardButton(text="🖼 Edit Media", callback_data=f"edit_media_{job_id}"),
            InlineKeyboardButton(text="⏱ Edit Interval", callback_data=f"change_interval_{job_id}")
        ],
        [InlineKeyboardButton(text="⬅️ Back", callback_data=f"manage_job_{job_id}")]
    ])

def session_selection_keyboard(sessions: List[SessionInfo], action: str) -> InlineKeyboardMarkup:
    buttons = []
    for session in sessions:
        status = "✅" if session.valid and not session.is_banned else "❌"
        premium = "🌟" if session.is_premium else ""
        banned = "🚫" if session.is_banned else ""
        buttons.append([InlineKeyboardButton(
            text=f"{status} {premium}{banned}{session.name[:15]}... ({session.phone or '?'})",
            callback_data=f"{action}_{session.name}"
        )])
    buttons.append([InlineKeyboardButton(text="⬅️ Back", callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def spam_reports_keyboard() -> InlineKeyboardMarkup:
    buttons = []
    for session_name, report in spam_reports.items():
        if session_name in active_sessions:
            buttons.append([InlineKeyboardButton(
                text=f"🚫 {session_name[:15]}... - {report.get('error', 'Unknown error')[:20]}...",
                callback_data=f"spam_report_{session_name}"
            )])
    buttons.append([InlineKeyboardButton(text="🔄 Refresh Reports", callback_data="refresh_spam_reports")])
    buttons.append([InlineKeyboardButton(text="⬅️ Back", callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# --- Session Management ---
async def validate_session(session_name: str) -> bool:
    """Validate a session and update its information"""
    session_path = os.path.join(SESSIONS_FOLDER, f"{session_name}.session")
    if not os.path.exists(session_path):
        return False

    client = TelegramClient(session_path, API_ID, API_HASH)
    try:
        await client.connect()
        
        if not await client.is_user_authorized():
            logger.error(f"Session {session_name} not authorized")
            active_sessions[session_name].valid = False
            active_sessions[session_name].is_banned = True
            active_sessions[session_name].last_error = "Not authorized"
            return False
        
        me = await client.get_me()
        phone = me.phone or ""
        
        # Initialize session info if not exists
        if session_name not in active_sessions:
            active_sessions[session_name] = SessionInfo(name=session_name)
        
        # Update session info
        active_sessions[session_name].phone = phone
        active_sessions[session_name].valid = True
        active_sessions[session_name].is_premium = getattr(me, 'premium', False)
        active_sessions[session_name].is_banned = False
        active_sessions[session_name].last_error = ""
        
        # Additional validation
        try:
            await client(functions.account.GetAccountTTLRequest())
            return True
        except Exception as e:
            logger.error(f"Account {session_name} might be limited: {e}")
            active_sessions[session_name].valid = False
            active_sessions[session_name].is_banned = True
            active_sessions[session_name].last_error = str(e)
            return False
            
    except (SessionRevokedError, AuthKeyError):
        logger.error(f"Session {session_name} revoked or auth error")
        active_sessions[session_name].valid = False
        active_sessions[session_name].is_banned = True
        active_sessions[session_name].last_error = "Session revoked"
        return False
    except InputUserDeactivatedError:
        logger.error(f"Session {session_name} deactivated")
        active_sessions[session_name].valid = False
        active_sessions[session_name].is_banned = True
        active_sessions[session_name].last_error = "Account deactivated"
        return False
    except Exception as e:
        logger.error(f"Error validating session {session_name}: {str(e)}")
        active_sessions[session_name].valid = False
        active_sessions[session_name].is_banned = True
        active_sessions[session_name].last_error = str(e)
        return False
    finally:
        try:
            await client.disconnect()
        except:
            pass

async def check_session_spam(session_name: str) -> bool:
    """Check if a session is flagged as spam"""
    if session_name not in active_sessions:
        return False
    
    session_path = os.path.join(SESSIONS_FOLDER, f"{session_name}.session")
    client = TelegramClient(session_path, API_ID, API_HASH)
    
    try:
        await client.connect()
        
        if not await client.is_user_authorized():
            active_sessions[session_name].is_banned = True
            active_sessions[session_name].last_error = "Not authorized"
            return True
        
        # Try a simple request to check for spam
        try:
            await client(functions.account.GetAccountTTLRequest())
            active_sessions[session_name].is_banned = False
            active_sessions[session_name].last_error = ""
            return False
        except Exception as e:
            active_sessions[session_name].is_banned = True
            active_sessions[session_name].last_error = str(e)
            spam_reports[session_name] = {
                "error": str(e),
                "timestamp": str(get_current_time())
            }
            await save_spam_reports()
            return True
            
    except Exception as e:
        active_sessions[session_name].is_banned = True
        active_sessions[session_name].last_error = str(e)
        spam_reports[session_name] = {
            "error": str(e),
            "timestamp": str(get_current_time())
        }
        await save_spam_reports()
        return True
    finally:
        try:
            await client.disconnect()
        except:
            pass

async def load_sessions() -> None:
    """Load and validate all sessions from the sessions folder"""
    session_files = [f for f in os.listdir(SESSIONS_FOLDER) if f.endswith('.session')][:MAX_SESSIONS]
    
    # Initialize session info for all files
    for session_file in session_files:
        session_name = os.path.splitext(session_file)[0]
        if session_name not in active_sessions:
            active_sessions[session_name] = SessionInfo(name=session_name)
    
    # Validate sessions in parallel
    validation_tasks = []
    for session_name in active_sessions:
        validation_tasks.append(validate_session(session_name))
    
    await asyncio.gather(*validation_tasks)

async def add_session_file(session_file: types.Document) -> Tuple[bool, str]:
    """Handle session file upload and validation"""
    if not session_file.file_name.endswith('.session'):
        return False, "File must have .session extension"
    
    session_name = os.path.splitext(session_file.file_name)[0]
    dest_path = os.path.join(SESSIONS_FOLDER, session_file.file_name)
    
    if os.path.exists(dest_path):
        return False, "Session with this name already exists"
    
    try:
        # Download the file
        await bot.download(session_file, destination=dest_path)
        
        # Validate the session
        is_valid = await validate_session(session_name)
        
        if is_valid:
            return True, f"✅ Session {session_name} added successfully"
        else:
            os.remove(dest_path)
            return False, "❌ Session is invalid or unauthorized"
            
    except Exception as e:
        logger.error(f"Error adding session: {str(e)}")
        if os.path.exists(dest_path):
            try:
                os.remove(dest_path)
            except:
                pass
        return False, f"❌ Failed to add session: {str(e)}"

async def remove_session_file(session_name: str) -> Tuple[bool, str]:
    """Remove a session file and its data"""
    session_path = os.path.join(SESSIONS_FOLDER, f"{session_name}.session")
    
    if not os.path.exists(session_path):
        return False, "Session file not found"
    
    try:
        os.remove(session_path)
        if session_name in active_sessions:
            del active_sessions[session_name]
        if session_name in spam_reports:
            del spam_reports[session_name]
            await save_spam_reports()
        return True, f"✅ Session {session_name} removed successfully"
    except Exception as e:
        logger.error(f"Error removing session {session_name}: {str(e)}")
        return False, f"❌ Failed to remove session: {str(e)}"

# --- Enhanced Message Sending with Media Support ---
async def send_from_session(
    session_name: str, 
    target: str, 
    message: str, 
    media_path: Optional[str] = None, 
    job_id: Optional[str] = None,
    retry: int = 0
) -> bool:
    """Improved message sending with better error handling and media support"""
    if session_name not in active_sessions or not active_sessions[session_name].valid:
        logger.error(f"Session {session_name} not valid")
        return False
    
    session_info = active_sessions[session_name]
    session_path = os.path.join(SESSIONS_FOLDER, f"{session_name}.session")
    client = TelegramClient(session_path, API_ID, API_HASH)
    
    try:
        await client.connect()
        
        if not await client.is_user_authorized():
            session_info.valid = False
            session_info.is_banned = True
            session_info.last_error = "Not authorized"
            logger.error(f"Session {session_name} not authorized")
            return False
        
        try:
            target_entity = extract_entity(target)
            
            # Try to join the group/channel first
            join_success = await join_group(client, target_entity)
            if join_success:
                session_info.join_success += 1
            else:
                session_info.join_failed += 1
                session_info.errors += 1
                session_info.last_error = f"Failed to join {target}"
                return False
            
            entity = await client.get_entity(target_entity)
            
            # Send message with media support
            if media_path and os.path.exists(media_path):
                # Determine media type by extension
                ext = os.path.splitext(media_path)[1].lower()
                if ext in ('.jpg', '.jpeg', '.png'):
                    await client.send_file(entity, media_path, caption=message)
                elif ext in ('.mp4', '.mov', '.avi'):
                    await client.send_file(entity, media_path, caption=message, supports_streaming=True)
                elif ext in ('.pdf', '.doc', '.docx', '.txt'):
                    await client.send_file(entity, media_path, caption=message, force_document=True)
                else:
                    await client.send_file(entity, media_path, caption=message)
            else:
                # Send text message only
                await client.send_message(
                    entity, 
                    message,
                    parse_mode='md' if any(c in message for c in '*_`[') else None
                )
            
            # Update session stats
            session_info.sent_count += 1
            session_info.last_used = get_current_time()
            session_info.errors = 0
            session_info.last_error = ""
            
            # Update job progress if available
            if job_id and job_id in scheduled_jobs:
                scheduled_jobs[job_id].total_sent += 1
                await save_schedules()
            
            return True
            
        except PeerFloodError:
            logger.error(f"Peer flood error from {session_name}")
            session_info.errors += 1
            session_info.last_error = "Peer flood error"
            session_info.is_banned = True
            spam_reports[session_name] = {
                "error": "Peer flood error",
                "timestamp": str(get_current_time())
            }
            await save_spam_reports()
            return False
        except FloodWaitError as e:
            logger.error(f"Flood wait for {e.seconds} seconds from {session_name}")
            session_info.errors += 1
            session_info.last_error = f"Flood wait {e.seconds}s"
            await asyncio.sleep(e.seconds)
            return False
        except (UserPrivacyRestrictedError, UserIsBlockedError):
            logger.error(f"Privacy restriction from {session_name}")
            session_info.errors += 1
            session_info.last_error = "Privacy restriction"
            return False
        except ChannelPrivateError:
            logger.error(f"Channel private error from {session_name}")
            session_info.errors += 1
            session_info.last_error = "Channel private"
            return False
        except ChatWriteForbiddenError:
            logger.error(f"Write forbidden in chat from {session_name}")
            session_info.errors += 1
            session_info.last_error = "Write forbidden"
            return False
        except ChatAdminRequiredError:
            logger.error(f"Admin required in chat from {session_name}")
            session_info.errors += 1
            session_info.last_error = "Admin required"
            return False
        except Exception as e:
            logger.error(f"Send error from {session_name} to {target}: {e}")
            session_info.errors += 1
            session_info.last_error = str(e)
            if "spam" in str(e).lower() or "ban" in str(e).lower():
                session_info.is_banned = True
                spam_reports[session_name] = {
                    "error": str(e),
                    "timestamp": str(get_current_time())
                }
                await save_spam_reports()
            return False
    
    except Exception as e:
        logger.error(f"Connection error in {session_name}: {e}")
        session_info.errors += 1
        session_info.last_error = str(e)
        return False
    finally:
        try:
            await client.disconnect()
        except:
            pass
        await asyncio.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

# --- Command Handlers ---
@router.message(Command("start"))
async def start_handler(msg: types.Message):
    """Handle /start command with new welcome flow"""
    if msg.from_user.id != OWNER_ID:
        return await msg.answer("🚫 Access denied. This bot is private.")
    
    await msg.answer(
        f"👋 Welcome {hbold(msg.from_user.first_name)}!\n"
        "This is an advanced Telegram bulk message sender bot with:\n"
        "- Multi-user group messaging\n"
        "- Media/file attachments\n"
        "- Scheduling capabilities\n"
        "- Spam detection system\n"
        "- Live progress updates\n\n"
        "Click the button below to get started:",
        reply_markup=welcome_menu()
    )

@router.message(Command("stats"))
async def stats_command(msg: types.Message):
    """Handle /stats command"""
    if msg.from_user.id != OWNER_ID:
        return await msg.answer("🚫 Access denied.")
    await show_statistics(msg)

@router.message(Command("help"))
async def help_command(msg: types.Message):
    """Show help information"""
    help_text = (
        "📚 Bot Help Guide:\n\n"
        "1. First add session files (.session)\n"
        "2. Set target (username/phone/URL) and message\n"
        "3. Optionally attach media (image, video, document)\n"
        "4. Choose to send now or schedule\n\n"
        "⏰ Scheduling:\n"
        "- Set interval in minutes (e.g., 15)\n"
        "- Manage active schedules\n"
        "- Edit/delete schedules anytime\n\n"
        "⚠️ Spam Check:\n"
        "- Detect banned/limited accounts\n"
        "- View error reports\n"
        "- Remove problematic sessions\n\n"
        "🔄 Live Updates:\n"
        "- Real-time progress tracking\n"
        "- Cancel running jobs\n\n"
        "🛠 Commands:\n"
        "/start - Restart bot\n"
        "/stats - Show statistics\n"
        "/help - This message"
    )
    await msg.answer(help_text, reply_markup=main_menu())

@router.message(Command("clean"))
async def clean_command(msg: types.Message):
    """Clean up media folder"""
    if msg.from_user.id != OWNER_ID:
        return await msg.answer("🚫 Access denied.")
    
    await msg.answer(
        "Are you sure you want to clear all media files?",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Yes", callback_data="confirm_clean"),
                InlineKeyboardButton(text="❌ No", callback_data="cancel_action")
            ]
        ])
    )

# --- Callback Handlers ---
@router.callback_query(F.data == "show_full_menu")
async def show_full_menu_handler(query: types.CallbackQuery):
    """Show the full menu after clicking BULK MESSAGE"""
    try:
        await query.message.edit_text(
            "📋 Main Menu - Select an option:",
            reply_markup=main_menu()
        )
    except Exception as e:
        logger.error(f"Error showing full menu: {e}")
        await query.answer("Main Menu")

@router.callback_query(F.data == "main_menu")
async def return_to_menu(query: types.CallbackQuery):
    """Return to main menu"""
    try:
        await query.message.edit_text(
            "Main Menu:",
            reply_markup=main_menu()
        )
    except Exception as e:
        logger.error(f"Error returning to menu: {e}")
        await query.answer("Main Menu")

@router.callback_query(F.data == "cancel_action")
async def cancel_action(query: types.CallbackQuery, state: FSMContext):
    """Cancel current action"""
    await state.clear()
    try:
        await query.message.edit_text(
            "Action cancelled.",
            reply_markup=main_menu()
        )
    except Exception as e:
        logger.error(f"Error cancelling action: {e}")
        await query.answer("Action cancelled")

@router.callback_query(F.data == "confirm_clean")
async def confirm_clean_handler(query: types.CallbackQuery):
    """Clean media folder"""
    try:
        if not os.path.exists(MEDIA_FOLDER):
            os.makedirs(MEDIA_FOLDER)
        
        # Remove all files in media folder
        for filename in os.listdir(MEDIA_FOLDER):
            file_path = os.path.join(MEDIA_FOLDER, filename)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
            except Exception as e:
                logger.error(f"Error deleting {file_path}: {e}")
        
        # Clear media from all jobs
        for job_id in scheduled_jobs:
            scheduled_jobs[job_id].media = None
        await save_schedules()
        
        # Clear campaign media
        campaign_stats["media"] = None
        
        await query.message.edit_text(
            "✅ All media files cleared successfully!",
            reply_markup=main_menu()
        )
    except Exception as e:
        logger.error(f"Error cleaning media: {e}")
        await query.message.edit_text(
            "❌ Failed to clear media files",
            reply_markup=main_menu()
        )

@router.callback_query(F.data == "set_target")
async def set_target_handler(query: types.CallbackQuery, state: FSMContext):
    """Initiate target setting"""
    await state.set_state(BotStates.setting_target)
    try:
        await query.message.edit_text(
            "📌 Send the target username, phone number, or invite link:\n"
            "Examples:\n"
            "- Username: @channel or channel\n"
            "- Phone: +1234567890\n"
            "- Invite: https://t.me/joinchat/ABC123\n"
            "- Multiple: group1,group2,@channel3",
            reply_markup=cancel_button()
        )
    except Exception as e:
        logger.error(f"Error setting target: {e}")
        await query.answer("Send the target username")

@router.message(BotStates.setting_target)
async def save_target(msg: types.Message, state: FSMContext):
    """Save target information"""
    target = msg.text.strip() if msg.text else ""
    if not target:
        await msg.answer("Please enter a valid target", reply_markup=cancel_button())
        return
    
    campaign_stats["target"] = target
    await state.clear()
    await msg.answer(
        f"✅ Target set to: {hcode(target)}",
        reply_markup=main_menu()
    )

@router.callback_query(F.data == "set_message")
async def set_message_handler(query: types.CallbackQuery, state: FSMContext):
    """Initiate message setting"""
    await state.set_state(BotStates.setting_message)
    try:
        await query.message.edit_text(
            "💬 Send the message text you want to send:\n"
            "Supports Markdown formatting:\n"
            "*bold*, _italic_, `code`, [link](url)\n\n"
            "For long messages, they will be automatically split.",
            reply_markup=cancel_button()
        )
    except Exception as e:
        logger.error(f"Error setting message: {e}")
        await query.answer("Send the message text")

@router.message(BotStates.setting_message)
async def save_message(msg: types.Message, state: FSMContext):
    """Save message content"""
    if not msg.text:
        await msg.answer("Please enter a valid message", reply_markup=cancel_button())
        return
    
    message = msg.text.strip()
    if not message:
        await msg.answer("Please enter a valid message", reply_markup=cancel_button())
        return
    
    if len(message) > MAX_MESSAGE_LENGTH:
        for chunk in split_long_message(message):
            await msg.answer(chunk)
        await msg.answer("⚠️ Your message was too long and was split into multiple parts.")
    
    campaign_stats["message"] = message
    await state.clear()
    await msg.answer(
        "✅ Message saved!",
        reply_markup=main_menu()
    )

@router.callback_query(F.data == "set_media")
async def set_media_handler(query: types.CallbackQuery, state: FSMContext):
    """Initiate media setting"""
    await state.set_state(BotStates.setting_media)
    try:
        await query.message.edit_text(
            "🖼 Send an image, video, or document to attach to your message:\n"
            "Supported formats:\n"
            "- Images: JPG, PNG\n"
            "- Videos: MP4, MOV\n"
            "- Documents: PDF, DOC, TXT\n\n"
            "Send /clear to remove current media or /cancel to skip.",
            reply_markup=cancel_button()
        )
    except Exception as e:
        logger.error(f"Error setting media: {e}")
        await query.answer("Send media file")

@router.message(BotStates.setting_media, F.content_type.in_({ContentType.PHOTO, ContentType.VIDEO, ContentType.DOCUMENT}))
async def save_media(msg: types.Message, state: FSMContext):
    """Save media file"""
    try:
        # Create media folder if not exists
        if not os.path.exists(MEDIA_FOLDER):
            os.makedirs(MEDIA_FOLDER)
        
        # Determine file type and extension
        if msg.photo:
            file_id = msg.photo[-1].file_id
            ext = ".jpg"
        elif msg.video:
            file_id = msg.video.file_id
            ext = ".mp4"
        elif msg.document:
            file_id = msg.document.file_id
            ext = os.path.splitext(msg.document.file_name)[1] if msg.document.file_name else ".bin"
        
        # Download the file
        file = await bot.get_file(file_id)
        media_path = f"{MEDIA_FOLDER}/{file_id}{ext}"
        await bot.download_file(file.file_path, media_path)
        
        # Save to campaign stats
        campaign_stats["media"] = media_path
        await state.clear()
        
        # Show confirmation with file type
        file_type = "photo" if msg.photo else "video" if msg.video else "document"
        await msg.answer(
            f"✅ {file_type.capitalize()} attached successfully!",
            reply_markup=main_menu()
        )
    except Exception as e:
        logger.error(f"Error saving media: {e}")
        await msg.answer(
            "❌ Failed to save media. Please try again.",
            reply_markup=cancel_button()
        )

@router.message(BotStates.setting_media, Command("clear"))
async def clear_media(msg: types.Message, state: FSMContext):
    """Clear media attachment"""
    campaign_stats["media"] = None
    await state.clear()
    await msg.answer(
        "Media attachment cleared.",
        reply_markup=main_menu()
    )

@router.callback_query(F.data == "send_messages")
async def send_messages_handler(query: types.CallbackQuery):
    """Handle message sending options"""
    if not campaign_stats.get("target") or not campaign_stats.get("message"):
        try:
            await query.message.edit_text(
                "⚠️ Please set both target and message first.",
                reply_markup=main_menu()
            )
        except Exception as e:
            logger.error(f"Error in send messages handler: {e}")
            await query.answer("Please set both target and message first.")
        return
    
    try:
        media_status = "📎 Media attached" if campaign_stats.get("media") else "No media"
        await query.message.edit_text(
            "Choose how you want to send:\n"
            f"Target: {hcode(campaign_stats['target'])}\n"
            f"Message: {hcode(campaign_stats['message'][:50])}...\n"
            f"{media_status}",
            reply_markup=send_options()
        )
    except Exception as e:
        logger.error(f"Error showing send options: {e}")
        await query.answer("Choose sending method")

@router.callback_query(F.data == "send_now")
async def send_now_handler(query: types.CallbackQuery):
    """Send messages immediately"""
    try:
        msg = await query.message.edit_text(
            "🚀 Sending messages now...",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🛑 Cancel Sending", callback_data="cancel_sending")]
            ])
        )
        active_messages["send_now"] = msg
    except Exception as e:
        logger.error(f"Error starting send: {e}")
        await query.answer("Sending messages...")
    
    start_time = time.time()
    result = await send_bulk_messages(
        campaign_stats["target"],
        campaign_stats["message"],
        campaign_stats.get("media"),
        "send_now"
    )
    elapsed = time.time() - start_time
    
    status_message = (
        f"📊 Sending completed in {elapsed:.2f} seconds\n"
        f"✅ Successful: {result['sent']}\n"
        f"❌ Failed: {result['failed']}\n"
        f"📋 Total sessions used: {result['total']}"
    )
    
    try:
        if "send_now" in active_messages:
            del active_messages["send_now"]
        await query.message.edit_text(
            status_message,
            reply_markup=main_menu()
        )
    except Exception as e:
        logger.error(f"Error showing send results: {e}")
        await query.answer(status_message)

@router.callback_query(F.data == "cancel_sending")
async def cancel_sending_handler(query: types.CallbackQuery):
    """Cancel current sending operation"""
    if "send_now" in active_messages:
        del active_messages["send_now"]
    await query.message.edit_text(
        "🛑 Message sending cancelled!",
        reply_markup=main_menu()
    )
    await query.answer("Sending cancelled")

@router.callback_query(F.data.startswith("cancel_job_"))
async def cancel_job_handler(query: types.CallbackQuery):
    """Cancel a scheduled job"""
    job_id = query.data.split("_", 2)[2]
    if job_id in active_messages:
        del active_messages[job_id]
    await query.message.edit_text(
        f"🛑 Job {job_id[:6]}... cancelled!",
        reply_markup=main_menu()
    )
    await query.answer("Job cancelled")

@router.callback_query(F.data == "schedule_current")
async def schedule_current_handler(query: types.CallbackQuery, state: FSMContext):
    """Schedule current message"""
    await state.set_state(BotStates.setting_interval)
    try:
        media_status = "with media" if campaign_stats.get("media") else "without media"
        await query.message.edit_text(
            f"⏳ Scheduling message {media_status}\n"
            "Enter interval in minutes (e.g., 15 for every 15 minutes):",
            reply_markup=cancel_button()
        )
    except Exception as e:
        logger.error(f"Error scheduling message: {e}")
        await query.answer("Enter interval in minutes")

@router.message(BotStates.setting_interval)
async def save_schedule_interval(msg: types.Message, state: FSMContext):
    """Save schedule interval and create job"""
    try:
        interval = int(msg.text.strip())
        if interval < 1:
            raise ValueError
    except:
        await msg.answer("Please enter a valid number (minutes):", reply_markup=cancel_button())
        return
    
    targets = [t.strip() for t in campaign_stats["target"].split(',') if t.strip()]
    if not targets:
        await msg.answer("Invalid targets, please set again", reply_markup=main_menu())
        return
    
    job_id = f"job_{int(time.time())}"
    schedule_job(
        job_id,
        targets,
        campaign_stats["message"],
        campaign_stats.get("media"),
        interval
    )
    
    await state.clear()
    await msg.answer(
        f"✅ Scheduled messages every {interval} minutes\n"
        f"Targets: {', '.join(targets)}\n"
        f"Media: {'Yes' if campaign_stats.get('media') else 'No'}\n"
        f"Job ID: {job_id[:8]}...",
        reply_markup=main_menu()
    )

@router.callback_query(F.data == "show_stats")
async def show_stats_handler(query: types.CallbackQuery):
    """Show statistics"""
    await show_statistics(query.message)

async def show_statistics(message: types.Message):
    """Display detailed statistics"""
    valid_sessions = sum(1 for s in active_sessions.values() if s.valid and not s.is_banned)
    total_sessions = len(active_sessions)
    premium_sessions = sum(1 for s in active_sessions.values() if s.is_premium)
    banned_sessions = sum(1 for s in active_sessions.values() if s.is_banned)
    
    stats_message = (
        f"📊 Bot Statistics\n\n"
        f"• Sessions: {valid_sessions}/{total_sessions} active\n"
        f"• Premium: {premium_sessions} | Banned: {banned_sessions}\n"
        f"• Total messages sent: {campaign_stats.get('total_sent', 0)}\n"
    )
    
    if campaign_stats.get("last_run"):
        stats_message += f"• Last run: {format_time(campaign_stats['last_run'])}\n"
    
    if scheduled_jobs:
        stats_message += f"\n⏰ Active Schedules: {len(scheduled_jobs)}\n"
        for job in list(scheduled_jobs.values())[:3]:
            time_left = format_timedelta(job.next_run - get_current_time()) if job.next_run else "Now"
            has_media = "📎" if job.media else ""
            stats_message += (
                f"  • {job.job_id[:6]}... | Targets: {len(job.targets)} | "
                f"Every {job.interval}m | Next: {time_left} {has_media}\n"
            )
        if len(scheduled_jobs) > 3:
            stats_message += f"  • And {len(scheduled_jobs)-3} more...\n"
    
    # Add top performing sessions
    top_sessions = sorted(
        [s for s in active_sessions.values() if s.valid and not s.is_banned],
        key=lambda x: x.sent_count,
        reverse=True
    )[:3]
    
    if top_sessions:
        stats_message += "\n🏆 Top Sessions:\n"
        for i, session in enumerate(top_sessions, 1):
            premium = "🌟" if session.is_premium else ""
            stats_message += (
                f"{i}. {premium}{session.name[:10]}... - "
                f"{session.sent_count} sends | "
                f"Last used: {format_time(session.last_used) if session.last_used else 'Never'}\n"
            )
    
    try:
        await message.answer(
            stats_message,
            reply_markup=main_menu()
        )
    except Exception as e:
        logger.error(f"Error showing statistics: {e}")
        for chunk in split_long_message(stats_message):
            await message.answer(chunk)

@router.callback_query(F.data == "add_session")
async def add_session_handler(query: types.CallbackQuery, state: FSMContext):
    """Initiate session upload"""
    if len(active_sessions) >= MAX_SESSIONS:
        await query.answer(f"Maximum {MAX_SESSIONS} sessions reached")
        return
    
    await state.set_state(BotStates.adding_session)
    try:
        await query.message.edit_text(
            "📲 Please upload a .session file:\n\n"
            "1. Export from Telegram Desktop (Settings > Advanced > Export Telegram data)\n"
            "2. Upload the .session file here\n\n"
            "Note: The account must be already logged in",
            reply_markup=cancel_button()
        )
    except Exception as e:
        logger.error(f"Error adding session: {e}")
        await query.answer("Please upload a .session file")

@router.message(BotStates.adding_session, F.document)
async def handle_session_upload(msg: types.Message, state: FSMContext):
    """Handle session file upload"""
    if not msg.document.file_name.endswith('.session'):
        await msg.answer(
            "❌ File must have .session extension",
            reply_markup=cancel_button()
        )
        return
    
    session_name = os.path.splitext(msg.document.file_name)[0]
    dest_path = os.path.join(SESSIONS_FOLDER, msg.document.file_name)
    
    # Check if session exists
    if os.path.exists(dest_path):
        await msg.answer(
            f"❌ Session {hcode(session_name)} already exists",
            reply_markup=cancel_button()
        )
        return
    
    try:
        # Show downloading status
        status_msg = await msg.answer("⬇️ Downloading session file...")
        
        # Download the file
        await bot.download(msg.document, destination=dest_path)
        
        # Show validating status
        await status_msg.edit_text("🔍 Validating session...")
        
        # Validate the session
        is_valid = await validate_session(session_name)
        
        if is_valid:
            session = active_sessions[session_name]
            await status_msg.edit_text(
                f"✅ Session {hcode(session_name)} added successfully\n"
                f"Phone: {session.phone or '?'}\n"
                f"Premium: {'Yes' if session.is_premium else 'No'}\n"
                f"Valid: {'Yes' if session.valid else 'No'}\n"
                f"Total valid sessions now: {sum(1 for s in active_sessions.values() if s.valid and not s.is_banned)}",
                reply_markup=main_menu()
            )
        else:
            await status_msg.edit_text(
                f"❌ Session {hcode(session_name)} is invalid or unauthorized",
                reply_markup=main_menu()
            )
            try:
                os.remove(dest_path)
            except Exception as e:
                logger.error(f"Error removing invalid session: {e}")
    except Exception as e:
        logger.error(f"Error adding session: {str(e)}")
        try:
            await status_msg.edit_text(
                f"❌ Failed to add session: {str(e)}",
                reply_markup=cancel_button()
            )
        except:
            await msg.answer(
                f"❌ Failed to add session: {str(e)}",
                reply_markup=cancel_button()
            )
        try:
            if os.path.exists(dest_path):
                os.remove(dest_path)
        except Exception as e:
            logger.error(f"Error cleaning up failed session: {e}")
    finally:
        await state.clear()

@router.callback_query(F.data == "remove_session")
async def remove_session_handler(query: types.CallbackQuery, state: FSMContext):
    """Initiate session removal"""
    if not active_sessions:
        try:
            await query.message.edit_text(
                "No sessions available to remove.",
                reply_markup=main_menu()
            )
        except Exception as e:
            logger.error(f"Error removing session: {e}")
            await query.answer("No sessions available")
        return
    
    await state.set_state(BotStates.removing_session)
    try:
        await query.message.edit_text(
            "Select a session to remove:",
            reply_markup=session_selection_keyboard(
                list(active_sessions.values()),
                "remove"
            )
        )
    except Exception as e:
        logger.error(f"Error showing session removal options: {e}")
        await query.answer("Select a session to remove")

@router.callback_query(F.data.startswith("remove_"))
async def confirm_remove_session(query: types.CallbackQuery):
    """Confirm session removal"""
    session_name = query.data.split("_", 1)[1]
    try:
        session = active_sessions[session_name]
        await query.message.edit_text(
            f"Are you sure you want to remove this session?\n\n"
            f"Name: {session_name}\n"
            f"Phone: {session.phone or '?'}\n"
            f"Messages sent: {session.sent_count}\n"
            f"Status: {'✅ Valid' if session.valid else '❌ Invalid'} "
            f"{'🚫 Banned' if session.is_banned else ''}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="✅ Yes",
                        callback_data=f"confirm_remove_{session_name}"
                    ),
                    InlineKeyboardButton(
                        text="❌ No",
                        callback_data="main_menu"
                    )
                ]
            ])
        )
    except Exception as e:
        logger.error(f"Error confirming session removal: {e}")
        await query.answer(f"Confirm removal of {session_name}")

@router.callback_query(F.data.startswith("confirm_remove_"))
async def execute_remove_session(query: types.CallbackQuery, state: FSMContext):
    """Execute session removal"""
    session_name = query.data.split("_", 2)[2]
    success, message = await remove_session_file(session_name)
    await state.clear()
    try:
        await query.message.edit_text(
            message,
            reply_markup=main_menu()
        )
    except Exception as e:
        logger.error(f"Error completing session removal: {e}")
        await query.answer(message)

@router.callback_query(F.data == "refresh_sessions")
async def refresh_sessions_handler(query: types.CallbackQuery):
    """Refresh session status"""
    try:
        await query.message.edit_text("🔄 Refreshing session status...")
    except Exception as e:
        logger.error(f"Error starting session refresh: {e}")
        await query.answer("Refreshing sessions...")
    
    await load_sessions()
    valid_count = sum(1 for s in active_sessions.values() if s.valid and not s.is_banned)
    banned_count = sum(1 for s in active_sessions.values() if s.is_banned)
    
    try:
        await query.message.edit_text(
            f"✅ Sessions refreshed\n"
            f"Valid sessions: {valid_count}/{len(active_sessions)}\n"
            f"Premium sessions: {sum(1 for s in active_sessions.values() if s.is_premium)}\n"
            f"Banned sessions: {banned_count}",
            reply_markup=main_menu()
        )
    except Exception as e:
        logger.error(f"Error showing refreshed sessions: {e}")
        await query.answer(f"Sessions refreshed: {valid_count} valid")

# --- Schedule Management Handlers ---
@router.callback_query(F.data == "manage_schedules")
async def manage_schedules_handler(query: types.CallbackQuery):
    """Show all scheduled jobs"""
    if not scheduled_jobs:
        try:
            await query.message.edit_text(
                "No active schedules. Create one first.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="➕ New Schedule", callback_data="schedule_messages")],
                    [InlineKeyboardButton(text="⬅️ Back", callback_data="main_menu")]
                ])
            )
        except Exception as e:
            logger.error(f"Error managing schedules: {e}")
            await query.answer("No active schedules")
        return
    
    try:
        await query.message.edit_text(
            "⏰ Active Schedules:",
            reply_markup=schedule_keyboard(list(scheduled_jobs.values()))
        )
    except Exception as e:
        logger.error(f"Error showing schedules: {e}")
        await query.answer("Manage schedules")

@router.callback_query(F.data.startswith("manage_job_"))
async def manage_job_handler(query: types.CallbackQuery):
    """Manage individual job"""
    job_id = query.data.split("_", 2)[2]
    if job_id not in scheduled_jobs:
        await query.answer("Job not found")
        return
    
    job = scheduled_jobs[job_id]
    time_left = format_timedelta(job.next_run - get_current_time()) if job.next_run else "Now"
    
    try:
        media_info = "\nMedia attached: Yes" if job.media else ""
        await query.message.edit_text(
            f"⏰ Schedule Details:\n\n"
            f"ID: {job.job_id}\n"
            f"Targets: {len(job.targets)} groups\n"
            f"Interval: Every {job.interval} minutes\n"
            f"Next run: In {time_left}{media_info}\n"
            f"Total sent: {job.total_sent}\n"
            f"Total failed: {job.total_failed}\n\n"
            f"Message preview:\n{hcode(job.message[:200])}",
            reply_markup=job_management_keyboard(job_id)
        )
    except Exception as e:
        logger.error(f"Error showing job details: {e}")
        await query.answer("Job details")

@router.callback_query(F.data.startswith("run_job_"))
async def run_job_handler(query: types.CallbackQuery):
    """Run scheduled job immediately"""
    job_id = query.data.split("_", 2)[2]
    if job_id not in scheduled_jobs:
        await query.answer("Job not found")
        return
    
    job = scheduled_jobs[job_id]
    await query.answer("Running job now...")
    
    result = await send_scheduled_messages(job.job_id, job.targets, job.message, job.media)
    try:
        await query.message.answer(
            f"✅ Ran schedule {job_id[:6]}...\n"
            f"Sent to {len(job.targets)} groups\n"
            f"Media: {'Yes' if job.media else 'No'}\n"
            f"Results: {sum(r['sent'] for r in result)} successful, {sum(r['failed'] for r in result)} failed",
            reply_markup=main_menu()
        )
    except Exception as e:
        logger.error(f"Error showing job results: {e}")
        await query.answer("Job completed")

@router.callback_query(F.data.startswith("edit_job_"))
async def edit_job_handler(query: types.CallbackQuery):
    """Edit job options"""
    job_id = query.data.split("_", 2)[2]
    if job_id not in scheduled_jobs:
        await query.answer("Job not found")
        return
    
    try:
        await query.message.edit_text(
            "What would you like to edit?",
            reply_markup=edit_job_keyboard(job_id)
        )
    except Exception as e:
        logger.error(f"Error editing job: {e}")
        await query.answer("Edit options")

@router.callback_query(F.data.startswith("edit_targets_"))
async def edit_job_targets_handler(query: types.CallbackQuery, state: FSMContext):
    """Edit job targets"""
    job_id = query.data.split("_", 2)[2]
    if job_id not in scheduled_jobs:
        await query.answer("Job not found")
        return
    
    await state.update_data(job_id=job_id)
    await state.set_state(BotStates.editing_job_targets)
    try:
        await query.message.edit_text(
            "📌 Enter new targets (comma separated):\n"
            f"Current: {', '.join(scheduled_jobs[job_id].targets)}",
            reply_markup=cancel_button()
        )
    except Exception as e:
        logger.error(f"Error editing job targets: {e}")
        await query.answer("Enter new targets")

@router.message(BotStates.editing_job_targets)
async def save_job_targets(msg: types.Message, state: FSMContext):
    """Save edited job targets"""
    data = await state.get_data()
    job_id = data.get("job_id")
    if not job_id or job_id not in scheduled_jobs:
        await msg.answer("Job not found", reply_markup=main_menu())
        await state.clear()
        return
    
    targets = [t.strip() for t in msg.text.split(',') if t.strip()]
    if not targets:
        await msg.answer("Invalid targets, please try again", reply_markup=cancel_button())
        return
    
    # Update job
    job = scheduled_jobs[job_id]
    job.targets = targets
    schedule_job(job_id, job.targets, job.message, job.media, job.interval)
    
    await state.clear()
    await msg.answer(
        f"✅ Updated targets for schedule {job_id[:6]}...\n"
        f"New targets: {', '.join(targets)}",
        reply_markup=main_menu()
    )

@router.callback_query(F.data.startswith("edit_message_"))
async def edit_job_message_handler(query: types.CallbackQuery, state: FSMContext):
    """Edit job message"""
    job_id = query.data.split("_", 2)[2]
    if job_id not in scheduled_jobs:
        await query.answer("Job not found")
        return
    
    await state.update_data(job_id=job_id)
    await state.set_state(BotStates.editing_job_message)
    try:
        await query.message.edit_text(
            "💬 Enter new message:\n"
            f"Current: {scheduled_jobs[job_id].message[:100]}...",
            reply_markup=cancel_button()
        )
    except Exception as e:
        logger.error(f"Error editing job message: {e}")
        await query.answer("Enter new message")

@router.message(BotStates.editing_job_message)
async def save_job_message(msg: types.Message, state: FSMContext):
    """Save edited job message"""
    data = await state.get_data()
    job_id = data.get("job_id")
    if not job_id or job_id not in scheduled_jobs:
        await msg.answer("Job not found", reply_markup=main_menu())
        await state.clear()
        return
    
    message = msg.text.strip()
    if not message:
        await msg.answer("Invalid message, please try again", reply_markup=cancel_button())
        return
    
    # Update job
    job = scheduled_jobs[job_id]
    job.message = message
    schedule_job(job_id, job.targets, job.message, job.media, job.interval)
    
    await state.clear()
    await msg.answer(
        f"✅ Updated message for schedule {job_id[:6]}...\n"
        f"New message preview: {message[:100]}...",
        reply_markup=main_menu()
    )

@router.callback_query(F.data.startswith("edit_media_"))
async def edit_job_media_handler(query: types.CallbackQuery, state: FSMContext):
    """Edit job media"""
    job_id = query.data.split("_", 2)[2]
    if job_id not in scheduled_jobs:
        await query.answer("Job not found")
        return
    
    await state.update_data(job_id=job_id)
    await state.set_state(BotStates.editing_job_media)
    try:
        current_media = scheduled_jobs[job_id].media
        media_status = f"Current media: {current_media}" if current_media else "No media currently attached"
        await query.message.edit_text(
            f"🖼 Edit media attachment\n{media_status}\n\n"
            "Send new media file or /clear to remove current media:",
            reply_markup=cancel_button()
        )
    except Exception as e:
        logger.error(f"Error editing job media: {e}")
        await query.answer("Edit media")

Here's the continuation of the code from the media editing handler:

```python
@router.message(BotStates.editing_job_media, F.content_type.in_({ContentType.PHOTO, ContentType.VIDEO, ContentType.DOCUMENT}))
async def save_job_media(msg: types.Message, state: FSMContext):
    """Save edited job media"""
    data = await state.get_data()
    job_id = data.get("job_id")
    if not job_id or job_id not in scheduled_jobs:
        await msg.answer("Job not found", reply_markup=main_menu())
        await state.clear()
        return
    
    try:
        # Create media folder if not exists
        if not os.path.exists(MEDIA_FOLDER):
            os.makedirs(MEDIA_FOLDER)
        
        # Determine file type and extension
        if msg.photo:
            file_id = msg.photo[-1].file_id
            ext = ".jpg"
        elif msg.video:
            file_id = msg.video.file_id
            ext = ".mp4"
        elif msg.document:
            file_id = msg.document.file_id
            ext = os.path.splitext(msg.document.file_name)[1] if msg.document.file_name else ".bin"
        
        # Download the file
        file = await bot.get_file(file_id)
        media_path = f"{MEDIA_FOLDER}/{file_id}{ext}"
        await bot.download_file(file.file_path, media_path)
        
        # Update job
        job = scheduled_jobs[job_id]
        job.media = media_path
        schedule_job(job_id, job.targets, job.message, job.media, job.interval)
        
        await state.clear()
        await msg.answer(
            f"✅ Updated media for schedule {job_id[:6]}...",
            reply_markup=main_menu()
        )
    except Exception as e:
        logger.error(f"Error saving job media: {e}")
        await msg.answer(
            "❌ Failed to update media. Please try again.",
            reply_markup=cancel_button()
        )

@router.message(BotStates.editing_job_media, Command("clear"))
async def clear_job_media(msg: types.Message, state: FSMContext):
    """Clear job media"""
    data = await state.get_data()
    job_id = data.get("job_id")
    if not job_id or job_id not in scheduled_jobs:
        await msg.answer("Job not found", reply_markup=main_menu())
        await state.clear()
        return
    
    # Update job
    job = scheduled_jobs[job_id]
    job.media = None
    schedule_job(job_id, job.targets, job.message, job.media, job.interval)
    
    await state.clear()
    await msg.answer(
        f"✅ Removed media from schedule {job_id[:6]}...",
        reply_markup=main_menu()
    )

@router.callback_query(F.data.startswith("change_interval_"))
async def change_interval_handler(query: types.CallbackQuery, state: FSMContext):
    """Change job interval"""
    job_id = query.data.split("_", 2)[2]
    if job_id not in scheduled_jobs:
        await query.answer("Job not found")
        return
    
    await state.update_data(job_id=job_id)
    await state.set_state(BotStates.editing_job_interval)
    try:
        await query.message.edit_text(
            "⏱ Enter new interval in minutes:\n"
            f"Current: Every {scheduled_jobs[job_id].interval} minutes",
            reply_markup=cancel_button()
        )
    except Exception as e:
        logger.error(f"Error changing interval: {e}")
        await query.answer("Enter new interval")

@router.message(BotStates.editing_job_interval)
async def save_job_interval(msg: types.Message, state: FSMContext):
    """Save edited job interval"""
    data = await state.get_data()
    job_id = data.get("job_id")
    if not job_id or job_id not in scheduled_jobs:
        await msg.answer("Job not found", reply_markup=main_menu())
        await state.clear()
        return
    
    try:
        interval = int(msg.text.strip())
        if interval < 1:
            raise ValueError
    except:
        await msg.answer("Please enter a valid number (minutes):", reply_markup=cancel_button())
        return
    
    # Update job
    job = scheduled_jobs[job_id]
    job.interval = interval
    schedule_job(job_id, job.targets, job.message, job.media, job.interval)
    
    await state.clear()
    await msg.answer(
        f"✅ Updated interval for schedule {job_id[:6]}...\n"
        f"New interval: Every {interval} minutes",
        reply_markup=main_menu()
    )

@router.callback_query(F.data.startswith("delete_job_"))
async def delete_job_handler(query: types.CallbackQuery):
    """Confirm job deletion"""
    job_id = query.data.split("_", 2)[2]
    if job_id not in scheduled_jobs:
        await query.answer("Job not found")
        return
    
    job = scheduled_jobs[job_id]
    try:
        media_status = "with media" if job.media else "without media"
        await query.message.edit_text(
            f"Are you sure you want to delete this schedule?\n\n"
            f"ID: {job_id[:6]}...\n"
            f"Targets: {len(job.targets)} groups\n"
            f"Interval: Every {job.interval} minutes\n"
            f"Media: {media_status}\n"
            f"Total sent: {job.total_sent}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ Yes", callback_data=f"confirm_delete_{job_id}"),
                    InlineKeyboardButton(text="❌ No", callback_data=f"manage_job_{job_id}")
                ]
            ])
        )
    except Exception as e:
        logger.error(f"Error confirming job deletion: {e}")
        await query.answer("Confirm deletion")

@router.callback_query(F.data.startswith("confirm_delete_"))
async def confirm_delete_handler(query: types.CallbackQuery):
    """Delete job after confirmation"""
    job_id = query.data.split("_", 2)[2]
    if job_id not in scheduled_jobs:
        await query.answer("Job not found")
        return
    
    job = scheduled_jobs[job_id]
    targets = job.targets
    delete_job(job_id)
    
    try:
        await query.message.edit_text(
            f"✅ Deleted schedule {job_id[:6]}...\n"
            f"Targets: {len(targets)} groups\n"
            f"Total active schedules: {len(scheduled_jobs)}",
            reply_markup=main_menu()
        )
    except Exception as e:
        logger.error(f"Error completing job deletion: {e}")
        await query.answer(f"Deleted schedule {job_id[:6]}...")

# --- Spam Check Handlers ---
@router.callback_query(F.data == "check_spam")
async def check_spam_handler(query: types.CallbackQuery):
    """Initiate spam checking"""
    await query.answer("Checking for spam/banned accounts...")
    
    # Check all sessions for spam/bans
    checking_msg = await query.message.answer("🔄 Checking all sessions for spam/bans...")
    
    check_tasks = []
    for session_name in active_sessions:
        check_tasks.append(check_session_spam(session_name))
    
    await asyncio.gather(*check_tasks)
    
    banned_count = sum(1 for s in active_sessions.values() if s.is_banned)
    await checking_msg.edit_text(
        f"⚠️ Spam Check Complete\n"
        f"Total sessions: {len(active_sessions)}\n"
        f"Banned/limited: {banned_count}",
        reply_markup=spam_reports_keyboard()
    )

@router.callback_query(F.data.startswith("spam_report_"))
async def show_spam_report(query: types.CallbackQuery):
    """Show details of a spam report"""
    session_name = query.data.split("_", 2)[2]
    if session_name not in spam_reports:
        await query.answer("No report found for this session")
        return
    
    report = spam_reports[session_name]
    session = active_sessions.get(session_name)
    
    text = (
        f"🚫 Spam Report for {session_name}\n\n"
        f"Phone: {session.phone if session else '?'}\n"
        f"Last Error: {report.get('error', 'Unknown')}\n"
        f"Timestamp: {report.get('timestamp', 'Unknown')}\n\n"
        "This session may be banned or limited."
    )
    
    await query.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🗑 Remove Session", 
                    callback_data=f"remove_spam_{session_name}"
                ),
                InlineKeyboardButton(
                    text="🔄 Recheck", 
                    callback_data=f"recheck_spam_{session_name}"
                )
            ],
            [InlineKeyboardButton(text="⬅️ Back", callback_data="check_spam")]
        ])
    )

@router.callback_query(F.data.startswith("remove_spam_"))
async def remove_spam_session(query: types.CallbackQuery):
    """Remove a spam-reported session"""
    session_name = query.data.split("_", 2)[2]
    success, message = await remove_session_file(session_name)
    
    if success:
        await query.message.edit_text(
            f"✅ {message}\n"
            f"Spam reports updated.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Back", callback_data="check_spam")]
            ])
        )
    else:
        await query.message.edit_text(
            f"❌ {message}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Back", callback_data="check_spam")]
            ])
        )

@router.callback_query(F.data.startswith("recheck_spam_"))
async def recheck_spam_session(query: types.CallbackQuery):
    """Recheck a spam-reported session"""
    session_name = query.data.split("_", 2)[2]
    await query.answer(f"Rechecking session {session_name}...")
    
    is_banned = await check_session_spam(session_name)
    
    if is_banned:
        await query.message.edit_text(
            f"⚠️ Session {session_name} is still banned/limited\n"
            f"Error: {active_sessions[session_name].last_error}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="🗑 Remove Session", 
                        callback_data=f"remove_spam_{session_name}"
                    ),
                    InlineKeyboardButton(
                        text="🔄 Recheck", 
                        callback_data=f"recheck_spam_{session_name}"
                    )
                ],
                [InlineKeyboardButton(text="⬅️ Back", callback_data="check_spam")]
            ])
        )
    else:
        if session_name in spam_reports:
            del spam_reports[session_name]
            await save_spam_reports()
        
        await query.message.edit_text(
            f"✅ Session {session_name} is now working properly!",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Back", callback_data="check_spam")]
            ])
        )

@router.callback_query(F.data == "refresh_spam_reports")
async def refresh_spam_reports_handler(query: types.CallbackQuery):
    """Refresh spam reports list"""
    await check_spam_handler(query)

# --- Error Handler ---
@router.errors()
async def error_handler(event: types.ErrorEvent):
    """Handle all bot errors"""
    logger.error(f"Error occurred: {event.exception}", exc_info=True)
    if isinstance(event.update, types.CallbackQuery):
        try:
            await event.update.message.answer(
                "⚠️ An error occurred. Please try again.",
                reply_markup=main_menu()
            )
        except:
            await event.update.answer("⚠️ An error occurred")
    elif isinstance(event.update, types.Message):
        await event.update.answer(
            "⚠️ An error occurred. Please try again.",
            reply_markup=main_menu()
        )

# --- Startup and Shutdown ---
async def on_startup():
    """Initialize the bot with enhanced setup"""
    # Start scheduler now that we have a running event loop
    scheduler.start()
    
    # Load data
    await load_sessions()
    await load_schedules()
    await load_spam_reports()
    
    # Check for spam/banned sessions
    check_tasks = []
    for session_name in active_sessions:
        check_tasks.append(check_session_spam(session_name))
    await asyncio.gather(*check_tasks)
    
    valid_count = sum(1 for s in active_sessions.values() if s.valid and not s.is_banned)
    premium_count = sum(1 for s in active_sessions.values() if s.is_premium)
    schedule_count = len(scheduled_jobs)
    banned_count = sum(1 for s in active_sessions.values() if s.is_banned)
    
    logger.info(
        f"Bot started with {valid_count} valid sessions "
        f"({premium_count} premium), {banned_count} banned, "
        f"and {schedule_count} schedules"
    )
    
    # Notify owner
    await bot.send_message(
        OWNER_ID,
        f"🤖 Bot started successfully!\n"
        f"• Sessions: {valid_count}/{len(active_sessions)} active\n"
        f"• Premium: {premium_count} accounts\n"
        f"• Banned: {banned_count} accounts\n"
        f"• Schedules: {schedule_count} active\n"
        f"• Last target: {campaign_stats.get('target', 'None')}",
        reply_markup=main_menu()
    )

async def on_shutdown():
    """Shutdown the bot gracefully"""
    logger.info("Shutting down scheduler...")
    try:
        scheduler.shutdown(wait=False)
    except Exception as e:
        logger.error(f"Error shutting down scheduler: {e}")
    logger.info("Bot shutdown complete")

# --- Main ---
if __name__ == "__main__":
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    logger.info("Starting bot...")
    
    try:
        dp.run_polling(bot)
    except KeyboardInterrupt:
        logger.info("Bot stopped gracefully")
    except Exception as e:
        logger.error(f"Bot crashed: {e}")
        asyncio.run(bot.send_message(OWNER_ID, f"⚠️ Bot crashed: {e}"))