import os
import asyncio
import logging
import random
import time
import re
import json
from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
import pytz

from aiogram import Bot, Dispatcher, Router, types, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.markdown import hbold, hcode, hlink
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from telethon import TelegramClient, functions
from telethon.errors import (
    PeerFloodError, FloodWaitError, UserPrivacyRestrictedError,
    UserIsBlockedError, SessionPasswordNeededError, PhoneNumberInvalidError,
    SessionRevokedError, AuthKeyError
)

# --- Configuration ---
API_ID = 25781839
API_HASH = '20a3f2f168739259a180dcdd642e196c'
BOT_TOKEN = '7857537951:AAG71zV2gsW3Eg97x2c2cVgoXzFCDZ3iHpU'
OWNER_ID = 7584086775
SESSIONS_FOLDER = "sessions"
SCHEDULES_FILE = "schedules.json"
BATCH_SIZE = 5
MAX_RETRIES = 3
MIN_DELAY = 2  # seconds between messages
MAX_DELAY = 4  # seconds between messages
MAX_MESSAGE_LENGTH = 4096  # Telegram message limit

# --- Initialization ---
if not os.path.exists(SESSIONS_FOLDER):
    os.makedirs(SESSIONS_FOLDER)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
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

class ScheduledJob:
    def __init__(self, job_id: str, targets: List[str], message: str, interval: int, next_run: datetime):
        self.job_id = job_id
        self.targets = targets
        self.message = message
        self.interval = interval
        self.next_run = next_run

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
    editing_job_interval = State()

# --- Bot Setup ---
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)

# --- Global State ---
active_sessions: Dict[str, SessionInfo] = {}
scheduled_jobs: Dict[str, ScheduledJob] = {}
campaign_stats = {
    "total_sent": 0,
    "last_run": None,
    "target": "",
    "message": ""
}

# Initialize scheduler
scheduler = AsyncIOScheduler(timezone="UTC")
scheduler.start()

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

async def save_schedules():
    """Save scheduled jobs to file"""
    data = {
        job_id: {
            "targets": job.targets,
            "message": job.message,
            "interval": job.interval,
            "next_run": job.next_run.isoformat()
        }
        for job_id, job in scheduled_jobs.items()
    }
    with open(SCHEDULES_FILE, 'w') as f:
        json.dump(data, f)

async def load_schedules():
    """Load scheduled jobs from file"""
    if not os.path.exists(SCHEDULES_FILE):
        return
    
    try:
        with open(SCHEDULES_FILE, 'r') as f:
            data = json.load(f)
        
        for job_id, job_data in data.items():
            next_run = datetime.fromisoformat(job_data["next_run"])
            if next_run > get_current_time():
                schedule_job(
                    job_id,
                    job_data["targets"],
                    job_data["message"],
                    job_data["interval"],
                    next_run
                )
    except Exception as e:
        logger.error(f"Error loading schedules: {e}")

def schedule_job(job_id: str, targets: List[str], message: str, interval: int, next_run: datetime = None):
    """Create or update a scheduled job"""
    if not next_run:
        next_run = get_current_time() + timedelta(minutes=interval)
    
    # Remove existing job if it exists
    if job_id in scheduled_jobs:
        scheduler.remove_job(job_id)
    
    job = ScheduledJob(job_id, targets, message, interval, next_run)
    scheduled_jobs[job_id] = job
    
    scheduler.add_job(
        send_scheduled_messages,
        trigger=IntervalTrigger(minutes=interval),
        args=[targets, message],
        id=job_id,
        next_run_time=next_run
    )
    
    asyncio.create_task(save_schedules())

def delete_job(job_id: str):
    """Remove a scheduled job"""
    if job_id in scheduled_jobs:
        scheduler.remove_job(job_id)
        del scheduled_jobs[job_id]
        asyncio.create_task(save_schedules())
        return True
    return False

async def send_scheduled_messages(targets: List[str], message: str):
    """Send messages to multiple groups on schedule"""
    valid_sessions = [s.name for s in active_sessions.values() if s.valid]
    if not valid_sessions:
        logger.warning("No valid sessions for scheduled messages")
        return
    
    results = []
    for target in targets:
        result = await send_bulk_messages(target, message)
        results.append(result)
        logger.info(f"Scheduled send to {target}: {result['sent']}/{result['total']} successful")
    
    # Update next run time
    job_id = scheduler.current_job.id
    if job_id in scheduled_jobs:
        scheduled_jobs[job_id].next_run = get_current_time() + timedelta(
            minutes=scheduled_jobs[job_id].interval
        )
        await save_schedules()
    
    # Notify owner if any failures
    if any(r['failed'] > 0 for r in results):
        await bot.send_message(
            OWNER_ID,
            f"⚠️ Scheduled job had failures\n"
            f"Job ID: {job_id[:8]}...\n"
            f"Total targets: {len(targets)}\n"
            f"Success: {sum(r['sent'] for r in results)}\n"
            f"Failures: {sum(r['failed'] for r in results)}"
        )

async def send_bulk_messages(target: str, message: str) -> Dict[str, int]:
    """Send messages using all valid sessions with batch processing"""
    valid_sessions = [s.name for s in active_sessions.values() if s.valid]
    total_sessions = len(valid_sessions)
    
    if not valid_sessions:
        return {"sent": 0, "total": 0, "failed": 0}
    
    sent_count = 0
    failed_count = 0
    
    # Process in batches to avoid overwhelming
    for i in range(0, total_sessions, BATCH_SIZE):
        batch = valid_sessions[i:i+BATCH_SIZE]
        results = await asyncio.gather(
            *(send_from_session(s, target, message) for s in batch))
        sent_count += sum(results)
        failed_count += len(results) - sum(results)
    
    # Update campaign stats
    campaign_stats["total_sent"] += sent_count
    campaign_stats["last_run"] = get_current_time()
    campaign_stats["target"] = target
    campaign_stats["message"] = message
    
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
            InlineKeyboardButton(text="🚀 Send Messages", callback_data="send_messages"),
            InlineKeyboardButton(text="📊 Statistics", callback_data="show_stats")
        ],
        [
            InlineKeyboardButton(text="➕ Add Session", callback_data="add_session"),
            InlineKeyboardButton(text="🗑 Remove Session", callback_data="remove_session")
        ],
        [
            InlineKeyboardButton(text="⏰ Manage Schedules", callback_data="manage_schedules"),
            InlineKeyboardButton(text="🔄 Refresh", callback_data="refresh_sessions")
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
        time_left = format_timedelta(job.next_run - get_current_time())
        buttons.append([InlineKeyboardButton(
            text=f"⏰ {job.job_id[:6]}... | {len(job.targets)} groups | Every {job.interval}m | Next: {time_left}",
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
        [InlineKeyboardButton(text="⬅️ Back", callback_data=f"manage_job_{job_id}")]
    ])

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
            logger.warning(f"Session {session_name} not authorized")
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
        
        # Additional validation
        try:
            await client(functions.account.GetAccountTTLRequest())
            return True
        except Exception as e:
            logger.warning(f"Account {session_name} might be limited: {e}")
            active_sessions[session_name].valid = False
            return False
            
    except (SessionRevokedError, AuthKeyError):
        logger.warning(f"Session {session_name} revoked or auth error")
        return False
    except Exception as e:
        logger.error(f"Error validating session {session_name}: {str(e)}")
        return False
    finally:
        try:
            await client.disconnect()
        except:
            pass

async def load_sessions() -> None:
    """Load and validate all sessions from the sessions folder"""
    session_files = [f for f in os.listdir(SESSIONS_FOLDER) if f.endswith('.session')]
    
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
        return True, f"✅ Session {session_name} removed successfully"
    except Exception as e:
        logger.error(f"Error removing session {session_name}: {str(e)}")
        return False, f"❌ Failed to remove session: {str(e)}"

# --- Enhanced Message Sending ---
async def send_from_session(session_name: str, target: str, message: str, retry: int = 0) -> bool:
    """Improved message sending with better error handling"""
    if session_name not in active_sessions or not active_sessions[session_name].valid:
        logger.warning(f"Session {session_name} not valid")
        return False
    
    session_info = active_sessions[session_name]
    session_path = os.path.join(SESSIONS_FOLDER, f"{session_name}.session")
    client = TelegramClient(session_path, API_ID, API_HASH)
    
    try:
        await client.connect()
        
        if not await client.is_user_authorized():
            session_info.valid = False
            logger.warning(f"Session {session_name} not authorized")
            return False
        
        try:
            # Improved entity resolution
            if target.startswith('+') and is_valid_phone(target):
                user = await client.get_entity(target)
            elif target.startswith('@'):
                user = await client.get_entity(target)
            else:
                user = await client.get_entity(f'@{target.lstrip("@")}')
        except ValueError as e:
            logger.warning(f"Invalid target {target}: {e}")
            return False
        except Exception as e:
            logger.warning(f"Error resolving {target}: {e}")
            return False
        
        # Send message with markdown support
        try:
            await client.send_message(
                user.id, 
                message,
                parse_mode='md' if any(c in message for c in '*_`[') else None
            )
            
            # Update session stats
            session_info.sent_count += 1
            session_info.last_used = get_current_time()
            return True
            
        except Exception as e:
            logger.warning(f"Send error from {session_name} to {target}: {e}")
            return False
    
    except Exception as e:
        logger.error(f"Connection error in {session_name}: {e}")
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
        return await msg.answer("🚫 Access denied.")
    
    await msg.answer(
        f"👋 Welcome {hbold(msg.from_user.first_name)}!\n"
        "This is an advanced Telegram bulk message sender bot.\n"
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
        "2. Set target (username/phone) and message\n"
        "3. Choose to send now or schedule\n\n"
        "⏰ Scheduling:\n"
        "- Set interval in minutes (e.g., 15)\n"
        "- Manage active schedules\n"
        "- Edit/delete schedules anytime\n\n"
        "🛠 Commands:\n"
        "/start - Restart bot\n"
        "/stats - Show statistics\n"
        "/help - This message"
    )
    await msg.answer(help_text, reply_markup=main_menu())

# --- Callback Handlers ---
@router.callback_query(F.data == "show_full_menu")
async def show_full_menu_handler(query: types.CallbackQuery):
    """Show the full menu after clicking BULK MESSAGE"""
    try:
        await query.message.edit_text(
            "📋 Main Menu - Select an option:",
            reply_markup=main_menu()
        )
    except:
        await query.answer("Main Menu")

@router.callback_query(F.data == "main_menu")
async def return_to_menu(query: types.CallbackQuery):
    """Return to main menu"""
    try:
        await query.message.edit_text(
            "Main Menu:",
            reply_markup=main_menu()
        )
    except:
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
    except:
        await query.answer("Action cancelled")

@router.callback_query(F.data == "set_target")
async def set_target_handler(query: types.CallbackQuery, state: FSMContext):
    """Initiate target setting"""
    await state.set_state(BotStates.setting_target)
    try:
        await query.message.edit_text(
            "📌 Send the target username or phone number (without @):\n"
            "Examples:\n"
            "- username: username\n"
            "- phone: +1234567890\n"
            "- multiple: group1,group2 (for schedules)",
            reply_markup=cancel_button()
        )
    except:
        await query.answer("Send the target username")

@router.message(BotStates.setting_target)
async def save_target(msg: types.Message, state: FSMContext):
    """Save target information"""
    target = msg.text.strip()
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
            "*bold*, _italic_, `code`, [link](url)",
            reply_markup=cancel_button()
        )
    except:
        await query.answer("Send the message text")

@router.message(BotStates.setting_message)
async def save_message(msg: types.Message, state: FSMContext):
    """Save message content"""
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

@router.callback_query(F.data == "send_messages")
async def send_messages_handler(query: types.CallbackQuery):
    """Handle message sending options"""
    if not campaign_stats.get("target") or not campaign_stats.get("message"):
        try:
            await query.message.edit_text(
                "⚠️ Please set both target and message first.",
                reply_markup=main_menu()
            )
        except:
            await query.answer("Please set both target and message first.")
        return
    
    try:
        await query.message.edit_text(
            "Choose how you want to send:\n"
            f"Target: {hcode(campaign_stats['target'])}\n"
            f"Message: {hcode(campaign_stats['message'][:50])}...",
            reply_markup=send_options()
        )
    except:
        await query.answer("Choose sending method")

@router.callback_query(F.data == "send_now")
async def send_now_handler(query: types.CallbackQuery):
    """Send messages immediately"""
    try:
        await query.message.edit_text("🚀 Sending messages now...")
    except:
        await query.answer("Sending messages...")
    
    start_time = time.time()
    result = await send_bulk_messages(campaign_stats["target"], campaign_stats["message"])
    elapsed = time.time() - start_time
    
    status_message = (
        f"📊 Sending completed in {elapsed:.2f} seconds\n"
        f"✅ Successful: {result['sent']}\n"
        f"❌ Failed: {result['failed']}\n"
        f"📋 Total sessions used: {result['total']}"
    )
    
    try:
        await query.message.edit_text(
            status_message,
            reply_markup=main_menu()
        )
    except:
        await query.answer(status_message)

@router.callback_query(F.data == "schedule_current")
async def schedule_current_handler(query: types.CallbackQuery, state: FSMContext):
    """Schedule current message"""
    await state.set_state(BotStates.setting_interval)
    try:
        await query.message.edit_text(
            "⏳ Enter interval in minutes (e.g., 15 for every 15 minutes):",
            reply_markup=cancel_button()
        )
    except:
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
    schedule_job(job_id, targets, campaign_stats["message"], interval)
    
    await state.clear()
    await msg.answer(
        f"✅ Scheduled messages every {interval} minutes\n"
        f"Targets: {', '.join(targets)}\n"
        f"Job ID: {job_id[:8]}...",
        reply_markup=main_menu()
    )

@router.callback_query(F.data == "show_stats")
async def show_stats_handler(query: types.CallbackQuery):
    """Show statistics"""
    await show_statistics(query.message)

async def show_statistics(message: types.Message):
    """Display detailed statistics"""
    valid_sessions = sum(1 for s in active_sessions.values() if s.valid)
    total_sessions = len(active_sessions)
    premium_sessions = sum(1 for s in active_sessions.values() if s.is_premium)
    
    stats_message = (
        f"📊 Bot Statistics\n\n"
        f"• Sessions: {valid_sessions}/{total_sessions} active ({premium_sessions} premium)\n"
        f"• Total messages sent: {campaign_stats.get('total_sent', 0)}\n"
    )
    
    if campaign_stats.get("last_run"):
        stats_message += f"• Last run: {format_time(campaign_stats['last_run'])}\n"
    
    if scheduled_jobs:
        stats_message += f"\n⏰ Active Schedules: {len(scheduled_jobs)}\n"
        for job in list(scheduled_jobs.values())[:3]:
            time_left = format_timedelta(job.next_run - get_current_time())
            stats_message += (
                f"  • {job.job_id[:6]}... | {len(job.targets)} groups | "
                f"Every {job.interval}m | Next: {time_left}\n"
            )
        if len(scheduled_jobs) > 3:
            stats_message += f"  • And {len(scheduled_jobs)-3} more...\n"
    
    # Add top performing sessions
    top_sessions = sorted(
        [s for s in active_sessions.values() if s.valid],
        key=lambda x: x.sent_count,
        reverse=True
    )[:3]
    
    if top_sessions:
        stats_message += "\n🏆 Top Sessions:\n"
        for i, session in enumerate(top_sessions, 1):
            premium = "🌟" if session.is_premium else ""
            stats_message += (
                f"{i}. {premium}{session.name[:10]}... - "
                f"{session.sent_count} sends\n"
            )
    
    try:
        await message.answer(
            stats_message,
            reply_markup=main_menu()
        )
    except:
        for chunk in split_long_message(stats_message):
            await message.answer(chunk)

@router.callback_query(F.data == "add_session")
async def add_session_handler(query: types.CallbackQuery, state: FSMContext):
    """Initiate session upload"""
    await state.set_state(BotStates.adding_session)
    try:
        await query.message.edit_text(
            "📲 Please upload a .session file:\n\n"
            "1. Export from Telegram Desktop (Settings > Advanced > Export Telegram data)\n"
            "2. Upload the .session file here\n\n"
            "Note: The account must be already logged in",
            reply_markup=cancel_button()
        )
    except:
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
            await status_msg.edit_text(
                f"✅ Session {hcode(session_name)} added successfully\n"
                f"Phone: {active_sessions[session_name].phone or '?'}\n"
                f"Premium: {'Yes' if active_sessions[session_name].is_premium else 'No'}\n"
                f"Total valid sessions now: {sum(1 for s in active_sessions.values() if s.valid)}",
                reply_markup=main_menu()
            )
        else:
            await status_msg.edit_text(
                f"❌ Session {hcode(session_name)} is invalid or unauthorized",
                reply_markup=main_menu()
            )
            try:
                os.remove(dest_path)
            except:
                pass
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
        except:
            pass
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
        except:
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
    except:
        await query.answer("Select a session to remove")

@router.callback_query(F.data.startswith("remove_"))
async def confirm_remove_session(query: types.CallbackQuery):
    """Confirm session removal"""
    session_name = query.data.split("_", 1)[1]
    try:
        await query.message.edit_text(
            f"Are you sure you want to remove session {hcode(session_name)}?",
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
    except:
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
    except:
        await query.answer(message)

@router.callback_query(F.data == "refresh_sessions")
async def refresh_sessions_handler(query: types.CallbackQuery):
    """Refresh session status"""
    try:
        await query.message.edit_text("🔄 Refreshing session status...")
    except:
        await query.answer("Refreshing sessions...")
    
    await load_sessions()
    valid_count = sum(1 for s in active_sessions.values() if s.valid)
    
    try:
        await query.message.edit_text(
            f"✅ Sessions refreshed\n"
            f"Valid sessions: {valid_count}/{len(active_sessions)}\n"
            f"Premium sessions: {sum(1 for s in active_sessions.values() if s.is_premium)}",
            reply_markup=main_menu()
        )
    except:
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
        except:
            await query.answer("No active schedules")
        return
    
    try:
        await query.message.edit_text(
            "⏰ Active Schedules:",
            reply_markup=schedule_keyboard(list(scheduled_jobs.values()))
        )
    except:
        await query.answer("Manage schedules")

@router.callback_query(F.data.startswith("manage_job_"))
async def manage_job_handler(query: types.CallbackQuery):
    """Manage individual job"""
    job_id = query.data.split("_", 2)[2]
    if job_id not in scheduled_jobs:
        await query.answer("Job not found")
        return
    
    job = scheduled_jobs[job_id]
    time_left = format_timedelta(job.next_run - get_current_time())
    
    try:
        await query.message.edit_text(
            f"⏰ Schedule Details:\n\n"
            f"ID: {job.job_id}\n"
            f"Targets: {len(job.targets)} groups\n"
            f"Interval: Every {job.interval} minutes\n"
            f"Next run: In {time_left}\n\n"
            f"Message preview:\n{hcode(job.message[:200])}",
            reply_markup=job_management_keyboard(job_id)
        )
    except:
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
    
    result = await send_scheduled_messages(job.targets, job.message)
    await query.message.answer(
        f"✅ Ran schedule {job_id[:6]}...\n"
        f"Sent to {len(job.targets)} groups\n"
        f"Results: {sum(r['sent'] for r in result)} successful, {sum(r['failed'] for r in result)} failed",
        reply_markup=main_menu()
    )

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
    except:
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
    except:
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
    schedule_job(job_id, job.targets, job.message, job.interval)
    
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
    except:
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
        await msg.answer("Invalid message, please try again", reply_mup=cancel_button())
        return
    
    # Update job
    job = scheduled_jobs[job_id]
    job.message = message
    schedule_job(job_id, job.targets, job.message, job.interval)
    
    await state.clear()
    await msg.answer(
        f"✅ Updated message for schedule {job_id[:6]}...\n"
        f"New message preview: {message[:100]}...",
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
    except:
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
    schedule_job(job_id, job.targets, job.message, job.interval)
    
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
    
    try:
        await query.message.edit_text(
            f"Are you sure you want to delete schedule {job_id[:6]}...?\n"
            f"Targets: {len(scheduled_jobs[job_id].targets)} groups\n"
            f"Interval: Every {scheduled_jobs[job_id].interval} minutes",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ Yes", callback_data=f"confirm_delete_{job_id}"),
                    InlineKeyboardButton(text="❌ No", callback_data=f"manage_job_{job_id}")
                ]
            ])
        )
    except:
        await query.answer("Confirm deletion")

@router.callback_query(F.data.startswith("confirm_delete_"))
async def confirm_delete_handler(query: types.CallbackQuery):
    """Delete job after confirmation"""
    job_id = query.data.split("_", 2)[2]
    if job_id not in scheduled_jobs:
        await query.answer("Job not found")
        return
    
    targets = scheduled_jobs[job_id].targets
    delete_job(job_id)
    
    try:
        await query.message.edit_text(
            f"✅ Deleted schedule {job_id[:6]}...\n"
            f"Targets: {len(targets)} groups\n"
            f"Total active schedules: {len(scheduled_jobs)}",
            reply_markup=main_menu()
        )
    except:
        await query.answer(f"Deleted schedule {job_id[:6]}...")

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

# --- Startup ---
async def on_startup():
    """Initialize the bot with enhanced setup"""
    await load_sessions()
    await load_schedules()
    
    valid_count = sum(1 for s in active_sessions.values() if s.valid)
    premium_count = sum(1 for s in active_sessions.values() if s.is_premium)
    schedule_count = len(scheduled_jobs)
    
    logger.info(
        f"Bot started with {valid_count} valid sessions "
        f"({premium_count} premium) and {schedule_count} schedules"
    )
    
    # Notify owner
    await bot.send_message(
        OWNER_ID,
        f"🤖 Bot started successfully!\n"
        f"• Sessions: {valid_count}/{len(active_sessions)} active\n"
        f"• Schedules: {schedule_count} active\n"
        f"• Last target: {campaign_stats.get('target', 'None')}",
        reply_markup=main_menu()
    )

# --- Main ---
if __name__ == "__main__":
    dp.startup.register(on_startup)
    logger.info("Starting bot...")
    
    try:
        dp.run_polling(bot)
    except KeyboardInterrupt:
        logger.info("Bot stopped gracefully")
    except Exception as e:
        logger.error(f"Bot crashed: {e}")
        asyncio.run(bot.send_message(OWNER_ID, f"⚠️ Bot crashed: {e}"))