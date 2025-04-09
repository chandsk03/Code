import os
import asyncio
import logging
import random
import time
from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass
from datetime import datetime
import pytz

from aiogram import Bot, Dispatcher, Router, types, F
from aiogram.enums import ParseMode
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.markdown import hbold, hcode
from telethon import TelegramClient, functions
from telethon.errors import (
    PeerFloodError, FloodWaitError, UserPrivacyRestrictedError,
    UserIsBlockedError, SessionPasswordNeededError, PhoneNumberInvalidError,
    SessionRevokedError, AuthKeyError
)

# --- Config ---
API_ID = 25781839  # Consider moving to environment variables
API_HASH = '20a3f2f168739259a180dcdd642e196c'
BOT_TOKEN = '7857537951:AAG71zV2gsW3Eg97x2c2cVgoXzFCDZ3iHpU'
OWNER_ID = 7584086775
SESSIONS_FOLDER = "sessions"
BATCH_SIZE = 5  # Number of parallel sessions to process
MAX_RETRIES = 3
MIN_DELAY = 2  # Minimum delay between messages in seconds
MAX_DELAY = 4  # Maximum delay between messages in seconds
MAX_MESSAGE_LENGTH = 4096  # Telegram message limit

# --- Logging ---
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

class BotStates(StatesGroup):
    setting_target = State()
    setting_message = State()
    adding_session = State()

# --- Bot Setup ---
bot = Bot(token=BOT_TOKEN, parse_mode=ParseMode.HTML)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)

# --- Global State ---
active_sessions: Dict[str, SessionInfo] = {}
campaign_stats = {
    "total_sent": 0,
    "last_run": None,
    "target": "",
    "message": ""
}

# --- Helper Functions ---
def get_current_time() -> datetime:
    return datetime.now(pytz.utc)

def format_time(dt: datetime) -> str:
    return dt.astimezone(pytz.utc).strftime('%Y-%m-%d %H:%M:%S UTC')

def split_long_message(text: str) -> List[str]:
    """Split long messages into chunks that fit Telegram's limit"""
    return [text[i:i+MAX_MESSAGE_LENGTH] for i in range(0, len(text), MAX_MESSAGE_LENGTH)]

# --- Keyboard Menus ---
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
            InlineKeyboardButton(text="🔄 Refresh Sessions", callback_data="refresh_sessions")
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def cancel_button() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Cancel", callback_data="cancel_action")]
    ])

def session_selection_keyboard(sessions: List[str], action: str) -> InlineKeyboardMarkup:
    buttons = []
    for session in sessions:
        buttons.append([InlineKeyboardButton(
            text=f"📱 {session}",
            callback_data=f"{action}_{session}"
        )])
    buttons.append([InlineKeyboardButton(text="⬅️ Back", callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# --- Session Management ---
async def load_sessions() -> None:
    """Load and validate all sessions from the sessions folder"""
    if not os.path.exists(SESSIONS_FOLDER):
        os.makedirs(SESSIONS_FOLDER)
    
    session_files = [f for f in os.listdir(SESSIONS_FOLDER) if f.endswith('.session')]
    
    for session_file in session_files:
        session_name = os.path.splitext(session_file)[0]
        if session_name not in active_sessions:
            active_sessions[session_name] = SessionInfo(name=session_name)
    
    # Validate all sessions in parallel
    validation_tasks = []
    for session_name in active_sessions:
        validation_tasks.append(validate_session(session_name))
    
    await asyncio.gather(*validation_tasks)

async def validate_session(session_name: str) -> None:
    """Check if a session is valid and get its phone number"""
    session_path = os.path.join(SESSIONS_FOLDER, f"{session_name}.session")
    client = TelegramClient(session_path, API_ID, API_HASH)
    
    try:
        await client.connect()
        if not await client.is_user_authorized():
            active_sessions[session_name].valid = False
            return
        
        # Get session info
        me = await client.get_me()
        active_sessions[session_name].phone = me.phone or ""
        active_sessions[session_name].valid = True
        
        # Check if the account is limited
        try:
            await client(functions.account.GetAccountTTLRequest())
        except Exception as e:
            logger.warning(f"Account {session_name} might be limited: {e}")
            active_sessions[session_name].valid = False
        
    except (SessionRevokedError, AuthKeyError):
        active_sessions[session_name].valid = False
    except Exception as e:
        logger.error(f"Error validating session {session_name}: {e}")
        active_sessions[session_name].valid = False
    finally:
        await client.disconnect()

async def add_session_file(session_file: types.Document) -> Tuple[bool, str]:
    """Add a new session file to the sessions folder"""
    if not session_file.file_name.endswith('.session'):
        return False, "File must have .session extension"
    
    session_name = os.path.splitext(session_file.file_name)[0]
    dest_path = os.path.join(SESSIONS_FOLDER, session_file.file_name)
    
    if os.path.exists(dest_path):
        return False, "Session with this name already exists"
    
    try:
        await bot.download(session_file, destination=dest_path)
        await validate_session(session_name)
        return True, f"Session {session_name} added successfully"
    except Exception as e:
        logger.error(f"Error adding session: {e}")
        return False, f"Failed to add session: {e}"

async def remove_session(session_name: str) -> Tuple[bool, str]:
    """Remove a session file"""
    session_path = os.path.join(SESSIONS_FOLDER, f"{session_name}.session")
    
    if not os.path.exists(session_path):
        return False, "Session file not found"
    
    try:
        os.remove(session_path)
        if session_name in active_sessions:
            del active_sessions[session_name]
        return True, f"Session {session_name} removed successfully"
    except Exception as e:
        logger.error(f"Error removing session {session_name}: {e}")
        return False, f"Failed to remove session: {e}"

# --- Message Sending ---
async def send_from_session(session_name: str, target: str, message: str, retry: int = 0) -> bool:
    """Send a message from a specific session"""
    if session_name not in active_sessions or not active_sessions[session_name].valid:
        return False
    
    session_info = active_sessions[session_name]
    session_path = os.path.join(SESSIONS_FOLDER, f"{session_name}.session")
    client = TelegramClient(session_path, API_ID, API_HASH)
    
    try:
        await client.connect()
        if not await client.is_user_authorized():
            session_info.valid = False
            return False
        
        user = await client.get_entity(target)
        await client.send_message(user.id, message)
        
        # Update session stats
        session_info.sent_count += 1
        session_info.last_used = get_current_time()
        return True
    
    except (UserPrivacyRestrictedError, UserIsBlockedError):
        return False
    except PeerFloodError:
        if retry < MAX_RETRIES:
            await asyncio.sleep(random.randint(3, 6))
            return await send_from_session(session_name, target, message, retry + 1)
        session_info.errors += 1
        return False
    except FloodWaitError as e:
        wait_time = e.seconds
        logger.warning(f"Flood wait for {session_name}: {wait_time} seconds")
        await asyncio.sleep(wait_time)
        return await send_from_session(session_name, target, message, retry + 1)
    except (SessionPasswordNeededError, PhoneNumberInvalidError, SessionRevokedError):
        session_info.valid = False
        return False
    except Exception as e:
        logger.warning(f"Error from {session_name}: {e}")
        session_info.errors += 1
        return False
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass
        await asyncio.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

async def send_bulk_messages(target: str, message: str) -> Dict[str, int]:
    """Send messages using all valid sessions"""
    valid_sessions = [s.name for s in active_sessions.values() if s.valid]
    total_sessions = len(valid_sessions)
    
    if not valid_sessions:
        return {"sent": 0, "total": 0, "failed": 0}
    
    # Process in batches to avoid overwhelming
    sent_count = 0
    failed_count = 0
    
    for i in range(0, total_sessions, BATCH_SIZE):
        batch = valid_sessions[i:i+BATCH_SIZE]
        results = await asyncio.gather(
            *(send_from_session(s, target, message) for s in batch)
        )
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

# --- Command Handlers ---
@router.message(Command("start"))
async def start_handler(msg: types.Message):
    if msg.from_user.id != OWNER_ID:
        return await msg.answer("🚫 Access denied.")
    
    await msg.answer(
        f"👋 Welcome {hbold(msg.from_user.first_name)}!\n"
        "This is an advanced Telegram message sender bot.\n"
        "Use the buttons below to control the bot:",
        reply_markup=main_menu()
    )

@router.message(Command("stats"))
async def stats_command(msg: types.Message):
    if msg.from_user.id != OWNER_ID:
        return await msg.answer("🚫 Access denied.")
    
    await show_statistics(msg)

# --- Callback Handlers ---
@router.callback_query(F.data == "main_menu")
async def return_to_menu(query: types.CallbackQuery):
    await query.message.edit_text(
        "Main Menu:",
        reply_markup=main_menu()
    )

@router.callback_query(F.data == "cancel_action")
async def cancel_action(query: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await query.message.edit_text(
        "Action cancelled.",
        reply_markup=main_menu()
    )

@router.callback_query(F.data == "set_target")
async def set_target_handler(query: types.CallbackQuery, state: FSMContext):
    await state.set_state(BotStates.setting_target)
    await query.message.edit_text(
        "📌 Send the target username or phone number (without @):",
        reply_markup=cancel_button()
    )

@router.message(BotStates.setting_target)
async def save_target(msg: types.Message, state: FSMContext):
    target = msg.text.strip()
    campaign_stats["target"] = target
    await state.clear()
    await msg.answer(
        f"✅ Target set to: {hcode(target)}",
        reply_markup=main_menu()
    )

@router.callback_query(F.data == "set_message")
async def set_message_handler(query: types.CallbackQuery, state: FSMContext):
    await state.set_state(BotStates.setting_message)
    await query.message.edit_text(
        "💬 Send the message text you want to send:",
        reply_markup=cancel_button()
    )

@router.message(BotStates.setting_message)
async def save_message(msg: types.Message, state: FSMContext):
    message = msg.text.strip()
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
    if not campaign_stats.get("target") or not campaign_stats.get("message"):
        await query.message.edit_text(
            "⚠️ Please set both target and message first.",
            reply_markup=main_menu()
        )
        return
    
    await query.message.edit_text(
        "🚀 Starting message sending process...\n"
        f"Target: {hcode(campaign_stats['target'])}\n"
        f"Message: {hcode(campaign_stats['message'][:50])}..."
    )
    
    start_time = time.time()
    result = await send_bulk_messages(campaign_stats["target"], campaign_stats["message"])
    elapsed = time.time() - start_time
    
    status_message = (
        f"📊 Sending completed in {elapsed:.2f} seconds\n"
        f"✅ Successful: {result['sent']}\n"
        f"❌ Failed: {result['failed']}\n"
        f"📋 Total sessions: {result['total']}"
    )
    
    await query.message.edit_text(
        status_message,
        reply_markup=main_menu()
    )

@router.callback_query(F.data == "show_stats")
async def show_stats_handler(query: types.CallbackQuery):
    await show_statistics(query.message)

async def show_statistics(message: types.Message):
    valid_sessions = sum(1 for s in active_sessions.values() if s.valid)
    total_sessions = len(active_sessions)
    
    stats_message = (
        f"📊 Bot Statistics\n\n"
        f"• Active sessions: {valid_sessions}/{total_sessions}\n"
        f"• Total messages sent: {campaign_stats.get('total_sent', 0)}\n"
    )
    
    if campaign_stats.get("last_run"):
        stats_message += (
            f"• Last run: {format_time(campaign_stats['last_run'])}\n"
            f"• Last target: {hcode(campaign_stats.get('target', 'None'))}\n"
        )
    
    # Add top performing sessions
    top_sessions = sorted(
        [s for s in active_sessions.values() if s.valid],
        key=lambda x: x.sent_count,
        reverse=True
    )[:5]
    
    if top_sessions:
        stats_message += "\n🏆 Top Performing Sessions:\n"
        for i, session in enumerate(top_sessions, 1):
            stats_message += (
                f"{i}. {session.name} ({session.phone or '?'}) - "
                f"{session.sent_count} messages\n"
            )
    
    await message.answer(
        stats_message,
        reply_markup=main_menu()
    )

@router.callback_query(F.data == "add_session")
async def add_session_handler(query: types.CallbackQuery, state: FSMContext):
    await state.set_state(BotStates.adding_session)
    await query.message.edit_text(
        "📲 Please upload a .session file:",
        reply_markup=cancel_button()
    )

@router.message(BotStates.adding_session, F.document)
async def handle_session_upload(msg: types.Message, state: FSMContext):
    success, message = await add_session_file(msg.document)
    if success:
        await msg.answer(
            f"✅ {message}\n"
            f"Total valid sessions now: {sum(1 for s in active_sessions.values() if s.valid)}",
            reply_markup=main_menu()
        )
    else:
        await msg.answer(
            f"❌ {message}\n"
            "Please try again or cancel the operation.",
            reply_markup=cancel_button()
        )
    await state.clear()

@router.callback_query(F.data == "remove_session")
async def remove_session_handler(query: types.CallbackQuery):
    if not active_sessions:
        await query.message.edit_text(
            "No sessions available to remove.",
            reply_markup=main_menu()
        )
        return
    
    await query.message.edit_text(
        "Select a session to remove:",
        reply_markup=session_selection_keyboard(
            [s.name for s in active_sessions.values()],
            "remove"
        )
    )

@router.callback_query(F.data.startswith("remove_"))
async def confirm_remove_session(query: types.CallbackQuery):
    session_name = query.data.split("_", 1)[1]
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

@router.callback_query(F.data.startswith("confirm_remove_"))
async def execute_remove_session(query: types.CallbackQuery):
    session_name = query.data.split("_", 2)[2]
    success, message = await remove_session(session_name)
    await query.message.edit_text(
        message,
        reply_markup=main_menu()
    )

@router.callback_query(F.data == "refresh_sessions")
async def refresh_sessions_handler(query: types.CallbackQuery):
    await query.message.edit_text("🔄 Refreshing session status...")
    await load_sessions()
    valid_count = sum(1 for s in active_sessions.values() if s.valid)
    await query.message.edit_text(
        f"✅ Sessions refreshed\nValid sessions: {valid_count}/{len(active_sessions)}",
        reply_markup=main_menu()
    )

# --- Error Handler ---
@router.errors()
async def error_handler(event: types.ErrorEvent):
    logger.error(f"Error occurred: {event.exception}", exc_info=True)
    if isinstance(event.update, types.CallbackQuery):
        await event.update.message.answer(
            "⚠️ An error occurred. Please try again.",
            reply_markup=main_menu()
        )
    elif isinstance(event.update, types.Message):
        await event.update.answer(
            "⚠️ An error occurred. Please try again.",
            reply_markup=main_menu()
        )

# --- Startup ---
async def on_startup():
    await load_sessions()
    logger.info(f"Bot started with {sum(1 for s in active_sessions.values() if s.valid)} valid sessions")

# --- Main ---
if __name__ == "__main__":
    dp.startup.register(on_startup)
    logger.info("Starting bot...")
    dp.run_polling(bot)