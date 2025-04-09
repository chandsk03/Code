import os
import asyncio
import logging
import random
import time
from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass
from datetime import datetime
import pytz
import re

from aiogram import Bot, Dispatcher, Router, types, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
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

# --- Configuration ---
API_ID = 25781839
API_HASH = '20a3f2f168739259a180dcdd642e196c'
BOT_TOKEN = '7857537951:AAG71zV2gsW3Eg97x2c2cVgoXzFCDZ3iHpU'
OWNER_ID = 7584086775
SESSIONS_FOLDER = "sessions"
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

class BotStates(StatesGroup):
    setting_target = State()
    setting_message = State()
    adding_session = State()
    removing_session = State()

# --- Bot Setup ---
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
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
    return [text[i:i+MAX_MESSAGE_LENGTH] for i in range(0, len(text), MAX_MESSAGE_LENGTH)]

def is_valid_phone(phone: str) -> bool:
    """Check if phone number is in valid format"""
    return bool(re.match(r'^\+?[1-9]\d{7,14}$', phone))

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

def session_selection_keyboard(sessions: List[SessionInfo], action: str) -> InlineKeyboardMarkup:
    buttons = []
    for session in sessions:
        status = "✅" if session.valid else "❌"
        buttons.append([InlineKeyboardButton(
            text=f"{status} {session.name} ({session.phone or '?'})",
            callback_data=f"{action}_{session.name}"
        )])
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

# --- Message Sending ---
async def send_from_session(session_name: str, target: str, message: str, retry: int = 0) -> bool:
    """Send a message from a specific session with retry logic"""
    if session_name not in active_sessions or not active_sessions[session_name].valid:
        return False
    
    session_info = active_sessions[session_name]
    session_path = os.path.join(SESSIONS_FOLDER, f"{session_name}.session")
    client = TelegramClient(session_path, API_ID, API_HASH)
    
    try:
        await client.connect()
        
        # Re-check authorization in case session became invalid
        if not await client.is_user_authorized():
            session_info.valid = False
            return False
        
        # Get target entity
        try:
            if target.startswith('+') and is_valid_phone(target):
                user = await client.get_entity(target)
            else:
                user = await client.get_entity(target if target.startswith('@') else f'@{target}')
        except Exception as e:
            logger.warning(f"Error getting entity {target} from {session_name}: {str(e)}")
            return False
        
        # Send message
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
        logger.error(f"Error from {session_name}: {str(e)}")
        session_info.errors += 1
        return False
    finally:
        try:
            await client.disconnect()
        except:
            pass
        await asyncio.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

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
            *(send_from_session(s, target, message) for s in batch)
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
    """Handle /start command"""
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
    """Handle /stats command"""
    if msg.from_user.id != OWNER_ID:
        return await msg.answer("🚫 Access denied.")
    await show_statistics(msg)

# --- Callback Handlers ---
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
            "- phone: +1234567890",
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
            "💬 Send the message text you want to send:",
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
    """Handle message sending"""
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
            "🚀 Starting message sending process...\n"
            f"Target: {hcode(campaign_stats['target'])}\n"
            f"Message preview: {hcode(campaign_stats['message'][:50])}..."
        )
    except:
        await query.answer("Starting message sending process...")
    
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

@router.callback_query(F.data == "show_stats")
async def show_stats_handler(query: types.CallbackQuery):
    """Show statistics"""
    await show_statistics(query.message)

async def show_statistics(message: types.Message):
    """Display detailed statistics"""
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
    
    # Add premium session info
    premium_sessions = sum(1 for s in active_sessions.values() if s.is_premium)
    if premium_sessions > 0:
        stats_message += f"• Premium sessions: {premium_sessions}\n"
    
    # Add top performing sessions
    top_sessions = sorted(
        [s for s in active_sessions.values() if s.valid],
        key=lambda x: x.sent_count,
        reverse=True
    )[:5]
    
    if top_sessions:
        stats_message += "\n🏆 Top Performing Sessions:\n"
        for i, session in enumerate(top_sessions, 1):
            premium = "🌟" if session.is_premium else ""
            stats_message += (
                f"{i}. {premium}{session.name} ({session.phone or '?'}) - "
                f"{session.sent_count} messages\n"
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
    """Initialize the bot"""
    await load_sessions()
    valid_count = sum(1 for s in active_sessions.values() if s.valid)
    premium_count = sum(1 for s in active_sessions.values() if s.is_premium)
    logger.info(f"Bot started with {valid_count} valid sessions ({premium_count} premium)")

# --- Main ---
if __name__ == "__main__":
    dp.startup.register(on_startup)
    logger.info("Starting bot...")
    try:
        dp.run_polling(bot)
    except KeyboardInterrupt:
        logger.info("Bot stopped gracefully")
    except Exception as e:
        logger.error(f"Bot crashed: {str(e)}")