import os
import asyncio
import logging
import random
from typing import Optional
from aiogram import Bot, Dispatcher, Router, types, F
from aiogram.enums import ParseMode
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.markdown import hbold
from telethon import TelegramClient
from telethon.errors import (
    PeerFloodError, FloodWaitError, UserPrivacyRestrictedError,
    UserIsBlockedError, SessionPasswordNeededError
)

# --- Config ---
API_ID = 25781839
API_HASH = '20a3f2f168739259a180dcdd642e196c'
BOT_TOKEN = '7857537951:AAG71zV2gsW3Eg97x2c2cVgoXzFCDZ3iHpU'
OWNER_ID = 758408677
SESSIONS_FOLDER = "sessions"
BATCH_SIZE = 5
MAX_RETRIES = 3
MIN_DELAY = 2
MAX_DELAY = 4

# --- Logging ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Bot Setup ---
bot = Bot(token=BOT_TOKEN, default=types.DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

# --- Global State ---
state = {"target": "", "message": ""}

# --- Inline Buttons ---
def main_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📍 Set Target", callback_data="set_target")],
        [InlineKeyboardButton(text="💬 Set Message", callback_data="set_message")],
        [InlineKeyboardButton(text="🚀 Send", callback_data="send_messages")],
        [InlineKeyboardButton(text="📊 Session Stats", callback_data="session_stats")]
    ])

# --- Session Checker ---
async def is_session_valid(session_file: str) -> Optional[str]:
    session_name = os.path.splitext(session_file)[0]
    path = os.path.join(SESSIONS_FOLDER, session_name)
    client = TelegramClient(path, API_ID, API_HASH)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            return None
        return session_name
    except:
        return None
    finally:
        await client.disconnect()

# --- Send Message ---
async def send_from_session(session_name: str, target: str, message: str, retry=0) -> bool:
    path = os.path.join(SESSIONS_FOLDER, session_name)
    client = TelegramClient(path, API_ID, API_HASH)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            return False
        user = await client.get_entity(target)
        await client.send_message(user.id, message)
        return True
    except (UserPrivacyRestrictedError, UserIsBlockedError):
        return False
    except PeerFloodError:
        if retry < MAX_RETRIES:
            await asyncio.sleep(random.randint(3, 6))
            return await send_from_session(session_name, target, message, retry + 1)
    except FloodWaitError as e:
        await asyncio.sleep(e.seconds)
        return await send_from_session(session_name, target, message, retry + 1)
    except SessionPasswordNeededError:
        return False
    except Exception as e:
        logger.warning(f"Error from {session_name}: {e}")
        return False
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass
        await asyncio.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

# --- Commands ---
@router.message(F.text == "/start")
async def start_handler(msg: types.Message):
    if msg.from_user.id != OWNER_ID:
        return await msg.answer("Access denied.")
    await msg.answer(f"Welcome {hbold(msg.from_user.first_name)}!\nUse buttons below:", reply_markup=main_menu())

# --- Button Callbacks ---
@router.callback_query(F.data == "set_target")
async def set_target_handler(query: types.CallbackQuery):
    await query.message.answer("Send the target username (without @):")
    dp.message.register(callback=save_target)

async def save_target(msg: types.Message):
    state["target"] = msg.text.strip()
    await msg.answer(f"Target set to: @{state['target']}", reply_markup=main_menu())

@router.callback_query(F.data == "set_message")
async def set_message_handler(query: types.CallbackQuery):
    await query.message.answer("Send the message text:")
    dp.message.register(callback=save_message)

async def save_message(msg: types.Message):
    state["message"] = msg.text.strip()
    await msg.answer("Message saved!", reply_markup=main_menu())

@router.callback_query(F.data == "send_messages")
async def send_bulk(query: types.CallbackQuery):
    await query.message.answer("Preparing to send messages...")
    if not state["target"] or not state["message"]:
        return await query.message.answer("Set both target and message first.", reply_markup=main_menu())

    files = [f for f in os.listdir(SESSIONS_FOLDER) if f.endswith(".session")]
    valid = await asyncio.gather(*(is_session_valid(f) for f in files))
    sessions = [s for s in valid if s]

    if not sessions:
        return await query.message.answer("No valid sessions found.")

    await query.message.answer(f"Sending to @{state['target']} using {len(sessions)} sessions...")

    sent = 0
    for i in range(0, len(sessions), BATCH_SIZE):
        batch = sessions[i:i+BATCH_SIZE]
        results = await asyncio.gather(*(send_from_session(s, state["target"], state["message"]) for s in batch))
        sent += sum(results)

    await query.message.answer(f"Done.\nMessages sent: {sent}/{len(sessions)}", reply_markup=main_menu())

@router.callback_query(F.data == "session_stats")
async def session_stats(query: types.CallbackQuery):
    files = [f for f in os.listdir(SESSIONS_FOLDER) if f.endswith(".session")]
    valid = await asyncio.gather(*(is_session_valid(f) for f in files))
    active = sum(1 for v in valid if v)
    await query.message.answer(f"📊 Session Stats:\n• Total: {len(files)}\n• Valid: {active}\n• Invalid: {len(files) - active}", reply_markup=main_menu())

# --- Runner ---
if __name__ == "__main__":
    print("Bot is running...")
    asyncio.run(dp.start_polling(bot))
