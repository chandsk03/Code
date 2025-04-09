import os
import time
import random
import asyncio
import logging
from typing import List, Optional
from termcolor import colored
from telethon import TelegramClient
from telethon.errors import (
    PeerFloodError, FloodWaitError, UserPrivacyRestrictedError,
    UserIsBlockedError, SessionPasswordNeededError
)
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.utils.markdown import hcode
from aiogram.fsm.storage.memory import MemoryStorage

# --- Config ---
API_ID = 25781839
API_HASH = '20a3f2f168739259a180dcdd642e196c'
BOT_TOKEN = '7857537951:AAG71zV2gsW3Eg97x2c2cVgoXzFCDZ3iHpU'
OWNER_ID = 758408677

SESSIONS_FOLDER = "sessions"
MIN_DELAY = 2
MAX_DELAY = 4
BATCH_SIZE = 5
MAX_RETRIES = 3

# --- Global state ---
target_username = ""
message_text = ""

# --- Logger ---
logging.basicConfig(filename='telegram_sender.log', level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger()

# --- Bot setup ---
bot = Bot(token=BOT_TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher(storage=MemoryStorage())

# --- Utilities ---
def printc(msg): print(colored(msg, "cyan"))

def get_inline_menu():
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Set Target", callback_data="set_target"),
         InlineKeyboardButton(text="Set Message", callback_data="set_message")],
        [InlineKeyboardButton(text="Start Sending", callback_data="send")],
        [InlineKeyboardButton(text="Session Stats", callback_data="admin")]
    ])
    return kb

async def is_session_valid(session_file: str) -> Optional[str]:
    session_name = os.path.splitext(session_file)[0]
    session_path = os.path.join(SESSIONS_FOLDER, session_name)
    client = TelegramClient(session_path, API_ID, API_HASH)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            return None
        return session_name
    except:
        return None
    finally:
        await client.disconnect()

async def send_from_session(session_name: str, target: str, message: str, retry_count: int = 0) -> bool:
    session_path = os.path.join(SESSIONS_FOLDER, session_name)
    client = TelegramClient(session_path, API_ID, API_HASH)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            return False
        user = await client.get_entity(target)
        await client.send_message(user.id, message)
        logger.info(f"Sent to {target} from {session_name}")
        return True
    except (UserPrivacyRestrictedError, UserIsBlockedError):
        return False
    except PeerFloodError:
        if retry_count < MAX_RETRIES:
            await asyncio.sleep(random.randint(5, 10))
            return await send_from_session(session_name, target, message, retry_count + 1)
    except FloodWaitError as e:
        await asyncio.sleep(e.seconds)
        return await send_from_session(session_name, target, message, retry_count + 1)
    except:
        return False
    finally:
        await client.disconnect()
        await asyncio.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

# --- Handlers ---
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return await message.reply("Unauthorized.")
    await message.answer("Welcome to the Telegram Bulk Sender Bot!", reply_markup=get_inline_menu())

@dp.callback_query(F.data == "set_target")
async def cb_set_target(callback: types.CallbackQuery):
    await callback.message.answer("Send me the target username (without @):")
    await bot.send_chat_action(callback.from_user.id, "typing")

@dp.callback_query(F.data == "set_message")
async def cb_set_message(callback: types.CallbackQuery):
    await callback.message.answer("Send me the message to deliver:")
    await bot.send_chat_action(callback.from_user.id, "typing")

@dp.callback_query(F.data == "admin")
async def cb_admin(callback: types.CallbackQuery):
    session_files = [f for f in os.listdir(SESSIONS_FOLDER) if f.endswith(".session")]
    valid = await asyncio.gather(*(is_session_valid(f) for f in session_files))
    valid_sessions = [s for s in valid if s]
    invalid = len(session_files) - len(valid_sessions)
    msg = (f"<b>Session Stats:</b>\n"
           f"- Total: <code>{len(session_files)}</code>\n"
           f"- Valid: <code>{len(valid_sessions)}</code>\n"
           f"- Invalid: <code>{invalid}</code>")
    await callback.message.answer(msg, reply_markup=get_inline_menu())

@dp.callback_query(F.data == "send")
async def cb_send(callback: types.CallbackQuery):
    if not target_username or not message_text:
        return await callback.message.answer("Set target and message first!")
    session_files = [f for f in os.listdir(SESSIONS_FOLDER) if f.endswith(".session")]
    valid = await asyncio.gather(*(is_session_valid(f) for f in session_files))
    valid_sessions = [s for s in valid if s]
    if not valid_sessions:
        return await callback.message.answer("No valid sessions found.")
    
    await callback.message.answer(f"Sending message to <b>@{target_username}</b> from <b>{len(valid_sessions)}</b> sessions...")

    sent = 0
    for i in range(0, len(valid_sessions), BATCH_SIZE):
        batch = valid_sessions[i:i + BATCH_SIZE]
        results = await asyncio.gather(*(send_from_session(s, target_username, message_text) for s in batch))
        sent += sum(results)

    await callback.message.answer(f"<b>Done!</b>\nSent: <code>{sent}/{len(valid_sessions)}</code>", reply_markup=get_inline_menu())

@dp.message()
async def collect_user_input(message: types.Message):
    global target_username, message_text
    if message.from_user.id != OWNER_ID:
        return
    if not target_username:
        target_username = message.text.strip().replace("@", "")
        await message.answer(f"Target set to <b>@{target_username}</b>", reply_markup=get_inline_menu())
    elif not message_text:
        message_text = message.text.strip()
        await message.answer("Message content saved!", reply_markup=get_inline_menu())
    else:
        await message.answer("Use the buttons to proceed.", reply_markup=get_inline_menu())

# --- Bot Runner ---
async def main():
    printc("Bot is running...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(colored("Bot stopped.", "red"))
