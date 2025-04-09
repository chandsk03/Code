import os
import time
import random
import asyncio
import logging
from typing import List, Optional
from termcolor import colored
from telethon.sync import TelegramClient
from telethon.errors import (
    PeerFloodError, FloodWaitError, UserPrivacyRestrictedError,
    UserIsBlockedError, SessionPasswordNeededError
)
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from tqdm import tqdm

# --- API Credentials ---
API_ID = 25781839
API_HASH = '20a3f2f168739259a180dcdd642e196c'

# --- Telegram Bot Token & Your User ID ---
BOT_TOKEN = '7857537951:AAG71zV2gsW3Eg97x2c2cVgoXzFCDZ3iHpU'
OWNER_ID = 7584086775  # Replace with your Telegram user ID

# --- Config ---
SESSIONS_FOLDER = "sessions"
MIN_DELAY = 2
MAX_DELAY = 4
BATCH_SIZE = 5
MAX_RETRIES = 3

# --- Global State ---
target_username = ""
message_text = ""

# --- Logging ---
logging.basicConfig(filename='telegram_sender.log', level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger()

# --- Aiogram Bot ---
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

def log_console(msg): print(colored(msg, "cyan"))

# --- Utility Functions ---
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

# --- Bot Commands ---
@dp.message_handler(commands=['start'])
async def start_command(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return await message.reply("Unauthorized.")
    await message.reply("Welcome to the Telegram Bulk Sender Bot!\n\nUse:\n/target username\n/message your text\n/send")

@dp.message_handler(commands=['target'])
async def set_target(message: types.Message):
    global target_username
    if message.from_user.id != OWNER_ID:
        return
    target_username = message.text.split(" ", 1)[1].strip()
    await message.reply(f"Target set to: @{target_username}")

@dp.message_handler(commands=['message'])
async def set_message(message: types.Message):
    global message_text
    if message.from_user.id != OWNER_ID:
        return
    message_text = message.text.split(" ", 1)[1].strip()
    await message.reply("Message set!")

@dp.message_handler(commands=['send'])
async def send_bulk(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    if not target_username or not message_text:
        return await message.reply("Set target and message first using /target and /message.")

    await message.reply("Checking sessions...")
    session_files = [f for f in os.listdir(SESSIONS_FOLDER) if f.endswith(".session")]
    valid = await asyncio.gather(*(is_session_valid(f) for f in session_files))
    valid_sessions = [s for s in valid if s]

    if not valid_sessions:
        return await message.reply("No valid sessions found!")

    await message.reply(f"Sending message to @{target_username} using {len(valid_sessions)} sessions...")

    sent = 0
    for i in range(0, len(valid_sessions), BATCH_SIZE):
        batch = valid_sessions[i:i+BATCH_SIZE]
        results = await asyncio.gather(*(send_from_session(s, target_username, message_text) for s in batch))
        sent += sum(results)

    await message.reply(f"Done!\nMessages sent: {sent}/{len(valid_sessions)}")

# --- Bot Runner ---
if __name__ == "__main__":
    log_console("Bot is running...")
    executor.start_polling(dp, skip_updates=True)
