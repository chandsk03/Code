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
from tqdm import tqdm

# --- Telegram API Credentials ---
API_ID = 25781839
API_HASH = '20a3f2f168739259a180dcdd642e196c'

# --- Configuration ---
SESSIONS_FOLDER = "sessions"
MIN_DELAY = 2
MAX_DELAY = 4
BATCH_SIZE = 5
MAX_RETRIES = 3

# --- Logging ---
logging.basicConfig(
    filename='telegram_sender.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- Utility ---
def print_info(message: str): print(colored(f"[*] {message}", "blue"))
def print_success(message: str): print(colored(f"[✓] {message}", "green"))
def print_error(message: str): print(colored(f"[!] {message}", "red"))
def print_warning(message: str): print(colored(f"[!] {message}", "yellow"))

# --- Session Check ---
async def is_session_valid(session_file: str) -> Optional[str]:
    session_name = os.path.splitext(session_file)[0]
    session_path = os.path.join(SESSIONS_FOLDER, session_name)

    client = TelegramClient(session_path, API_ID, API_HASH)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            print_warning(f"Session '{session_name}' is unauthorized.")
            return None
        return session_name
    except Exception as e:
        print_error(f"Session check failed '{session_name}': {e}")
        return None
    finally:
        await client.disconnect()

# --- Message Sender ---
async def send_from_session(session_name: str, target: str, message: str, retry_count: int = 0) -> bool:
    session_path = os.path.join(SESSIONS_FOLDER, session_name)
    client = TelegramClient(session_path, API_ID, API_HASH)

    try:
        await client.connect()
        if not await client.is_user_authorized():
            print_warning(f"Unauthorized session: {session_name}")
            return False

        user = await client.get_entity(target)
        await client.send_message(user.id, message)
        print_success(f"Message sent from {session_name}")
        logger.info(f"Sent to {target} from {session_name}")
        return True

    except UserPrivacyRestrictedError:
        print_warning(f"Privacy restricted for {session_name}")
    except UserIsBlockedError:
        print_warning(f"Blocked by user: {session_name}")
    except PeerFloodError:
        print_error(f"PeerFloodError in {session_name}")
        if retry_count < MAX_RETRIES:
            wait = random.uniform(5, 10) * (retry_count + 1)
            print_info(f"Retrying in {wait:.1f}s (attempt {retry_count + 1})...")
            await asyncio.sleep(wait)
            return await send_from_session(session_name, target, message, retry_count + 1)
    except FloodWaitError as e:
        print_error(f"FloodWaitError: Wait {e.seconds}s in {session_name}")
        await asyncio.sleep(e.seconds)
        return await send_from_session(session_name, target, message, retry_count + 1)
    except SessionPasswordNeededError:
        print_error(f"2FA enabled (skipped): {session_name}")
    except Exception as e:
        print_error(f"Error in {session_name}: {e}")
    finally:
        await client.disconnect()
        await asyncio.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
    return False

# --- Main Orchestration ---
async def main():
    print(colored("Welcome to the Telegram Message Sender!", "cyan"))
    target = input(colored("Enter target username (without @): ", "cyan")).strip()
    message = input(colored("Enter message to send: ", "cyan")).strip()

    if not target or not message:
        print_error("Target or message is empty.")
        return

    if not os.path.exists(SESSIONS_FOLDER):
        print_error(f"Sessions folder '{SESSIONS_FOLDER}' not found.")
        return

    session_files = [f for f in os.listdir(SESSIONS_FOLDER) if f.endswith(".session")]
    if not session_files:
        print_error("No .session files found.")
        return

    print_info(f"Checking {len(session_files)} session(s)...")
    valid_sessions = await asyncio.gather(*(is_session_valid(f) for f in session_files))
    valid_sessions = [s for s in valid_sessions if s]

    print_success(f"{len(valid_sessions)} valid session(s) found.")

    if not valid_sessions:
        print_error("No working sessions.")
        return

    print_info(f"Sending messages to @{target}...")
    sent = 0

    with tqdm(total=len(valid_sessions), bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]") as bar:
        for i in range(0, len(valid_sessions), BATCH_SIZE):
            batch = valid_sessions[i:i+BATCH_SIZE]
            results = await asyncio.gather(*(send_from_session(s, target, message) for s in batch))
            sent += sum(results)
            bar.update(len(batch))

    # Summary
    print_success("Finished message delivery.")
    print("\nSummary:")
    print(f"- Total sessions: {len(session_files)}")
    print(f"- Valid sessions: {len(valid_sessions)}")
    print(f"- Messages sent: {sent}")
    print(f"- Failed: {len(valid_sessions) - sent}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print_error("Interrupted by user.")
    except Exception as e:
        print_error(f"Unexpected error: {e}")
