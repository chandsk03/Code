import os  # Fixed import
import time
import random
import asyncio
import logging
from termcolor import colored
from telethon.sync import TelegramClient
from telethon.errors import (
    PeerFloodError, FloodWaitError, UserPrivacyRestrictedError,
    UserIsBlockedError, SessionPasswordNeededError
)
from tqdm import tqdm
from typing import List, Optional  # Added for type hints

# --- Telegram API Credentials ---
api_id = 25781839
api_hash = '20a3f2f168739259a180dcdd642e196c'

# --- Config ---
SESSIONS_FOLDER = "sessions"
MIN_DELAY = 2
MAX_DELAY = 4
BATCH_SIZE = 5
MAX_RETRIES = 3  # Added retry mechanism

# --- Inputs ---
target_username = input("Enter target username (without @): ").strip()
message_text = input("Enter message to send: ").strip()

# --- Setup logging ---
logging.basicConfig(
    filename='message_sender.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)  # Improved logger initialization

async def is_session_valid(session_file: str) -> Optional[str]:
    """Check if a session file is valid and authorized."""
    session_name = os.path.splitext(session_file)[0]
    session_path = os.path.join(SESSIONS_FOLDER, session_name)
    
    async with TelegramClient(session_path, api_id, api_hash) as client:
        try:
            await client.connect()
            if not await client.is_user_authorized():
                print(colored(f"[!] UNAUTHORIZED: {session_name}", "yellow"))
                logger.warning(f"Unauthorized session: {session_name}")
                return None
            return session_name
        except Exception as e:
            logger.error(f"Error checking session {session_name}: {e}")
            print(colored(f"[!] ERROR checking session {session_name}: {e}", "red"))
            return None

async def send_from_session(session_name: str, retry_count: int = 0) -> None:
    """Send message from a single session with retry logic."""
    session_path = os.path.join(SESSIONS_FOLDER, session_name)
    
    async with TelegramClient(session_path, api_id, api_hash) as client:
        try:
            await client.start()
            user = await client.get_entity(target_username)
            await client.send_message(user.id, message_text)
            print(colored(f"[+] Message sent from {session_name}", "green"))
            logger.info(f"Message sent from {session_name}")
        
        except UserPrivacyRestrictedError:
            print(colored(f"[!] User privacy restricted in {session_name}", "yellow"))
            logger.warning(f"User privacy restricted in {session_name}")
        
        except UserIsBlockedError:
            print(colored(f"[!] Blocked by user in {session_name}", "yellow"))
            logger.warning(f"Blocked by user in {session_name}")
        
        except PeerFloodError as e:
            print(colored(f"[!] SPAM DETECTED (PeerFloodError) in {session_name}", "red"))
            logger.error(f"PeerFloodError in {session_name}")
            if retry_count < MAX_RETRIES:
                wait_time = random.uniform(5, 10)
                print(colored(f"[*] Retrying after {wait_time:.1f}s...", "yellow"))
                await asyncio.sleep(wait_time)
                await send_from_session(session_name, retry_count + 1)
        
        except FloodWaitError as e:
            print(colored(f"[!] FloodWaitError: Sleeping {e.seconds}s ({session_name})", "red"))
            logger.warning(f"FloodWaitError: Sleeping {e.seconds}s in {session_name}")
            await asyncio.sleep(e.seconds)
            if retry_count < MAX_RETRIES:
                await send_from_session(session_name, retry_count + 1)
        
        except SessionPasswordNeededError:
            print(colored(f"[!] 2FA enabled (not supported): {session_name}", "magenta"))
            logger.error(f"2FA enabled in {session_name}")
        
        except Exception as e:
            logger.error(f"Unexpected error in {session_name}: {e}")
            print(colored(f"[!] Error in {session_name}: {e}", "red"))
        
        finally:
            await asyncio.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

async def main() -> None:
    """Main function to orchestrate the message sending process."""
    if not os.path.exists(SESSIONS_FOLDER):
        print(colored(f"[!] Folder '{SESSIONS_FOLDER}' not found!", "red"))
        logger.error(f"Session folder '{SESSIONS_FOLDER}' not found")
        return

    session_files = [f for f in os.listdir(SESSIONS_FOLDER) if f.endswith(".session")]
    if not session_files:
        print(colored("[!] No .session files found!", "red"))
        logger.error("No session files found")
        return

    print(colored(f"[*] Checking {len(session_files)} sessions...", "cyan"))
    valid_sessions: List[str] = [
        session_name 
        for session_file in session_files 
        if (session_name := await is_session_valid(session_file))
    ]

    if not valid_sessions:
        print(colored("[!] No valid sessions found!", "red"))
        logger.error("No valid sessions found")
        return

    print(colored(f"[✓] {len(valid_sessions)} valid sessions found.", "green"))
    print(colored(f"[*] Sending messages to @{target_username}...", "cyan"))

    with tqdm(total=len(valid_sessions), desc="Sending messages") as pbar:
        for i in range(0, len(valid_sessions), BATCH_SIZE):
            batch = valid_sessions[i:i + BATCH_SIZE]
            await asyncio.gather(*(send_from_session(s) for s in batch))
            pbar.update(len(batch))

    print(colored("[✓] All messages sent.", "green"))
    logger.info("All messages sent successfully")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[!] Script interrupted by user.")
        logger.warning("Script interrupted by user")
    except Exception as e:
        print(colored(f"[!] Unexpected error: {e}", "red"))
        logger.error(f"Unexpected error in main: {e}")
