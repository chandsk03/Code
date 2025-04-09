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
API_ID = 25781839  # Replace with your own
API_HASH = '20a3f2f168739259a180dcdd642e196c'  # Replace with your own

# --- Configuration ---
SESSIONS_FOLDER = "sessions"
MIN_DELAY = 2
MAX_DELAY = 4
BATCH_SIZE = 5
MAX_RETRIES = 3

# --- Setup Logging ---
logging.basicConfig(
    filename='telegram_sender.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- Utility Functions ---
def print_info(message: str) -> None:
    print(colored(f"[*] {message}", "blue"))

def print_success(message: str) -> None:
    print(colored(f"[✓] {message}", "green"))

def print_error(message: str) -> None:
    print(colored(f"[!] {message}", "red"))

def print_warning(message: str) -> None:
    print(colored(f"[!] {message}", "yellow"))

# --- Core Functions ---
async def is_session_valid(session_file: str) -> Optional[str]:
    """Check if a session file is valid and authorized."""
    session_name = os.path.splitext(session_file)[0]
    session_path = os.path.join(SESSIONS_FOLDER, session_name)
    
    async with TelegramClient(session_path, API_ID, API_HASH) as client:
        try:
            await client.connect()
            if not await client.is_user_authorized():
                print_warning(f"Session '{session_name}' is unauthorized.")
                logger.warning(f"Unauthorized session: {session_name}")
                return None
            me = await client.get_me()
            return me.phone  # Return phone number as session identifier
        except Exception as e:
            print_error(f"Error checking session '{session_name}': {e}")
            logger.error(f"Error checking session {session_name}: {e}")
            return None

async def send_from_session(session_phone: str, target: str, message: str, retry_count: int = 0) -> bool:
    """Send a message from a session with retry logic."""
    session_path = os.path.join(SESSIONS_FOLDER, session_phone)
    success = False
    
    async with TelegramClient(session_path, API_ID, API_HASH) as client:
        try:
            await client.start()
            user = await client.get_entity(target)
            await client.send_message(user.id, message)
            print_success(f"Message sent successfully from {session_phone}")
            logger.info(f"Message sent from {session_phone} to {target}")
            success = True
        
        except UserPrivacyRestrictedError:
            print_warning(f"User privacy restricted for {session_phone}")
            logger.warning(f"Privacy restricted for {session_phone}")
        
        except UserIsBlockedError:
            print_warning(f"Blocked by user from {session_phone}")
            logger.warning(f"Blocked by user for {session_phone}")
        
        except PeerFloodError:
            print_error(f"Spam detected (PeerFloodError) for {session_phone}")
            logger.error(f"PeerFloodError for {session_phone}")
            if retry_count < MAX_RETRIES:
                wait_time = random.uniform(5, 10) * (retry_count + 1)
                print_info(f"Retrying after {wait_time:.1f}s (Attempt {retry_count + 2}/{MAX_RETRIES + 1})")
                await asyncio.sleep(wait_time)
                return await send_from_session(session_phone, target, message, retry_count + 1)
        
        except FloodWaitError as e:
            print_error(f"FloodWaitError: Waiting {e.seconds}s for {session_phone}")
            logger.warning(f"FloodWaitError: Waiting {e.seconds}s for {session_phone}")
            await asyncio.sleep(e.seconds)
            if retry_count < MAX_RETRIES:
                return await send_from_session(session_phone, target, message, retry_count + 1)
        
        except SessionPasswordNeededError:
            print_error(f"2FA enabled (not supported) for {session_phone}")
            logger.error(f"2FA enabled for {session_phone}")
        
        except Exception as e:
            print_error(f"Error sending from {session_phone}: {e}")
            logger.error(f"Error for {session_phone}: {e}")
        
        finally:
            await asyncio.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
    
    return success

async def main() -> None:
    """Main function to orchestrate the message sending process."""
    # Welcome message
    print(colored("Welcome to the Telegram Message Sender!", "cyan"))

    # Get user inputs with validation
    target = input(colored("Please enter the target username (without @): ", "cyan")).strip()
    if not target:
        print_error("Username cannot be empty.")
        return
    
    message = input(colored("Please enter the message to send: ", "cyan")).strip()
    if not message:
        print_error("Message cannot be empty.")
        return

    # Check sessions folder
    if not os.path.exists(SESSIONS_FOLDER):
        print_error(f"Folder '{SESSIONS_FOLDER}' not found!")
        logger.error(f"Session folder '{SESSIONS_FOLDER}' not found")
        return

    session_files = [f for f in os.listdir(SESSIONS_FOLDER) if f.endswith(".session")]
    if not session_files:
        print_error("No .session files found!")
        logger.error("No session files found")
        return

    # Validate sessions
    print_info(f"Checking {len(session_files)} sessions in '{SESSIONS_FOLDER}' folder...")
    valid_sessions: List[str] = []
    tasks = [is_session_valid(f) for f in session_files]
    results = await asyncio.gather(*tasks)
    valid_sessions = [r for r in results if r is not None]
    
    invalid_count = len(session_files) - len(valid_sessions)
    if not valid_sessions:
        print_error("No valid sessions found!")
        logger.error("No valid sessions found")
        return
    
    print_success(f"Found {len(valid_sessions)} valid session(s). ({invalid_count} invalid)")

    # Send messages
    print_info(f"Sending messages to @{target}...")
    sent_count = 0
    with tqdm(total=len(valid_sessions), bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]") as pbar:
        for i in range(0, len(valid_sessions), BATCH_SIZE):
            batch = valid_sessions[i:i + BATCH_SIZE]
            results = await asyncio.gather(
                *(send_from_session(s, target, message) for s in batch)
            )
            sent_count += sum(1 for r in results if r)
            pbar.update(len(batch))

    # Summary
    print_success("All messages sent successfully.")
    print("\nSummary:")
    print(f"- Total sessions: {len(session_files)}")
    print(f"- Valid sessions: {len(valid_sessions)}")
    print(f"- Messages sent: {sent_count}")
    print(f"- Failed attempts: {len(valid_sessions) - sent_count}")

    # Farewell
    print(colored("\nThank you for using the Telegram Message Sender!", "cyan"))

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print_error("Script interrupted by user.")
        logger.warning("Script interrupted by user")
    except Exception as e:
        print_error(f"Unexpected error: {e}")
        logger.error(f"Unexpected error: {e}")
