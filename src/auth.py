from telethon import TelegramClient
from src.config import Config
import logging
import asyncio

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    logger.info("Starting authentication flow...")
    client = TelegramClient(Config.SESSION_NAME, Config.API_ID, Config.API_HASH)
    
    await client.start()
    
    me = await client.get_me()
    logger.info(f"Authenticated successfully as {me.first_name} (ID: {me.id})")
    logger.info("Session file saved. You can now start the main service.")
    
    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
