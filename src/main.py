import asyncio
import logging
import os
import signal
from typing import List, Union, Tuple

from src.config import Config, Route
from src.database import Database, Deduper
from src.listener import Listener
from src.sender import BotSender
from src.normalizer import UnifiedMessage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def sender_worker(queue: asyncio.Queue, sender: BotSender):
    logger.info("Sender worker started")
    while True:
        try:
            item: Tuple[Route, Union[UnifiedMessage, List[UnifiedMessage]]] = await queue.get()
            route, payload = item
            
            target = route.target_id
            target_topic = None # Target topic not supported in current Route config
            
            if isinstance(payload, list):
                # Album
                await sender.send_album(target, payload, target_topic)
                # Cleanup tmp files
                for msg in payload:
                    if msg.media_path and os.path.exists(msg.media_path):
                        os.remove(msg.media_path)
            else:
                # Single message
                await sender.send_message(target, payload, target_topic)
                if payload.media_path and os.path.exists(payload.media_path):
                    os.remove(payload.media_path)
                    
            queue.task_done()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Worker error: {e}", exc_info=True)
            # Ideally verify if task_done needs calling or if we retry
            # Here we just log and continue to avoid killing worker

async def main():
    # Init DB
    db = Database()
    await db.init_db()
    deduper = Deduper(db)
    
    # Init Queue
    queue = asyncio.Queue()
    
    # Init Sender
    sender = BotSender()
    
    # Start Worker
    worker_task = asyncio.create_task(sender_worker(queue, sender))
    
    # Init Listener
    listener = Listener(deduper, queue)
    
    logger.info("Service initialized. Press Ctrl+C to stop.")
    
    try:
        await listener.start()
    except KeyboardInterrupt:
        logger.info("Stopping...")
    finally:
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass
        logger.info("Stopped.")

if __name__ == "__main__":
    asyncio.run(main())
