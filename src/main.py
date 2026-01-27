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

async def sender_worker(queue: asyncio.Queue, sender: BotSender, deduper: Deduper):
    logger.info("Sender worker started")
    while True:
        try:
            item: Tuple[Route, Union[UnifiedMessage, List[UnifiedMessage]]] = await queue.get()
            route, payload = item
            
            target = route.target_id
            target_topic = None # Target topic not supported in current Route config
            
            if isinstance(payload, list):
                # Album
                msgs = payload
                # Find reply target
                reply_to_target_id = None
                # Use the first message of the album to find reply target if any
                first_msg = msgs[0]
                if first_msg.reply_to_msg_id:
                     reply_to_target_id = await deduper.get_target_message_id(
                         route.name, first_msg.source_chat_id, first_msg.reply_to_msg_id
                     )

                target_message_ids = await sender.send_album(target, msgs, target_topic, reply_to_target_id)
                
                # Mapping target IDs back to source IDs
                if target_message_ids:
                    for i, msg in enumerate(msgs):
                        if i < len(target_message_ids):
                            await deduper.update_target_message_id(
                                route.name, msg.source_chat_id, msg.source_message_id, target_message_ids[i]
                            )

                # Cleanup tmp files
                for msg in msgs:
                    if msg.media_path and os.path.exists(msg.media_path):
                        os.remove(msg.media_path)
            else:
                # Single message
                msg = payload
                reply_to_target_id = None
                if msg.reply_to_msg_id:
                    reply_to_target_id = await deduper.get_target_message_id(
                        route.name, msg.source_chat_id, msg.reply_to_msg_id
                    )

                target_message_id = await sender.send_message(target, msg, target_topic, reply_to_target_id)
                
                if target_message_id:
                    await deduper.update_target_message_id(
                        route.name, msg.source_chat_id, msg.source_message_id, target_message_id
                    )

                if msg.media_path and os.path.exists(msg.media_path):
                    os.remove(msg.media_path)
                    
            queue.task_done()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Worker error: {e}", exc_info=True)

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
    worker_task = asyncio.create_task(sender_worker(queue, sender, deduper))
    
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
