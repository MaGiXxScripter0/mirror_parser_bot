import asyncio
import logging
import datetime
import os
from collections import defaultdict
from typing import Dict, List, Set, Tuple

from telethon import TelegramClient, events
from telethon.tl.types import Message

from src.config import Config, Route
from src.database import Deduper
from src.normalizer import Normalizer, UnifiedMessage

logger = logging.getLogger(__name__)

class Listener:
    def __init__(self, deduper: Deduper, queue: asyncio.Queue):
        self.client = TelegramClient(Config.SESSION_NAME, Config.API_ID, Config.API_HASH)
        self.deduper = deduper
        self.queue = queue
        
        # Album buffering
        # grouped_id -> { "messages": [msgs], "expiry": timestamp }
        self.album_buffer: Dict[int, Dict] = {} 
        self.album_lock = asyncio.Lock()
        
        # Route lookups
        self.source_routes: Dict[int, List[Route]] = defaultdict(list)
        for route in Config.ROUTES:
            self.source_routes[route.source_id].append(route)

    async def start(self):
        logger.info("Starting Telethon Client...")
        await self.client.start()
        
        # Register event handler
        self.client.add_event_handler(self._handle_new_message, events.NewMessage())
        
        logger.info("Listener started.")
        await self.client.run_until_disconnected()

    def _should_process(self, message: Message, route: Route) -> bool:
        # 1. Time filter (skip old messages)
        # message.date is usually timezone-aware UTC.
        now = datetime.datetime.now(datetime.timezone.utc)
        if (now - message.date).total_seconds() > 300: # 5 minutes
            logger.debug(f"Skipping old message {message.id} from {message.date}")
            return False

        # 2. Topic filter
        if route.source_topic_id:
            # Check if message belongs to this topic (thread)
            # In Telethon, reply_to.forum_topic is boolean, 
            # reply_to.reply_to_top_id or reply_to.reply_to_msg_id helps identify thread.
            # But the most reliable for "is in topic X" is message.reply_to.reply_to_top_id == topic_id OR message.id == topic_id (if it's the start)
            
            # Actually for incoming forum messages:
            # message.reply_to can be None if it's the first message effectively? 
            # Or if it's a general topic?
            # Usually: msg.reply_to.reply_to_top_id is the Thread ID. 
            # If reply_to is None, it might be in General (0 or 1).
            
            # Let's check logic:
            if not message.reply_to:
                # If expecting a specific topic but message has no reply info, it MIGHT be General or just a channel post.
                # If Configured Topic is NOT None, and message implies no topic... skip?
                # Actually, some updates have message.topic_id (not in all versions/types).
                # safely check attributes
                 return False

            thread_id = getattr(message.reply_to, 'reply_to_top_id', None) or getattr(message.reply_to, 'forum_topic', None)
            # This is tricky with Telethon versions.
            # A common way: message.action might be null.
            # If msg.reply_to.reply_to_top_id exists, that's the thread ID.
            
            # If it's a direct reply to the topic start, top_id might be set.
            # Simplification:
            current_thread = message.reply_to.reply_to_top_id if message.reply_to else None
            
            if current_thread != route.source_topic_id:
                return False
                
        return True

    async def _handle_new_message(self, event: events.NewMessage.Event):
        message: Message = event.message
        chat_id = message.chat_id
        
        # 1. Match Routes
        possible_routes = self.source_routes.get(chat_id)
        if not possible_routes:
            # logger.debug(f"Message from {chat_id} ignored (no route)") # noisy if in many chats
            return

        for route in possible_routes:
            # 2. Filter
            if not self._should_process(message, route):
                continue
                
            # 3. Deduplicate
            if await self.deduper.is_processed(route, message.id):
                logger.debug(f"Skipping duplicate {message.id}")
                continue

            # 4. Grouping (Albums)
            if message.grouped_id:
                await self._handle_album(message, route)
            else:
                await self._process_single_message(message, route)

    async def _process_single_message(self, message: Message, route: Route):
        # Normalize
        unified_msg = await Normalizer.normalize(message)
        
        # Download media if exists
        if unified_msg.media_type:
            path = await self.client.download_media(message, file="src/tmp/")
            unified_msg.media_path = path

        # Add to DB (optimistic or pending? We usually mark processed here to avoid re-queueing)
        # But if send fails, we might want to retry.
        # For strict once-delivery, we mark processed AFTER push to queue or BEFORE?
        # Requirement says "Status in DB", so maybe we insert as 'pending' then update.
        # For simplicity/mvp: Check exists -> Push Queue -> Worker Sends -> Mark Processed?
        # Or Mark Processed -> Push.
        # If we Mark Processed first, and Queue dies, we lose message.
        # If we Push first, and listener crashes, next start re-processes. Better.
        
        # But wait, Deduper checks DB.
        # Let's Insert into DB as 'processed' (or 'pending'), then push to queue.
        # Queue item: (route, unified_msg)
        
        await self.queue.put((route, unified_msg))
        
        # We mark it processed in DB to prevent immediate re-reading if we restart fast,
        # BUT ideally we mark it 'done' only after success.
        # Given "Deduper" logic in requirements: "Check if processed".
        # Let's commit to DB now.
        await self.deduper.add_processed(route, message.id)

    async def _handle_album(self, message: Message, route: Route):
        grouped_id = message.grouped_id
        
        async with self.album_lock:
            if grouped_id not in self.album_buffer:
                self.album_buffer[grouped_id] = {
                    "messages": [],
                    "route": route,
                    "created_at": asyncio.get_event_loop().time(),
                    "timer_task": asyncio.create_task(self._flush_album(grouped_id))
                }
            
            # Add message
            self.album_buffer[grouped_id]["messages"].append(message)
            
            # Avoid dupes in DB? 
            # We track (route, chat, msg_id).
            await self.deduper.add_processed(route, message.id, grouped_id=grouped_id)

    async def _flush_album(self, grouped_id: int):
        # Wait for more messages
        await asyncio.sleep(2.0)
        
        async with self.album_lock:
            data = self.album_buffer.pop(grouped_id, None)
            
        if not data:
            return
            
        messages: List[Message] = data["messages"]
        route: Route = data["route"]
        
        # Sort by ID to keep order
        messages.sort(key=lambda m: m.id)
        
        # Prepare Unified Messages
        unified_group = []
        for msg in messages:
            uni = await Normalizer.normalize(msg)
            if uni.media_type:
                path = await self.client.download_media(msg, file="src/tmp/")
                uni.media_path = path
            unified_group.append(uni)
            
        # Push list to queue
        # Queue needs to handle List[UnifiedMessage] or UnifiedMessage
        await self.queue.put((route, unified_group))
