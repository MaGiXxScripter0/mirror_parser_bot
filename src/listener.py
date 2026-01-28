import asyncio
import logging
import datetime
import os
from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple

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
        
        # Per-chat processing
        self.chat_queues: Dict[int, asyncio.Queue] = {}
        self.chat_tasks: Dict[int, asyncio.Task] = {}
        self.polling_task: Optional[asyncio.Task] = None
        self.last_seen: Dict[int, int] = {}
        
        # Route lookups
        self.source_routes: Dict[int, List[Route]] = defaultdict(list)
        for route in Config.ROUTES:
            self.source_routes[route.source_id].append(route)

    async def start(self):
        logger.info("Starting Telethon Client...")
        await self.client.start()
        
        # Register event handler
        self.client.add_event_handler(self._handle_new_message, events.NewMessage())
        
        if Config.POLLING_INTERVAL > 0:
            self.polling_task = asyncio.create_task(self._poll_sources())
        
        logger.info("Listener started.")
        await self.client.run_until_disconnected()

    async def _handle_new_message(self, event: events.NewMessage.Event):
        message: Message = event.message
        chat_id = message.chat_id
        self._update_last_seen(chat_id, message.id)
        
        if chat_id not in self.chat_queues:
            self.chat_queues[chat_id] = asyncio.Queue()
            self.chat_tasks[chat_id] = asyncio.create_task(self._chat_processor(chat_id))
            
        await self.chat_queues[chat_id].put((message, False))

    async def _chat_processor(self, chat_id: int):
        """Processes messages for a specific chat sequentially."""
        logger.debug(f"Started processor for chat {chat_id}")
        
        # Buffer for albums
        album_id: Optional[int] = None
        album_messages: List[Message] = []
        album_route: Optional[Route] = None
        
        while True:
            try:
                # If we have a pending album, wait with timeout
                if album_id is not None:
                    try:
                        # Wait for next part or flush
                        message, allow_old = await asyncio.wait_for(self.chat_queues[chat_id].get(), timeout=2.0)
                    except asyncio.TimeoutError:
                        # Flush album
                        await self._flush_album(album_route, album_messages)
                        album_id = None
                        album_messages = []
                        album_route = None
                        continue
                else:
                    # Normal wait
                    message, allow_old = await self.chat_queues[chat_id].get()

                # Process message
                possible_routes = self.source_routes.get(chat_id)
                if not possible_routes:
                    self.chat_queues[chat_id].task_done()
                    continue

                for route in possible_routes:
                    if not self._should_process(message, route, allow_old=allow_old):
                        continue
                    
                    if await self.deduper.is_processed(route, message.id):
                        continue

                    if message.grouped_id:
                        # Start or continue album
                        if album_id == message.grouped_id:
                            album_messages.append(message)
                        else:
                            # If a NEW album starts before old one flushed (unlikely in same chat?)
                            if album_id is not None:
                                await self._flush_album(album_route, album_messages)
                            
                            album_id = message.grouped_id
                            album_messages = [message]
                            album_route = route
                        
                        # Mark processed in DB now to prevent dups if album takes long
                        await self.deduper.add_processed(route, message.id, grouped_id=album_id)
                    else:
                        # If a single message arrives during an album collecting...
                        # In Telegram, albums are usually sent Together. 
                        # But if a single message arrives, we should probably flush album first 
                        # to preserve order if the single message was meant to be AFTER.
                        if album_id is not None:
                            await self._flush_album(album_route, album_messages)
                            album_id = None
                            album_messages = []
                        
                        await self._process_single_message(message, route)
                
                self.chat_queues[chat_id].task_done()

            except Exception as e:
                logger.error(f"Error in chat processor {chat_id}: {e}", exc_info=True)

    async def _poll_sources(self):
        await self._init_last_seen()
        while True:
            try:
                for source_id in self.source_routes.keys():
                    min_id = self.last_seen.get(source_id, 0)
                    async for message in self.client.iter_messages(
                        source_id, min_id=min_id, reverse=True
                    ):
                        self._update_last_seen(source_id, message.id)
                        await self._ensure_chat_queue(source_id)
                        await self.chat_queues[source_id].put((message, True))
                await asyncio.sleep(Config.POLLING_INTERVAL)
            except Exception as e:
                logger.error(f"Polling error: {e}", exc_info=True)
                await asyncio.sleep(max(Config.POLLING_INTERVAL, 1))

    async def _init_last_seen(self):
        for source_id in self.source_routes.keys():
            try:
                msgs = await self.client.get_messages(source_id, limit=1)
                if msgs:
                    self.last_seen[source_id] = msgs[0].id
            except Exception as e:
                logger.warning(f"Failed to init last_seen for {source_id}: {e}")

    def _update_last_seen(self, chat_id: int, message_id: int):
        current = self.last_seen.get(chat_id, 0)
        if message_id > current:
            self.last_seen[chat_id] = message_id

    async def _ensure_chat_queue(self, chat_id: int):
        if chat_id not in self.chat_queues:
            self.chat_queues[chat_id] = asyncio.Queue()
            self.chat_tasks[chat_id] = asyncio.create_task(self._chat_processor(chat_id))

    async def _process_single_message(self, message: Message, route: Route):
        unified_msg = await Normalizer.normalize(message)
        if unified_msg.media_type:
            path = await self.client.download_media(message, file="src/tmp/")
            unified_msg.media_path = path

        await self.queue.put((route, unified_msg))
        await self.deduper.add_processed(route, message.id)

    async def _flush_album(self, route: Route, messages: List[Message]):
        if not messages or not route:
            return
            
        messages.sort(key=lambda m: m.id)
        unified_group = []
        for msg in messages:
            uni = await Normalizer.normalize(msg)
            if uni.media_type:
                path = await self.client.download_media(msg, file="src/tmp/")
                uni.media_path = path
            unified_group.append(uni)
            
        await self.queue.put((route, unified_group))

    @staticmethod
    def _extract_topic_id(message: Message) -> Optional[int]:
        # Prefer direct attribute if present on the message
        topic_id = getattr(message, "reply_to_top_id", None)
        if topic_id:
            return topic_id
        if message.reply_to:
            topic_id = getattr(message.reply_to, "reply_to_top_id", None)
            if topic_id:
                return topic_id
            if getattr(message.reply_to, "forum_topic", False):
                return getattr(message.reply_to, "reply_to_msg_id", None)
        return None

    def _should_process(self, message: Message, route: Route, allow_old: bool = False) -> bool:
        # 1. Time filter
        if not allow_old:
            now = datetime.datetime.now(datetime.timezone.utc)
            if (now - message.date).total_seconds() > 300:
                return False

        # 2. Topic filter
        if route.source_topic_id:
            current_thread = self._extract_topic_id(message)
            if current_thread != route.source_topic_id:
                return False
                
        # 3. Content Filter
        text_content = message.message or ""
        if text_content:
            lower_text = text_content.lower()
            for word in Config.BLACKLIST_WORDS:
                if word in lower_text:
                    logger.info(f"Skipping message {message.id}: contains blacklist word '{word}'")
                    return False
        return True
