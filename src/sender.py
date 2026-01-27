import asyncio
import logging
import os
from typing import List, Union, Optional
from aiogram import Bot, exceptions
from aiogram.types import FSInputFile, InputMediaPhoto, InputMediaVideo, InputMediaDocument, InputMediaAudio
from aiogram.utils.token import TokenValidationError

from src.normalizer import UnifiedMessage
from src.config import Config

logger = logging.getLogger(__name__)

class BotSender:
    def __init__(self):
        try:
            self.bot = Bot(token=Config.BOT_TOKEN)
        except TokenValidationError:
            logger.error("Invalid Bot Token")
            self.bot = None

    async def _handle_flood_wait(self, e: exceptions.TelegramRetryAfter):
        logger.warning(f"FloodWait: sleeping {e.retry_after}s")
        await asyncio.sleep(e.retry_after)

    async def send_message(self, target_chat_id: int, message: UnifiedMessage, topic_id: int = None, reply_to_message_id: int = None) -> Optional[int]:
        if not self.bot:
            return None

        try:
            # Prepare entities logic: aiogram expects list of MessageEntity objects or None
            # But the send methods usually take `entities` as a list of types.MessageEntity.
            # Since we converted to dicts in Normalizer, we might need to rely on aiogram's adaptability 
            # or reconstruct objects. Ideally, we just pass parameters compatible with api.
            
            # Actually, aiogram `bot.send_message` expects `entities` as `List[MessageEntity]`.
            # Let's import MessageEntity and reconstruct.
            from aiogram.types import MessageEntity, ReplyParameters
            
            entities = [MessageEntity(**e) for e in message.entities] if message.entities else None
            
            # Common args
            kwargs = {
                "chat_id": target_chat_id,
                "message_thread_id": topic_id,
                "reply_parameters": ReplyParameters(message_id=reply_to_message_id) if reply_to_message_id else None
            }

            sent_msg = None
            if message.media_path:
                media_file = FSInputFile(message.media_path)
                
                if message.media_type == "photo":
                    sent_msg = await self.bot.send_photo(photo=media_file, caption=message.text, caption_entities=entities, **kwargs)
                elif message.media_type == "video":
                    sent_msg = await self.bot.send_video(video=media_file, caption=message.text, caption_entities=entities, supports_streaming=True, **kwargs)
                elif message.media_type == "document":
                    sent_msg = await self.bot.send_document(document=media_file, caption=message.text, caption_entities=entities, **kwargs)
                elif message.media_type == "voice":
                    sent_msg = await self.bot.send_voice(voice=media_file, caption=message.text, caption_entities=entities, **kwargs)
                elif message.media_type == "audio":
                    sent_msg = await self.bot.send_audio(audio=media_file, caption=message.text, caption_entities=entities, **kwargs)
                elif message.media_type == "animation":
                    sent_msg = await self.bot.send_animation(animation=media_file, caption=message.text, caption_entities=entities, **kwargs)
                elif message.media_type == "sticker":
                    sent_msg = await self.bot.send_sticker(sticker=media_file, **kwargs)
                else:
                    # Fallback or unknown
                    logger.warning(f"Unknown media type {message.media_type}, sending as document")
                    sent_msg = await self.bot.send_document(document=media_file, caption=message.text, caption_entities=entities, **kwargs)
            else:
                # Text only
                if message.text:
                    sent_msg = await self.bot.send_message(text=message.text, entities=entities, **kwargs)
                else:
                    logger.debug(f"Skipping empty message {message.source_message_id}")

            return sent_msg.message_id if sent_msg else None

        except exceptions.TelegramRetryAfter as e:
            await self._handle_flood_wait(e)
            return await self.send_message(target_chat_id, message, topic_id, reply_to_message_id) # Retry
        except Exception as e:
            logger.error(f"Failed to send message to {target_chat_id}: {e}", exc_info=True)
            return None

    async def send_album(self, target_chat_id: int, messages: List[UnifiedMessage], topic_id: int = None, reply_to_message_id: int = None) -> List[int]:
        if not self.bot or not messages:
            return []

        try:
            # Construct MediaGroup
            # Only the first item can usually have a caption (or logic depends on client), 
            # Bot API treats captions per item, but official clients often show one.
            # We will attach captions to respective items.
            
            media_group = []
            from aiogram.types import MessageEntity, ReplyParameters
            
            for msg in messages:
                if not msg.media_path:
                    continue
                    
                file = FSInputFile(msg.media_path)
                entities = [MessageEntity(**e) for e in msg.entities] if msg.entities else None
                
                if msg.media_type == "photo":
                    media_group.append(InputMediaPhoto(media=file, caption=msg.text, caption_entities=entities))
                elif msg.media_type == "video":
                     media_group.append(InputMediaVideo(media=file, caption=msg.text, caption_entities=entities))
                elif msg.media_type == "document":
                     media_group.append(InputMediaDocument(media=file, caption=msg.text, caption_entities=entities))
                elif msg.media_type == "audio":
                     media_group.append(InputMediaAudio(media=file, caption=msg.text, caption_entities=entities))
                # Voice, sticker, animation rarely in albums in the same way with mixed types, usually photos/videos
            
            if not media_group:
                return []

            sent_messages = await self.bot.send_media_group(
                chat_id=target_chat_id, 
                media=media_group, 
                message_thread_id=topic_id,
                reply_parameters=ReplyParameters(message_id=reply_to_message_id) if reply_to_message_id else None
            )
            return [m.message_id for m in sent_messages]
            
        except exceptions.TelegramRetryAfter as e:
            await self._handle_flood_wait(e)
            return await self.send_album(target_chat_id, messages, topic_id, reply_to_message_id)
        except Exception as e:
            logger.error(f"Failed to send album to {target_chat_id}: {e}", exc_info=True)
            return []
