from dataclasses import dataclass, field
from typing import List, Optional, Union, Any
from datetime import datetime
from telethon import types, utils

@dataclass
class UnifiedMessage:
    text: str = ""
    entities: List[Any] = field(default_factory=list)
    media_path: Optional[str] = None
    media_type: Optional[str] = None # photo, video, document, audio, voice, sticker, animation
    grouped_id: Optional[int] = None
    reply_to_msg_id: Optional[int] = None # If we want to support threaded replies in target later
    
    # Metadata
    source_chat_id: int = 0
    source_topic_id: Optional[int] = None
    source_message_id: int = 0
    date: datetime = field(default_factory=datetime.utcnow)
    
    # For album grouping
    is_album_part: bool = False
    
class Normalizer:
    @staticmethod
    def get_effective_text(message: types.Message) -> str:
        return message.message or ""

    @staticmethod
    def map_entities(telethon_entities: List[types.TypeMessageEntity], text: str) -> List[dict]:
        """
        Convert Telethon entities to Bot API compatible entities (dict format for now, 
        or aiogram types if we imported them).
        
        Bot API Entity Types:
        mention, hashtag, cashtag, bot_command, url, email, phone_number, bold, italic, 
        underline, strikethrough, spoiler, code, pre, text_link, text_mention, custom_emoji
        """
        if not telethon_entities:
            return []
            
        api_entities = []
        
        # Telethon and Bot API both use offset/length logic primarily.
        # However, Telethon entities are objects, Bot API expects JSON-serializable usually 
        # or Aiogram MessageEntity objects. We will return dicts for simplicity in passing to Sender.
        
        for entity in telethon_entities:
            entity_type = None
            extra = {}
            
            if isinstance(entity, types.MessageEntityBold):
                entity_type = "bold"
            elif isinstance(entity, types.MessageEntityItalic):
                entity_type = "italic"
            elif isinstance(entity, types.MessageEntityCode):
                entity_type = "code"
            elif isinstance(entity, types.MessageEntityPre):
                entity_type = "pre"
                extra["language"] = entity.language
            elif isinstance(entity, types.MessageEntityTextUrl):
                entity_type = "text_link"
                extra["url"] = entity.url
            elif isinstance(entity, types.MessageEntityUrl):
                entity_type = "url"
            elif isinstance(entity, types.MessageEntityMention):
                entity_type = "mention"
            elif isinstance(entity, types.MessageEntityUnderline):
                entity_type = "underline"
            elif isinstance(entity, types.MessageEntityStrike):
                entity_type = "strikethrough"
            elif isinstance(entity, types.MessageEntitySpoiler):
                entity_type = "spoiler"
            elif isinstance(entity, types.MessageEntityCustomEmoji):
                entity_type = "custom_emoji"
                extra["custom_emoji_id"] = str(entity.document_id)

            if entity_type:
                data = {
                    "type": entity_type,
                    "offset": entity.offset,
                    "length": entity.length
                }
                data.update(extra)
                api_entities.append(data)
                
        return api_entities

    @staticmethod
    def determine_media_type(message: types.Message) -> Optional[str]:
        # Prioritize sticker check
        if message.sticker:
            return "sticker"
            
        # Check by MIME type if it looks like a sticker but message.sticker was False (happens sometimes)
        if message.document:
            mime_type = message.document.mime_type
            if mime_type == "image/webp" or mime_type == "application/x-tgsticker" or mime_type == "video/webm":
                # It might be a sticker
                # Additional check: attribute?
                # But if it's webp, usually we want to treat it as sticker or photo?
                # For safety, if it's specifically webp/tgsticker, we try sticker.
                return "sticker"

        if message.photo:
            return "photo"
        if message.video:
            # Check if it's an animation (gif) or round video
            if message.video.attributes:
                for attr in message.video.attributes:
                    if isinstance(attr, types.DocumentAttributeAnimated):
                        return "animation"
                    # Round video note not explicitly requested but falls under video/voice sometimes depending on client
            return "video"
        if message.voice:
            return "voice"
        if message.audio:
            return "audio"
        if message.document:
            return "document"
        return None

    @classmethod
    async def normalize(cls, message: types.Message) -> UnifiedMessage:
        # Extract topic ID
        topic_id = None
        if message.reply_to:
            topic_id = getattr(message.reply_to, 'reply_to_top_id', None)
            if not topic_id and getattr(message.reply_to, 'forum_topic', False):
                # If explicit forum reply but no top_id, maybe reply_to_msg_id is the start?
                # Or it is the topic thread logic dependent on exact TL schema.
                # Common pattern: reply_to_top_id is the thread ID.
                # If it is None, check if reply_to_msg_id could be it.
                topic_id = getattr(message.reply_to, 'reply_to_msg_id', None)

        msg = UnifiedMessage(
            source_chat_id=message.chat_id,
            source_topic_id=topic_id,
            source_message_id=message.id,
            date=message.date,
            grouped_id=message.grouped_id,
            is_album_part=bool(message.grouped_id)
        )
        
        msg.text = cls.get_effective_text(message)
        msg.entities = cls.map_entities(message.entities, msg.text)
        
        # 1. Calculate current length in UTF-16 code units (for Telegram offsets)
        # Python strings are weird, but encoding to utf-16-le gives 2 bytes per unit.
        current_len_utf16 = len(msg.text.encode('utf-16-le')) // 2
        
        # 2. Append footer
        footer_text = "\n\n@mirors_sliv"
        msg.text += footer_text
        
        # 3. Add entity for @mirors_sliv
        # Offset is current_len + 1 (for \n)
        # Length is 12 (@mirors_sliv)
        
        # Only add if it's text (though media captions also support entities)
        # Bot API supports entities for captions too.
        
        # However, checking if msg.text was empty? 
        # If empty, initial len is 0. offset is 1. "\n@..." -> it will start with newline.
        # Usually captions can't start with newline if empty? 
        # If text is empty, maybe skip \n? -> "@mirors_sliv"
        
        if current_len_utf16 > 0:
            offset = current_len_utf16 + 1
            # Check length of footer username
            # "@mirors_sliv" -> 12 chars.
            length = 12 
        else:
            # If empty text, we might want to avoid leading newline?
            # User said "via new line", usually implies separation.
            # But if empty, just "@mirors_sliv" looks better than "\n@mirors_sliv".
            # Let's trim the newline if empty.
            msg.text = "@mirors_sliv" # Overwrite the += result correction
            offset = 0
            length = 12

        msg.entities.append({
            "type": "mention",
            "offset": offset,
            "length": length
        })
        
        msg.media_type = cls.determine_media_type(message)
        
        return msg
