import unittest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch
from datetime import datetime, timezone, timedelta

from src.normalizer import Normalizer, UnifiedMessage
from src.database import Deduper
from src.listener import Listener
from src.config import Route, Config

class TestNormalizer(unittest.TestCase):
    def test_text_normalization(self):
        msg = MagicMock()
        msg.id = 1
        msg.chat_id = 100
        msg.date = datetime.now()
        msg.grouped_id = None
        msg.message = "Hello World"
        msg.entities = []
        msg.reply_to = None  # Explicitly set to None
        msg.photo = None
        msg.video = None
        msg.voice = None
        msg.audio = None
        msg.document = None
        msg.sticker = None

        unified = asyncio.run(Normalizer.normalize(msg))
        self.assertEqual(unified.text, "Hello World")
        self.assertEqual(unified.source_chat_id, 100)
        self.assertIsNone(unified.source_topic_id)
        
    def test_topic_id_extraction(self):
        msg = MagicMock()
        msg.id = 5
        msg.chat_id = 200
        msg.date = datetime.now()
        msg.grouped_id = None
        msg.message = "Topic Msg"
        msg.entities = []
        msg.photo = None
        # ... 
        
        # Test with reply_to_top_id
        msg.reply_to.reply_to_top_id = 123
        msg.reply_to.forum_topic = True
        
        unified = asyncio.run(Normalizer.normalize(msg))
        self.assertEqual(unified.source_topic_id, 123)
        
        # Test fallback to reply_to_msg_id if top_id is missing but forum_topic is set?
        # Based on logic:
        # if not topic_id and getattr(message.reply_to, 'forum_topic', False):
        #    topic_id = getattr(message.reply_to, 'reply_to_msg_id', None)
        
        msg2 = MagicMock()
        msg2.id = 6
        msg2.chat_id = 200
        msg2.date = datetime.now()
        msg2.grouped_id = None
        msg2.message = "Fallback"
        msg2.entities = []
        msg2.photo = None
        msg2.reply_to.reply_to_top_id = None
        msg2.reply_to.forum_topic = True
        msg2.reply_to.reply_to_msg_id = 456
        
        unified2 = asyncio.run(Normalizer.normalize(msg2))
        self.assertEqual(unified2.source_topic_id, 456)
        msg = MagicMock()
        msg.id = 2
        msg.chat_id = 100
        msg.date = datetime.now()
        msg.grouped_id = None
        msg.message = ""
        msg.entities = []
        msg.photo = True
        msg.video = None
        # ... other attrs None

        unified = asyncio.run(Normalizer.normalize(msg))
        self.assertEqual(unified.media_type, "photo")

class TestListenerFilters(unittest.TestCase):
    def setUp(self):
        self.deduper = AsyncMock(spec=Deduper)
        self.queue = asyncio.Queue()
        
        # Patch TelegramClient
        patcher = patch('src.listener.TelegramClient')
        self.MockClient = patcher.start()
        self.addCleanup(patcher.stop)
        
        # Setup mock client instance
        self.mock_client_instance = self.MockClient.return_value
        
        self.listener = Listener(self.deduper, self.queue)

    def test_time_filter(self):
        # Old message
        msg = MagicMock()
        # Ensure UTC timezone awareness to match listener logic
        msg.date = datetime.now(timezone.utc) - timedelta(minutes=10)
        route = Route("test_route", 1, 2)
        
        self.assertFalse(self.listener._should_process(msg, route))
        
        # New message
        msg.date = datetime.now(timezone.utc) - timedelta(minutes=1)
        self.assertTrue(self.listener._should_process(msg, route))

    def test_topic_filter(self):
        route = Route("topic_route", 1, 2, source_topic_id=55)
        
        # Message in different topic
        msg_wrong = MagicMock()
        msg_wrong.date = datetime.now(timezone.utc)
        msg_wrong.reply_to.reply_to_top_id = 99
        self.assertFalse(self.listener._should_process(msg_wrong, route))

        # Message in correct topic
        msg_right = MagicMock()
        msg_right.date = datetime.now(timezone.utc)
        msg_right.reply_to.reply_to_top_id = 55
        self.assertTrue(self.listener._should_process(msg_right, route))
        
        # Message with no reply info (assuming topic required)
        msg_no_reply = MagicMock()
        msg_no_reply.date = datetime.now(timezone.utc)
        msg_no_reply.reply_to = None
        self.assertFalse(self.listener._should_process(msg_no_reply, route))

if __name__ == '__main__':
    unittest.main()
