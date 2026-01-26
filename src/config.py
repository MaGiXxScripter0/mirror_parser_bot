import os
import logging
from typing import List, Optional
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

import yaml

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class Route:
    name: str
    source_id: int
    target_id: int
    source_topic_id: Optional[int] = None

class Config:
    API_ID = int(os.getenv("API_ID", 0))
    API_HASH = os.getenv("API_HASH", "")
    BOT_TOKEN = os.getenv("BOT_TOKEN", "")
    DB_NAME = os.getenv("DB_NAME", "messages.db")
    SESSION_NAME = "src/sessions/userbot"
    ROUTES_FILE = "routes.yml"

    ROUTES: List[Route] = []

    @classmethod
    def load_routes(cls):
        if not os.path.exists(cls.ROUTES_FILE):
             logger.warning(f"{cls.ROUTES_FILE} not found!")
             return

        try:
            with open(cls.ROUTES_FILE, 'r') as f:
                data = yaml.safe_load(f)
                
            if not data:
                logger.warning("Routes file is empty")
                return
                
            for item in data:
                try:
                    topic_id = item.get("sourceTopicId")
                    if topic_id is not None:
                        topic_id = int(topic_id)

                    route = Route(
                        name=str(item.get("name", "Unknown")),
                        source_id=int(item["sourceId"]),
                        target_id=int(item["targetId"]),
                        source_topic_id=topic_id
                    )
                    cls.ROUTES.append(route)
                except KeyError as e:
                    logger.error(f"Missing required field in route config: {e}")
        except Exception as e:
            logger.error(f"Failed to load routes from {cls.ROUTES_FILE}: {e}")

Config.load_routes()
