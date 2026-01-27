import datetime
from typing import Optional
from sqlalchemy import Column, Integer, String, DateTime, Index
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy.future import select

from src.config import Config

Base = declarative_base()

class ProcessedMessage(Base):
    __tablename__ = "processed_messages"

    id = Column(Integer, primary_key=True)
    route_name = Column(String, nullable=False) # e.g. "source:target"
    source_chat_id = Column(Integer, nullable=False)
    source_message_id = Column(Integer, nullable=False)
    grouped_id = Column(Integer, nullable=True)
    target_message_id = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    
    # Unique constraint for single messages
    __table_args__ = (
        Index('idx_unique_message', 'route_name', 'source_chat_id', 'source_message_id', unique=True),
    )

class Database:
    def __init__(self):
        self.engine = create_async_engine(f"sqlite+aiosqlite:///{Config.DB_NAME}", echo=False)
        self.async_session = async_sessionmaker(self.engine, expire_on_commit=False)

    async def init_db(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            
            # Simple migration to add target_message_id if it's missing (e.g. from older schema)
            # aiosqlite/sqlalchemy async doesn't easily support column check via metadata.create_all
            from sqlalchemy import text
            try:
                # Check columns
                result = await conn.execute(text("PRAGMA table_info(processed_messages)"))
                columns = [row[1] for row in result.fetchall()]
                if "target_message_id" not in columns:
                    logger.info("Database migration: adding 'target_message_id' column to 'processed_messages'")
                    await conn.execute(text("ALTER TABLE processed_messages ADD COLUMN target_message_id INTEGER"))
            except Exception as e:
                logger.error(f"Failed to run database migration: {e}")

    async def get_session(self) -> AsyncSession:
        return self.async_session()

class Deduper:
    def __init__(self, db: Database):
        self.db = db

    async def is_processed(self, route: Config.ROUTES[0].__class__, message_id: int) -> bool:
        # Use route.name from config as the unique identifier for the route configuration
        route_name = route.name
        async with await self.db.get_session() as session:
            stmt = select(ProcessedMessage).where(
                ProcessedMessage.route_name == route_name,
                ProcessedMessage.source_chat_id == route.source_id,
                ProcessedMessage.source_message_id == message_id
            )
            result = await session.execute(stmt)
            return result.scalar_one_or_none() is not None

    async def add_processed(self, route: Config.ROUTES[0].__class__, message_id: int, grouped_id: Optional[int] = None):
        route_name = route.name
        async with await self.db.get_session() as session:
            try:
                msg = ProcessedMessage(
                    route_name=route_name,
                    source_chat_id=route.source_id,
                    source_message_id=message_id,
                    grouped_id=grouped_id
                )
                session.add(msg)
                await session.commit()
            except Exception as e:
                # E.g. UniqueViolation if race condition, though queue should prevent it
                await session.rollback()
                raise e

    async def get_target_message_id(self, route_name: str, source_chat_id: int, source_message_id: int) -> Optional[int]:
        async with await self.db.get_session() as session:
            stmt = select(ProcessedMessage.target_message_id).where(
                ProcessedMessage.route_name == route_name,
                ProcessedMessage.source_chat_id == source_chat_id,
                ProcessedMessage.source_message_id == source_message_id
            )
            result = await session.execute(stmt)
            return result.scalar_one_or_none()

    async def update_target_message_id(self, route_name: str, source_chat_id: int, source_message_id: int, target_message_id: int):
        async with await self.db.get_session() as session:
            stmt = select(ProcessedMessage).where(
                ProcessedMessage.route_name == route_name,
                ProcessedMessage.source_chat_id == source_chat_id,
                ProcessedMessage.source_message_id == source_message_id
            )
            result = await session.execute(stmt)
            msg = result.scalar_one_or_none()
            if msg:
                msg.target_message_id = target_message_id
                await session.commit()
