import unittest
import asyncio
import os
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from src.database import Base, Deduper, ProcessedMessage
from src.config import Route

class TestDatabase(unittest.TestCase):
    async def asyncSetUp(self):
        # Use in-memory SQLite for testing
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
        
        # Create tables
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            
        self.async_session = async_sessionmaker(self.engine, expire_on_commit=False)
        
        # Mock Database class to return our session
        class MockDB:
            def __init__(self, session_maker):
                self.session_maker = session_maker
            async def get_session(self):
                return self.session_maker()
                
        self.db = MockDB(self.async_session)
        self.deduper = Deduper(self.db)

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self.asyncSetUp())

    def tearDown(self):
        self.loop.close()

    def test_deduplication(self):
        async def run_test():
            route1 = Route("route1", 100, 200)
            route2 = Route("route2", 101, 200)
            
            # 1. Check not exists
            exists = await self.deduper.is_processed(route1, 1)
            self.assertFalse(exists)
            
            # 2. Add
            await self.deduper.add_processed(route1, 1)
            
            # 3. Check exists
            exists = await self.deduper.is_processed(route1, 1)
            self.assertTrue(exists)
            
            # 4. Check other message not exists
            exists = await self.deduper.is_processed(route1, 2)
            self.assertFalse(exists)
            
            # 5. Check distinct route
            exists = await self.deduper.is_processed(route2, 1)
            self.assertFalse(exists)
            
        self.loop.run_until_complete(run_test())

    def test_duplicate_constraint(self):
        async def run_test():
            route = Route("route_dupe", 100, 200)
            await self.deduper.add_processed(route, 5)
            
            # Try add again - should fail or be handled silently if logic changed, 
            # but here we expect unique constraint exception if not caught,
            # OR our code catches it. In src/database.py we catch and raise.
            
            from sqlalchemy.exc import IntegrityError
            
            with self.assertRaises(IntegrityError):
                 await self.deduper.add_processed(route, 5)
                 
        self.loop.run_until_complete(run_test())

if __name__ == '__main__':
    unittest.main()
