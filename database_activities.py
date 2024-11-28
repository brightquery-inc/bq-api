from pydantic_settings import BaseSettings

class Settings(BaseSettings):    
    REDIS_URL: str = "redis://localhost"

settings = Settings()

# from databases import Database
import aioredis
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend

# Singleton pattern for database and cache
class DatabaseManager:
    _instance = None
   
    def __init__(self):
        self.redis = aioredis.from_url(settings.REDIS_URL)
       
    @classmethod
    async def get_instance(cls):
        if not cls._instance:
            cls._instance = cls()
            await cls._instance.database.connect()
            FastAPICache.init(RedisBackend(cls._instance.redis))
        return cls._instance
    