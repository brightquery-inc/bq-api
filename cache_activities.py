from functools import lru_cache
from typing import Dict, Optional, Any
import hashlib
import json
from fastapi import Request
from config import *

class TwoLevelCache:
    def __init__(self, redis_client):
        self.redis = redis_client
        self._local_cache = {}
        
    @lru_cache(maxsize=128)
    def get_matrix_mapping(self, search_product: str, search_universe: str) -> Dict:
        """In-memory cache for matrix mappings"""
        return FIELD_MATRIX_MAPPING[search_product][search_universe]
    
    async def get_cached_response(
        self, 
        endpoint: str, 
        params: Dict
    ) -> Optional[Dict]:
        """Redis cache for API responses"""
        cache_key = self._generate_cache_key(endpoint, params)
        print('cached key:', cache_key)
        cached = await self.redis.get(cache_key)
        if cached:
            return json.loads(cached)
        return None

    async def set_cached_response(
        self, 
        endpoint: str, 
        params: Dict, 
        response: Dict,
        expire: int = 300
    ):
        cache_key = self._generate_cache_key(endpoint, params)
        print('cached key:', cache_key)
        await self.redis.set(
            cache_key,            
            json.dumps(response),
            ex=expire,            
        )

    def _generate_cache_key(self, endpoint: str, params: Dict) -> str:
        """Generate deterministic cache key"""
        param_string = json.dumps(params, sort_keys=True)
        print('param_string:::',param_string)
        return f"api:{endpoint}:{hashlib.md5(param_string.encode()).hexdigest()}"