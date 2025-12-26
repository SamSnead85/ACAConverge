"""
Caching service for query results and schema
"""

import json
import hashlib
import time
from typing import Any, Dict, Optional, Callable
from functools import wraps
from dataclasses import dataclass, asdict
from collections import OrderedDict


@dataclass
class CacheEntry:
    """Cache entry with metadata"""
    value: Any
    created_at: float
    ttl: int  # Time to live in seconds
    hits: int = 0
    
    def is_expired(self) -> bool:
        return time.time() > (self.created_at + self.ttl)


class InMemoryCache:
    """
    Simple in-memory cache with TTL and LRU eviction
    For production, use Redis
    """
    
    def __init__(self, max_size: int = 1000, default_ttl: int = 300):
        self.max_size = max_size
        self.default_ttl = default_ttl
        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._stats = {
            "hits": 0,
            "misses": 0,
            "evictions": 0
        }
    
    def _make_key(self, *args, **kwargs) -> str:
        """Generate cache key from arguments"""
        key_data = json.dumps({"args": args, "kwargs": kwargs}, sort_keys=True, default=str)
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def _evict_if_needed(self):
        """Evict oldest entries if over capacity"""
        while len(self._cache) >= self.max_size:
            self._cache.popitem(last=False)
            self._stats["evictions"] += 1
    
    def _evict_expired(self):
        """Remove expired entries"""
        expired = [k for k, v in self._cache.items() if v.is_expired()]
        for k in expired:
            del self._cache[k]
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        self._evict_expired()
        
        if key in self._cache:
            entry = self._cache[key]
            if not entry.is_expired():
                entry.hits += 1
                self._stats["hits"] += 1
                # Move to end (LRU)
                self._cache.move_to_end(key)
                return entry.value
            else:
                del self._cache[key]
        
        self._stats["misses"] += 1
        return None
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None):
        """Set value in cache"""
        self._evict_if_needed()
        
        self._cache[key] = CacheEntry(
            value=value,
            created_at=time.time(),
            ttl=ttl or self.default_ttl
        )
        # Move to end (most recently used)
        self._cache.move_to_end(key)
    
    def delete(self, key: str) -> bool:
        """Delete entry from cache"""
        if key in self._cache:
            del self._cache[key]
            return True
        return False
    
    def clear(self):
        """Clear all cache entries"""
        self._cache.clear()
    
    def get_stats(self) -> Dict:
        """Get cache statistics"""
        total = self._stats["hits"] + self._stats["misses"]
        hit_rate = (self._stats["hits"] / total * 100) if total > 0 else 0
        
        return {
            "size": len(self._cache),
            "max_size": self.max_size,
            "hits": self._stats["hits"],
            "misses": self._stats["misses"],
            "hit_rate": f"{hit_rate:.1f}%",
            "evictions": self._stats["evictions"]
        }


# Global cache instance
cache = InMemoryCache(max_size=500, default_ttl=300)


def cached(ttl: int = 300, key_prefix: str = ""):
    """
    Decorator for caching function results
    
    Args:
        ttl: Time to live in seconds
        key_prefix: Optional prefix for cache key
    """
    def decorator(func: Callable):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            # Skip first arg if it's 'self' or 'cls'
            cache_args = args[1:] if args and hasattr(args[0], '__class__') else args
            key = f"{key_prefix}:{func.__name__}:{cache._make_key(*cache_args, **kwargs)}"
            
            # Try to get from cache
            result = cache.get(key)
            if result is not None:
                return result
            
            # Call function and cache result
            result = await func(*args, **kwargs)
            cache.set(key, result, ttl)
            return result
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            cache_args = args[1:] if args and hasattr(args[0], '__class__') else args
            key = f"{key_prefix}:{func.__name__}:{cache._make_key(*cache_args, **kwargs)}"
            
            result = cache.get(key)
            if result is not None:
                return result
            
            result = func(*args, **kwargs)
            cache.set(key, result, ttl)
            return result
        
        # Return appropriate wrapper based on function type
        import asyncio
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper
    
    return decorator


class QueryResultCache:
    """
    Specialized cache for query results
    Caches based on job_id and query hash
    """
    
    def __init__(self, max_per_job: int = 100, ttl: int = 600):
        self.max_per_job = max_per_job
        self.ttl = ttl
        self._cache: Dict[str, OrderedDict[str, CacheEntry]] = {}
    
    def _query_hash(self, query: str) -> str:
        """Hash a query for caching"""
        normalized = ' '.join(query.lower().split())
        return hashlib.md5(normalized.encode()).hexdigest()[:12]
    
    def get(self, job_id: str, query: str) -> Optional[Dict]:
        """Get cached query result"""
        if job_id not in self._cache:
            return None
        
        query_hash = self._query_hash(query)
        if query_hash not in self._cache[job_id]:
            return None
        
        entry = self._cache[job_id][query_hash]
        if entry.is_expired():
            del self._cache[job_id][query_hash]
            return None
        
        entry.hits += 1
        return entry.value
    
    def set(self, job_id: str, query: str, result: Dict):
        """Cache query result"""
        if job_id not in self._cache:
            self._cache[job_id] = OrderedDict()
        
        # Evict if over limit
        while len(self._cache[job_id]) >= self.max_per_job:
            self._cache[job_id].popitem(last=False)
        
        query_hash = self._query_hash(query)
        self._cache[job_id][query_hash] = CacheEntry(
            value=result,
            created_at=time.time(),
            ttl=self.ttl
        )
    
    def invalidate_job(self, job_id: str):
        """Invalidate all cache for a job"""
        if job_id in self._cache:
            del self._cache[job_id]
    
    def get_stats(self) -> Dict:
        """Get cache statistics"""
        total_entries = sum(len(v) for v in self._cache.values())
        return {
            "jobs_cached": len(self._cache),
            "total_entries": total_entries,
            "ttl_seconds": self.ttl
        }


# Global query cache
query_cache = QueryResultCache()
