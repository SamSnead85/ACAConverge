"""
Redis Cache Layer for ACA DataHub
High-performance caching with Redis backend support
"""

from typing import Optional, Any, Dict, Union
from datetime import datetime, timedelta
import json
import hashlib


class RedisCacheLayer:
    """
    Redis-compatible caching layer.
    Uses in-memory fallback when Redis is not available.
    """
    
    def __init__(self, redis_url: str = None):
        self.redis_client = None
        self.connected = False
        self._memory_cache: Dict[str, dict] = {}
        
        if redis_url:
            try:
                import redis
                self.redis_client = redis.from_url(redis_url)
                self.redis_client.ping()
                self.connected = True
                print("Redis connected successfully")
            except Exception as e:
                print(f"Redis connection failed, using memory cache: {e}")
    
    def _serialize(self, value: Any) -> str:
        """Serialize value to JSON string"""
        return json.dumps(value, default=str)
    
    def _deserialize(self, value: str) -> Any:
        """Deserialize JSON string to value"""
        if value is None:
            return None
        return json.loads(value)
    
    def set(
        self, 
        key: str, 
        value: Any, 
        ttl: int = 3600,
        namespace: str = "default"
    ) -> bool:
        """
        Set a cached value with TTL.
        
        Args:
            key: Cache key
            value: Value to cache (will be JSON serialized)
            ttl: Time to live in seconds
            namespace: Cache namespace for organization
        """
        full_key = f"{namespace}:{key}"
        serialized = self._serialize(value)
        
        if self.connected and self.redis_client:
            try:
                self.redis_client.setex(full_key, ttl, serialized)
                return True
            except Exception as e:
                print(f"Redis set error: {e}")
        
        # Fallback to memory cache
        self._memory_cache[full_key] = {
            "value": serialized,
            "expires_at": datetime.utcnow() + timedelta(seconds=ttl)
        }
        return True
    
    def get(self, key: str, namespace: str = "default") -> Optional[Any]:
        """
        Get a cached value.
        
        Args:
            key: Cache key
            namespace: Cache namespace
            
        Returns:
            Cached value or None if not found/expired
        """
        full_key = f"{namespace}:{key}"
        
        if self.connected and self.redis_client:
            try:
                value = self.redis_client.get(full_key)
                return self._deserialize(value)
            except Exception as e:
                print(f"Redis get error: {e}")
        
        # Fallback to memory cache
        if full_key in self._memory_cache:
            entry = self._memory_cache[full_key]
            if entry["expires_at"] > datetime.utcnow():
                return self._deserialize(entry["value"])
            else:
                del self._memory_cache[full_key]
        
        return None
    
    def delete(self, key: str, namespace: str = "default") -> bool:
        """Delete a cached value"""
        full_key = f"{namespace}:{key}"
        
        if self.connected and self.redis_client:
            try:
                self.redis_client.delete(full_key)
                return True
            except Exception:
                pass
        
        if full_key in self._memory_cache:
            del self._memory_cache[full_key]
            return True
        
        return False
    
    def clear_namespace(self, namespace: str) -> int:
        """Clear all keys in a namespace"""
        count = 0
        
        if self.connected and self.redis_client:
            try:
                pattern = f"{namespace}:*"
                keys = self.redis_client.keys(pattern)
                if keys:
                    count = self.redis_client.delete(*keys)
                return count
            except Exception:
                pass
        
        # Memory cache fallback
        keys_to_delete = [k for k in self._memory_cache if k.startswith(f"{namespace}:")]
        for k in keys_to_delete:
            del self._memory_cache[k]
        return len(keys_to_delete)
    
    def get_or_set(
        self, 
        key: str, 
        factory_fn, 
        ttl: int = 3600,
        namespace: str = "default"
    ) -> Any:
        """
        Get cached value or compute and cache it.
        
        Args:
            key: Cache key
            factory_fn: Function to call if not cached
            ttl: Time to live in seconds
            namespace: Cache namespace
        """
        value = self.get(key, namespace)
        if value is not None:
            return value
        
        # Compute value
        value = factory_fn()
        self.set(key, value, ttl, namespace)
        return value
    
    # =========================================================================
    # Query Result Caching
    # =========================================================================
    
    def cache_query(
        self, 
        job_id: str, 
        query: str, 
        result: Any,
        ttl: int = 300
    ):
        """Cache a query result"""
        query_hash = hashlib.md5(query.encode()).hexdigest()[:16]
        key = f"query:{job_id}:{query_hash}"
        self.set(key, result, ttl, namespace="queries")
    
    def get_cached_query(self, job_id: str, query: str) -> Optional[Any]:
        """Get a cached query result"""
        query_hash = hashlib.md5(query.encode()).hexdigest()[:16]
        key = f"query:{job_id}:{query_hash}"
        return self.get(key, namespace="queries")
    
    # =========================================================================
    # Session Caching
    # =========================================================================
    
    def cache_session(self, session_id: str, user_data: dict, ttl: int = 1800):
        """Cache a user session"""
        self.set(session_id, user_data, ttl, namespace="sessions")
    
    def get_session(self, session_id: str) -> Optional[dict]:
        """Get a cached session"""
        return self.get(session_id, namespace="sessions")
    
    def invalidate_session(self, session_id: str):
        """Invalidate a session"""
        self.delete(session_id, namespace="sessions")
    
    # =========================================================================
    # Rate Limiting
    # =========================================================================
    
    def check_rate_limit(
        self, 
        identifier: str, 
        limit: int = 100, 
        window: int = 60
    ) -> tuple:
        """
        Check and update rate limit using sliding window.
        
        Args:
            identifier: User ID, IP, or API key
            limit: Maximum requests per window
            window: Window size in seconds
            
        Returns:
            (allowed: bool, remaining: int, reset_at: datetime)
        """
        key = f"rate:{identifier}"
        now = datetime.utcnow()
        
        if self.connected and self.redis_client:
            try:
                pipe = self.redis_client.pipeline()
                pipe.incr(key)
                pipe.expire(key, window)
                results = pipe.execute()
                count = results[0]
                
                remaining = max(0, limit - count)
                reset_at = now + timedelta(seconds=window)
                
                return (count <= limit, remaining, reset_at)
            except Exception:
                pass
        
        # Memory fallback
        full_key = f"ratelimit:{key}"
        if full_key not in self._memory_cache:
            self._memory_cache[full_key] = {
                "value": json.dumps({"count": 0}),
                "expires_at": now + timedelta(seconds=window)
            }
        
        entry = self._memory_cache[full_key]
        if entry["expires_at"] < now:
            entry = {
                "value": json.dumps({"count": 0}),
                "expires_at": now + timedelta(seconds=window)
            }
            self._memory_cache[full_key] = entry
        
        data = json.loads(entry["value"])
        data["count"] += 1
        entry["value"] = json.dumps(data)
        
        remaining = max(0, limit - data["count"])
        return (data["count"] <= limit, remaining, entry["expires_at"])
    
    # =========================================================================
    # Cache Statistics
    # =========================================================================
    
    def get_stats(self) -> dict:
        """Get cache statistics"""
        if self.connected and self.redis_client:
            try:
                info = self.redis_client.info()
                return {
                    "backend": "redis",
                    "connected": True,
                    "used_memory": info.get("used_memory_human"),
                    "keys": self.redis_client.dbsize(),
                    "hits": info.get("keyspace_hits", 0),
                    "misses": info.get("keyspace_misses", 0)
                }
            except Exception:
                pass
        
        # Memory cache stats
        now = datetime.utcnow()
        valid_keys = sum(1 for v in self._memory_cache.values() if v["expires_at"] > now)
        
        return {
            "backend": "memory",
            "connected": False,
            "keys": valid_keys,
            "total_entries": len(self._memory_cache)
        }
    
    def cleanup_expired(self) -> int:
        """Clean up expired entries from memory cache"""
        now = datetime.utcnow()
        expired = [k for k, v in self._memory_cache.items() if v["expires_at"] < now]
        for k in expired:
            del self._memory_cache[k]
        return len(expired)


# Singleton instance (uses memory cache if Redis not configured)
import os
redis_url = os.getenv("REDIS_URL")
cache = RedisCacheLayer(redis_url)
