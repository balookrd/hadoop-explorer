import time
import threading
from collections import OrderedDict
from typing import Optional


class L1RevokedTokenCache:
    """
    Потокобезопасный L1 In-Memory LRU-кэш для отозванных токенов с TTL.
    Обеспечивает защиту от Fail-Open при сбоях Redis/БД и O(1) скорость проверки.
    """

    def __init__(self, max_size: int = 10000):
        self.max_size = max_size
        self._cache: OrderedDict[str, float] = OrderedDict()
        self._lock = threading.Lock()

    def add(self, key: str, expires_at_ts: Optional[float] = None):
        if not key:
            return
        now = time.time()
        exp_ts = expires_at_ts if (expires_at_ts is not None and expires_at_ts > 0) else (now + 86400.0)
        with self._lock:
            if len(self._cache) >= self.max_size and key not in self._cache:
                self._cache.popitem(last=False)
            self._cache[key] = exp_ts
            self._cache.move_to_end(key)

    def contains(self, key: str) -> bool:
        if not key:
            return False
        now = time.time()
        with self._lock:
            if key not in self._cache:
                return False
            exp_ts = self._cache[key]
            if exp_ts < now:
                del self._cache[key]
                return False
            self._cache.move_to_end(key)
            return True

    def cleanup(self):
        now = time.time()
        with self._lock:
            expired_keys = [k for k, exp in self._cache.items() if exp < now]
            for k in expired_keys:
                del self._cache[k]

    def clear(self):
        with self._lock:
            self._cache.clear()

    def __len__(self) -> int:
        with self._lock:
            return len(self._cache)
