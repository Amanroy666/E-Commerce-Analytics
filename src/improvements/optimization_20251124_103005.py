"""
Performance optimization module
"""
from typing import List, Dict
import time

def batch_processor(items: List[Dict], batch_size: int = 100):
    """Process items in batches for better performance"""
    for i in range(0, len(items), batch_size):
        batch = items[i:i+batch_size]
        yield batch

def cached_operation(func):
    """Simple caching decorator"""
    cache = {}
    def wrapper(*args):
        if args in cache:
            return cache[args]
        result = func(*args)
        cache[args] = result
        return result
    return wrapper
