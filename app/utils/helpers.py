import asyncio
from typing import Any

def run_async(func, *args, **kwargs):
    try:
        return asyncio.run(func(*args, **kwargs))
    except RuntimeError:
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(func(*args, **kwargs))

# Safe getter for multiple possible column names


def get_val(row, *keys, fallback: Any =""):
    for key in keys:
        val = row.get(key)
        if val not in (None, ""):
            return val
    return fallback
