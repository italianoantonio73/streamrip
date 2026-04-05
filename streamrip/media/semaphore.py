import asyncio
from contextlib import nullcontext

from ..config import DownloadsConfig

_unlimited = nullcontext()
_global_semaphore: None | tuple[int, asyncio.Semaphore] = None
_global_retry_lock: asyncio.Lock | None = None


def global_retry_lock() -> asyncio.Lock:
    """A global lock that serializes retry attempts across all tracks.

    When multiple tracks hit CDN errors simultaneously, concurrent retries
    create burst patterns that worsen the problem. This lock ensures only
    one track retries at a time.
    """
    global _global_retry_lock
    if _global_retry_lock is None:
        _global_retry_lock = asyncio.Lock()
    return _global_retry_lock


def global_download_semaphore(c: DownloadsConfig) -> asyncio.Semaphore | nullcontext:
    """A global semaphore that limit the number of total tracks being downloaded
    at once.

    If concurrency is disabled in the config, the semaphore is set to 1.
    Otherwise it's set to `max_connections`.
    A negative `max_connections` value means there is no maximum and no semaphore is used.

    Since it is global, only one value of `max_connections` is allowed per session.
    """
    global _unlimited, _global_semaphore

    if c.concurrency:
        max_connections = c.max_connections if c.max_connections > 0 else None
    else:
        max_connections = 1

    if max_connections is None:
        return _unlimited

    if max_connections <= 0:
        raise Exception(f"{max_connections = } too small")

    if _global_semaphore is None:
        _global_semaphore = (max_connections, asyncio.Semaphore(max_connections))

    assert (
        max_connections == _global_semaphore[0]
    ), f"Already have other global semaphore {_global_semaphore}"

    return _global_semaphore[1]
