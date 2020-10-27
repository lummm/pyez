import asyncio
import logging
from typing import Awaitable


def run_as_forever_task(fn: Awaitable) -> asyncio.Task:
    async def _fn():
        while True:
            try:
                await fn()
            except asyncio.CancelledError:
                return
            except Exception as e:
                logging.exception("""
---------------------------------------------------------- loop crashed with:
%s
-----------------------------------------------------------------------------
                """, e)
                asyncio.get_event_loop() \
                       .stop()
                return
    return asyncio.create_task(_fn())
