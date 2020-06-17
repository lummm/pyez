from typing import Any
from typing import Awaitable
from typing import Callable
from typing import List

from types import SimpleNamespace

import zmq.asyncio


DEFAULT_POLL_INTERVAL_MS = 3000

Ctx = zmq.asyncio.Context
Frames = List[bytes]
Handler = Callable[[Frames],
                   Awaitable[Frames]]


class App(SimpleNamespace):
    c: zmq.asyncio.Context = zmq.asyncio.Context()
    con_s: str = ""
    dealer: zmq.asyncio.Socket = None
    poller: zmq.asyncio.Poller = zmq.asyncio.Poller()
    poll_interval_ms: int = DEFAULT_POLL_INTERVAL_MS
    service_name: bytes = b""


app: App
handler: Handler


def init(
        *,
        con_s: str,
        handler_impl: Handler,
        service_name: str
) -> None:
    global app
    global handler
    app = App(
        con_s=con_s,
        service_name=service_name
    )
    handler = handler_impl
    return
