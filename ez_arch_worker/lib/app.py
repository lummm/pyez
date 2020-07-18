from typing import Awaitable
from typing import Callable
from typing import Dict
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
    handler: Handler
    dealer: zmq.asyncio.Socket = None
    poller: zmq.asyncio.Poller = zmq.asyncio.Poller()
    poll_interval_ms: int = DEFAULT_POLL_INTERVAL_MS
    req_ids: Dict[bytes, bool] = {}
    service_name: bytes = b""


state = App(handler=None)


def init(
        *,
        con_s: str,
        handler_impl: Handler,
        service_name: bytes
) -> None:
    state.con_s = con_s
    state.service_name = service_name
    state.handler = handler_impl  # type: ignore
    return
