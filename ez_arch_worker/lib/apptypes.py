import asyncio
from typing import Awaitable
from typing import Callable
from typing import NamedTuple
from typing import Tuple

import zmq.asyncio


DEFAULT_POLL_INTERVAL_S = 3


Frames = Tuple[bytes, ...]
Handler = Callable[[Frames], Awaitable[Frames]]


class App(NamedTuple):
    router_host: str
    router_port: int
    poller: zmq.asyncio.Poller
    in_s: zmq.Socket
    out_s: zmq.Socket
    service_name: bytes
    service_port: int
    handler: Handler
    c: zmq.asyncio.Context = zmq.asyncio.Context()
    poll_interval_s: int = DEFAULT_POLL_INTERVAL_S
