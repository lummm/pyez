import asyncio
from typing import Awaitable
from typing import Callable
from typing import NamedTuple
from typing import Tuple

import zmq.asyncio


Frames = Tuple[bytes, ...]
Handler = Callable[[Frames], Awaitable[Frames]]


class App(NamedTuple):
    c: zmq.asyncio.Context
    poller: zmq.asyncio.Poller
    in_s: zmq.Socket
    out_s: zmq.Socket
    service_name: bytes
    service_port: int
    handler: Handler
