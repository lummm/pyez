from typing import Awaitable
from typing import Callable
from typing import List
from typing import NamedTuple

import zmq.asyncio

Ctx = zmq.asyncio.Context
Frames = List[bytes]

Socket = zmq.asyncio.Socket
Poller = zmq.asyncio.Poller


class Work(NamedTuple):
    req_id: bytes
    body: Frames


Handler = Callable[[Frames],
                   Awaitable[Frames]]
