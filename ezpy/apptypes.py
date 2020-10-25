from typing import Awaitable
from typing import Callable
from typing import List

import zmq.asyncio

Ctx = zmq.asyncio.Context
Frames = List[bytes]
Handler = Callable[[Frames],
                   Awaitable[Frames]]

Socket = zmq.asyncio.Socket
Poller = zmq.asyncio.Poller
