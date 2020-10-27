import asyncio
import logging
from typing import Dict
from typing import List
from typing import Literal
import os

import zmq

from .apptypes import Ctx
from .apptypes import Frames
from .apptypes import Socket
from .gen import run_as_forever_task
from .msg import request


DEFAULT_TIMEOUT = 5000

ResponseStatus = Literal["OK", "EZ_ERR", "SERVICE_ERR"]


def get_req_id() -> bytes:
    return os.urandom(4)


class ClientConnection:
    context: Ctx
    con_s: str
    dealer: Socket
    identity: bytes
    listen_task: asyncio.Task
    responses: Dict[bytes, asyncio.Queue]

    def __init__(self, con_s: str,):
        self.con_s = con_s
        self.identity = os.urandom(8)
        self.responses = {}
        self.listen_task = None
        return

    async def __aenter__(self):
        self.context = Ctx()
        self.dealer = self.context.socket(zmq.DEALER)
        self.dealer.setsockopt(zmq.IDENTITY, self.identity)
        self.dealer.connect(self.con_s)
        logging.info("client connected to %s", self.con_s)
        await self._setup_response_listener()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self.dealer:
            self.dealer.close(0)
        if self.context:
            self.context.destroy(0)
        return

    async def _setup_response_listener(self) -> None:
        async def do_listen():
            frames = await self.dealer.recv_multipart()
            assert b"" == frames[0]
            req_id = frames[1]
            response = frames[2:]
            if req_id not in self.responses:
                logging.warn("received response for %s after request expired",
                             req_id)
                return
            self.responses[req_id].put_nowait(response)
            return
        self.listen_task = run_as_forever_task(do_listen)
        return

    async def req(
            self,
            service_name: bytes,
            body: Frames,
            timeout: int = DEFAULT_TIMEOUT
    ) -> List[bytes]:
        req_id = get_req_id()
        self.responses[req_id] = asyncio.Queue(1)
        self.dealer.send_multipart(
            request(req_id, service_name, body))
        res: Frames = []
        try:
            res = await asyncio.wait_for(self.responses[req_id].get(),
                                         timeout / 1000.0)
        except asyncio.TimeoutError:
            logging.warn("req %s timed out", req_id)
            return [b"EZ_ERR", b"TIMEOUT"]
        finally:
            self.responses.pop(req_id, None)
        return res
