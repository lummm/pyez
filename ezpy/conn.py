import asyncio
import logging
import os

import zmq

from .apptypes import Ctx
from .apptypes import Frames
from .apptypes import Handler
from .apptypes import Socket
from .gen import run_as_forever_task
from .msg import heartbeat


class Connection:
    context: Ctx
    con_s: str
    dealer: Socket
    service_name: bytes
    identity: bytes
    handler: Handler
    heartbeat_task: asyncio.Task
    liveliness_s: int

    def __init__(
            self, *,
            con_s: str,
            service_name: bytes,
            livelieness_s: int
    ):
        self.con_s = con_s
        self.service_name = service_name
        self.identity = os.urandom(8)
        self.liveliness_s = livelieness_s
        self.context = None
        self.dealer = None
        self.heartbeat_task = None
        return

    async def __aenter__(self) -> None:
        self.context = Ctx()
        self.dealer = self.context.socket(zmq.DEALER)
        self.dealer.setsockopt(zmq.IDENTITY,
                               self.service_name + b"-" + self.identity)
        self.dealer.connect(self.con_s)
        logging.info("dealer connected to %s", self.con_s)
        await self.setup_heartbeat()
        return

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
        if self.dealer:
            self.dealer.close(0)
        if self.context:
            self.context.destroy(0)
        return

    async def connect(self) -> None:
        self.context = Ctx()
        self.dealer = self.context.socket(zmq.DEALER)
        self.dealer.setsockopt(zmq.IDENTITY,
                               self.service_name + b"-" + self.identity)
        self.dealer.connect(self.con_s)
        # self.poller = Poller()
        # self.poller.register(self.dealer, zmq.POLLIN)
        logging.info("dealer connected to %s", self.con_s)
        await self.setup_heartbeat()
        return

    async def send(self, frames: Frames) -> None:
        self.dealer.send_multipart(frames)
        return

    async def setup_heartbeat(self) -> None:
        async def do_heartbeat():
            logging.info("heartbeat")
            await self.send(heartbeat(self.service_name))
            await asyncio.sleep(self.liveliness_s)
            return
        self.heartbeat_task = run_as_forever_task(do_heartbeat)
        return
