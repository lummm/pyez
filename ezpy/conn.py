import asyncio
import logging
import os

import zmq

from .apptypes import Ctx
from .apptypes import Frames
from .apptypes import Handler
from .apptypes import Socket
from .apptypes import Work
from .gen import run_as_forever_task
from .msg import heartbeat
from .msg import ack


class Connection:
    context: Ctx
    con_s: str
    dealer: Socket
    service_name: bytes
    identity: bytes
    heartbeat_task: asyncio.Task
    listen_task: asyncio.Task
    liveliness_s: int
    work_q: asyncio.Queue

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
        self.listen_task = None
        self.work_q = None
        return

    async def __aenter__(self):
        self.context = Ctx()
        self.dealer = self.context.socket(zmq.DEALER)
        self.dealer.setsockopt(zmq.IDENTITY,
                               self.service_name + b"-" + self.identity)
        self.dealer.connect(self.con_s)
        logging.info("dealer connected to %s", self.con_s)
        self.work_q = asyncio.Queue(1)
        await self.setup_heartbeat()
        await self.setup_listen()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
        if self.dealer:
            self.dealer.close(0)
        if self.context:
            self.context.destroy(0)
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

    async def setup_listen(self) -> None:
        async def do_listen():
            logging.info("my dealer: %s", self.dealer)
            frames = await self.dealer.recv_multipart()
            assert b"" == frames[0]
            work = Work(
                return_addr=frames[1],
                req_id=frames[2],
                body=frames[3:]
            )
            await self.work_q.put(work)
            return
        self.listen_task = run_as_forever_task(do_listen)
        return

    async def serve(self, handler: Handler) -> None:
        while True:
            work: Work = await self.work_q.get()
            await self.send(ack(work.req_id))
            res = await handler(work.body)
            # send the response...
        return
