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
from .msg import response


class WorkerConnection:
    context: Ctx
    con_s: str
    dealer: Socket
    service_name: bytes
    identity: bytes
    heartbeat_task: asyncio.Task
    listen_task: asyncio.Task
    liveliness_s: float
    work_q: asyncio.Queue
    q_length: int
    active_workers: int

    async def serve(self, handler: Handler) -> None:
        async def do_work() -> None:
            work: Work = await self.work_q.get()
            if self.active_workers >= self.q_length:
                return
            self.active_workers += 1
            await self._send(ack(work.req_id))
            reply = await handler(work.body)
            await self._send(response(work.req_id, reply))
            self.active_workers -= 1
            return
        await asyncio.gather(*[
            run_as_forever_task(do_work)
            for i in range(self.q_length + 1)])
        return

    def __init__(
            self, *,
            con_s: str,
            service_name: bytes,
            liveliness: int,
            q_length: int = 50
    ):
        self.con_s = con_s
        self.service_name = service_name
        self.identity = os.urandom(8)
        self.liveliness_s = liveliness / 1000.0
        self.q_length = q_length
        self.active_workers = 0
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
        logging.info("worker connected to %s", self.con_s)
        self.work_q = asyncio.Queue(1)
        await self._setup_heartbeat()
        await self._setup_listen()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
        if self.dealer:
            self.dealer.close(0)
        if self.context:
            self.context.destroy(0)
        return

    async def _send(self, frames: Frames) -> None:
        self.dealer.send_multipart(frames)
        return

    async def _setup_heartbeat(self) -> None:
        async def do_heartbeat():
            await self._send(heartbeat(self.service_name))
            await asyncio.sleep(self.liveliness_s)
            return
        self.heartbeat_task = run_as_forever_task(do_heartbeat)
        return

    async def _setup_listen(self) -> None:
        async def do_listen():
            frames = await self.dealer.recv_multipart()
            assert b"" == frames[0]
            work = Work(
                req_id=frames[1],
                body=frames[2:])
            await self.work_q.put(work)
            return
        self.listen_task = run_as_forever_task(do_listen)
        return
