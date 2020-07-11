import asyncio
import os
import logging
import time
from typing import Callable
from typing import Dict
from typing import List
from typing import Tuple
from types import SimpleNamespace

import zmq
import zmq.asyncio

import ez_arch_worker.lib.protoc as protoc


POLL_INTERVAL_MS = 1000
WORKER_ONLINE_TIMEOUT_S = 7

Mocks = Callable[[List[bytes]], List[bytes]]


class App(SimpleNamespace):
    EZ_INPUT_PORT: int
    EZ_WORKER_PORT: int
    service_name: bytes
    ctx: zmq.asyncio.Context
    mocks: Mocks
    poller: zmq.asyncio.Poller
    input_router: zmq.asyncio.Socket
    worker_addr: bytes = b""
    worker_router: zmq.asyncio.Socket


app = App()


async def reconnect(timeout_s: float) -> None:
    if getattr(app, "ctx", None):
        app.ctx.destroy(0)
    ctx = zmq.asyncio.Context()
    poller = zmq.asyncio.Poller()
    input_router = ctx.socket(zmq.ROUTER)
    worker_router = ctx.socket(zmq.ROUTER)

    input_router.bind("tcp://*:{}".format(app.EZ_INPUT_PORT))
    logging.info("input listening at %s", app.EZ_INPUT_PORT)
    poller.register(input_router, zmq.POLLIN)

    worker_router.bind("tcp://*:{}".format(app.EZ_WORKER_PORT))
    logging.info("worker router listening at %s", app.EZ_WORKER_PORT)
    poller.register(worker_router, zmq.POLLIN)

    app.ctx = ctx
    app.poller = poller
    app.input_router = input_router
    app.worker_router = worker_router

    loop = asyncio.get_event_loop()
    timeout = time.time() + timeout_s
    try:
        while time.time() < timeout:
            frames = None
            try:
                frames = await asyncio.wait_for(
                    app.worker_router.recv_multipart(), 1)
            except asyncio.TimeoutError:
                pass
            if frames:
                await handle_worker(frames)
            if app.worker_addr:
                logging.info("service online")
                return
    except Exception as e:
        logging.exception("service failed to come online: %s", e)
        loop.stop()
        return
    logging.error("service failed to come online")
    loop.stop()
    return


async def handle_req(
        return_addr: bytes,
        frames
) -> None:
    request_id = frames[0]
    if app.service_name == frames[1]:  # the target service
        body = frames[2:]
        app.worker_router.send_multipart(
            [app.worker_addr, b"", return_addr, request_id] + body
        )
        return
    # a mock
    body = frames[1:]
    res_body = app.mocks(body)
    res = [return_addr, b"", request_id] + res_body
    handle_reply(res)
    return


def handle_reply(frames) -> None:
    app.input_router.send_multipart(frames)
    return


async def handle_input(frames) -> None:
    return_addr = frames[0]
    msg_type = frames[2]
    body = frames[3:]
    assert msg_type == protoc.CLIENT
    await handle_req(return_addr, body)
    return


def on_heartbeat(addr: bytes, body) -> None:
    assert body[0] == app.service_name
    app.worker_addr = addr
    return


async def handle_worker(frames) -> None:
    worker_addr = frames[0]
    assert b"" == frames[1]
    assert protoc.WORKER == frames[2]
    msg_type = frames[3]
    body = frames[4:]
    if msg_type == protoc.HEARTBEAT:
        on_heartbeat(worker_addr, body)
        return
    if msg_type == protoc.REPLY:
        handle_reply(body)
    return


async def route_loop() -> None:
    sockets = dict(await app.poller.poll(POLL_INTERVAL_MS))
    if app.input_router in sockets:
        frames = await app.input_router.recv_multipart()
        asyncio.create_task(handle_input(frames))
    elif app.worker_router in sockets:
        frames = await app.worker_router.recv_multipart()
        asyncio.create_task(handle_worker(frames))
    return


def endpoint_mock(
        endpoints: Dict[Tuple[bytes, bytes], Callable]
) -> Mocks:
    import json

    def do_mock(frames: List[bytes]) -> List[bytes]:
        service_name = frames[0]
        path = frames[1]
        args = json.loads(frames[2])
        if (service_name, path) not in endpoints:
            raise Exception("no mock for {} {}".format(service_name, path))
        handler = endpoints[(service_name, path)]
        return handler(**args)
    return do_mock


class Router:

    task: asyncio.Task = None

    async def start(
            self,
            service_name: bytes,
            mocks: Mocks = lambda x: [],
            online_timeout_s: int = WORKER_ONLINE_TIMEOUT_S
    ):
        if self.task:
            logging.error("already started")
            raise Exception("mock router already started")
        assert type(service_name) == bytes
        app.service_name = service_name
        app.mocks = mocks
        app.worker_addr = b""
        await reconnect(online_timeout_s)
        self.task = asyncio.create_task(self._run())
        return

    async def close(self):
        if self.task:
            self.task.cancel()
            self.task = None
        if app.ctx:
            app.ctx.destroy()
        app.worker_addr = b""
        return

    def is_running(self) -> bool:
        return not not self.task

    async def _run(self):
        logging.info("running mock router")
        try:
            while True:
                await route_loop()
        except asyncio.CancelledError:
            logging.info("routing shutting down")
            return
        return


def get_mock_router(
        *,
        ez_input_port: int,
        ez_worker_port: int,
) -> Router:
    app.EZ_INPUT_PORT = ez_input_port
    app.EZ_WORKER_PORT = ez_worker_port
    return Router()
