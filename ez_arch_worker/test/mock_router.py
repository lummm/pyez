import asyncio
import os
import logging
from typing import NamedTuple
from types import SimpleNamespace

import zmq
import zmq.asyncio

import protoc


POLL_INTERVAL_MS = 3000
WORKER_LIFETIME_S = 4000


class _ENV(NamedTuple):
    EZ_INPUT_PORT: int = int(os.environ["EZ_INPUT_PORT"])
    EZ_WORKER_PORT: int = int(os.environ["EZ_WORKER_PORT"])
    SERVICE_NAME: str = os.environ["SERVICE_NAME"]


class App(SimpleNamespace):
    service_name: bytes
    ctx: zmq.asyncio.Context
    poller: zmq.asyncio.Poller
    input_router: zmq.asyncio.Socket
    worker_addr: bytes
    worker_router: zmq.asyncio.Socket


ENV = _ENV()
app = App()


async def reconnect() -> None:
    if getattr(app, "poller", None):
        [app.poller.unregister(s) for s in app.poller.sockets]
    if getattr(app, "ctx", None):
        app.ctx.destroy(0)
    ctx = zmq.asyncio.Context()
    poller = zmq.asyncio.Poller()
    input_router = ctx.socket(zmq.ROUTER)
    worker_router = ctx.socket(zmq.ROUTER)

    input_router.bind("tcp://*:{}".format(ENV.EZ_INPUT_PORT))
    logging.info("input listening at %s", ENV.EZ_INPUT_PORT)
    poller.register(input_router, zmq.POLLIN)

    worker_router.bind("tcp://*:{}".format(ENV.EZ_WORKER_PORT))
    logging.info("worker router listening at %s", ENV.EZ_WORKER_PORT)
    poller.register(worker_router, zmq.POLLIN)

    app.ctx = ctx
    app.poller = poller
    app.input_router = input_router
    app.worker_router = worker_router
    return


async def handle_req(
        return_addr: bytes,
        frames
) -> None:
    request_id = frames[0]
    assert app.service_name == frames[1]
    body = frames[2:]
    app.worker_router.send_multipart(
        [app.worker_addr, b"", return_addr, request_id] + body
    )
    return


async def handle_reply(frames) -> None:
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
        await handle_reply(body)
    return


async def route_loop() -> None:
    sockets = dict(await app.poller.poll(POLL_INTERVAL_MS))
    if app.input_router in sockets:
        frames = await app.input_router.recv_multipart()
        asyncio.create_task(handle_input(frames))
    if app.worker_router in sockets:
        frames = await app.worker_router.recv_multipart()
        asyncio.create_task(handle_worker(frames))
    return


async def run_mock_router():
    await reconnect()
    while True:
        await route_loop()
    return


async def main():
    app.service_name = ENV.SERVICE_NAME.encode("utf-8")
    logging.basicConfig(level="INFO")
    await run_mock_router()


if __name__ == '__main__':
    asyncio.run(main())
