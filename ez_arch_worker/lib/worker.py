import asyncio
import logging

from ez_arch_worker.lib.app import App
from ez_arch_worker.lib.app import Frames
import ez_arch_worker.lib.msg as msg


async def handle(
        app: App,
        work: Frames,
        return_addr: bytes,
        request_id: bytes
) -> None:
    reply = await app.handler(app.impl_state, work)
    msg.send_response(app, return_addr, request_id, reply)
    return


async def listen_loop_body(
        app: App,
        loop: asyncio.AbstractEventLoop
) -> None:
    msg.send_heartbeat(app)
    items = await app.poller.poll(app.poll_interval_ms)
    for socket, _event in items:
        frames = await socket.recv_multipart()
        assert b"" == frames[0]
        client_return_addr = frames[1]
        request_id = frames[2]
        req_body = frames[3:]
        loop.create_task(
            handle(app, req_body, client_return_addr, request_id))
    return


async def run_listen_loop(app: App) -> None:
    loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
    while True:
        try:
            await listen_loop_body(app, loop)
        except Exception as e:
            logging.exception("worker died: %s", e)
            loop.stop()
            return
    return


async def run_worker(app: App) -> None:
    try:
        app = await msg.connect(app)
    except Exception as e:
        logging.exception("failed to connect: %s", e)
        loop = asyncio.get_event_loop()
        loop.stop()
        return
    await run_listen_loop(app)
    return
