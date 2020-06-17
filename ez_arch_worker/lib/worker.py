import asyncio
import logging

from ez_arch_worker.lib.app import app
from ez_arch_worker.lib.app import handler
from ez_arch_worker.lib.app import Frames
import ez_arch_worker.lib.msg as msg


async def handle(
        work: Frames,
        return_addr: bytes,
        request_id: bytes
) -> None:
    try:
        reply = await handler(work)
        msg.send_response(return_addr, request_id, reply)
    except Exception as e:
        loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
        logging.exception("worker died: %s", e)
        loop.stop()
    return


async def listen_loop_body(
        loop: asyncio.AbstractEventLoop
) -> None:
    msg.send_heartbeat()
    items = await app.poller.poll(app.poll_interval_ms)
    for socket, _event in items:
        frames = await socket.recv_multipart()
        assert b"" == frames[0]
        client_return_addr = frames[1]
        request_id = frames[2]
        req_body = frames[3:]
        loop.create_task(
            handle(req_body, client_return_addr, request_id))
    return


async def run_listen_loop() -> None:
    loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
    while True:
        try:
            await listen_loop_body(loop)
        except Exception as e:
            logging.exception("worker died: %s", e)
            loop.stop()
            return
    return


async def run_worker() -> None:
    try:
        await msg.connect()
    except Exception as e:
        logging.exception("failed to connect: %s", e)
        loop = asyncio.get_event_loop()
        loop.stop()
        return
    await run_listen_loop()
    return
