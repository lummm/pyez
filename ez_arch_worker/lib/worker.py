import asyncio
import logging

from ez_arch_worker.lib.app import App
from ez_arch_worker.lib.app import Frames
import ez_arch_worker.lib.msg as msg


async def handle(
        app: App,
        work: Frames,
        return_addr: bytes
) -> None:
    reply = await app.handler(app.impl_state, work)
    msg.send_response(app, return_addr, reply)
    return


async def run_handle_loop(app: App) -> None:
    loop = asyncio.get_event_loop()
    while True:
        try:
            client_return_addr, work = await app.q.get()
            loop.create_task(handle(app, work, client_return_addr))
        except Exception as e:
            logging.exception("worker died: %s", e)
            loop.stop()
            return
    return


async def listen_loop_body(app: App) -> None:
    msg.send_heartbeat(app)
    items = await app.poller.poll(app.poll_interval_ms)
    for socket, _event in items:
        frames = await socket.recv_multipart()
        assert b"" == frames[0]
        client_return_addr = frames[1]
        req_body = frames[2:]
        app.q.put_nowait((client_return_addr, req_body))
    return


async def run_listen_loop(app: App) -> None:
    loop = asyncio.get_running_loop()
    while True:
        try:
            await listen_loop_body(app)
        except Exception as e:
            logging.exception("worker died: %s", e)
            loop.stop()
            return
    return


async def run_worker(app: App) -> None:
    loop = asyncio.get_event_loop()
    try:
        app = await msg.connect(app)
        app = app._replace(q=asyncio.Queue())
    except Exception as e:
        logging.exception("failed to connect: %s", e)
        loop.stop()
        return
    await asyncio.gather(
        run_handle_loop(app),
        run_listen_loop(app)
    )
    return
