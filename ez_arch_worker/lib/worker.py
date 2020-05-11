import asyncio
import logging

from ez_arch_worker.lib.app import App
from ez_arch_worker.lib.app import Handler
from ez_arch_worker.lib.app import State
import ez_arch_worker.lib.msg as msg


async def loop_body(
        app: App
)-> App:
    msg.send_heartbeat(app)
    items = await app.poller.poll(app.poll_interval_ms)
    for socket, _event in items:
        frames = await socket.recv_multipart()
        assert b"" == frames[0]
        client_return_addr = frames[1]
        req_body = frames[2:]
        state, reply = await app.handler(app.impl_state, req_body)
        app = app._replace(impl_state = state)
        msg.send_response(app, client_return_addr, reply)
    return app


async def run_main_loop(
        app: App
)-> None:
    loop = asyncio.get_running_loop()
    try:
        app = await msg.connect(app)
    except Exception as e:
        logging.exception("failed to connect: %s", e)
        loop.stop()
        return
    while True:
        try:
            app = await loop_body(app)
        except Exception as e:
            logging.exception("worker died: %s", e)
            loop.stop()
            return
    return
