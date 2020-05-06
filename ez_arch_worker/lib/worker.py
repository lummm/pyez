import asyncio
import logging

import zmq

from ez_arch_worker.lib.apptypes import App
from ez_arch_worker.lib.apptypes import Frames
from ez_arch_worker.lib.apptypes import Handler
from ez_arch_worker.lib.env import ENV


async def handle_work(
        app: App,
        frames: Frames
)-> None:
    logging.debug("recvd frames: %s", frames)
    router_addr = frames[0]
    assert app.service_name == frames[1]
    return_addr = frames[2]
    assert b"" == frames[3]
    body = frames[4:]
    response = await app.handler(body)
    app.out_s.send_multipart(
        [b"", b"RESP", return_addr] + list(response)
    )
    return


async def loop_body(
        app: App
)-> None:
    items = await app.poller.poll(ENV.POLL_INTERVAL_S * 1000)
    if not items:
        return
    items_dict = dict(items)
    if items_dict.get(app.in_s):
        frames = await app.in_s.recv_multipart()
        asyncio.create_task(handle_work(app, tuple(frames)))
    return


async def reconnect(
        app: App
)-> App:
    if app.in_s:
        app.poller.unregister(app.in_s)
        app.in_s.close()
    if app.out_s:
        app.out_s.close()
    poller = zmq.asyncio.Poller()
    router = app.c.socket(zmq.ROUTER)
    router_con_s = "tcp://%s:%s" % (
        ENV.ROUTER_HOST, app.service_port
    )
    router.connect(router_con_s)
    logging.info("router connected to %s", router_con_s)
    dealer = app.c.socket(zmq.DEALER)
    dealer_con_s = "tcp://%s:%s" % (
        ENV.ROUTER_HOST, ENV.ROUTER_PORT
    )
    dealer.connect(dealer_con_s)
    logging.info("dealer connected to %s", dealer_con_s)
    poller.register(router, zmq.POLLIN)
    return app._replace(
        in_s = router,
        out_s = dealer,
        poller = poller,
    )


async def run_main_loop(
        app: App
)-> None:
    while True:
        try:
            await loop_body(app)
        except Exception as e:
            logging.exception("worker died: %s", e)
            loop = asyncio.get_running_loop()
            loop.stop()
    return


async def start_worker(
        service_name: bytes,
        service_port: int,
        handler: Handler
)-> None:
    app = App(
        c = zmq.asyncio.Context(),
        in_s = None,
        out_s = None,
        poller = None,
        service_name = service_name,
        service_port = service_port,
        handler = handler,
    )
    app = await reconnect(app)
    await run_main_loop(app)
    return
