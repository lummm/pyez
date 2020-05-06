import asyncio
import logging

from ez_arch_worker.lib import worker
from ez_arch_worker.lib import apptypes


Frames = apptypes.Frames
Handler = apptypes.Handler



async def run_worker(
        service_name: bytes,
        service_port: int,
        handler: Handler,
        router_host: str = "127.0.0.1",
        router_port: int = 5555,
        poll_interval_s: int = apptypes.DEFAULT_POLL_INTERVAL_S
)-> None:
    app = apptypes.App(
        in_s = None,
        out_s = None,
        poller = None,
        service_name = service_name,
        service_port = service_port,
        handler = handler,
        poll_interval_s = poll_interval_s,
        router_host = router_host,
        router_port = router_port,
    )
    await worker.run_main_loop(app)
    return
