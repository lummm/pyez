import asyncio

from ez_arch_worker.lib import worker
from ez_arch_worker.lib import apptypes


Frames = apptypes.Frames
Handler = apptypes.Handler


async def run_worker(
        service_name: bytes,
        service_port: int,
        handler: Handler
)-> None:
    loop = asyncio.get_running_loop()
    loop.create_task(worker.start_worker(
        service_name,
        service_port,
        handler
    ))
    return
