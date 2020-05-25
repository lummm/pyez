from typing import Callable

import zmq
import zmq.asyncio

from ez_arch_worker.lib.app import Ctx
from ez_arch_worker.lib.app import Frames
import ez_arch_worker.lib.protoc as protoc


EzClient = Callable[[str, Frames], Frames]

DEFAULT_TIMEOUT_MS = 3000
DEFAULT_ATTEMPTS = 3


async def single_req(
        c: Ctx,
        host: str,
        port: int,
        frames: Frames,
        timeout_ms: int = DEFAULT_TIMEOUT_MS
) -> Frames:
    con_s = "tcp://{}:{}".format(host, port)
    socket = c.socket(zmq.REQ)
    socket.connect(con_s)
    req_frames = [protoc.CLIENT] + frames
    socket.setsockopt(zmq.RCVTIMEO, timeout_ms)
    socket.send_multipart(req_frames)
    res = await socket.recv_multipart()
    return res


async def full_req(
        c: Ctx,
        host: str,
        port: int,
        frames: Frames,
        timeout_ms: int = DEFAULT_TIMEOUT_MS,
        attempts: int = DEFAULT_ATTEMPTS
) -> Frames:
    attempt = 0
    res: Frames = []
    while not res:
        try:
            res = await single_req(c, host, port, frames, timeout_ms)
        except Exception as e:
            if attempt < attempts:
                attempt = attempt + 1
            else:
                raise e
    return res


async def new_client(
        host: str,
        port: int
) -> EzClient:
    ctx = zmq.asyncio.Context()

    async def r(
            frames: Frames,
            timeout_ms=DEFAULT_TIMEOUT_MS,
            retries=DEFAULT_ATTEMPTS
    ):
        return await full_req(ctx, host, port, frames, timeout_ms, retries)
    return r
