import asyncio
import logging
from typing import Awaitable
from typing import Callable
from typing import NamedTuple

import zmq
import zmq.asyncio

from ez_arch_worker.lib.app import Ctx
from ez_arch_worker.lib.app import Frames
import ez_arch_worker.lib.protoc as protoc


EzClient = Callable[[Frames, int, int], Awaitable[Frames]]

DEFAULT_TIMEOUT_MS = 5000
DEFAULT_ATTEMPTS = 2
DEFAULT_SOCKET_POOL = 100


class ClientState(NamedTuple):
    ctx: zmq.asyncio.Context
    con_s: str
    sockets: asyncio.Queue


def new_socket(state: ClientState) -> zmq.asyncio.Socket:
    socket = state.ctx.socket(zmq.DEALER)
    socket.connect(state.con_s)
    return socket


async def single_req(
        socket: zmq.asyncio.Socket,
        frames: Frames,
        timeout_ms: int = DEFAULT_TIMEOUT_MS
) -> Frames:
    req_frames = [b"", protoc.CLIENT] + frames
    socket.setsockopt(zmq.RCVTIMEO, timeout_ms)
    socket.send_multipart(req_frames)
    res = await socket.recv_multipart()
    return res


async def full_req(
        state: ClientState,
        frames: Frames,
        timeout_ms: int = DEFAULT_TIMEOUT_MS,
        attempts: int = DEFAULT_ATTEMPTS
) -> Frames:
    attempt = 1
    res: Frames = []
    while not res:
        socket = await state.sockets.get()
        try:
            res = await single_req(socket, frames, timeout_ms)
            state.sockets.put_nowait(socket)
        except Exception as e:
            socket.setsockopt(zmq.LINGER, 0)
            socket.close()
            state.sockets.put_nowait(new_socket(state))
            attempt = attempt + 1
            if attempt > attempts:
                raise e
    return res


async def new_client(
        host: str,
        port: int,
        socket_pool: int = DEFAULT_SOCKET_POOL
) -> EzClient:
    state = ClientState(
        ctx=zmq.asyncio.Context(),
        con_s="tcp://{}:{}".format(host, port),
        sockets=asyncio.Queue(socket_pool)
    )
    for i in range(socket_pool):
        state.sockets.put_nowait(new_socket(state))
    logging.info("opened %s DEALER sockets", socket_pool)

    async def r(
            frames: Frames,
            timeout_ms: int = DEFAULT_TIMEOUT_MS,
            retries: int = DEFAULT_ATTEMPTS
    ):
        res = await full_req(state, frames, timeout_ms, retries)
        return res
    return r
