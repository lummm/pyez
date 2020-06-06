import asyncio
import logging
import time
from typing import Awaitable
from typing import Callable
from typing import Dict
from typing import Tuple
from types import SimpleNamespace

import zmq
import zmq.asyncio

from ez_arch_worker.lib.app import Ctx
from ez_arch_worker.lib.app import Frames
import ez_arch_worker.lib.protoc as protoc


EzClient = Callable[[Frames, int, int], Awaitable[Frames]]

DEFAULT_TIMEOUT_MS = 5000
DEFAULT_ATTEMPTS = 2
DEFAULT_SOCKET_POOL = 100


class ClientState(SimpleNamespace):
    ctx: zmq.asyncio.Context
    con_s: str
    socket: zmq.asyncio.Socket
    responses: Dict[bytes, asyncio.Queue]


def reconnect(state: ClientState) -> None:
    if state.socket:
        state.socket.setsockopt(zmq.LINGER, 0)
        state.socket.close()
    socket = state.ctx.socket(zmq.DEALER)
    socket.connect(state.con_s)
    logging.info("dealer connected to %s", state.con_s)
    state.socket = socket
    return


def get_req_id() -> bytes:
    return b"%f" % time.time()


async def single_req(
        state: ClientState,
        req_id: bytes,
        frames: Frames,
        timeout_ms: int = DEFAULT_TIMEOUT_MS
) -> Frames:
    req_frames = [b"", protoc.CLIENT, req_id] + frames
    state.socket.send_multipart(req_frames)
    res = await asyncio.wait_for(
        state.responses[req_id].get(), timeout_ms / 1000.0
    )
    return res


async def full_req(
        state: ClientState,
        frames: Frames,
        timeout_ms: int = DEFAULT_TIMEOUT_MS,
        attempts: int = DEFAULT_ATTEMPTS
) -> Tuple[ClientState, Frames]:
    attempt = 1
    req_id = get_req_id()
    res: Frames = []
    state.responses[req_id] = asyncio.Queue(1)
    while not res:
        try:
            res = await single_req(state, req_id, frames, timeout_ms)
            state.responses.pop(req_id, None)
        except Exception as e:
            reconnect(state)
            attempt = attempt + 1
            if attempt > attempts:
                state.responses.pop(req_id, None)
                raise e
    return state, res


async def listen_for_responses(state: ClientState) -> None:
    loop = asyncio.get_event_loop()
    while True:
        try:
            res = await state.socket.recv_multipart()
            assert b"" == res[0]
            req_id = res[1]
            response = res[2:]
            if req_id not in state.responses:
                logging.error("received response for %s after request expired",
                              req_id)
                return
            state.responses[req_id].put_nowait(response)
        except Exception as e:
            logging.exception("died handling response: %s", e)
            loop.stop()
            return
    return


async def new_client(
        host: str,
        port: int
) -> EzClient:
    state = ClientState(
        ctx=zmq.asyncio.Context(),
        con_s="tcp://{}:{}".format(host, port),
        socket=None,
        responses={},
    )
    reconnect(state)
    loop = asyncio.get_event_loop()
    loop.create_task(listen_for_responses(state))

    async def r(
            frames: Frames,
            timeout_ms: int = DEFAULT_TIMEOUT_MS,
            retries: int = DEFAULT_ATTEMPTS
    ):
        nonlocal state
        state, res = await full_req(state, frames, timeout_ms, retries)
        return res
    return r
