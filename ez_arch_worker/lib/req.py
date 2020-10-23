import asyncio
import logging
import os
import time
from typing import Dict
from typing import Tuple
from types import SimpleNamespace

import zmq
import zmq.asyncio

from ez_arch_worker.lib.app import Frames
import ez_arch_worker.lib.protoc as protoc


DEFAULT_TIMEOUT_MS = 5000
DEFAULT_ATTEMPTS = 1
DEFAULT_SOCKET_POOL = 100


class ClientState(SimpleNamespace):
    host: str
    port: int
    ctx: zmq.asyncio.Context
    con_s: str
    socket: zmq.asyncio.Socket
    responses: Dict[bytes, asyncio.Queue]
    identity: bytes = os.urandom(8)


def reconnect(state: ClientState) -> None:
    if state.socket:
        state.socket.setsockopt(zmq.LINGER, 0)
        state.socket.close()
    socket = state.ctx.socket(zmq.DEALER)
    socket.setsockopt(zmq.IDENTITY, state.identity)
    socket.connect(state.con_s)
    logging.info("dealer connected to %s", state.con_s)
    state.socket = socket
    return


def get_req_id() -> bytes:
    return (b"%f" % time.time()) + os.urandom(1)


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
        except asyncio.TimeoutError as e:
            attempt = attempt + 1
            if attempt > attempts:
                logging.error("request timed out")
                state.responses.pop(req_id, None)
                raise e
    return state, res


async def listen_for_responses(state: ClientState) -> None:
    while True:
        res: Frames
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
        except asyncio.CancelledError:
            return
        except Exception as e:
            if res:
                logging.exception("died handling response frames: %s - %s",
                                  res, e)
            else:
                logging.exception("died handling response - %s", e)
    return


async def new_state(host: str, port: int) -> ClientState:
    state = ClientState(
        host=host,
        port=port,
        ctx=zmq.asyncio.Context(),
        con_s="tcp://{}:{}".format(host, port),
        socket=None,
        responses={},
    )
    reconnect(state)
    return state


class EzClient():
    def __init__(
            self,
            state: ClientState,
            listen_task: asyncio.Task
    ) -> None:
        self.state = state
        self.listen_task = listen_task
        return

    async def r(
            self,
            frames: Frames,
            timeout_ms: int = DEFAULT_TIMEOUT_MS,
            retries: int = DEFAULT_ATTEMPTS
    ) -> Frames:
        state, res = await full_req(self.state, frames, timeout_ms, retries)
        return res

    async def reset(self) -> None:
        await self.close()
        self.state = await new_state(self.state.host, self.state.port)
        loop = asyncio.get_event_loop()
        self.listen_task = loop.create_task(listen_for_responses(self.state))
        return

    async def close(self) -> None:
        self.listen_task.cancel()
        self.state.ctx.destroy()
        return


async def new_client(
        host: str,
        port: int
) -> EzClient:
    state = await new_state(host, port)
    loop = asyncio.get_event_loop()
    listen_task = loop.create_task(listen_for_responses(state))
    client = EzClient(state, listen_task)
    return client
