#!/usr/bin/env python3

import asyncio
import json
import logging
import os
import time
from typing import Dict
from typing import List
from typing import NamedTuple
from typing import Tuple

import zmq.asyncio

import ez_arch_worker.lib.protoc as protoc

import framework


REQUEST_TIMEOUT_S = 5


class State(NamedTuple):
    addr: bytes = b""
    router: zmq.asyncio.Socket = None
    service_name: bytes = b""


class TopLevelMsg(NamedTuple):
    addr: bytes
    msg_type: bytes
    cmd_type: bytes             # only for WORKER
    dest_service: bytes         # only for CLIENT
    body: List[bytes]


def parse_top_msg(frames: List[bytes]) -> TopLevelMsg:
    addr = frames[0]
    assert frames[1] == b""
    msg_type = frames[2]
    desc = frames[3]
    body = frames[4:]
    if msg_type == protoc.WORKER:
        return TopLevelMsg(addr=addr, msg_type=msg_type, cmd_type=desc,
                           body=body, dest_service=None)
    # else CLIENT
    return TopLevelMsg(addr=addr, msg_type=msg_type, cmd_type=None, body=body,
                       dest_service=desc)


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.DEBUG,
        format=f"%(asctime)s.%(msecs)03d "
        "%(levelname)s %(module)s - %(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return


async def wait_msg(
        s: State,
        msg_cmd_types: List[Tuple[bytes, bytes]],
        timeout: int
) -> TopLevelMsg:
    poller = zmq.asyncio.Poller()
    poller.register(s.router, zmq.POLLIN)
    exp = time.time() + timeout
    while time.time() < exp:
        res = await poller.poll(500)
        if not res:
            continue
        frames = await s.router.recv_multipart()
        # logging.info("frames: %s", frames)
        msg = parse_top_msg(frames)
        if (msg.msg_type, msg.cmd_type) not in msg_cmd_types:
            continue
        return msg
    raise Exception("timed out waiting for msg")


async def wait_online(
        s: State
) -> bytes:                     # addr
    heartbeat = await wait_msg(s, [(protoc.WORKER, protoc.HEARTBEAT)], 5)
    service_name = heartbeat.body[0]
    assert service_name == s.service_name
    addr = heartbeat.addr
    return addr


async def wait_response(
        s: State,
        return_addr: bytes,
        mocker
) -> List[bytes]:
    exp = time.time() + REQUEST_TIMEOUT_S
    while time.time() < exp:
        res = await wait_msg(
            s, [(protoc.WORKER, protoc.REPLY), (protoc.CLIENT, None)], 1
        )
        logging.info("got res: %s", res)
    raise Exception("no response within timeout")


def send_work(
        s: State,
        work: List[bytes]
) -> bytes:                     # mock return address to a client
    return_addr = os.urandom(2)
    frames = [s.addr, b"", return_addr] + work
    logging.info("sending frames: %s", frames)
    s.router.send_multipart(frames)
    return return_addr


async def request(
        s: State,
        url: str,
        body: Dict
) -> List[bytes]:
    work = [
        url.encode("utf-8"),
        json.dumps(body).encode("utf-8")
    ]
    return_addr = send_work(s, work)
    res = await wait_response(s, return_addr, lambda x: x)
    return res


async def main(test_port: int, test_service_name: bytes):
    setup_logging()
    router = await framework.init_input_output(test_port)
    s = State(
        router=router,
        service_name=test_service_name
    )
    addr = await wait_online(s)
    s = s._replace(addr=addr)
    res = await request(s, "/token/issue",
                        {"user_id": "hihih", "token_type": "SESSION"}
    )
    # now, you need a way to mock sub calls,
    logging.info("res: %s", res)
    return


if __name__ == "__main__":
    import sys
    test_port = int(sys.argv[1])
    test_service_name = sys.argv[2].encode("utf-8")
    asyncio.run(main(
        test_port, test_service_name
    ))
