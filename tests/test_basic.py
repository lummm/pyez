#!/usr/bin/env python3

import asyncio
import logging
from multiprocessing import Process
import time
from typing import NamedTuple

import zmq

import ez_arch_worker.api as ez_worker

WORKER = b"\x01"
HEARTBEAT = b"\x01"
REPLY = b"\x02"


class WorkerState(NamedTuple):
    req_count: int = 0


class TestConf(NamedTuple):
    work_dealer_port: int = 10000
    work_router_port: int = 10001
    service_name: bytes = b"TEST"


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s.%(msecs)03d "
        "%(levelname)s %(module)s - %(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return


async def worker_handler(
        state: WorkerState,
        frames: ez_worker.Frames
):
    response = [b"ECHO", b"%d" % state.req_count] + frames
    new_state = state._replace(
        req_count=state.req_count + 1
    )
    return (new_state, response)


def run_worker(conf: TestConf):
    asyncio.run(
        ez_worker.run_worker(
            service_name=conf.service_name,
            handler=worker_handler,
            initial_state=WorkerState(),
            listen_host="localhost",
            port=conf.work_router_port,
        )
    )
    return


def run_pipe(conf: TestConf):
    c = zmq.Context()
    router = c.socket(zmq.ROUTER)
    dealer = c.socket(zmq.DEALER)
    router.bind("tcp://*:{}".format(conf.work_router_port))
    dealer.bind("tcp://*:{}".format(conf.work_dealer_port))
    poller = zmq.Poller()
    poller.register(router, zmq.POLLIN)
    poller.register(dealer, zmq.POLLIN)
    while True:
        items = poller.poll()
        for socket, _event in items:
            frames = socket.recv_multipart()
            if socket == dealer:
                router.send_multipart(frames)
            if socket == router:
                dealer.send_multipart(frames)
    return


def verify_heartbeat(conf: TestConf, frames):
    assert conf.service_name == frames[0]
    logging.info("heartbeat OK")
    return


def verify_response(
        request_addr: bytes,
        request,
        frames,
        request_id: int
):
    assert request_addr == frames[0]
    assert b"" == frames[1]
    body_actual = frames[2:]
    state = WorkerState(req_count=request_id)
    _state, body = asyncio.run(worker_handler(state, request))
    if not body == body_actual:
        logging.error("expected body %s", body)
        logging.error("actual body %s", body_actual)
        raise Exception("BAD RESPONSE")
    logging.info("response OK")
    return


def send_work_request(
        router: zmq.Socket,
        router_addr: bytes,
        worker_addr: bytes,
        request
):
    router.send_multipart(
        [router_addr, worker_addr, b""] + request
    )
    return


def parse_msg(frames):
    router_addr = frames[0]
    worker_addr = frames[1]
    assert b"" == frames[2]
    assert WORKER == frames[3]
    msg_type = frames[4]
    body = frames[5:]
    return router_addr, worker_addr, msg_type, body


def run_tests(conf: TestConf):
    c = zmq.Context()
    router = c.socket(zmq.ROUTER)
    poller = zmq.Poller()
    poller.register(router, zmq.POLLIN)
    router.connect("tcp://localhost:{}".format(conf.work_dealer_port))
    items = poller.poll(5000)
    if not items:
        raise Exception("NO HEARTBEAT FROM WORKER")
    frames = router.recv_multipart()
    router_addr, worker_addr, msg_type, body = parse_msg(frames)
    assert msg_type == HEARTBEAT
    verify_heartbeat(conf, body)

    def test_request(req_id: int):
        client_addr = b"CLIENT_ADDR"
        request = [b"a", b"test", b"request"]
        send_work_request(router,
                          router_addr,
                          worker_addr,
                          [client_addr] + request)
        items = poller.poll(5000)
        if not items:
            raise Exception("NO RESPONSE FROM WORKER")
        msg_type, body = None, None
        timeout = time.time() + 5
        while msg_type != REPLY:
            if time.time() > timeout:
                raise Exception("REQUEST TIMED OUT")
            frames = router.recv_multipart()
            _router_addr, _worker_addr, msg_type, body = parse_msg(frames)
        verify_response(
            client_addr, request, body, req_id
        )
        return
    test_request(0)
    test_request(1)
    test_request(2)
    return


def main():
    setup_logging()
    logging.info("---- BEGIN ----")
    conf = TestConf()
    pipe_ps = Process(target=run_pipe, args=(conf,))
    app_ps = Process(target=run_worker, args=(conf,))
    app_ps.daemon = True
    pipe_ps.daemon = True
    pipe_ps.start()
    app_ps.start()
    run_tests(conf)
    logging.info("---- OK! ----")
    if not app_ps.is_alive():
        raise Exception("WORKER PROCESS CRASHED")
    return


if __name__ == "__main__":
    main()
