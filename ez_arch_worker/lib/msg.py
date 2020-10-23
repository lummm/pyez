import logging
import os

import zmq

from ez_arch_worker.lib.app import state as app
from ez_arch_worker.lib.app import Frames
import ez_arch_worker.lib.protoc as protoc


def send(
        frames: Frames
) -> None:
    frames = [b"", protoc.WORKER] + frames  # INPUT FLAT
    app.dealer.send_multipart(frames)
    return


def send_response(
        dest: bytes,
        request_id: bytes,
        reply: Frames
) -> None:
    frames = [protoc.REPLY, dest, b"", request_id] + reply
    send(frames)
    return


def send_heartbeat() -> None:
    frames = [
        protoc.HEARTBEAT,       # WORKER LEVEL 1
        app.service_name        # LEVEL 2 HEARTBEAT
    ]
    send(frames)
    return


async def connect() -> None:
    dealer = app.c.socket(zmq.DEALER)
    dealer.setsockopt(
        zmq.IDENTITY,
        app.service_name + b"-" + app.identity)
    dealer.connect(app.con_s)
    logging.info("dealer connected to %s", app.con_s)
    app.poller.register(dealer, zmq.POLLIN)
    app.dealer = dealer
    return
