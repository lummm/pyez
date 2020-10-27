from .apptypes import Frames


WORKER = b"\x01"
CLIENT = b"\x02"

HEARTBEAT = b"\x01"
REPLY = b"\x02"
ACK = b"\x03"


def heartbeat(service_name: bytes) -> Frames:
    return _worker_msg([HEARTBEAT, service_name])


def ack(req_id: bytes) -> Frames:
    return _worker_msg([ACK, req_id])


def response(
        req_id: bytes,
        reply: Frames
) -> Frames:
    return _worker_msg([REPLY, req_id] + reply)


def request(
        req_id: bytes,
        service_name: bytes,
        body: Frames
) -> Frames:
    return [b"", CLIENT, req_id, service_name] + body


def _worker_msg(frames: Frames) -> Frames:
    return [b"", WORKER] + frames
