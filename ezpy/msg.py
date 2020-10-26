from .apptypes import Frames


WORKER = b"\x01"
CLIENT = b"\x02"

HEARTBEAT = b"\x01"
REPLY = b"\x02"
ACK = b"\x03"


def heartbeat(service_name: bytes) -> Frames:
    return _as_msg([HEARTBEAT, service_name])


def ack(req_id: bytes) -> Frames:
    return _as_msg([ACK, req_id])


def response(
        return_addr: bytes,
        req_id: bytes,
        reply: Frames
) -> Frames:
    return _as_msg([REPLY, return_addr, b"", req_id] + reply)


def _as_msg(frames: Frames) -> Frames:
    return [b"", WORKER] + frames
