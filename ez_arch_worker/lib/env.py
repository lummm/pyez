import os
from typing import NamedTuple


class _ENV(NamedTuple):
    POLL_INTERVAL_S: int = int(os.environ.get("POLL_INTERVAL_S", 3))
    ROUTER_HOST: str = os.environ["ROUTER_HOST"]
    ROUTER_PORT: int = int(os.environ["ROUTER_PORT"])
ENV = _ENV()
