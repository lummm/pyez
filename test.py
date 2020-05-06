#!/usr/bin/env python3

import asyncio
import logging
import os

import ez_arch_worker.api as worker


def setup_logging():
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format=f"%(asctime)s.%(msecs)03d "
        "%(levelname)s %(module)s - %(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


async def handler(frames: worker.Frames)-> worker.Frames:
    return ( b"done", b"echo", ) + frames


def main():
    setup_logging()
    loop = asyncio.get_event_loop()
    loop.create_task(
        worker.run_worker(
            b"ECHO",
            9100,
            handler,
            router_host = os.environ["ROUTER_HOST"],
            router_port = int(os.environ["ROUTER_PORT"])
        )
    )
    loop.run_forever()
    return


if __name__ == "__main__":
    main()
