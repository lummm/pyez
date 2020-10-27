import asyncio
import logging
import threading

import ezpy


ZMQ_REQ_PORT = 9999
WORKER_PORT = 9998
LIVELINESS = 3000

SERVER_DURATION = 2000


def new_con():
    return ezpy.WorkerConnection(
        con_s=f"tcp://localhost:{WORKER_PORT}",
        service_name=b"TEST",
        liveliness=LIVELINESS)


async def serve(handler):
    async with new_con() as conn:
        try:
            await asyncio.wait_for(conn.serve(handler),
                                   SERVER_DURATION / 1000.0)
        except asyncio.TimeoutError:
            return
    return


async def test_serve_ok() -> None:
    REQ = [b"big", b"test"]
    RES = [b"big", b"response"]

    async def handler(req: ezpy.Frames):
        assert req == REQ
        return [b"OK"] + RES

    async def do_req():
        async with ezpy.ClientConnection(
                f"tcp://localhost:{ZMQ_REQ_PORT}") as conn:
            res = await conn.req(b"TEST", REQ)
            assert res == ([b"OK"] + RES)
        return

    server_task = asyncio.create_task(serve(handler))
    await do_req()
    await server_task
    return


TESTS = [
    test_serve_ok,
]


async def main():
    logging.basicConfig(level=logging.INFO)
    for test in TESTS:
        await test()
        logging.info(f"""
---------------- test success: {test.__name__}
""")
    return


if __name__ == '__main__':
    asyncio.run(main())
