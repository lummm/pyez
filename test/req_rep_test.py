import asyncio
import logging

import pyez


ZMQ_REQ_PORT = 9999
WORKER_PORT = 9998
LIVELINESS = 3000

SERVER_DURATION = 2000


def new_con():
    return pyez.WorkerConnection(
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

    async def handler(req: pyez.Frames):
        assert req == REQ
        return [b"OK"] + RES

    async def do_req():
        async with pyez.ClientConnection(
                f"tcp://localhost:{ZMQ_REQ_PORT}") as conn:
            res = await conn.req(b"TEST", REQ)
            assert res == ([b"OK"] + RES)
        return

    server_task = asyncio.create_task(serve(handler))
    await do_req()
    await server_task
    return


async def test_serve_err() -> None:
    REQ = [b"big", b"test"]
    RES = [b"big", b"response"]

    async def handler(req: pyez.Frames):
        assert req == REQ
        return [b"ERR"] + RES

    async def do_req():
        async with pyez.ClientConnection(
                f"tcp://localhost:{ZMQ_REQ_PORT}") as conn:
            res = await conn.req(b"TEST", REQ)
            assert res == ([b"SERVICE_ERR"] + RES)
        return

    server_task = asyncio.create_task(serve(handler))
    await do_req()
    await server_task
    return


async def test_timeout() -> None:
    async def handler(req: pyez.Frames):
        await asyncio.sleep(100)
        return [b"OK", b"nothing"]

    async def do_req():
        async with pyez.ClientConnection(
                f"tcp://localhost:{ZMQ_REQ_PORT}") as conn:
            res = await conn.req(b"TEST", [b"any", b"thing"])
            assert res == ([b"EZ_ERR", b"TIMEOUT"])
        return

    server_task = asyncio.create_task(serve(handler))
    await do_req()
    await server_task
    return


async def test_no_service() -> None:
    async def do_req():
        async with pyez.ClientConnection(
                f"tcp://localhost:{ZMQ_REQ_PORT}") as conn:
            res = await conn.req(b"TEST", [b"any", b"thing"],
                                 timeout=10000)
            assert res == ([b"EZ_ERR", b"NO_SERVICE"])
        return

    await do_req()
    return


TESTS = [
    test_serve_ok,
    test_serve_err,
    test_timeout,
    test_no_service,
]


async def main():
    logging.basicConfig(level=logging.INFO)
    for test in TESTS:
        await test()
        await asyncio.sleep(LIVELINESS / 1000.0)
        logging.info(f"""
---------------- test success: {test.__name__}
""")
    return


if __name__ == '__main__':
    asyncio.run(main())
