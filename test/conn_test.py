#!/usr/bin/env python3.8

import asyncio
import logging

import ezpy


async def main():
    logging.basicConfig(level=logging.INFO)
    async with ezpy.Connection(
        con_s="tcp://localhost:9004",
        service_name=b"TEST",
        livelieness_s=1
    ) as conns:
    # conn = ezpy.Connection(
    #     con_s="tcp://localhost:9004",
    #     service_name=b"TEST",
    #     livelieness_s=1
    # )
    # await conn.connect()
        await asyncio.sleep(3)
    logging.info("OK")
    return


if __name__ == '__main__':
    asyncio.run(main())
