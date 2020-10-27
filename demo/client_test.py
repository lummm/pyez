#!/usr/bin/env python3.8

import asyncio
import logging

import ezpy


async def main():
    logging.basicConfig(level=logging.INFO)
    async with ezpy.ClientConnection("tcp://localhost:9999") as conn:
        res = await asyncio.gather(*[
            conn.req(b"TEST", [b"request"])
            for i in range(10)
        ])
        logging.info("got reply: %s", res)
    logging.info("OK")
    return


if __name__ == '__main__':
    asyncio.run(main())
