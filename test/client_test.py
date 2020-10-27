#!/usr/bin/env python3.8

import asyncio
import json
import logging

import ezpy


async def main():
    logging.basicConfig(level=logging.INFO)
    async with ezpy.ClientConnection("tcp://localhost:9000") as conn:
        res = await conn.req(b"TEST", [b"request"])
        logging.info("got reply: %s", res)
    logging.info("OK")
    return


if __name__ == '__main__':
    asyncio.run(main())
