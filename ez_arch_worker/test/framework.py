import zmq.asyncio


async def init_input_output(port: int,) -> zmq.asyncio.Socket:
    "returns sender"
    c = zmq.asyncio.Context()
    router = c.socket(zmq.ROUTER)
    router.bind("tcp://*:{}".format(port))
    return router
