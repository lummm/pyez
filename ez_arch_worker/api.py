import ez_arch_worker.lib.app as ez_app
import ez_arch_worker.lib.worker as worker
import ez_arch_worker.lib.req as req


Frames = ez_app.Frames
Handler = ez_app.Handler
EzClient = req.EzClient


async def run_worker(
        *,
        service_name: bytes,
        handler: Handler,
        listen_host: str,
        port: int,
        poll_interval_ms: int = None
) -> None:
    ez_app.init(
        con_s="tcp://{}:{}".format(listen_host, port),
        handler_impl=handler,
        service_name=service_name
    )
    if poll_interval_ms is not None:
        ez_app.state.poll_interval_ms = poll_interval_ms
    await worker.run_worker()
    return


new_client = req.new_client
