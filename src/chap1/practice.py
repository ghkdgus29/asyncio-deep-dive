import asyncio


async def run_blocking_safely(
    func,
    /,
    *args,
    timeout: float | None = None,
):
    coro = asyncio.to_thread(func, *args)

    if timeout is None:
        return await coro

    return await asyncio.wait_for(coro, timeout=timeout)
