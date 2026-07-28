import asyncio
import threading
import time

from src.logger import log


async def heartbeat() -> None:
    for index in range(6):
        log(f"heartbeat={index}")
        await asyncio.sleep(0.2)


def blocking_sleep(duration: float) -> None:
    log(f"    worker thread={threading.current_thread().name}")
    time.sleep(duration)
    log(f"    worker thread={threading.current_thread().name} done")


async def blocking_task() -> None:
    log("blocking task: start")
    await asyncio.to_thread(blocking_sleep, 1.0)
    log("blocking task: end")


async def main() -> None:
    await asyncio.gather(
        heartbeat(),
        blocking_task(),
    )

    # try:
    #     await asyncio.wait_for(blocking_task(), timeout=0.3)
    # except TimeoutError:
    #     log("caller gave up (TimeoutError)")

    # await asyncio.sleep(1.5)
    # log("main: done waiting to observe background thread")


asyncio.run(main())
