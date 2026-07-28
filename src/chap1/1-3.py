import asyncio
import time

from src.logger import log


async def heartbeat() -> None:
    for index in range(6):
        log(f"heartbeat={index}")
        await asyncio.sleep(0.2)


async def blocking_task() -> None:
    log("blocking task: start")
    time.sleep(1.0)
    log("blocking task: end")


async def main() -> None:
    await asyncio.gather(
        heartbeat(),
        blocking_task(),
    )


asyncio.run(main())
