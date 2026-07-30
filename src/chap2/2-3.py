import asyncio

from src.logger import log


async def non_blocking_work(name: str, delay: float) -> None:
    log(f"{name}: start")
    await asyncio.sleep(delay)
    log(f"{name}: end")


# async def main() -> None:
#     await non_blocking_work("A", 0.3)
#     await non_blocking_work("B", 0.3)
#     await non_blocking_work("C", 0.3)


async def main() -> None:
    tasks = [
        asyncio.create_task(non_blocking_work(name, 0.3), name=name)
        for name in ("A", "B", "C")
    ]
    await asyncio.gather(*tasks)


asyncio.run(main())
