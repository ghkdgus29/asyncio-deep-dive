import asyncio
import time

from src.logger import log


async def non_blocking_work(name: str, delay: float) -> None:
    log(f"{name}: start")
    await asyncio.sleep(delay)
    log(f"{name}: end")


async def main() -> None:
    started = time.perf_counter()

    await asyncio.gather(
        non_blocking_work("A", 0.5),
        non_blocking_work("B", 0.5),
        non_blocking_work("C", 0.5),
    )
    # await non_blocking_work("A", 0.5)
    # await non_blocking_work("B", 0.5)
    # await non_blocking_work("C", 0.5)

    print(f"elapsed={time.perf_counter() - started:.3f}")


if __name__ == "__main__":
    asyncio.run(main())
