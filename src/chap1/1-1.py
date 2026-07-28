import time

from src.logger import log


def blocking_work(name: str, delay: float) -> None:
    log(f"{name}: start")
    time.sleep(delay)
    log(f"{name}: end")


def main() -> None:
    started = time.perf_counter()

    blocking_work("A", 0.5)
    blocking_work("B", 0.5)
    blocking_work("C", 0.5)

    print(f"elapsed={time.perf_counter() - started:.3f}")


if __name__ == "__main__":
    main()
