import asyncio

import pytest

from src.chap5.practice import process_with_resource


class FakeResource:
    def __init__(
        self,
        *,
        read_delay: float = 0,
        open_error: BaseException | None = None,
        read_error: BaseException | None = None,
        close_error: BaseException | None = None,
    ) -> None:
        self.read_delay = read_delay
        self.open_error = open_error
        self.read_error = read_error
        self.close_error = close_error
        self.open_count = 0
        self.close_count = 0

    async def open(self) -> None:
        self.open_count += 1
        if self.open_error is not None:
            raise self.open_error

    async def read(self) -> str:
        await asyncio.sleep(self.read_delay)
        if self.read_error is not None:
            raise self.read_error
        return "payload"

    async def close(self) -> None:
        self.close_count += 1
        if self.close_error is not None:
            raise self.close_error


@pytest.mark.asyncio
async def test_resource_closes_after_success() -> None:
    resource = FakeResource()

    result = await process_with_resource(resource, timeout=1)

    assert result == "payload"
    assert resource.close_count == 1


@pytest.mark.asyncio
async def test_resource_closes_after_read_failure() -> None:
    resource = FakeResource(read_error=ValueError("broken"))

    with pytest.raises(ValueError, match="broken"):
        await process_with_resource(resource, timeout=1)

    assert resource.close_count == 1


@pytest.mark.asyncio
async def test_resource_closes_after_timeout() -> None:
    resource = FakeResource(read_delay=1)

    with pytest.raises(TimeoutError):
        await process_with_resource(resource, timeout=0.01)

    assert resource.close_count == 1


@pytest.mark.asyncio
async def test_resource_closes_after_external_cancellation() -> None:
    resource = FakeResource(read_delay=10)
    task = asyncio.create_task(process_with_resource(resource, timeout=20))
    await asyncio.sleep(0)

    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert resource.close_count == 1


@pytest.mark.asyncio
async def test_resource_does_not_close_when_open_fails() -> None:
    resource = FakeResource(open_error=RuntimeError("open failed"))

    with pytest.raises(RuntimeError, match="open failed"):
        await process_with_resource(resource, timeout=1)

    assert resource.close_count == 0


@pytest.mark.asyncio
async def test_resource_propagates_close_failure() -> None:
    resource = FakeResource(close_error=RuntimeError("close failed"))

    with pytest.raises(RuntimeError, match="close failed"):
        await process_with_resource(resource, timeout=1)

    assert resource.close_count == 1
