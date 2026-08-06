import asyncio

import pytest

from src.chap8.practice import (
    read_frame,
    start_echo_server,
    write_frame,
)


async def open_client(
    server: asyncio.Server,
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    socket = server.sockets[0]
    host, port = socket.getsockname()[:2]
    return await asyncio.open_connection(host, port)


@pytest.mark.asyncio
async def test_echoes_single_frame() -> None:
    server = await start_echo_server(
        "127.0.0.1",
        0,
        frame_timeout=1,
        max_frame_size=1024,
    )
    reader, writer = await open_client(server)

    try:
        await write_frame(writer, b"hello")
        assert await read_frame(reader, max_frame_size=1024) == b"hello"
    finally:
        writer.close()
        await writer.wait_closed()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_accepts_frame_sent_in_small_chunks() -> None:
    server = await start_echo_server(
        "127.0.0.1",
        0,
        frame_timeout=1,
        max_frame_size=1024,
    )
    reader, writer = await open_client(server)
    payload = b"chunked"
    frame = len(payload).to_bytes(4, "big") + payload

    try:
        for byte in frame:
            writer.write(bytes([byte]))
            await writer.drain()

        assert await read_frame(reader, max_frame_size=1024) == payload
    finally:
        writer.close()
        await writer.wait_closed()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_closes_connection_for_oversized_frame() -> None:
    server = await start_echo_server(
        "127.0.0.1",
        0,
        frame_timeout=1,
        max_frame_size=8,
    )
    reader, writer = await open_client(server)

    try:
        writer.write((9).to_bytes(4, "big"))
        await writer.drain()

        assert await asyncio.wait_for(reader.read(1), timeout=0.5) == b""
    finally:
        writer.close()
        await writer.wait_closed()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_closes_idle_connection_after_timeout() -> None:
    server = await start_echo_server(
        "127.0.0.1",
        0,
        frame_timeout=0.02,
        max_frame_size=1024,
    )
    reader, writer = await open_client(server)

    try:
        assert await asyncio.wait_for(reader.read(1), timeout=0.5) == b""
    finally:
        writer.close()
        await writer.wait_closed()
        server.close()
        await server.wait_closed()
