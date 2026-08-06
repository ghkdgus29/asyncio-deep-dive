import asyncio


async def write_frame(writer: asyncio.StreamWriter, payload: bytes) -> None:
    """하나의 length-prefixed frame을 전송한다.

    Args:
        writer: StreamWriter
        payload: 전송할 데이터
    """
    # TODO: payload 길이를 4-byte unsigned big-endian으로 인코딩
    # TODO: length prefix + payload 전송
    # TODO: await writer.drain()으로 flow control
    frame = len(payload).to_bytes(4, "big") + payload
    writer.write(frame)
    await writer.drain()


async def read_frame(
    reader: asyncio.StreamReader,
    *,
    max_frame_size: int,
) -> bytes:
    """payload 하나를 반환한다.

    Args:
        reader: StreamReader
        max_frame_size: 최대 frame 크기

    Returns:
        수신한 payload

    Raises:
        ConnectionError: frame 크기가 max_frame_size를 초과한 경우
        TimeoutError: frame timeout 초과 시
    """
    # TODO: 4-byte length prefix 읽기
    # TODO: 길이가 max_frame_size보다 크면 연결 종료
    # TODO: 해당 길이만큼 payload 읽기
    length_prefix = await reader.readexactly(4)
    payload_length = int.from_bytes(length_prefix)
    if payload_length > max_frame_size:
        raise ConnectionError
    return await reader.readexactly(payload_length)


async def start_echo_server(
    host: str,
    port: int,
    *,
    frame_timeout: float,
    max_frame_size: int,
) -> asyncio.Server:
    """echo server를 시작한다.

    Args:
        host: 바인딩할 호스트
        port: 바인딩할 포트 (0이면 OS가 할당)
        frame_timeout: frame 읽기 timeout (초)
        max_frame_size: 최대 frame 크기

    Returns:
        시작된 asyncio.Server
    """
    # TODO: 각 연결에 대해 handler 실행
    # TODO: read_frame으로 payload 읽기
    # TODO: write_frame으로 echo
    # TODO: timeout 및 max_frame_size 처리
    # TODO: asyncio.start_server로 server 시작

    async def handle_echo(reader, writer):
        async with asyncio.timeout(frame_timeout):
            data = await read_frame(reader, max_frame_size=max_frame_size)
        await write_frame(writer, data)

    return await asyncio.start_server(handle_echo, host, port)
