"""Async Daemon client — wraps the local roboatd IPC socket."""

from __future__ import annotations

import asyncio
import base64
import struct
from typing import AsyncIterator, Optional

_HEADER = struct.Struct(">I")
MAX_MSG_SIZE = 4 * 1024 * 1024
DEFAULT_SOCKET = "/var/run/roboat/roboatd.sock"


class Stream:
    """A bidirectional data stream managed by the daemon."""

    def __init__(
        self,
        daemon: "Daemon",
        stream_id: int,
        class_: str,
        from_agent_id: str,
    ) -> None:
        self.stream_id = stream_id
        self.class_ = class_
        self.from_agent_id = from_agent_id
        self._daemon = daemon
        self._queue: asyncio.Queue[bytes] = asyncio.Queue()

    async def send(self, data: bytes) -> None:
        encoded = base64.b64encode(data).decode("ascii")
        await self._daemon._send_msg({"op": "send", "stream_id": self.stream_id, "data": encoded})

    async def recv(self) -> bytes:
        return await self._queue.get()

    async def close(self) -> None:
        await self._daemon._send_msg({"op": "close", "stream_id": self.stream_id})
        del self._daemon._streams[self.stream_id]


class Daemon:
    """
    Thin async client for a local roboatd IPC socket.

    Usage::

        rt = Daemon()
        await rt.connect()
        await rt.listen(agent_id="agt_B")
        async for stream in rt.incoming():
            data = await stream.recv()
            await stream.send(b"pong")
    """

    def __init__(self, socket_path: str = DEFAULT_SOCKET) -> None:
        self._socket_path = socket_path
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._streams: dict[int, Stream] = {}
        self._incoming_queue: asyncio.Queue[Stream] = asyncio.Queue()
        self._pending_dials: dict[str, asyncio.Future[Stream]] = {}
        self._recv_task: Optional[asyncio.Task] = None
        self._write_lock = asyncio.Lock()

    async def connect(self) -> None:
        self._reader, self._writer = await asyncio.open_unix_connection(self._socket_path)
        self._recv_task = asyncio.create_task(self._recv_loop())

    async def close(self) -> None:
        if self._recv_task:
            self._recv_task.cancel()
        if self._writer:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except Exception:
                pass

    async def __aenter__(self) -> "Daemon":
        await self.connect()
        return self

    async def __aexit__(self, *_) -> None:
        await self.close()

    async def listen(
        self,
        agent_id: str,
        registry_token: Optional[str] = None,
        tunnel_endpoint: Optional[str] = None,
    ) -> None:
        msg: dict = {"op": "listen", "agent_id": agent_id}
        if registry_token:
            msg["registry_token"] = registry_token
        if tunnel_endpoint:
            msg["tunnel_endpoint"] = tunnel_endpoint
        await self._send_msg(msg)
        resp = await self._read_msg()
        if resp.get("op") == "error":
            raise RuntimeError(f"listen failed: {resp.get('message')}")
        if resp.get("op") != "listening":
            raise RuntimeError(f"unexpected response to listen: {resp}")

    async def incoming(self) -> AsyncIterator[Stream]:
        while True:
            stream = await self._incoming_queue.get()
            yield stream

    async def dial(
        self, target_agent_id: str, stream_class: str = "control"
    ) -> Stream:
        request_id = f"dial-{id(target_agent_id)}"
        fut: asyncio.Future[Stream] = asyncio.get_event_loop().create_future()
        self._pending_dials[request_id] = fut
        await self._send_msg({
            "op": "dial",
            "target_agent_id": target_agent_id,
            "stream_class": stream_class,
            "request_id": request_id,
        })
        return await fut

    async def ping(self) -> None:
        await self._send_msg({"op": "ping"})

    # ── Internal ──────────────────────────────────────────────────────────────

    async def _send_msg(self, obj: dict) -> None:
        import json
        payload = json.dumps(obj).encode("utf-8")
        header = struct.pack(">I", len(payload))
        async with self._write_lock:
            self._writer.write(header + payload)
            await self._writer.drain()

    async def _read_msg(self) -> dict:
        import json
        header = await self._reader.readexactly(4)
        length = struct.unpack(">I", header)[0]
        if length > MAX_MSG_SIZE:
            raise ValueError(f"daemon message too large: {length}")
        data = await self._reader.readexactly(length)
        return json.loads(data)

    async def _recv_loop(self) -> None:
        import json
        try:
            while True:
                try:
                    header = await self._reader.readexactly(4)
                except asyncio.IncompleteReadError:
                    break
                length = struct.unpack(">I", header)[0]
                if length > MAX_MSG_SIZE:
                    break
                data = await self._reader.readexactly(length)
                msg = json.loads(data)
                await self._dispatch(msg)
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

    async def _dispatch(self, msg: dict) -> None:
        op = msg.get("op")

        if op == "recv":
            sid = msg["stream_id"]
            stream = self._streams.get(sid)
            if stream:
                raw = base64.b64decode(msg["data"])
                await stream._queue.put(raw)

        elif op == "incoming":
            sid = msg["stream_id"]
            stream = Stream(
                self,
                stream_id=sid,
                class_=msg.get("class", "control"),
                from_agent_id=msg.get("from_agent_id", ""),
            )
            self._streams[sid] = stream
            await self._incoming_queue.put(stream)

        elif op == "connected":
            request_id = msg.get("request_id", "")
            fut = self._pending_dials.pop(request_id, None)
            if fut and not fut.done():
                sid = msg["stream_id"]
                stream = Stream(
                    self,
                    stream_id=sid,
                    class_="control",
                    from_agent_id=msg.get("target_agent_id", ""),
                )
                self._streams[sid] = stream
                fut.set_result(stream)

        elif op == "closed":
            sid = msg["stream_id"]
            self._streams.pop(sid, None)

        elif op == "error":
            request_id = msg.get("request_id", "")
            fut = self._pending_dials.pop(request_id, None)
            if fut and not fut.done():
                fut.set_exception(RuntimeError(msg.get("message", "unknown error")))

        elif op in ("listening", "pong"):
            pass
