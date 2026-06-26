#!/usr/bin/env python3
"""
Responder agent (Python): waits for a connection, receives a message, replies.

Prerequisites:
  - roboatd running on /tmp/roboat-responder.sock with ROBOAT_LISTEN_PORT=11412
  - Run before go_initiator

Usage:
  ROBOAT_SOCKET=/tmp/roboat-responder.sock python3 python_responder.py
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../clients/python"))

from roboat import Daemon


async def main():
    socket = os.environ.get("ROBOAT_SOCKET", "/tmp/roboat-responder.sock")
    rt = Daemon(socket_path=socket)
    await rt.connect()

    await rt.listen(agent_id="agt_responder")
    print("responder: listening for incoming connections...")

    async for stream in rt.incoming():
        print(f"responder: incoming stream {stream.stream_id} from {stream.from_agent_id}")
        data = await stream.recv()
        print(f"responder: received: {data.decode()!r}")
        await stream.send(b"hello from python responder!")
        await stream.close()
        print("responder: done")
        break

    await rt.close()


asyncio.run(main())
