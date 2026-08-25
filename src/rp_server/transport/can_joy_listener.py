# SPDX-License-Identifier: GPL-3.0
# Copyright (C) 2026 changchuanyong (https://github.com/changchuanyong)

"""Map SocketCAN input frames to virtual gamepad buttons."""

from __future__ import annotations

import asyncio
import logging
import socket
import struct
from typing import Any, Optional

from ..protocol.at_parser import AtCommand

logger = logging.getLogger("rp_server.can_joy")

CAN_MTU = 16
CANFD_MTU = 72
CAN_EFF_FLAG = 0x80000000
CAN_RTR_FLAG = 0x40000000
CAN_ERR_FLAG = 0x20000000
CAN_SFF_MASK = 0x000007FF
CAN_RAW_FILTER = 1
CAN_RAW_FD_FRAMES = 5
SOL_CAN_RAW = 101


def decode_can_frame(frame: bytes) -> Optional[tuple[int, bytes]]:
    """Decode a raw classic CAN or CAN FD SocketCAN frame."""
    if len(frame) == CAN_MTU:
        can_id, length, data = struct.unpack("=IB3x8s", frame)
        return can_id, data[:min(length, 8)]
    if len(frame) == CANFD_MTU:
        can_id, length, _flags, _res0, _res1, data = struct.unpack(
            "=IBBBB64s", frame
        )
        return can_id, data[:min(length, 64)]
    return None


class CANJoyListener:
    """Listen for specific CAN frames and convert each match to an LB click."""

    def __init__(
        self,
        at_handler: Any,
        interface: str = "can_top",
        can_id: int = 0x003,
        payloads: tuple[bytes, ...] = (
            bytes.fromhex("010001AA00000000"),
            bytes.fromhex("010001AA00000001"),
        ),
        click_ms: int = 50,
    ):
        self._handler = at_handler
        self._interface = interface
        self._can_id = can_id
        self._payloads = tuple(payloads)
        self._click_seconds = max(0, click_ms) / 1000.0
        self._sock: Optional[socket.socket] = None
        self._reader_task: Optional[asyncio.Task] = None
        self._click_task: Optional[asyncio.Task] = None
        self._button_id = 0

    async def start(self) -> bool:
        """Open the CAN FD raw socket, leaving the service degraded on failure."""
        if self._reader_task is not None:
            return True
        try:
            sock = socket.socket(socket.AF_CAN, socket.SOCK_RAW, socket.CAN_RAW)
            sock.setsockopt(SOL_CAN_RAW, CAN_RAW_FD_FRAMES, 1)
            # Accept only standard data frame 0x003; reject extended, remote, and error frames.
            filter_mask = CAN_SFF_MASK | CAN_EFF_FLAG | CAN_RTR_FLAG | CAN_ERR_FLAG
            sock.setsockopt(
                SOL_CAN_RAW,
                CAN_RAW_FILTER,
                struct.pack("=II", self._can_id, filter_mask),
            )
            sock.setblocking(False)
            sock.bind((self._interface,))
        except (AttributeError, OSError) as exc:
            try:
                sock.close()
            except (NameError, OSError):
                pass
            logger.error("CAN 手柄监听启动失败: interface=%s error=%s", self._interface, exc)
            return False

        self._sock = sock
        self._reader_task = asyncio.create_task(self._read_loop())
        logger.info(
            "CAN 手柄监听已启动: interface=%s id=0x%03X data=%s",
            self._interface,
            self._can_id,
            "/".join(payload.hex().upper() for payload in self._payloads),
        )
        return True

    async def stop(self) -> None:
        """Stop listening and ensure that LB ends in the released state."""
        reader_task = self._reader_task
        self._reader_task = None
        if reader_task is not None:
            reader_task.cancel()

        click_task = self._click_task
        self._click_task = None
        if click_task is not None:
            click_task.cancel()

        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None

        tasks = [task for task in (reader_task, click_task) if task is not None]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _read_loop(self) -> None:
        if self._sock is None:
            return
        loop = asyncio.get_running_loop()
        try:
            while True:
                frame = await loop.sock_recv(self._sock, CANFD_MTU)
                self.process_frame(frame)
        except asyncio.CancelledError:
            raise
        except (OSError, RuntimeError) as exc:
            logger.error("CAN 手柄监听中断: %s", exc)

    def process_frame(self, frame: bytes) -> bool:
        """Process one frame and return True when an LB click task is started."""
        decoded = decode_can_frame(frame)
        if decoded is None:
            return False
        raw_can_id, payload = decoded
        if raw_can_id != self._can_id or payload not in self._payloads:
            return False
        if self._click_task is not None and not self._click_task.done():
            return False
        self._click_task = asyncio.create_task(self._click_lb())
        return True

    async def _click_lb(self) -> None:
        try:
            await self._dispatch_button("down")
            await asyncio.sleep(self._click_seconds)
        finally:
            await self._dispatch_button("up")

    async def _dispatch_button(self, state: str) -> None:
        self._button_id += 1
        cmd = AtCommand.parse(f"AT+BTN=lb,{state},{self._button_id}")
        if cmd is not None:
            await self._handler.dispatch(cmd)
