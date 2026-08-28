# SPDX-License-Identifier: GPL-3.0
# Copyright (C) 2026 mustaf-osman (https://github.com/mustaf-osman)
# Copyright (C) 2026 wentywenty (https://github.com/wentywenty)

"""通过 Unix socket 读取 BMS daemon 推送的电池状态。"""

import logging
import socket
import struct
import threading
from typing import Optional


logger = logging.getLogger("rp_server.bms")

# 对应 roboparty_bms/include/bms_driver.hpp 中 pack(1) 的 BatteryStatus。
_STATUS_STRUCT = struct.Struct("=7dIH2d33sHHHIIB")
_CONNECT_TIMEOUT = 2.0
_IO_TIMEOUT = 0.5
_RECONNECT_INTERVAL = 1.0


class BMSDriver:
    def __init__(self):
        self._socket_path = "/tmp/bms.sock"
        self._socket: socket.socket | None = None
        self._reader_thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._connected_event = threading.Event()
        self._data_lock = threading.Lock()
        self._last_status: dict | None = None

    def init(self, config: dict) -> bool:
        self.deinit()
        self._socket_path = config.get("bms", {}).get("socket_path", "/tmp/bms.sock")
        self._stop_event.clear()
        self._reader_thread = threading.Thread(
            target=self._reader_loop,
            name="bms_reader",
            daemon=True,
        )
        self._reader_thread.start()

        if not self._connected_event.wait(_CONNECT_TIMEOUT):
            logger.warning("BMS daemon not reachable at %s", self._socket_path)
            self.deinit()
            return False

        logger.info("BMS initialised from Unix socket %s", self._socket_path)
        return True

    def deinit(self):
        self._stop_event.set()
        sock = self._socket
        self._socket = None
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            sock.close()

        thread = self._reader_thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=_IO_TIMEOUT + 0.5)
        self._reader_thread = None
        self._connected_event.clear()
        with self._data_lock:
            self._last_status = None

    @property
    def ready(self) -> bool:
        return self._connected_event.is_set()

    def read(self) -> Optional[dict]:
        if not self.ready:
            return None
        with self._data_lock:
            return dict(self._last_status) if self._last_status is not None else None

    def _reader_loop(self):
        while not self._stop_event.is_set():
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.settimeout(_IO_TIMEOUT)
            try:
                sock.connect(self._socket_path)
                self._socket = sock
                self._connected_event.set()
                logger.info("Connected to BMS daemon at %s", self._socket_path)
                self._receive_frames(sock)
            except OSError as exc:
                if self._connected_event.is_set() and not self._stop_event.is_set():
                    logger.warning("BMS daemon disconnected: %s", exc)
            finally:
                self._connected_event.clear()
                if self._socket is sock:
                    self._socket = None
                sock.close()

            self._stop_event.wait(_RECONNECT_INTERVAL)

    def _receive_frames(self, sock: socket.socket):
        pending = bytearray()
        while not self._stop_event.is_set():
            try:
                chunk = sock.recv(4096)
            except socket.timeout:
                continue
            if not chunk:
                raise ConnectionError("Unix socket closed")

            pending.extend(chunk)
            while len(pending) >= _STATUS_STRUCT.size:
                frame = bytes(pending[:_STATUS_STRUCT.size])
                del pending[:_STATUS_STRUCT.size]
                status = self._decode_status(frame)
                with self._data_lock:
                    self._last_status = status

    @staticmethod
    def _decode_status(frame: bytes) -> dict:
        (
            voltage,
            current,
            temperature,
            percentage,
            _charge,
            capacity,
            _design_capacity,
            protect_status,
            work_state,
            _max_cell_voltage,
            _min_cell_voltage,
            _serial_number,
            _sw_version,
            _hw_version,
            soh,
            cycles,
            io_state,
            power_on,
        ) = _STATUS_STRUCT.unpack(frame)
        if 0.0 <= percentage <= 1.0:
            percentage *= 100.0
        return {
            "voltage": float(voltage),
            "current": float(current),
            "soc": float(percentage),
            "temp": float(temperature),
            "capacity": float(capacity),
            "soh": float(soh),
            "cycles": int(cycles),
            "state": str(work_state),
            "protect": int(protect_status),
            "io_state": int(io_state),
            "power_on": bool(power_on),
        }
