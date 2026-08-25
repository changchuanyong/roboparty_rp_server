# SPDX-License-Identifier: GPL-3.0
# Copyright (C) 2026 mustaf-osman (https://github.com/mustaf-osman)
# Copyright (C) 2026 wentywenty (https://github.com/wentywenty)

"""UDP listener: receive App JSON → translate to AT commands → dispatch to robot.

Data format (App UDP joystick protocol):

    {"type":"control", "sequence":1, "timestamp":..., "token":"...",
     "left_stick_x":0.0, "left_stick_y":0.0,
     "right_stick_x":0.0, "right_stick_y":0.0,
     "btn_a":false, "btn_b":false, "btn_x":false, "btn_y":false,
     "btn_lb":false, "btn_rb":false, "btn_lt":false, "btn_rt":false,
     "dpad_up":false, "dpad_down":false, "dpad_left":false, "dpad_right":false}

Also accepts legacy format (without "type"/"token" fields).
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Optional

from ..protocol.at_parser import AtCommand, CmdType
from ..udp_auth import UDPAuthenticator
from ..udp_session import SessionManager, SessionState, UDPSession

logger = logging.getLogger("rp_server.udp")
packet_logger = logging.getLogger("rp_server.packets")

# Axis name mapping: JSON field → AT+JOY axis name
_AXIS_MAP: dict[str, str] = {
    "left_stick_x": "lx",
    "left_stick_y": "ly",
    "right_stick_x": "rx",
    "right_stick_y": "ry",
}

# Button name mapping: JSON field → AT+BTN button name
_BTN_MAP: dict[str, str] = {
    "btn_a": "a",
    "btn_b": "b",
    "btn_x": "x",
    "btn_y": "y",
    "btn_lb": "lb",
    "btn_rb": "rb",
    "btn_lt": "ltb",
    "btn_rt": "rtb",
    "dpad_up": "du",
    "dpad_down": "dd",
    "dpad_left": "dl",
    "dpad_right": "dr",
}

# Dead zone: joystick values within ±DEAD_ZONE are ignored
DEAD_ZONE = 0.01
STOP_ACK_INTERVAL = 0.02
STOP_SCRIPT = "/home/orangepi/roboparty_rp_server/scripts/stop_robot.sh"


class UDPJoyListener:
    """Async UDP listener with auth, heartbeat and session management."""

    def __init__(
        self,
        at_handler: Any,
        host: str = "0.0.0.0",
        port: int = 9000,
        secret_key: str = "",
        token_ttl: int = 3600,
        session_timeout: float = 10.0,
        telemetry: Any = None,
        head: Any = None,
    ):
        self._handler = at_handler
        self._host = host
        self._port = port
        self._transport: Optional[asyncio.DatagramTransport] = None

        # 认证管理
        self._auth = UDPAuthenticator(secret_key, token_ttl)
        # 会话管理
        self._sessions = SessionManager(timeout=session_timeout)
        # 遥测数据源
        self._telemetry = telemetry
        # Head motor driver (head commands are ignored while it is not ready)
        self._head = head
        # 按钮状态（按会话地址隔离）
        self._btn_state: dict[str, dict[str, bool]] = {}
        self._btn_seq: dict[str, int] = {}

        # 定时任务
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        self._telemetry_tasks: dict[str, asyncio.Task] = {}  # addr_key → task
        self._stop_ack_tasks: dict[str, asyncio.Task] = {}
        self._software_stop_task: Optional[asyncio.Task] = None
        self._stop_latched = False

    # ------------------------------------------------------------------
    # Connection protocol (asyncio Datagram)
    # ------------------------------------------------------------------

    def connection_made(self, transport: asyncio.DatagramTransport) -> None:
        self._transport = transport
        logger.info("UDP listener ready on %s:%d", self._host, self._port)

    def datagram_received(self, data: bytes, addr: tuple) -> None:
        """Called by asyncio on each incoming UDP packet."""
        # 记录原始数据包内容
        packet_logger.info(
            "UDP_RECV src=%s:%d size=%d data=%s",
            addr[0], addr[1], len(data), data.decode("utf-8", errors="replace").strip()
        )
        self._sessions.update_activity(addr)
        try:
            self._process(data, addr)
        except Exception:
            logger.debug("UDP 数据包处理失败: %s:%d", addr[0], addr[1], exc_info=True)

    def error_received(self, exc: Exception) -> None:
        logger.warning("UDP error: %s", exc)

    def connection_lost(self, exc: Optional[Exception]) -> None:
        logger.info("UDP listener stopped")
        self._transport = None

    # ------------------------------------------------------------------
    # Start / stop
    # ------------------------------------------------------------------

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        await loop.create_datagram_endpoint(
            lambda: self,
            local_addr=(self._host, self._port),
        )
        # 启动心跳和清理任务
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())

    def stop(self) -> None:
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        if self._cleanup_task:
            self._cleanup_task.cancel()
        for task in self._telemetry_tasks.values():
            task.cancel()
        self._telemetry_tasks.clear()
        for task in self._stop_ack_tasks.values():
            task.cancel()
        self._stop_ack_tasks.clear()
        if self._transport:
            self._transport.close()
            self._transport = None

    async def _heartbeat_loop(self) -> None:
        """每秒向已连接会话发送心跳"""
        while True:
            try:
                await asyncio.sleep(1)
                for session in self._sessions.connected_sessions:
                    self._send_heartbeat(session.addr)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.debug("心跳发送失败", exc_info=True)

    async def _cleanup_loop(self) -> None:
        """每5秒清理超时会话"""
        while True:
            try:
                await asyncio.sleep(5)
                expired_sessions = self._sessions.cleanup_expired()
                expired_auth = self._auth.cleanup_expired()
                # 停止已过期会话的遥测任务
                for session in expired_sessions:
                    task = self._telemetry_tasks.pop(session.addr_key, None)
                    if task:
                        task.cancel()
                if expired_sessions or expired_auth:
                    logger.info("清理: 会话 %d 个, 认证 %d 个", len(expired_sessions), expired_auth)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.debug("会话清理失败", exc_info=True)

    async def _telemetry_loop(self, addr: tuple, addr_key: str) -> None:
        """定期发送遥测数据给已连接客户端"""
        last_motor_send = 0.0
        try:
            while True:
                await asyncio.sleep(0.02)
                if not self._telemetry:
                    continue

                # 向手柄 App 发送 UDP IMU JSON，与 WebSocket 的 @IMU 广播相互独立
                imu = self._telemetry.last_imu
                if imu:
                    self._send_json({
                        "type": "imu",
                        "timestamp": int(time.time() * 1000),
                        "quat": imu["quat"],
                        "ang_vel": imu["ang_vel"],
                        "lin_acc": imu["lin_acc"],
                        "temp": imu["temp"],
                    }, addr)

                # 发送电池数据
                battery = self._telemetry.last_battery
                if battery:
                    self._send_json({
                        "type": "battery",
                        "voltage": battery["voltage"],
                        "current": battery["current"],
                        "soc": battery["soc"],
                        "temp": battery["temp"],
                    }, addr)

                now = time.monotonic()
                if now - last_motor_send >= 1.0:
                    motors = getattr(self._telemetry, "last_motors", None)
                    if motors is not None:
                        self._send_json({
                            "type": "motors",
                            "timestamp": int(time.time() * 1000),
                            "id": [motor["id"] for motor in motors],
                            "position": [motor["position"] for motor in motors],
                            "speed": [motor["speed"] for motor in motors],
                            "torque": [motor["torque"] for motor in motors],
                            "temperature": [motor["temperature"] for motor in motors],
                            "error": [motor["error"] for motor in motors],
                            "mode": [motor["mode"] for motor in motors],
                            "response_count": [motor["response_count"] for motor in motors],
                        }, addr)
                    last_motor_send = now
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.debug("遥测发送失败: %s", addr_key, exc_info=True)

    # ------------------------------------------------------------------
    # Packet processing
    # ------------------------------------------------------------------

    def _process(self, data: bytes, addr: tuple) -> None:
        text = data.decode("utf-8", errors="replace").strip()
        if not text:
            return

        try:
            pkt: dict[str, Any] = json.loads(text)
        except json.JSONDecodeError:
            logger.debug("UDP: invalid JSON (%d bytes)", len(data))
            return

        msg_type = pkt.get("type", "")

        # 消息分发
        if msg_type == "auth_request":
            self._handle_auth_request(pkt, addr)
        elif msg_type == "challenge_response":
            self._handle_challenge_response(pkt, addr)
        elif msg_type == "stop":
            self._handle_stop(pkt, addr)
        elif msg_type == "control":
            self._handle_control(pkt, addr)
        elif msg_type == "heartbeat":
            self._handle_heartbeat(pkt, addr)
        elif msg_type == "head":
            self._handle_head(pkt, addr)
        elif msg_type == "skill_action":
            self._handle_skill_action(pkt, addr)
        elif not msg_type:
            # 兼容旧格式（无type字段视为control）
            self._handle_control(pkt, addr)

    def _handle_auth_request(self, pkt: dict, addr: tuple) -> None:
        """处理认证请求"""
        device_id = pkt.get("device_id", "")
        device_name = pkt.get("device_name", "")

        if not device_id:
            logger.warning("认证请求缺少 device_id: %s:%d", addr[0], addr[1])
            return

        session = UDPSession(
            addr=addr,
            device_id=device_id,
            device_name=device_name,
            state=SessionState.AUTHENTICATING,
        )
        self._sessions.add_session(session)

        challenge_code = self._auth.generate_challenge(addr, device_id)
        self._send_json({
            "type": "challenge",
            "challenge_code": challenge_code,
            "timestamp": int(time.time() * 1000),
        }, addr)

    def _handle_challenge_response(self, pkt: dict, addr: tuple) -> None:
        """处理挑战响应"""
        device_id = pkt.get("device_id", "")
        signature = pkt.get("signature", "")

        if not device_id or not signature:
            return

        token = self._auth.verify_signature(addr, device_id, signature)
        if token:
            self._sessions.set_connected(addr, token)
            addr_key = f"{addr[0]}:{addr[1]}"
            self._btn_state[addr_key] = {}
            self._btn_seq[addr_key] = 0

            # 启动遥测数据发送
            if self._telemetry and addr_key not in self._telemetry_tasks:
                self._telemetry_tasks[addr_key] = asyncio.create_task(
                    self._telemetry_loop(addr, addr_key)
                )

            self._send_json({
                "type": "auth_result",
                "ok": True,
                "token": token,
                "expires_at": int((time.time() + self._auth._token_ttl) * 1000),
            }, addr)
            logger.info("认证成功: device=%s", device_id)
        else:
            self._send_json({
                "type": "auth_result",
                "ok": False,
                "reason": "签名验证失败",
            }, addr)

    def _handle_stop(self, pkt: dict, addr: tuple) -> None:
        """Latch software stop and repeat the acknowledgment until confirmed."""
        addr_key = f"{addr[0]}:{addr[1]}"
        status = pkt.get("status", "")

        if status == "received":
            task = self._stop_ack_tasks.pop(addr_key, None)
            if task:
                task.cancel()
            logger.info("software stop acknowledgment confirmed by %s", addr_key)
            return

        if status:
            logger.debug("UDP: ignoring invalid stop status %r from %s", status, addr_key)
            return

        if not self._stop_latched:
            self._stop_latched = True
            joy = getattr(self._handler, "joy", None)
            if joy is not None:
                joy.reset()
            self._software_stop_task = asyncio.create_task(self._run_software_stop())
            logger.warning("software stop latched by %s", addr_key)

        task = self._stop_ack_tasks.get(addr_key)
        if task is None or task.done():
            self._stop_ack_tasks[addr_key] = asyncio.create_task(
                self._repeat_stop_ack(addr, addr_key)
            )

    async def _repeat_stop_ack(self, addr: tuple, addr_key: str) -> None:
        try:
            while True:
                self._send_json({"type": "stop", "status": "accepted"}, addr)
                await asyncio.sleep(STOP_ACK_INTERVAL)
        except asyncio.CancelledError:
            pass
        finally:
            current = asyncio.current_task()
            if self._stop_ack_tasks.get(addr_key) is current:
                self._stop_ack_tasks.pop(addr_key, None)

    async def _run_software_stop(self) -> None:
        try:
            proc = await asyncio.create_subprocess_exec(
                "/bin/bash",
                STOP_SCRIPT,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await proc.communicate()
            if proc.returncode == 0:
                logger.warning(
                    "software stop script finished: %s",
                    stdout.decode(errors="replace").strip(),
                )
            else:
                logger.error(
                    "software stop script failed rc=%d stderr=%s",
                    proc.returncode,
                    stderr.decode(errors="replace").strip(),
                )
        except Exception:
            logger.exception("failed to execute software stop script")
        finally:
            self._software_stop_task = None

    def _handle_control(self, pkt: dict, addr: tuple) -> None:
        """处理控制指令"""
        if self._stop_latched:
            return

        # 验证 Token
        token = pkt.get("token", "")
        if token:
            payload = self._auth.verify_token(token)
            if not payload:
                return
            session = self._sessions.get_session(addr)
            if not session or session.state != SessionState.CONNECTED:
                return

            timestamp = pkt.get("timestamp")
            if (
                not isinstance(timestamp, int)
                or isinstance(timestamp, bool)
                or timestamp <= 0
                or timestamp <= session.last_control_timestamp
            ):
                return

            # Update session state only after both replay checks pass.
            seq = pkt.get("sequence", 0)
            next_sequence = session.sequence
            if isinstance(seq, (int, float)):
                seq = int(seq)
                if seq and seq <= session.sequence:
                    return
                if seq:
                    next_sequence = seq

            session.sequence = next_sequence
            session.last_control_timestamp = timestamp

        addr_key = f"{addr[0]}:{addr[1]}"

        # Axes → AT+JOY
        for json_key, at_axis in _AXIS_MAP.items():
            val = pkt.get(json_key, 0.0)
            if not isinstance(val, (int, float)):
                continue
            if -DEAD_ZONE < val < DEAD_ZONE:
                continue
            clamped = max(-1.0, min(1.0, float(val)))
            self._dispatch(f"AT+JOY={at_axis},{clamped:.3f}")

        # Buttons → AT+BTN (only on state change)
        for json_key, at_name in _BTN_MAP.items():
            pressed = bool(pkt.get(json_key, False))
            prev = self._btn_state.get(addr_key, {}).get(at_name, False)
            if pressed == prev:
                continue
            self._btn_state.setdefault(addr_key, {})[at_name] = pressed
            self._btn_seq[addr_key] = self._btn_seq.get(addr_key, 0) + 1
            state = "down" if pressed else "up"
            self._dispatch(f"AT+BTN={at_name},{state},{self._btn_seq[addr_key]}")

    def _handle_head(self, pkt: dict, addr: tuple) -> None:
        """Handle head motor command (relative angle deltas, CCW positive)."""
        if self._stop_latched:
            return

        # Verify token
        token = pkt.get("token", "")
        if token:
            payload = self._auth.verify_token(token)
            if not payload:
                return
            session = self._sessions.get_session(addr)
            if not session or session.state != SessionState.CONNECTED:
                return

        if not self._head or not self._head.ready:
            logger.warning("head motor driver not ready, ignoring head command")
            return

        # motor1 → axis 1 (yaw), motor2 → axis 2 (pitch); relative deltas in deg
        for field, axis in (("motor1", 1), ("motor2", 2)):
            val = pkt.get(field)
            if not isinstance(val, (int, float)):
                continue
            self._head.move_relative(axis, float(val))

    def _handle_heartbeat(self, pkt: dict, addr: tuple) -> None:
        """处理客户端心跳"""
        token = pkt.get("token", "")
        if not token:
            return
        payload = self._auth.verify_token(token)
        if not payload:
            return
        self._sessions.update_heartbeat(addr)

    def _handle_skill_action(self, pkt: dict, addr: tuple) -> None:
        """处理技能动作请求"""
        if self._stop_latched:
            return

        # 验证 Token
        token = pkt.get("token", "")
        if not token:
            logger.debug("技能动作缺少 token: %s:%d", addr[0], addr[1])
            return

        payload = self._auth.verify_token(token)
        if not payload:
            logger.debug("技能动作 token 无效: %s:%d", addr[0], addr[1])
            return

        # 检查会话状态
        session = self._sessions.get_session(addr)
        if not session or session.state != SessionState.CONNECTED:
            logger.debug("技能动作会话未连接: %s:%d", addr[0], addr[1])
            return

        # 提取技能动作信息
        action_id = pkt.get("action_id", "")
        request_id = pkt.get("request_id", "")
        sequence = pkt.get("sequence", 0)

        logger.info("技能动作: action=%s request=%s seq=%s device=%s",
                    action_id, request_id, sequence, session.device_id)

        # TODO: 处理技能动作逻辑
        # 目前只记录日志，后续可扩展为执行具体动作

    def _send_json(self, data: dict, addr: tuple) -> None:
        """发送 JSON 数据"""
        if not self._transport:
            return
        try:
            msg = json.dumps(data).encode("utf-8")
            self._transport.sendto(msg, addr)
            packet_logger.info(
                "UDP_SEND dst=%s:%d data=%s",
                addr[0], addr[1], json.dumps(data)
            )
        except Exception:
            logger.debug("UDP 发送失败: %s:%d", addr[0], addr[1], exc_info=True)

    def _send_heartbeat(self, addr: tuple) -> None:
        """发送心跳响应"""
        self._send_json({
            "type": "heartbeat",
            "timestamp": int(time.time() * 1000),
        }, addr)

    def _dispatch(self, raw: str) -> None:
        """Parse raw AT line and feed into the handler (fire-and-forget)."""
        cmd = AtCommand.parse(raw)
        if cmd is None:
            return
        try:
            # dispatch() is async — schedule it on the event loop
            loop = asyncio.get_event_loop()
            loop.create_task(self._dispatch_async(cmd))
        except Exception:
            logger.debug("AT dispatch failed for %r", raw, exc_info=True)

    async def _dispatch_async(self, cmd) -> None:
        try:
            await self._handler.dispatch(cmd)  # dispatch() returns list[str]
        except Exception:
            logger.debug("AT dispatch failed", exc_info=True)
