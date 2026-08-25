# SPDX-License-Identifier: GPL-3.0
# Copyright (C) 2026 mustaf-osman (https://github.com/mustaf-osman)
# Copyright (C) 2026 wentywenty (https://github.com/wentywenty)

"""UDP 会话管理模块 — 服务器端客户端状态跟踪"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional

logger = logging.getLogger("rp_server.udp.session")


class SessionState(Enum):
    """会话状态"""
    DISCONNECTED = "disconnected"
    AUTHENTICATING = "authenticating"
    CONNECTED = "connected"


@dataclass
class UDPSession:
    """UDP 会话信息"""
    addr: tuple[str, int]
    device_id: str
    device_name: str = ""
    token: str = ""
    state: SessionState = SessionState.DISCONNECTED
    last_seen: datetime = field(default_factory=datetime.now)
    last_heartbeat: datetime = field(default_factory=datetime.now)
    sequence: int = 0
    last_control_timestamp: int = 0

    @property
    def addr_key(self) -> str:
        """地址键 (host:port)"""
        return f"{self.addr[0]}:{self.addr[1]}"


class SessionManager:
    """会话管理器"""

    def __init__(self, timeout: float = 10.0):
        """
        Args:
            timeout: 会话超时时间（秒）
        """
        self._sessions: dict[str, UDPSession] = {}
        self._timeout = timeout

    def add_session(self, session: UDPSession) -> None:
        """添加或更新会话"""
        self._sessions[session.addr_key] = session
        logger.info("会话添加: device=%s addr=%s state=%s",
                    session.device_id, session.addr_key, session.state.value)

    def remove_session(self, addr: tuple[str, int]) -> Optional[UDPSession]:
        """移除会话"""
        addr_key = f"{addr[0]}:{addr[1]}"
        session = self._sessions.pop(addr_key, None)
        if session:
            logger.info("会话移除: device=%s addr=%s", session.device_id, addr_key)
        return session

    def get_session(self, addr: tuple[str, int]) -> Optional[UDPSession]:
        """获取会话"""
        addr_key = f"{addr[0]}:{addr[1]}"
        return self._sessions.get(addr_key)

    def update_activity(self, addr: tuple[str, int]) -> None:
        """更新会话活动时间"""
        session = self.get_session(addr)
        if session:
            session.last_seen = datetime.now()

    def update_heartbeat(self, addr: tuple[str, int]) -> None:
        """更新会话心跳时间"""
        session = self.get_session(addr)
        if session:
            session.last_heartbeat = datetime.now()

    def set_connected(self, addr: tuple[str, int], token: str) -> None:
        """设置会话为已连接状态"""
        session = self.get_session(addr)
        if session:
            session.state = SessionState.CONNECTED
            session.token = token
            logger.info("会话已连接: device=%s addr=%s", session.device_id, session.addr_key)

    def cleanup_expired(self) -> list[UDPSession]:
        """清理超时的会话

        Returns:
            被移除的会话列表
        """
        now = datetime.now()
        expired = [
            session for session in self._sessions.values()
            if now - session.last_seen > timedelta(seconds=self._timeout)
        ]
        for session in expired:
            del self._sessions[session.addr_key]
            logger.info("会话超时移除: device=%s addr=%s (超时 %.1f 秒)",
                        session.device_id, session.addr_key, self._timeout)
        return expired

    @property
    def session_count(self) -> int:
        """当前会话数量"""
        return len(self._sessions)

    @property
    def connected_sessions(self) -> list[UDPSession]:
        """获取所有已连接的会话"""
        return [
            session for session in self._sessions.values()
            if session.state == SessionState.CONNECTED
        ]
