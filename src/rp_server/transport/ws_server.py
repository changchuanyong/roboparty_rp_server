# SPDX-License-Identifier: GPL-3.0
# Copyright (C) 2026 mustaf-osman (https://github.com/mustaf-osman)
# Copyright (C) 2026 wentywenty (https://github.com/wentywenty)

"""Transport layer — FastAPI + WebSocket + Serial + Bluetooth + UDP."""

import asyncio
import logging
import os
import time

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

from .. import __version__
from ..protocol.at_handler import AtHandler
from ..protocol.at_parser import AtCommand, resp_conn
from ..drivers.motors import MotorDriver
from ..drivers.head import HeadDriver
from ..drivers.imu import IMUDriver
from ..drivers.bms import BMSDriver
from ..drivers.joy import JoyDriver
from ..drivers.policy import PolicyDriver
from ..monitors import TelemetryMonitor
from ..state import AppState
from .serial_server import SerialATServer
from .bt_server import BTServer
from .udp_listener import UDPJoyListener
from .can_joy_listener import CANJoyListener

logger = logging.getLogger("rp_server.transport")
packet_logger = logging.getLogger("rp_server.packets")


def _missing_hardware(status: dict[str, bool], required: tuple[str, ...]) -> list[str]:
    return [name for name in required if not status.get(name, False)]


def create_app(config: dict) -> FastAPI:
    app = FastAPI(title="RoboParty RP Server", version=__version__)

    mock = bool(config.get("server", {}).get("mock")) or os.environ.get("RP_MOCK", "") in ("1", "true", "TRUE")
    if mock:
        config.setdefault("server", {})["mock"] = True
        logger.warning("running in MOCK mode (synthetic telemetry, no hardware pybind)")

    # --- drivers ---
    motors = MotorDriver()
    imu = IMUDriver()
    bms = BMSDriver()
    joy = JoyDriver()
    head = HeadDriver()
    policy = PolicyDriver(
        config.get("robot", {}).get("launch_cmd",
                                     "ros2 launch roboparty-inference inference.launch.py"))
    hardware_cfg = config.get("hardware", {})
    required_hardware = tuple(hardware_cfg.get("required", ("motors", "imu")))
    hardware_status = {
        "motors": mock,
        "imu": mock,
        "bms": mock,
        "joy": mock,
        "head": mock,
    }

    def hardware_ready() -> bool:
        return not _missing_hardware(hardware_status, required_hardware)

    # --- protocol ---
    at_handler = AtHandler(motors, imu, bms, joy, policy)

    # --- monitors ---
    telemetry = TelemetryMonitor(imu, bms, motors, config, mock=mock)

    rp = AppState(
        config=config,
        hardware_status=hardware_status,
        required_hardware=required_hardware,
        motors=motors,
        imu=imu,
        bms=bms,
        joy=joy,
        policy=policy,
        head=head,
        at_handler=at_handler,
        telemetry=telemetry,
        mock=mock,
    )
    app.state.rp = rp

    # --- transports ---
    transports_enabled = config.get("transports", {"ws": True, "serial": False, "bluetooth": False})

    scfg = config.get("serial", {})
    serial_srv = SerialATServer(
        scfg.get("port", "/dev/ttyAMA0"),
        scfg.get("baudrate", 115200),
        at_handler,
    ) if transports_enabled.get("serial") else None

    bcfg = config.get("bluetooth", {})
    bt_srv = BTServer(at_handler, channel=bcfg.get("channel", 1)) \
        if transports_enabled.get("bluetooth") else None

    ucfg = config.get("udp", {})
    auth_cfg = ucfg.get("auth", {})
    udp_srv = UDPJoyListener(
        at_handler,
        host=ucfg.get("host", "0.0.0.0"),
        port=ucfg.get("port", 9000),
        secret_key=auth_cfg.get("secret_key", ""),
        token_ttl=auth_cfg.get("token_ttl", 3600),
        session_timeout=ucfg.get("session_timeout", 10),
        telemetry=telemetry,
        head=head,
    ) if transports_enabled.get("udp") else None

    can_joy_cfg = config.get("can_gamepad", {})
    can_joy_payloads = can_joy_cfg.get("payloads")
    if can_joy_payloads is None:
        if "payload" in can_joy_cfg:
            can_joy_payloads = [can_joy_cfg["payload"]]
        else:
            can_joy_payloads = ["010001AA00000000", "010001AA00000001"]
    can_joy_srv = CANJoyListener(
        at_handler,
        interface=can_joy_cfg.get("interface", "can_top"),
        can_id=int(can_joy_cfg.get("can_id", 0x003)),
        payloads=tuple(bytes.fromhex(payload) for payload in can_joy_payloads),
        click_ms=int(can_joy_cfg.get("click_ms", 50)),
    ) if can_joy_cfg.get("enabled", False) else None

    # --- lifespan ---
    @app.on_event("startup")
    async def on_startup():
        if not mock:
            hardware_status.update({
                "motors": motors.init(config),
                "imu": imu.init(config),
                "bms": bms.init(config),
                "joy": joy.init(),
                "head": head.init(config),
            })
            missing = _missing_hardware(hardware_status, required_hardware)
            if missing:
                message = f"required hardware unavailable: {', '.join(missing)}"
                if hardware_cfg.get("fail_startup_if_unavailable", False):
                    logger.critical(message)
                    raise RuntimeError(message)
                logger.error("%s (service remains degraded)", message)
            else:
                logger.info("required hardware ready: %s", ", ".join(required_hardware))
        else:
            logger.info("mock: skipping hardware driver init")
        await telemetry.start()
        if serial_srv:
            await serial_srv.start()
        if bt_srv:
            await bt_srv.start()
        if udp_srv:
            await udp_srv.start()
        if can_joy_srv and not mock:
            await can_joy_srv.start()
        logger.info("rp_server ready mock=%s port_cfg=%s", mock, config.get("server", {}))

    @app.on_event("shutdown")
    async def on_shutdown():
        if bt_srv:
            await bt_srv.stop()
        if udp_srv:
            udp_srv.stop()
        if can_joy_srv:
            await can_joy_srv.stop()
        if serial_srv:
            await serial_srv.stop()
        await telemetry.stop()
        if policy.running:
            await policy.stop()
        if not mock:
            joy.deinit()
            imu.deinit()
            bms.deinit()
            motors.deinit()
            head.deinit()

    # ------------------------------------------------------------------
    # REST API
    # ------------------------------------------------------------------

    @app.get("/health")
    async def health():
        ready = True if mock else hardware_ready()
        return {
            "status": "ok" if ready else "degraded",
            "hw_ready": ready,
            "hardware": dict(hardware_status),
            "required_hardware": list(required_hardware),
            "mock": mock,
            "version": __version__,
        }

    @app.get("/sysinfo")
    async def sysinfo():
        import psutil
        load = []
        try:
            load = list(psutil.getloadavg())
        except (AttributeError, OSError):
            load = [0.0, 0.0, 0.0]
        return {
            "cpu": psutil.cpu_percent(interval=0.1),
            "mem": psutil.virtual_memory().percent,
            "load": load,
        }

    @app.get("/api/status")
    async def api_status():
        return JSONResponse({
            "hw_ready": True if mock else hardware_ready(),
            "hardware": dict(hardware_status),
            "mock": mock,
            "policy": policy.name,
            "policy_running": policy.running,
            "joy_device": joy.device_path,
            "motor_errors": [] if mock else motors.get_errors(),
            "battery": telemetry.last_battery or (None if mock else bms.read()),
            "imu": telemetry.last_imu or (None if mock else imu.read()),
        })

    # ------------------------------------------------------------------
    # WebSocket
    # ------------------------------------------------------------------

    @app.websocket("/ws")
    async def ws_endpoint(ws: WebSocket):
        client_addr = ws.client.host if ws.client else "unknown"
        logger.info("WebSocket 连接建立: %s", client_addr)
        connect_time = time.time()

        await ws.accept()
        q: asyncio.Queue = asyncio.Queue(maxsize=256)
        telemetry.add_client(q)

        async def sender():
            while True:
                msg = await q.get()
                try:
                    await ws.send_text(msg)
                except Exception:
                    break

        send_task = asyncio.create_task(sender())
        try:
            await ws.send_text(resp_conn(True, True if mock else hardware_ready()))
            async for raw in ws.iter_text():
                # 记录接收到的数据包
                packet_logger.info("WS_RECV src=%s data=%s", client_addr, raw.strip())
                cmd = AtCommand.parse(raw)
                if cmd is None:
                    continue
                try:
                    for resp in await at_handler.dispatch(cmd):
                        await ws.send_text(resp)
                except Exception as exc:
                    logger.warning("AT 命令执行失败: %s (命令: %s)", exc, raw.strip())
        except WebSocketDisconnect:
            pass
        finally:
            send_task.cancel()
            telemetry.remove_client(q)
            duration = time.time() - connect_time
            logger.info("WebSocket 连接断开: %s (时长: %.1fs)", client_addr, duration)

    return app
