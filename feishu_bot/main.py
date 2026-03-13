# -*- coding: utf-8 -*-
"""
飞书 BI 取数机器人 —— 主入口

通过飞书 WebSocket 长连接接收消息，无需服务器和公网 IP。
使用方式:在终端执行:python main.py
日志里 connected to wss://... 说明一切正常，这个终端要保持运行，不要关闭。
"""
import os
import urllib.request

# requests 库在 Windows 上会从注册表读取系统代理，即使系统代理已关闭也可能读到旧地址。
# 在任何网络库加载前覆盖掉代理检测，确保直连飞书。
urllib.request.getproxies = lambda: {}
for _proxy_var in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY",
                   "http_proxy", "https_proxy", "all_proxy"):
    os.environ.pop(_proxy_var, None)
os.environ["NO_PROXY"] = "*"
os.environ["no_proxy"] = "*"

import atexit
import json
import logging
import re
import signal
import sys
import threading
from collections import OrderedDict

import lark_oapi as lark
from lark_oapi import EventDispatcherHandler, ws, LogLevel
from lark_oapi.api.im.v1 import P2ImMessageReceiveV1

from core.feishu_client import FeishuClient
from core.cursor_client import CursorClient
from core.sql_executor import SQLExecutor
from core.workflow import Workflow

_log_fmt = "%(asctime)s [%(name)s] %(levelname)s: %(message)s"
_log_datefmt = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(level=logging.INFO, format=_log_fmt, datefmt=_log_datefmt)

_log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot.log")
_fh = logging.FileHandler(_log_file, encoding="utf-8")
_fh.setFormatter(logging.Formatter(_log_fmt, datefmt=_log_datefmt))
logging.getLogger().addHandler(_fh)
logger = logging.getLogger("main")


class EventDedup:
    """基于有序字典的事件去重，防止飞书超时重推导致重复处理。"""

    def __init__(self, max_size: int = 2000):
        self._seen: OrderedDict[str, bool] = OrderedDict()
        self._max = max_size

    def is_duplicate(self, event_id: str) -> bool:
        if event_id in self._seen:
            return True
        if len(self._seen) >= self._max:
            self._seen.popitem(last=False)
        self._seen[event_id] = True
        return False


PID_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".bot.pid")


def _check_single_instance():
    """确保只有一个机器人实例在运行，防止多进程抢消息。"""
    my_pid = os.getpid()

    if os.path.isfile(PID_FILE):
        try:
            with open(PID_FILE, "r") as f:
                old_pid = int(f.read().strip())
            if old_pid != my_pid and _is_process_alive(old_pid):
                logger.error(
                    "检测到机器人已在运行 (PID %d)，请勿重复启动！\n"
                    "如需重启，请先关闭旧终端或运行: taskkill /PID %d /F",
                    old_pid, old_pid)
                sys.exit(1)
        except (ValueError, OSError):
            pass

    with open(PID_FILE, "w") as f:
        f.write(str(my_pid))
    atexit.register(lambda: _remove_pid_file(my_pid))
    logger.info("单实例检查通过 (PID %d)", my_pid)


def _is_process_alive(pid: int) -> bool:
    """检查指定 PID 的进程是否还在运行（仅限 main.py）。"""
    try:
        import ctypes
        kernel32 = ctypes.windll.kernel32
        SYNCHRONIZE = 0x00100000
        handle = kernel32.OpenProcess(SYNCHRONIZE, False, pid)
        if handle:
            kernel32.CloseHandle(handle)
            return True
        return False
    except Exception:
        return False


def _remove_pid_file(my_pid: int):
    try:
        if os.path.isfile(PID_FILE):
            with open(PID_FILE, "r") as f:
                stored = int(f.read().strip())
            if stored == my_pid:
                os.remove(PID_FILE)
    except Exception:
        pass


def main():
    # ── 单实例检查 ─────────────────────────────────────────
    _check_single_instance()

    # ── 加载配置 ─────────────────────────────────────────
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, "config.json")

    if not os.path.isfile(config_path):
        logger.error(
            "未找到 config.json，请复制 config.json.example 为 config.json 并填写配置"
        )
        sys.exit(1)

    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    if "output_dir" in config and not os.path.isabs(config["output_dir"]):
        config["output_dir"] = os.path.normpath(os.path.join(base_dir, config["output_dir"]))

    # ── 初始化组件 ────────────────────────────────────────
    feishu_cfg = config["feishu"]
    feishu_client = FeishuClient(feishu_cfg["app_id"], feishu_cfg["app_secret"])

    cursor_cfg = config.get("cursor", {})
    if not cursor_cfg.get("workspace"):
        cursor_cfg["workspace"] = os.path.normpath(os.path.join(base_dir, ".."))
    elif not os.path.isabs(cursor_cfg["workspace"]):
        cursor_cfg["workspace"] = os.path.normpath(os.path.join(base_dir, cursor_cfg["workspace"]))
    cursor_client = CursorClient(cursor_cfg)

    sql_executor = SQLExecutor(config["database"])

    workflow = Workflow(feishu_client, cursor_client, sql_executor, config)

    def _cleanup():
        logger.info("正在关闭 SSH 隧道...")
        sql_executor.close()

    atexit.register(_cleanup)

    dedup = EventDedup()

    # ── 消息处理 ──────────────────────────────────────────
    def on_message(data: P2ImMessageReceiveV1):
        event_id = data.header.event_id
        if dedup.is_duplicate(event_id):
            return

        message = data.event.message
        sender = data.event.sender

        if message.message_type != "text":
            return

        chat_id = message.chat_id
        user_id = sender.sender_id.open_id

        try:
            content = json.loads(message.content)
            text = content.get("text", "")
        except Exception:
            return

        # 去掉 @机器人 的 mention 标记
        text = re.sub(r"@_user_\d+\s*", "", text).strip()
        if not text:
            return

        logger.info("收到消息 [user=%s]: %s", user_id, text[:80])

        # 在后台线程处理，避免 WebSocket 3 秒超时
        threading.Thread(
            target=_safe_handle,
            args=(workflow, user_id, chat_id, message.message_id, text),
            daemon=True,
        ).start()

    # ── 启动 WebSocket 长连接 ─────────────────────────────
    event_handler = (
        EventDispatcherHandler.builder("", "")
        .register_p2_im_message_receive_v1(on_message)
        .build()
    )

    logger.info("正在连接飞书长连接...")
    ws_client = ws.Client(
        feishu_cfg["app_id"],
        feishu_cfg["app_secret"],
        event_handler=event_handler,
        log_level=LogLevel.INFO,
    )
    ws_client.start()


def _safe_handle(workflow: Workflow, user_id, chat_id, message_id, text):
    try:
        workflow.handle_message(user_id, chat_id, message_id, text)
    except Exception:
        logger.exception("处理消息异常 [user=%s]", user_id)
        try:
            workflow.feishu.send_text(chat_id, "❌ 处理出错，请稍后重试。")
        except Exception:
            pass


if __name__ == "__main__":
    main()
