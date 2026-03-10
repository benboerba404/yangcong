# -*- coding: utf-8 -*-
"""
SQL 执行器：通过持久化 SSH 隧道连接跳板机执行 Spark SQL，返回 DataFrame。

优化点：
  - 持久化 SSH 隧道：首次连接后保持存活，后续查询复用（省去 3-5s 建连开销）
  - 线程安全：通过 Lock 保护隧道创建，支持多用户并发
  - 自动重连：隧道断开时自动重建
  - 预热接口：可在 SQL 生成期间提前建立隧道
"""
import logging
import threading
import time

import pandas as pd
from impala.dbapi import connect as impala_connect
from sshtunnel import SSHTunnelForwarder

logger = logging.getLogger(__name__)

_MAX_RETRIES = 2
_RETRY_DELAY = 5


class SQLExecutor:
    def __init__(self, config: dict):
        self.ssh_host = config["ssh_host"]
        self.ssh_user = config["ssh_user"]
        self.ssh_password = config["ssh_password"]
        self.db_host = config.get("db_host", "10.17.2.45")
        self.db_port = config.get("db_port", 10010)
        self.db_user = config.get("db_user") or config.get("username", "")
        self.db_password = config.get("db_password") or config.get("password", "")
        self.database = config.get("database", "tmp")

        self._tunnel: SSHTunnelForwarder | None = None
        self._lock = threading.Lock()

    # ── 隧道管理 ──────────────────────────────────────────

    def ensure_tunnel(self):
        """确保 SSH 隧道存活，供外部预热调用（线程安全）。"""
        with self._lock:
            if self._tunnel_alive():
                return
            self._close_tunnel_unsafe()
            self._open_tunnel_unsafe()

    def _tunnel_alive(self) -> bool:
        if self._tunnel is None:
            return False
        try:
            return self._tunnel.is_active and self._tunnel.tunnel_is_up
        except Exception:
            return False

    def _open_tunnel_unsafe(self):
        """创建新隧道（调用前必须持有 _lock）。"""
        t0 = time.time()
        logger.info("正在建立 SSH 隧道...")
        tunnel = SSHTunnelForwarder(
            (self.ssh_host, 22),
            ssh_username=self.ssh_user,
            ssh_password=self.ssh_password,
            remote_bind_address=(self.db_host, self.db_port),
            local_bind_address=("127.0.0.1", 0),
            set_keepalive=30,
        )
        tunnel.start()
        self._tunnel = tunnel
        logger.info("SSH 隧道已建立: 127.0.0.1:%d (%.1fs)", tunnel.local_bind_port, time.time() - t0)

    def _close_tunnel_unsafe(self):
        if self._tunnel:
            try:
                self._tunnel.stop()
            except Exception:
                pass
            self._tunnel = None

    def close(self):
        """关闭持久化隧道，程序退出时调用。"""
        with self._lock:
            self._close_tunnel_unsafe()

    # ── SQL 执行 ──────────────────────────────────────────

    def execute(self, sql: str) -> pd.DataFrame:
        """执行 SQL 并返回 DataFrame，失败自动重试。"""
        last_exc = None
        for attempt in range(1, _MAX_RETRIES + 2):
            try:
                return self._run(sql, attempt)
            except Exception as e:
                last_exc = e
                logger.warning("SQL 执行失败（第 %d 次）: %s", attempt, e)
                # 隧道可能断了，强制重建
                with self._lock:
                    self._close_tunnel_unsafe()
                if attempt <= _MAX_RETRIES:
                    logger.info("等待 %ds 后重试...", _RETRY_DELAY)
                    time.sleep(_RETRY_DELAY)
        raise last_exc

    def _run(self, sql: str, attempt: int = 1) -> pd.DataFrame:
        t0 = time.time()
        prefix = f"[第{attempt}次] " if attempt > 1 else ""

        self.ensure_tunnel()
        local_port = self._tunnel.local_bind_port

        t1 = time.time()
        conn = impala_connect(
            host="127.0.0.1",
            port=local_port,
            auth_mechanism="PLAIN",
            database=self.database,
            user=self.db_user,
            password=self.db_password,
        )
        cur = conn.cursor(dictify=True)
        logger.info("%s数据库连接成功 (%.1fs)", prefix, time.time() - t1)

        t2 = time.time()
        cur.execute(sql)
        rows = cur.fetchall()
        logger.info("%s查询完成: %d 行 (%.1fs)", prefix, len(rows), time.time() - t2)

        conn.close()
        logger.info("%sSQL 执行总耗时: %.1fs", prefix, time.time() - t0)
        return pd.DataFrame(rows)
