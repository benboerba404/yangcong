# -*- coding: utf-8 -*-
"""SQL 执行器：通过 SSH 隧道连接跳板机执行 Spark SQL，返回 DataFrame。"""
import logging
import time

import pandas as pd
from impala.dbapi import connect as impala_connect
from sshtunnel import SSHTunnelForwarder

logger = logging.getLogger(__name__)

_MAX_RETRIES = 2        # SQL 执行最多重试 2 次（共 3 次尝试）
_RETRY_DELAY = 5        # 重试前等待秒数


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

    def execute(self, sql: str) -> pd.DataFrame:
        """执行 SQL 并返回 DataFrame，失败自动重试。"""
        last_exc = None
        for attempt in range(1, _MAX_RETRIES + 2):
            try:
                return self._run(sql, attempt)
            except Exception as e:
                last_exc = e
                logger.warning("SQL 执行失败（第 %d 次）: %s", attempt, e)
                if attempt <= _MAX_RETRIES:
                    logger.info("等待 %ds 后重试...", _RETRY_DELAY)
                    time.sleep(_RETRY_DELAY)
        raise last_exc

    def _run(self, sql: str, attempt: int = 1) -> pd.DataFrame:
        t0 = time.time()
        prefix = f"[第{attempt}次] " if attempt > 1 else ""

        logger.info("%s正在建立 SSH 隧道...", prefix)
        with SSHTunnelForwarder(
            (self.ssh_host, 22),
            ssh_username=self.ssh_user,
            ssh_password=self.ssh_password,
            remote_bind_address=(self.db_host, self.db_port),
            local_bind_address=("127.0.0.1", 0),
        ) as tunnel:
            local_port = tunnel.local_bind_port
            logger.info("%s隧道已建立: 127.0.0.1:%d (%.1fs)", prefix, local_port, time.time() - t0)

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
