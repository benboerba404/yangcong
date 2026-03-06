# -*- coding: utf-8 -*-
"""
工作流编排：管理会话状态，串联 LLM → SQL 执行 → 结果返回 的完整链路。

优化点：
  - SQL 生成期间并行预热 SSH 隧道，执行阶段省去 3-5s 建连时间
  - 流式进度反馈：Cursor CLI 有输出就通知用户，减少等待焦虑
  - 减少重复消息：合并状态提示

两种模式（可全局切换）：
  - 审查模式（默认）：生成 SQL → 展示给用户确认 → 确认后执行
  - 快速模式：生成 SQL 后直接执行，跳过确认
"""
import json
import logging
import os
import threading
import time
from typing import Dict

from .feishu_client import FeishuClient
from .cursor_client import CursorClient
from .sql_executor import SQLExecutor

logger = logging.getLogger(__name__)

CONFIRM_KEYWORDS = {"确认", "执行", "跑", "ok", "OK", "可以", "没问题", "对", "yes", "Yes", "好"}
CANCEL_KEYWORDS  = {"取消", "算了", "不用了", "放弃"}
HELP_KEYWORDS    = {"帮助", "help", "用法", "使用说明"}
FAST_PREFIXES    = ("直接跑", "直接执行", "直接出数", "跳过确认")
MODIFY_PREFIXES  = ("修改", "改一下", "帮我改", "修改意见", "修改建议", "调整", "需要改")

FAST_MODE_ON_KEYWORDS  = {"快速模式", "不需要确认", "不用确认", "不需要sql", "不用sql",
                           "跳过sql", "直接出数模式", "快速出数"}
FAST_MODE_OFF_KEYWORDS = {"审查模式", "需要确认", "看sql", "要确认sql", "恢复确认"}

HELP_TEXT = """\
我是 BI 取数机器人，使用方式：

【审查模式（默认）】直接描述需求 → 展示 SQL → 确认后执行：
  「上个月各团队的营收」

确认 SQL 后可以：
  · 回复「确认」→ 执行并返回数据
  · 直接描述修改意见 → 重新生成 SQL
  · 回复「取消」→ 放弃本次查询

【快速模式】生成 SQL 后直接执行，不需要确认：
  发送「快速模式」或「不需要确认」→ 开启（之后所有查询都直接跑）
  发送「审查模式」或「需要确认」  → 关闭，恢复默认

【单次跳过确认】加前缀，仅此次跳过：
  「直接跑：上个月各团队的营收」

数据 ≤20行直接展示，>20行生成 Excel 文件发给你。"""


class Workflow:
    def __init__(
        self,
        feishu: FeishuClient,
        cursor_client: CursorClient,
        executor: SQLExecutor,
        config: dict,
    ):
        self.feishu = feishu
        self.cursor_client = cursor_client
        self.executor = executor
        self.output_dir = config.get("output_dir", "../output")
        self.data_threshold = config.get("data_threshold", 20)
        self.sessions: Dict[str, dict] = {}
        self.fast_mode_users: Dict[str, bool] = {}

        self._progress_interval = 15

    # ── 主入口 ────────────────────────────────────────────

    def handle_message(self, user_id: str, chat_id: str, message_id: str, text: str):
        text = text.strip()

        if text in HELP_KEYWORDS:
            self.feishu.send_text(chat_id, HELP_TEXT)
            return

        if text in FAST_MODE_ON_KEYWORDS:
            self.fast_mode_users[user_id] = True
            self.feishu.send_text(chat_id,
                "⚡ 已开启快速模式：之后的查询直接生成并执行，不再展示 SQL 确认。\n"
                "发送「审查模式」可恢复 SQL 确认流程。")
            return
        if text in FAST_MODE_OFF_KEYWORDS:
            self.fast_mode_users[user_id] = False
            self.feishu.send_text(chat_id,
                "📋 已切换到审查模式：每次查询会先展示 SQL，确认后再执行。")
            return

        session = self.sessions.get(user_id)

        if session:
            state = session.get("state")
            if state == "executing":
                self.feishu.send_text(chat_id, "⏳ SQL 正在执行中，请稍候，不要重复发送...")
                return
            if state == "wait_confirm":
                if text in CANCEL_KEYWORDS:
                    self._cancel(user_id, chat_id)
                elif text in CONFIRM_KEYWORDS:
                    self._execute(user_id, chat_id, session)
                else:
                    self._modify(user_id, chat_id, text, session)
                return

        if text in CONFIRM_KEYWORDS:
            self.feishu.send_text(chat_id, "⚠️ 没有待确认的查询，请直接发送数据需求。")
            return
        if text in CANCEL_KEYWORDS:
            self.feishu.send_text(chat_id, "✅ 没有进行中的查询。")
            return
        if text.startswith(MODIFY_PREFIXES):
            self.feishu.send_text(chat_id, "⚠️ 没有待修改的查询，请重新发送完整的数据需求。")
            return

        for prefix in FAST_PREFIXES:
            if text.startswith(prefix):
                requirement = text[len(prefix):].lstrip("：: ").strip()
                if requirement:
                    self._fast_mode(user_id, chat_id, requirement)
                else:
                    self.feishu.send_text(chat_id, "请在前缀后面描述您的需求。")
                return

        if self.fast_mode_users.get(user_id):
            self._fast_mode(user_id, chat_id, text)
        else:
            self._review_mode(user_id, chat_id, text)

    # ── 快速模式 ──────────────────────────────────────────

    def _fast_mode(self, user_id: str, chat_id: str, text: str):
        self.feishu.send_text(chat_id, "⏳ 正在生成并执行查询...")

        # 并行预热 SSH 隧道（SQL 生成期间提前建连）
        tunnel_thread = threading.Thread(target=self._safe_ensure_tunnel, daemon=True)
        tunnel_thread.start()

        history = self.sessions.get(user_id, {}).get("history")
        if history:
            history = list(history) + [{"role": "user", "content": text}]

        progress_cb = self._make_progress_callback(chat_id)
        result = self.cursor_client.generate_sql(text, history, on_progress=progress_cb)

        if not result.get("sql"):
            msg = result.get("understanding", "需求理解失败，请更详细地描述您的数据需求。")
            self.feishu.send_text(chat_id, f"❓ {msg}")
            return

        prev_history = history or []
        new_history = prev_history if history else [{"role": "user", "content": text}]
        new_history = new_history + [
            {"role": "assistant", "content": json.dumps(result, ensure_ascii=False)}
        ]
        self.sessions[user_id] = {"state": "done", "history": new_history[-12:]}

        # 等待隧道预热完成
        tunnel_thread.join(timeout=10)

        logger.info("快速模式 SQL 生成完成，直接执行")
        self._run_and_reply(chat_id, result["sql"], result.get("filename", "查询结果.xlsx"))

    # ── 审查模式 ──────────────────────────────────────────

    def _review_mode(self, user_id: str, chat_id: str, text: str):
        self.feishu.send_text(chat_id, "⏳ 正在生成 SQL，请稍候...")

        # 审查模式也预热隧道，这样确认后执行更快
        tunnel_thread = threading.Thread(target=self._safe_ensure_tunnel, daemon=True)
        tunnel_thread.start()

        progress_cb = self._make_progress_callback(chat_id)
        result = self.cursor_client.generate_sql(text, on_progress=progress_cb)

        if not result.get("sql"):
            msg = result.get("understanding", "需求理解失败，请更详细地描述您的数据需求。")
            self.feishu.send_text(chat_id, f"❓ {msg}")
            return

        self.sessions[user_id] = {
            "state":         "wait_confirm",
            "chat_id":       chat_id,
            "sql":           result["sql"],
            "filename":      result.get("filename", "查询结果.xlsx"),
            "output_fields": result.get("output_fields", []),
            "history": [
                {"role": "user",      "content": text},
                {"role": "assistant", "content": json.dumps(result, ensure_ascii=False)},
            ],
        }
        self._send_confirmation(chat_id, result)

    # ── 修改（审查模式内）────────────────────────────────

    def _modify(self, user_id: str, chat_id: str, text: str, session: dict):
        self.feishu.send_text(chat_id, "⏳ 正在修改 SQL...")

        history = list(session.get("history", []))
        history.append({"role": "user", "content": text})

        result = self.cursor_client.generate_sql(text, history)

        if not result.get("sql"):
            self.feishu.send_text(chat_id, f"❓ {result.get('understanding', '修改失败，请再描述一下。')}")
            return

        history.append({"role": "assistant", "content": json.dumps(result, ensure_ascii=False)})
        session.update({
            "sql":          result["sql"],
            "filename":     result.get("filename", session.get("filename")),
            "output_fields": result.get("output_fields", []),
            "history":      history,
        })
        self._send_confirmation(chat_id, result, modified=True)

    # ── 确认执行（审查模式内）────────────────────────────

    def _execute(self, user_id: str, chat_id: str, session: dict):
        session["state"] = "executing"
        success = self._run_and_reply(chat_id, session["sql"], session.get("filename", "查询结果.xlsx"))
        if success:
            self.sessions.pop(user_id, None)
        else:
            session["state"] = "wait_confirm"

    # ── 取消 ─────────────────────────────────────────────

    def _cancel(self, user_id: str, chat_id: str):
        self.sessions.pop(user_id, None)
        self.feishu.send_text(chat_id, "✅ 已取消。")

    # ── SQL 执行 + 结果返回 ──────────────────────────────

    def _run_and_reply(self, chat_id: str, sql: str, filename: str) -> bool:
        self.feishu.send_text(chat_id, "⏳ 正在执行 SQL...")
        try:
            df = self.executor.execute(sql)
        except Exception as e:
            logger.exception("SQL 执行失败")
            self.feishu.send_text(chat_id, f"❌ SQL 执行失败：{e}\n\n请回复「确认」重试，或直接描述修改意见。")
            return False

        row_count = len(df)

        if row_count == 0:
            self.feishu.send_text(chat_id, "⚠️ 查询结果为空，请检查筛选条件是否过严。")
        elif row_count <= self.data_threshold:
            table_text = df.to_csv(sep="\t", index=False)
            msg = f"✅ 查询完成，共 {row_count} 行\n\n{table_text}\n\n（可直接复制粘贴到 Excel）"
            self.feishu.send_text(chat_id, msg)
        else:
            os.makedirs(self.output_dir, exist_ok=True)
            filepath = os.path.join(self.output_dir, filename)
            df.to_excel(filepath, index=False, engine="openpyxl")
            self.feishu.send_text(chat_id, f"✅ 查询完成，共 {row_count} 行，正在上传文件...")
            self.feishu.send_file(chat_id, filepath, filename)
        return True

    # ── 辅助 ─────────────────────────────────────────────

    def _safe_ensure_tunnel(self):
        """安全预热 SSH 隧道，不抛异常（后台线程调用）。"""
        try:
            self.executor.ensure_tunnel()
        except Exception as e:
            logger.warning("SSH 隧道预热失败（不影响后续重试）: %s", e)

    def _make_progress_callback(self, chat_id: str):
        """
        创建节流进度回调：每 _progress_interval 秒最多发一次进度消息，
        避免刷屏又让用户知道还在处理。
        """
        state = {"last_notify": time.time(), "notified": False}

        def _cb(line: str):
            now = time.time()
            if now - state["last_notify"] >= self._progress_interval and not state["notified"]:
                self.feishu.send_text(chat_id, "⏳ Cursor 仍在处理中...")
                state["last_notify"] = now
                state["notified"] = True

        return _cb

    def _send_confirmation(self, chat_id: str, result: dict, modified: bool = False):
        title  = "📋 SQL（已修改）" if modified else "📋 SQL 预览"
        fields = "、".join(result.get("output_fields", [])) or "见 SQL"
        msg = f"""{title}
━━━━━━━━━━━━━━━━━━━━
{result['understanding']}
输出字段：{fields}

{result['sql']}

━━━━━━━━━━━━━━━━━━━━
回复「确认」执行 / 直接描述修改意见 / 「取消」放弃"""
        self.feishu.send_text(chat_id, msg)
