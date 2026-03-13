# -*- coding: utf-8 -*-
"""
工作流编排：管理会话状态，串联 LLM → SQL 执行 → 结果返回 的完整链路。

核心机制：
  - 请求版本号(req_id)：每个请求有唯一 ID，所有异步操作（发消息、改状态）前
    先检查自己是否仍为当前请求，过期则静默退出，防止旧线程干扰新请求。
  - 状态机：generating → wait_confirm → executing（期间拦截一切非取消操作）
  - SSH 隧道预热：SQL 生成期间并行建连，执行阶段省去 3-5s
  - 心跳：每 20 秒告知用户仍在处理

两种模式：
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
from .cursor_client import CursorClient, ProcRef
from .sql_executor import SQLExecutor

logger = logging.getLogger(__name__)

SESSION_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".sessions.json")
PERSISTABLE_STATES = ("wait_confirm",)

CONFIRM_KEYWORDS = {"确认", "执行", "跑", "ok", "OK", "可以", "没问题", "对", "yes", "Yes", "好"}
CANCEL_KEYWORDS  = {"取消", "算了", "不用了", "放弃"}
RESET_KEYWORDS   = {"重置", "重新开始", "清空", "新查询", "重新查询", "reset"}
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

【重置】遇到状态混乱时，发送「重置」清空当前会话状态。

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
        self._req_seq = 0
        self._load_sessions()

    # ── Session 持久化 ─────────────────────────────────────

    def _load_sessions(self):
        """启动时从磁盘恢复 wait_confirm 状态的 session，避免重启后用户确认失效。"""
        try:
            with open(SESSION_FILE, "r", encoding="utf-8") as f:
                saved = json.load(f)
            restored = 0
            for user_id, s in saved.items():
                if s.get("state") in PERSISTABLE_STATES:
                    self.sessions[user_id] = s
                    restored += 1
            if restored:
                logger.info("从磁盘恢复 %d 个待确认 session", restored)
        except FileNotFoundError:
            pass
        except Exception:
            logger.warning("加载 session 文件失败，跳过", exc_info=True)

    def _save_sessions(self):
        """将所有可持久化的 session 写入磁盘。"""
        to_save = {}
        for user_id, s in self.sessions.items():
            if s.get("state") not in PERSISTABLE_STATES:
                continue
            to_save[user_id] = {
                k: v for k, v in s.items()
                if k in ("state", "chat_id", "sql", "filename", "output_fields",
                         "history", "req_id")
            }
        try:
            with open(SESSION_FILE, "w", encoding="utf-8") as f:
                json.dump(to_save, f, ensure_ascii=False)
        except Exception:
            logger.warning("保存 session 文件失败", exc_info=True)

    # ── 请求版本号 ─────────────────────────────────────────

    def _next_req_id(self) -> int:
        self._req_seq += 1
        return self._req_seq

    def _is_current(self, user_id: str, req_id: int) -> bool:
        """检查 req_id 是否仍为该用户当前活跃请求。"""
        s = self.sessions.get(user_id)
        return s is not None and s.get("req_id") == req_id

    def _send_guarded(self, user_id: str, req_id: int, chat_id: str, text: str) -> bool:
        """仅当请求仍为当前请求时才发送消息，否则静默跳过。"""
        if not self._is_current(user_id, req_id):
            logger.info("请求 #%d 已过期 [user=%s]，跳过消息发送", req_id, user_id)
            return False
        self.feishu.send_text(chat_id, text)
        return True

    def _send_file_guarded(self, user_id: str, req_id: int, chat_id: str,
                           filepath: str, filename: str) -> bool:
        if not self._is_current(user_id, req_id):
            return False
        self.feishu.send_file(chat_id, filepath, filename)
        return True

    # ── 主入口 ────────────────────────────────────────────

    def handle_message(self, user_id: str, chat_id: str, message_id: str, text: str):
        text = text.strip()

        if text in RESET_KEYWORDS:
            self._force_reset(user_id, chat_id)
            return

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
            if state in ("generating", "modifying"):
                if text in CANCEL_KEYWORDS:
                    self._force_reset(user_id, chat_id)
                else:
                    action = "生成" if state == "generating" else "修改"
                    self.feishu.send_text(
                        chat_id,
                        f"⏳ 正在{action} SQL 中，请稍候...\n发送「取消」或「重置」可中止。")
                return
            if state == "executing":
                if text in CANCEL_KEYWORDS:
                    self.feishu.send_text(chat_id,
                        "⏳ SQL 正在数据库执行中，无法中断，请等待完成...")
                else:
                    self.feishu.send_text(chat_id,
                        "⏳ SQL 正在执行中，请稍候，不要重复发送...")
                return
            if state == "wait_confirm":
                if text in CANCEL_KEYWORDS:
                    self._cancel(user_id, chat_id)
                    return
                if text in CONFIRM_KEYWORDS:
                    self._execute(user_id, chat_id, session)
                    return
                if text.startswith(MODIFY_PREFIXES) or len(text) <= 20:
                    self._modify(user_id, chat_id, text, session)
                    return
                # 较长文本——提示用户先处理待确认 SQL，而不是静默丢弃 session
                logger.info("wait_confirm 状态收到长文本 [user=%s]: %s", user_id, text[:60])
                self.feishu.send_text(
                    chat_id,
                    "⚠️ 你有一条待确认的 SQL 还未处理：\n"
                    "- 发送「确认」执行\n"
                    "- 发送「取消」放弃\n"
                    "- 发送「重置」后再提新需求")
                return

        if text in CONFIRM_KEYWORDS:
            logger.warning(
                "用户发送确认但无 session [user=%s] sessions_keys=%s",
                user_id, list(self.sessions.keys()))
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
        prev_history = self.sessions.get(user_id, {}).get("history")

        req_id = self._next_req_id()
        stop_event = threading.Event()
        proc_ref = ProcRef()
        self.sessions[user_id] = {
            "req_id": req_id, "state": "generating", "chat_id": chat_id,
            "stop_event": stop_event, "proc_ref": proc_ref,
        }
        self.feishu.send_text(chat_id, "⏳ 正在生成并执行查询...\n发送「取消」可中止。")

        tunnel_thread = threading.Thread(target=self._safe_ensure_tunnel, daemon=True)
        tunnel_thread.start()

        history = prev_history
        if history:
            history = list(history) + [{"role": "user", "content": text}]

        self._start_heartbeat(user_id, req_id, chat_id, stop_event)

        try:
            result = self.cursor_client.generate_sql(text, history, proc_ref=proc_ref)
        finally:
            stop_event.set()

        if not self._is_current(user_id, req_id):
            logger.info("快速模式请求 #%d 已过期，放弃", req_id)
            return

        if not result.get("sql"):
            self.sessions.pop(user_id, None)
            msg = result.get("understanding", "需求理解失败，请更详细地描述您的数据需求。")
            self.feishu.send_text(chat_id, f"❓ {msg}")
            return

        new_history = (history or [{"role": "user", "content": text}]) + [
            {"role": "assistant", "content": json.dumps(result, ensure_ascii=False)}
        ]
        self.sessions[user_id] = {
            "req_id": req_id, "state": "executing", "chat_id": chat_id,
            "history": new_history[-12:],
        }

        tunnel_thread.join(timeout=10)

        logger.info("快速模式 #%d SQL 生成完成，直接执行", req_id)
        success = self._run_and_reply(user_id, req_id, chat_id,
                                      result["sql"], result.get("filename", "查询结果.xlsx"))

        if not self._is_current(user_id, req_id):
            return
        if success:
            self.sessions[user_id] = {
                "req_id": req_id, "state": "done", "chat_id": chat_id,
                "history": new_history[-12:],
            }
        else:
            self.sessions.pop(user_id, None)

    # ── 审查模式 ──────────────────────────────────────────

    def _review_mode(self, user_id: str, chat_id: str, text: str):
        prev_history = self.sessions.get(user_id, {}).get("history")

        req_id = self._next_req_id()
        stop_event = threading.Event()
        proc_ref = ProcRef()
        self.sessions[user_id] = {
            "req_id": req_id, "state": "generating", "chat_id": chat_id,
            "stop_event": stop_event, "proc_ref": proc_ref,
        }
        self.feishu.send_text(chat_id, "⏳ 正在生成 SQL，请稍候...\n发送「取消」可中止。")

        tunnel_thread = threading.Thread(target=self._safe_ensure_tunnel, daemon=True)
        tunnel_thread.start()

        history = None
        if prev_history:
            history = list(prev_history) + [{"role": "user", "content": text}]

        self._start_heartbeat(user_id, req_id, chat_id, stop_event)

        try:
            result = self.cursor_client.generate_sql(text, history=history, proc_ref=proc_ref)
        finally:
            stop_event.set()

        if not self._is_current(user_id, req_id):
            logger.info("审查模式请求 #%d 已过期，放弃 [user=%s]", req_id, user_id)
            return

        if not result.get("sql"):
            self.sessions.pop(user_id, None)
            self._save_sessions()
            msg = result.get("understanding", "需求理解失败，请更详细地描述您的数据需求。")
            logger.info("SQL 为空，session 清除 [user=%s] understanding=%s", user_id, msg[:80])
            self.feishu.send_text(chat_id, f"❓ {msg}")
            return

        new_history = (history or [{"role": "user", "content": text}]) + [
            {"role": "assistant", "content": json.dumps(result, ensure_ascii=False)},
        ]
        self.sessions[user_id] = {
            "req_id":        req_id,
            "state":         "wait_confirm",
            "chat_id":       chat_id,
            "sql":           result["sql"],
            "filename":      result.get("filename", "查询结果.xlsx"),
            "output_fields": result.get("output_fields", []),
            "history":       new_history[-12:],
        }
        self._save_sessions()
        logger.info("session → wait_confirm [user=%s req=%d]", user_id, req_id)
        self._send_confirmation(chat_id, result)

    # ── 修改（审查模式内）────────────────────────────────

    def _modify(self, user_id: str, chat_id: str, text: str, session: dict):
        req_id = self._next_req_id()
        stop_event = threading.Event()
        proc_ref = ProcRef()

        old_sql = session.get("sql", "")
        old_filename = session.get("filename", "查询结果.xlsx")
        old_history = list(session.get("history", []))

        session.update({
            "req_id": req_id, "state": "modifying",
            "stop_event": stop_event, "proc_ref": proc_ref,
        })
        self.feishu.send_text(chat_id, "⏳ 正在修改 SQL...\n发送「取消」可中止。")

        history = old_history + [{"role": "user", "content": text}]

        self._start_heartbeat(user_id, req_id, chat_id, stop_event)

        try:
            result = self.cursor_client.generate_sql(text, history, proc_ref=proc_ref)
        finally:
            stop_event.set()

        if not self._is_current(user_id, req_id):
            logger.info("修改请求 #%d 已过期，放弃", req_id)
            return

        if not result.get("sql"):
            session.update({
                "req_id": req_id, "state": "wait_confirm",
                "sql": old_sql, "filename": old_filename, "history": old_history,
            })
            self.feishu.send_text(
                chat_id,
                f"❓ {result.get('understanding', '修改失败，请再描述一下。')}\n\n"
                "可回复「确认」执行上一版 SQL，或重新描述修改意见。")
            return

        history.append({"role": "assistant", "content": json.dumps(result, ensure_ascii=False)})
        session.update({
            "req_id":        req_id,
            "state":         "wait_confirm",
            "sql":           result["sql"],
            "filename":      result.get("filename", old_filename),
            "output_fields": result.get("output_fields", []),
            "history":       history,
        })
        self._save_sessions()
        self._send_confirmation(chat_id, result, modified=True)

    # ── 确认执行（审查模式内）────────────────────────────

    def _execute(self, user_id: str, chat_id: str, session: dict):
        req_id = session.get("req_id", -1)
        session["state"] = "executing"
        sql = session["sql"]
        filename = session.get("filename", "查询结果.xlsx")

        success = self._run_and_reply(user_id, req_id, chat_id, sql, filename)

        if not self._is_current(user_id, req_id):
            return
        if success:
            self.sessions[user_id] = {
                "req_id": req_id, "state": "done",
                "chat_id": chat_id,
                "history": session.get("history", [])[-12:],
            }
        else:
            session["state"] = "wait_confirm"
        self._save_sessions()

    # ── 取消 / 强制重置 ─────────────────────────────────

    def _cancel(self, user_id: str, chat_id: str):
        self.sessions.pop(user_id, None)
        self._save_sessions()
        self.feishu.send_text(chat_id, "✅ 已取消。")

    def _force_reset(self, user_id: str, chat_id: str):
        """强制重置：立即终止所有进行中的操作并清空 session。"""
        session = self.sessions.pop(user_id, None)
        if session:
            evt = session.get("stop_event")
            if evt:
                evt.set()
            ref = session.get("proc_ref")
            if ref:
                ref.kill()
        self._save_sessions()
        self.feishu.send_text(chat_id, "✅ 已重置，请直接发送新的数据需求。")

    # ── SQL 执行 + 结果返回 ──────────────────────────────

    def _run_and_reply(self, user_id: str, req_id: int,
                       chat_id: str, sql: str, filename: str) -> bool:
        sql_preview = sql.strip()[:120].replace("\n", " ")
        if len(sql.strip()) > 120:
            sql_preview += "..."
        if not self._send_guarded(user_id, req_id, chat_id,
                                  f"⏳ 正在执行 SQL...\n{sql_preview}"):
            return False

        try:
            df = self.executor.execute(sql)
        except Exception as e:
            logger.exception("SQL 执行失败")
            self._send_guarded(
                user_id, req_id, chat_id,
                f"❌ SQL 执行失败：{e}\n\n请回复「确认」重试，或直接描述修改意见。")
            return False

        if not self._is_current(user_id, req_id):
            logger.info("请求 #%d 已过期，丢弃查询结果", req_id)
            return False

        row_count = len(df)

        if row_count == 0:
            self._send_guarded(user_id, req_id, chat_id,
                               "⚠️ 查询结果为空，请检查筛选条件是否过严。")
        elif row_count <= self.data_threshold:
            table_text = df.to_csv(sep="\t", index=False)
            self._send_guarded(
                user_id, req_id, chat_id,
                f"✅ 查询完成，共 {row_count} 行\n\n{table_text}\n\n（可直接复制粘贴到 Excel）")
        else:
            os.makedirs(self.output_dir, exist_ok=True)
            filepath = os.path.join(self.output_dir, filename)
            df.to_excel(filepath, index=False, engine="openpyxl")
            self._send_guarded(
                user_id, req_id, chat_id,
                f"✅ 查询完成，共 {row_count} 行，正在上传文件...")
            self._send_file_guarded(user_id, req_id, chat_id, filepath, filename)
        return True

    # ── 辅助 ─────────────────────────────────────────────

    def _safe_ensure_tunnel(self):
        try:
            self.executor.ensure_tunnel()
        except Exception as e:
            logger.warning("SSH 隧道预热失败（不影响后续重试）: %s", e)

    def _start_heartbeat(self, user_id: str, req_id: int,
                         chat_id: str, stop_event: threading.Event):
        """
        后台心跳：每 20 秒发一次进度提示。
        当 stop_event 被设置或 req_id 不再是当前请求时自动停止。
        """
        start_ts = time.time()
        interval = 20

        def _run():
            while not stop_event.wait(timeout=interval):
                if not self._is_current(user_id, req_id):
                    break
                elapsed = int(time.time() - start_ts)
                self._send_guarded(
                    user_id, req_id, chat_id,
                    f"⏳ Cursor 仍在处理中...（已等待约 {elapsed} 秒）\n发送「取消」可中止。")

        threading.Thread(target=_run, daemon=True).start()

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
