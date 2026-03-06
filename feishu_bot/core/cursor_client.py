# -*- coding: utf-8 -*-
"""
Cursor 无头模式客户端：通过 Cursor CLI（agent -p）将用户需求直接转发给 Cursor 处理。

优化点：
  - 流式输出：用 Popen 逐行读取，替代阻塞式 subprocess.run
  - 活动检测超时：若连续 idle_timeout 秒无新输出，视为卡死并终止进程
  - 进度回调：支持 on_progress 回调，实时通知调用方当前状态
  - stdin 传参：优先通过 stdin 传递 prompt，避免临时文件 I/O
"""
import json
import logging
import os
import re
import shutil
import subprocess
import threading
import time
from typing import Callable, List, Optional

logger = logging.getLogger(__name__)


def _find_agent_cmd() -> str:
    found = shutil.which("agent")
    if found:
        return found
    if os.name == "nt":
        candidate = os.path.join(
            os.environ.get("LOCALAPPDATA", ""), "cursor-agent", "agent.cmd"
        )
        if os.path.isfile(candidate):
            return candidate
    raise FileNotFoundError(
        "找不到 Cursor CLI（agent）。请先安装：\n"
        "  Windows: irm 'https://cursor.com/install?win32=true' | iex\n"
        "  macOS/Linux: curl https://cursor.com/install -fsS | bash"
    )


OUTPUT_FORMAT_INSTRUCTION = (
    "\n\n---\n"
    "请以 JSON 格式回复，包含以下字段：\n"
    '{"understanding": "对需求的理解", "sql": "完整SQL", '
    '"output_fields": ["字段1"], "estimated_rows": "少量/中等/大量", '
    '"filename": "文件名.xlsx"}\n'
    "如果需求不明确，sql 设为空字符串。只输出 JSON，不要有其他文字。"
)


class CursorClient:
    """通过 Cursor CLI 无头模式（agent -p）生成 SQL，支持流式输出和活动检测。"""

    def __init__(self, config: dict):
        self.workspace = config.get("workspace", "")
        self.model = config.get("model", "")
        self.api_key = config.get("api_key", "")
        self.timeout = config.get("timeout", 300)
        self.idle_timeout = config.get("idle_timeout", 90)
        self._agent_cmd = _find_agent_cmd()
        logger.info("Cursor CLI 路径: %s", self._agent_cmd)

    def generate_sql(
        self,
        user_message: str,
        history: Optional[List[dict]] = None,
        on_progress: Optional[Callable[[str], None]] = None,
    ) -> dict:
        """调用 Cursor 无头模式生成 SQL，失败自动重试最多 2 次。"""
        prompt = self._build_prompt(user_message, history)

        last_err = None
        for attempt in range(1, 3):
            try:
                output = self._call_cursor(prompt, on_progress)
                result = _extract_json(output)
                logger.info("Cursor 返回: %s", result.get("understanding", ""))
                return result
            except _IdleTimeoutError:
                logger.error("Cursor CLI 无响应超过 %ds，终止", self.idle_timeout)
                return {
                    "understanding": f"Cursor 长时间无响应（{self.idle_timeout}秒），已终止。请简化需求后重试。",
                    "sql": "", "output_fields": [], "estimated_rows": "未知",
                    "filename": "查询结果.xlsx",
                }
            except subprocess.TimeoutExpired:
                logger.error("Cursor CLI 超时 (%ds)，不再重试", self.timeout)
                return {
                    "understanding": f"Cursor 处理超时（{self.timeout}秒），请简化需求后重试。",
                    "sql": "", "output_fields": [], "estimated_rows": "未知",
                    "filename": "查询结果.xlsx",
                }
            except Exception as e:
                last_err = e
                logger.warning("Cursor CLI 调用失败（第 %d 次）: %s", attempt, e)
                if attempt < 2:
                    time.sleep(3)

        logger.error("Cursor CLI 均失败: %s", last_err)
        return {
            "understanding": f"Cursor 连接失败，已重试。错误：{last_err}\n请稍后再发送需求重试。",
            "sql": "", "output_fields": [], "estimated_rows": "未知",
            "filename": "查询结果.xlsx",
        }

    def _build_prompt(self, user_message: str, history: Optional[List[dict]]) -> str:
        if history:
            parts = []
            for msg in history:
                role = "用户" if msg["role"] == "user" else "AI"
                parts.append(f"{role}: {msg['content']}")
            parts.append(OUTPUT_FORMAT_INSTRUCTION)
            return "\n".join(parts)
        return user_message + OUTPUT_FORMAT_INSTRUCTION

    # ── CLI 调用（流式） ──────────────────────────────────

    def _call_cursor(self, prompt: str, on_progress: Optional[Callable] = None) -> str:
        prompt_file = os.path.join(self.workspace, ".cursor", "_prompt_tmp.md") if self.workspace else None
        if not prompt_file:
            import tempfile
            prompt_file = os.path.join(tempfile.gettempdir(), "cursor_prompt.md")
        try:
            return self._call_cursor_streaming(prompt, prompt_file, on_progress)
        finally:
            try:
                os.unlink(prompt_file)
            except OSError:
                pass

    def _build_cmd(self) -> list:
        if os.name == "nt" and self._agent_cmd.lower().endswith((".cmd", ".ps1")):
            agent_dir = os.path.dirname(self._agent_cmd)
            ps1_path = os.path.join(agent_dir, "cursor-agent.ps1")
            if os.path.isfile(ps1_path):
                return [
                    "powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass",
                    "-File", ps1_path,
                ]
        return [self._agent_cmd]

    def _call_cursor_streaming(
        self,
        prompt: str,
        prompt_file: str,
        on_progress: Optional[Callable] = None,
    ) -> str:
        os.makedirs(os.path.dirname(prompt_file), exist_ok=True)
        with open(prompt_file, "w", encoding="utf-8") as f:
            f.write(prompt)

        cmd = self._build_cmd()
        cmd.extend(["-p", "--output-format", "text", "--trust", "--mode", "ask"])

        if self.workspace:
            cmd.extend(["--workspace", self.workspace])
        if self.model:
            cmd.extend(["--model", self.model])

        cmd.append(f"Read the file at {prompt_file} and follow the instructions in it.")

        env = os.environ.copy()
        env["CURSOR_INVOKED_AS"] = "agent"
        if self.api_key:
            env["CURSOR_API_KEY"] = self.api_key

        logger.info("调用 Cursor CLI (streaming, workspace=%s)...", self.workspace)
        start_ts = time.time()

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            cwd=self.workspace or None,
        )

        output_chunks: list[str] = []
        last_activity = time.time()
        timed_out = False

        def _read_stderr():
            """后台线程消费 stderr，防止管道满阻塞。"""
            try:
                proc.stderr.read()
            except Exception:
                pass

        stderr_thread = threading.Thread(target=_read_stderr, daemon=True)
        stderr_thread.start()

        try:
            while True:
                # 检查总超时
                if time.time() - start_ts > self.timeout:
                    timed_out = True
                    proc.kill()
                    raise subprocess.TimeoutExpired(cmd, self.timeout)

                # 检查活动超时（连续无输出）
                if time.time() - last_activity > self.idle_timeout:
                    proc.kill()
                    raise _IdleTimeoutError(self.idle_timeout)

                line_bytes = proc.stdout.readline()
                if not line_bytes:
                    if proc.poll() is not None:
                        break
                    time.sleep(0.1)
                    continue

                line = _decode_output(line_bytes)
                output_chunks.append(line)
                last_activity = time.time()

                if on_progress and line.strip():
                    on_progress(line.strip())

        except (subprocess.TimeoutExpired, _IdleTimeoutError):
            proc.kill()
            raise
        finally:
            proc.wait(timeout=5)
            stderr_thread.join(timeout=3)

        elapsed = time.time() - start_ts
        full_output = "".join(output_chunks).strip()

        if proc.returncode != 0:
            stderr_text = ""
            try:
                raw_err = proc.stderr.read()
                if raw_err:
                    stderr_text = _decode_output(raw_err)[:500]
            except Exception:
                pass
            raise RuntimeError(f"Cursor CLI 退出码 {proc.returncode}: {stderr_text}")

        if not full_output:
            raise RuntimeError("Cursor CLI 返回空结果")

        logger.info("Cursor CLI 返回 %d 字符 (%.1fs)", len(full_output), elapsed)
        return full_output


class _IdleTimeoutError(Exception):
    """Cursor CLI 长时间无输出时抛出。"""
    def __init__(self, seconds: int):
        super().__init__(f"连续 {seconds} 秒无输出")


def _decode_output(raw: bytes) -> str:
    try:
        text = raw.decode("utf-8")
        if any("\u0400" <= c <= "\u04ff" for c in text[:500]):
            return raw.decode("gbk", errors="replace")
        return text
    except UnicodeDecodeError:
        return raw.decode("gbk", errors="replace")


def _extract_json(text: str) -> dict:
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    match = re.search(r"```(?:json)?\s*\n?(.*?)\n?```", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1).strip())
        except json.JSONDecodeError:
            pass
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            pass
    return {"understanding": "AI 返回格式异常，请重试", "sql": ""}
