# -*- coding: utf-8 -*-
"""飞书 API 客户端：发送文本消息、上传并发送文件。"""
import json
import logging
import time

import lark_oapi as lark
from lark_oapi.api.im.v1 import (
    CreateFileRequest,
    CreateFileRequestBody,
    CreateMessageRequest,
    CreateMessageRequestBody,
)

logger = logging.getLogger(__name__)

_MAX_RETRIES = 3
_RETRY_DELAYS = [1, 2, 4]  # 每次重试前等待秒数


def _retry(fn, *args, label="操作", **kwargs):
    """通用重试包装：最多尝试 _MAX_RETRIES 次，失败后返回最后一次的结果或抛异常。"""
    last_exc = None
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            result = fn(*args, **kwargs)
            if hasattr(result, "success") and not result.success():
                # 飞书 API 返回业务错误码，不重试（参数错误重试无意义）
                return result
            return result
        except Exception as e:
            last_exc = e
            logger.warning("%s 失败（第 %d 次）: %s", label, attempt, e)
            if attempt < _MAX_RETRIES:
                time.sleep(_RETRY_DELAYS[attempt - 1])
    raise last_exc


class FeishuClient:
    def __init__(self, app_id: str, app_secret: str):
        self.client = (
            lark.Client.builder()
            .app_id(app_id)
            .app_secret(app_secret)
            .log_level(lark.LogLevel.WARNING)
            .build()
        )

    def send_text(self, chat_id: str, text: str):
        content = json.dumps({"text": text})

        def _do():
            request = (
                CreateMessageRequest.builder()
                .receive_id_type("chat_id")
                .request_body(
                    CreateMessageRequestBody.builder()
                    .receive_id(chat_id)
                    .msg_type("text")
                    .content(content)
                    .build()
                )
                .build()
            )
            resp = self.client.im.v1.message.create(request)
            if not resp.success():
                raise RuntimeError(f"发送文本失败: code={resp.code} msg={resp.msg}")
            return resp

        try:
            return _retry(_do, label="发送文本消息")
        except Exception as e:
            logger.error("发送文本消息最终失败: %s", e)
            return None

    def send_file(self, chat_id: str, file_path: str, file_name: str):
        """上传文件并发送文件消息，两步均带重试。"""

        def _upload():
            with open(file_path, "rb") as f:
                req = (
                    CreateFileRequest.builder()
                    .request_body(
                        CreateFileRequestBody.builder()
                        .file_type("xls")
                        .file_name(file_name)
                        .file(f)
                        .build()
                    )
                    .build()
                )
                resp = self.client.im.v1.file.create(req)
                if not resp.success():
                    raise RuntimeError(f"上传文件失败: code={resp.code} msg={resp.msg}")
                return resp

        def _send(file_key: str):
            req = (
                CreateMessageRequest.builder()
                .receive_id_type("chat_id")
                .request_body(
                    CreateMessageRequestBody.builder()
                    .receive_id(chat_id)
                    .msg_type("file")
                    .content(json.dumps({"file_key": file_key}))
                    .build()
                )
                .build()
            )
            resp = self.client.im.v1.message.create(req)
            if not resp.success():
                raise RuntimeError(f"发送文件消息失败: code={resp.code} msg={resp.msg}")
            return resp

        try:
            upload_resp = _retry(_upload, label="上传文件")
            return _retry(_send, upload_resp.data.file_key, label="发送文件消息")
        except Exception as e:
            logger.error("发送文件最终失败: %s", e)
            self.send_text(chat_id, f"❌ 文件发送失败，请稍后重试。错误：{e}")
            return None
