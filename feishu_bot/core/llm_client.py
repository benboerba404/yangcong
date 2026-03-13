# -*- coding: utf-8 -*-
"""
LLM 客户端：加载知识库，根据用户需求生成 SQL。

支持两种 provider：
  - "openai_compatible"（默认）：DeepSeek / 通义千问等 OpenAI 兼容接口
  - "anthropic"：Anthropic Claude（Opus / Sonnet 等）
"""
import glob
import json
import logging
import os
import re
import time
from typing import List, Optional

logger = logging.getLogger(__name__)

SYSTEM_PROMPT_TEMPLATE = """\
你是一个 BI 数据分析助手，专门负责根据用户的数据需求编写 Spark SQL 查询语句。

## 你的知识库

以下知识库按模块组织，写 SQL 时务必严格遵守其中的口径和规则。

### 一、业务术语与指标定义（glossary）
{glossary}

### 二、默认筛选条件与通用规则（default-filters）
{default_filters}

### 三、枚举值字典（enums）
{enums}

### 四、数据表关系（table-relations）
{table_relations}

### 五、表结构元数据（table-schemas）
{table_schemas_overview}

### 六、标准 SQL 模板（sql-patterns）
{sql_patterns}

### 七、表结构 DDL（参考）
{table_schemas_ddl}

## SQL 编写规范（Spark SQL 3.3.3）

1. 禁止 `select *`，必须列出具体字段
2. 分区表的分区字段筛选必须写在 WHERE 后第一个条件
3. 强制使用 CTE（`with ... as` 语法），禁止多层嵌套子查询
4. 禁止隐式 JOIN（逗号分隔表名），优先 `left join`
5. SQL 末尾必须加 `limit 100000`，防止数据量过大
6. 输出字段中文别名用反引号包裹（如 as `中文名`）
7. 日期格式统一处理为 'yyyy-MM-dd'
8. 关键字小写（select、from、where、group by 等）

## events.frontend_event_orc 表特殊约束

- 必须添加分区条件：day（格式 yyyyMMdd）和 event_type
- event_type 取 event_key 前缀，共 6 种：click、dev、get、enter、popup、other
- 单次最多查询 7 天数据

## 输出要求

你必须以 JSON 格式输出，包含以下字段：
{{
  "understanding": "对需求的理解说明（1-3句话）",
  "sql": "完整的可执行 SQL 语句",
  "output_fields": ["输出字段1", "输出字段2"],
  "estimated_rows": "少量 / 中等 / 大量",
  "filename": "建议的输出文件名.xlsx"
}}

如果需求不明确或缺少关键信息，在 understanding 中说明缺少什么，sql 设为空字符串。\
"""


class LLMClient:
    """
    统一 LLM 客户端，通过 config["provider"] 自动切换后端：
      - "openai_compatible"（默认）：DeepSeek 等
      - "anthropic"：Claude Opus / Sonnet
    """

    def __init__(self, config: dict):
        self.provider = config.get("provider", "openai_compatible")
        self.model = config.get("model", "deepseek-chat")
        self.system_prompt = ""

        self._provider_config = config  # 保存配置，每次调用时新建 client 避免连接池失效

        if self.provider == "anthropic":
            from anthropic import Anthropic
            self._anthropic = Anthropic(api_key=config["api_key"])
        # openai_compatible 不在 __init__ 里预建 client，改为每次调用时新建

    def load_knowledge(self, knowledge_dir: str, schema_dir: str):
        glossary = _read_file(os.path.join(knowledge_dir, "glossary.md"))
        default_filters = _read_file(os.path.join(knowledge_dir, "default-filters.md"))
        enums = _read_file(os.path.join(knowledge_dir, "enums.md"))
        table_relations = _read_file(os.path.join(knowledge_dir, "table-relations.md"))
        table_schemas_overview = _read_file(os.path.join(knowledge_dir, "table-schemas.md"))
        sql_patterns = _read_file(os.path.join(knowledge_dir, "sql-patterns.md"))

        ddl_parts = []
        for sql_file in sorted(glob.glob(os.path.join(schema_dir, "*.sql"))):
            content = _read_file(sql_file)
            if content:
                name = os.path.basename(sql_file)
                ddl_parts.append(f"-- {name}\n{content[:6000]}")
        table_schemas_ddl = "\n\n".join(ddl_parts)

        self.system_prompt = SYSTEM_PROMPT_TEMPLATE.format(
            glossary=glossary,
            default_filters=default_filters,
            enums=enums,
            table_relations=table_relations,
            table_schemas_overview=table_schemas_overview,
            sql_patterns=sql_patterns,
            table_schemas_ddl=table_schemas_ddl,
        )
        logger.info(
            "知识库已加载（6 模块 + DDL），系统提示词 %d 字符",
            len(self.system_prompt),
        )

    def generate_sql(self, user_message: str, history: Optional[List[dict]] = None) -> dict:
        """调用 LLM 生成 SQL，失败自动重试最多 3 次，返回结构化结果 dict。"""
        last_err = None
        for attempt in range(1, 4):
            try:
                if self.provider == "anthropic":
                    content = self._call_anthropic(user_message, history)
                else:
                    content = self._call_openai(user_message, history)

                result = _extract_json(content)
                logger.info("LLM 返回: %s", result.get("understanding", ""))
                return result
            except Exception as e:
                last_err = e
                logger.warning("LLM 调用失败（第 %d 次）: %s", attempt, e)
                if attempt < 3:
                    time.sleep(2 * attempt)  # 2s、4s 后重试

        logger.error("LLM 调用 3 次均失败: %s", last_err)
        return {
            "understanding": f"AI 连接失败，已重试 3 次。错误：{last_err}\n请稍后再发送需求重试。",
            "sql": "",
            "output_fields": [],
            "estimated_rows": "未知",
            "filename": "查询结果.xlsx",
        }

    def _call_openai(self, user_message: str, history: Optional[List[dict]]) -> str:
        from openai import OpenAI

        # 每次新建 client，避免长时间运行后连接池连接失效导致 Connection error
        client = OpenAI(
            api_key=self._provider_config["api_key"],
            base_url=self._provider_config.get("base_url", "https://api.deepseek.com"),
            timeout=60.0,
        )

        messages = [{"role": "system", "content": self.system_prompt}]
        if history:
            messages.extend(history)
        else:
            messages.append({"role": "user", "content": user_message})

        response = client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=0.1,
            response_format={"type": "json_object"},
        )
        return response.choices[0].message.content

    def _call_anthropic(self, user_message: str, history: Optional[List[dict]]) -> str:
        messages = []
        if history:
            messages.extend(history)
        else:
            messages.append({"role": "user", "content": user_message})

        response = self._anthropic.messages.create(
            model=self.model,
            max_tokens=4096,
            temperature=0.1,
            system=self.system_prompt,
            messages=messages,
        )
        return response.content[0].text


def _extract_json(text: str) -> dict:
    """从 LLM 输出中提取 JSON，兼容直接 JSON 和 ```json 代码块包裹两种格式。"""
    text = text.strip()
    # 尝试直接解析
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    # 尝试从 ```json ... ``` 代码块中提取
    match = re.search(r"```(?:json)?\s*\n?(.*?)\n?```", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1).strip())
        except json.JSONDecodeError:
            pass
    # 兜底：尝试找第一个 { ... } 块
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            pass
    return {"understanding": "AI 返回格式异常，请重试", "sql": ""}


def _read_file(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        logger.warning("无法读取文件: %s", path)
        return ""
