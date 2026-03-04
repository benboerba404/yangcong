# 飞书 BI 取数机器人

在飞书聊天中直接输入数据需求，机器人自动理解、生成 SQL、执行查询、返回结果。

## 架构

```
用户在飞书聊天发消息
  → 飞书 WebSocket 长连接推送到本地
  → LLM (Claude / DeepSeek) 理解需求 + 生成 SQL
  → SSH 隧道连接跳板机执行 SQL
  → 数据量小：直接展示（可复制）
  → 数据量大：生成 Excel 文件发送
```

**无需服务器、无需公网 IP**，本地电脑运行即可。

---

## 准备工作（一次性配置）

### 第一步：创建飞书应用

1. 打开 [飞书开放平台](https://open.feishu.cn/app)，登录你的飞书账号
2. 点击 **「创建企业自建应用」**，填写名称（如"BI取数助手"）和描述
3. 进入应用后，在左侧找到 **「凭证与基础信息」**，复制 **App ID** 和 **App Secret**
4. 在左侧点击 **「添加应用能力」**，添加 **「机器人」** 能力
5. 在左侧点击 **「权限管理」**，搜索并开通以下权限：
   - `im:message` — 获取与发送单聊、群组消息
   - `im:message:send_as_bot` — 以应用的身份发消息
   - `im:resource` — 获取与上传图片或文件资源
6. 在左侧点击 **「事件与回调」**：
   - 订阅方式选择 **「使用长连接接收事件」**（关键！这样不需要服务器）
   - 添加事件：**`im.message.receive_v1`**（接收消息）
7. 在左侧点击 **「版本管理与发布」**：
   - 创建版本 → 提交审核
   - 让你们公司的飞书管理员审核通过即可
   - 审核通过后，在飞书里搜索你的机器人名字，就能找到它了

### 第二步：获取 LLM API Key（二选一）

#### 方案 A：Anthropic Claude（和 Cursor 里用的同款模型）

1. 打开 [Anthropic Console](https://console.anthropic.com)，注册账号
2. 进入 **「API Keys」** 页面，点击 **「Create Key」**，复制保存（只显示一次！）
3. 在 **「Plans & Billing」** 页面绑定信用卡并充值（$5 起）
4. 推荐模型：**Claude Sonnet**（每次约 ¥0.3，SQL 生成完全够用）

#### 方案 B：DeepSeek（性价比最高，推荐）

1. 打开 [DeepSeek 开放平台](https://platform.deepseek.com)，注册账号
2. 登录后在左侧找到 **「API Keys」**
3. 点击 **「创建 API Key」**，复制并保存（只显示一次！）
4. 在 **「充值」** 页面充值 ¥10（每次约 ¥0.01，够用几个月）

### 第三步：安装 Python 依赖

```bash
cd feishu_bot
pip install -r requirements.txt
```

### 第四步：填写配置

```bash
# 复制配置模板
copy config.json.example config.json
```

编辑 `config.json`，填入你获取的信息。`llm` 部分根据你选的方案填：

**方案 A — Claude：**
```json
"llm": {
  "provider": "anthropic",
  "api_key": "sk-ant-xxxxx（Anthropic 控制台复制的 Key）",
  "model": "claude-sonnet-4-20250514"
}
```

**方案 B — DeepSeek：**
```json
"llm": {
  "provider": "openai_compatible",
  "api_key": "sk-xxxxx（DeepSeek 控制台复制的 Key）",
  "base_url": "https://api.deepseek.com",
  "model": "deepseek-chat"
}
```

> `database` 部分的值和你现有的 `.cursor/skills/jump-sql-excel-export/jump_export_config.json` 完全一样，直接抄过来即可。

---

## 启动

```bash
cd feishu_bot
python main.py
```

看到类似 `connected to wss://...` 的日志说明连接成功。

保持这个终端窗口运行，机器人就在线了。关闭终端 = 机器人下线。

---

## 使用方式

在飞书里找到你的机器人，发送消息即可：

| 你发送 | 机器人做什么 |
|--------|-------------|
| `帮我看一下近半年小学日活趋势` | 理解需求 → 生成 SQL → 等你确认 |
| `确认` | 执行 SQL → 返回数据 |
| `时间改成近一年` | 修改 SQL → 重新确认 |
| `取消` | 放弃本次查询 |
| `帮助` | 显示使用说明 |

- 数据 ≤ 20 行：直接在聊天中展示（Tab 分隔，可直接复制粘贴到 Excel）
- 数据 > 20 行：生成 Excel 文件发送到聊天

---

## 注意事项

- 机器人只在你的电脑运行时在线（本地运行，非服务器部署）
- 需要连接公司 VPN 才能执行 SQL（和 Cursor 里跑一样）
- SQL 执行可能需要几十秒，请耐心等待
- `config.json` 包含敏感信息，不要提交到 Git

---

## 文件结构

```
feishu_bot/
├── main.py                 # 主入口（启动 WebSocket 长连接）
├── config.json             # 配置文件（需自行创建，已 gitignore）
├── config.json.example     # 配置模板
├── requirements.txt        # Python 依赖
├── README.md               # 本文件
└── core/
    ├── feishu_client.py    # 飞书 API（发消息、传文件）
    ├── llm_client.py       # LLM 集成（Claude / DeepSeek，需求理解 + SQL 生成）
    ├── sql_executor.py     # SQL 执行（SSH 隧道 + Spark SQL）
    └── workflow.py         # 工作流编排（会话状态管理）
```
