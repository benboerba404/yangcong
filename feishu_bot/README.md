# 飞书 BI 取数机器人

在飞书聊天中直接输入数据需求，机器人自动理解、生成 SQL、执行查询、返回结果。

## 架构

```
用户在飞书聊天发消息
  → 飞书 WebSocket 长连接推送到本地
  → Cursor CLI 无头模式（读取工作区知识库 + 规则，生成 SQL）
  → SSH 隧道连接跳板机执行 SQL
  → 数据量小：直接展示（可复制）
  → 数据量大：生成 Excel 文件发送
```

**无需服务器、无需公网 IP**，本地电脑运行即可。
SQL 生成由 Cursor CLI 完成，自动使用工作区中的知识库、规则和表结构定义。

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

### 第二步：安装 Cursor CLI

SQL 生成使用 Cursor 无头模式，需要先安装 Cursor CLI：

**Windows（PowerShell）：**
```powershell
irm 'https://cursor.com/install?win32=true' | iex
```

安装完成后登录：
```powershell
agent login
```

或者设置 API Key 环境变量（从 [Cursor 设置页](https://cursor.com/settings) 获取）：
```powershell
$env:CURSOR_API_KEY = "your_api_key_here"
```

验证安装：
```powershell
agent --version
```

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

编辑 `config.json`，填入你获取的信息：

- **feishu**：飞书应用的 App ID 和 App Secret
- **cursor**：Cursor CLI 配置（通常留空即可，会使用默认值）
  - `api_key`：留空则使用 `CURSOR_API_KEY` 环境变量或已登录的账号
  - `model`：留空则使用 Cursor 默认模型
  - `workspace`：留空则自动使用 bi-assistant 工作区根目录
  - `timeout`：CLI 超时时间（秒），默认 180
- **database**：跳板机和数据库连接信息（和 `.cursor/skills/jump-sql-excel-export/jump_export_config.json` 一致）

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
- SQL 生成通过 Cursor CLI 完成，使用你的 Cursor 订阅额度
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
    ├── cursor_client.py    # Cursor CLI 无头模式（知识库 + 规则 → SQL 生成）
    ├── sql_executor.py     # SQL 执行（SSH 隧道 + Spark SQL）
    └── workflow.py         # 工作流编排（会话状态管理）
```
