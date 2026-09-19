# ST LLM Gateway

一个跑在本地的LLM网关，专为 SillyTavern 和 Codex CLI 设计：把不同厂商、不同线路协议（wire protocol）的上游 API，统一转换成客户端能直接使用的本地 OpenAI 兼容接口。对 Termux（Android）场景做了针对性的性能与稳定性优化。

## 使用场景

```
SillyTavern --Custom OpenAI--> http://127.0.0.1:8317/v1 --> Gateway --> 代理项目 --> 指定模型
Codex CLI   --OPENAI_BASE_URL--> http://127.0.0.1:8317/v1 --> Gateway --> 代理项目 --> 指定模型
```

一个"代理项目"（Provider）只需要：

- 名称
- Base URL
- API Key
- Wire API：`openai-chat` / `responses` / `anthropic`
- 一个或多个模型 ID

模型自动发现（调用上游 `/models`）是可选功能，手动填写模型 ID 是一等公民——不少上游（比如千帆的 Token Plan）根本不暴露 `/models`，网关不依赖它也能正常工作。

## 快速开始

```bash
npm install
npm run build
npm start          # 或 HOST=0.0.0.0 PORT=8317 node dist/server.js 自定义监听
```

浏览器打开 `http://127.0.0.1:8317/` 进入管理页：添加代理项目、选择模型、保存。SillyTavern 的 Custom OpenAI 端点、Codex CLI 的 `OPENAI_BASE_URL` 都填 `http://127.0.0.1:8317/v1`。

Termux 环境建议配合仓库里的一键部署脚本使用（自动处理依赖安装、32 位设备兼容、进程管理、开机自启等）；Windows 下可以用 `scripts/start-gateway.bat` 双击启动。建议使用较新的 Node.js LTS（与部署脚本对 SillyTavern 的版本要求保持一致，20+）。

## 管理页功能

| 区块 | 说明 |
|---|---|
| ① 酒馆 / Codex 连接 | 展示对外地址，设置连接密钥（Bearer 鉴权），密钥存浏览器 localStorage |
| ② 添加 / 编辑代理项目 | 名称 / URL / 协议 / 密钥 / 手动模型列表；编辑时密钥留空表示保持不变 |
| ③ 选择模型 | 自动发现或手动填写后勾选对外暴露；编辑时按上游模型名对比做增量增删 |
| ④ 统计信息 | 请求数 / Token / 字节数 / 并发等累计指标，可一键清零 |
| ⑤ 实时连接状态 | 每个在途请求的阶段与流量，每秒自动刷新 |
| ⑥ 最近请求日志 | 状态码、耗时、错误信息，可一键清空 |
| ⑦ 已配置项目 | 启用 / 禁用 / 编辑 / 删除项目，删除单个模型；已禁用项目仍可见可恢复 |

## 功能特性

### 三种上游协议互转

- **openai-chat**：原样透传到 `<base_url>/chat/completions`，零转换开销。
- **responses**：把客户端发来的 Chat Completions 请求转换成 OpenAI Responses 格式发给上游，响应再转换回 Chat Completions 格式——这样即使上游只暴露 Responses API（比如 Codex 兼容的 Coding Plan），SillyTavern 也能直接用。Codex CLI 原生发 `/v1/responses` 时则直接透传。文本、工具调用（`function_call`/`function_call_output` 双向转换）、图片、`reasoning_effort` 都覆盖到了。
- **anthropic**：把 Chat Completions 请求转换成 Anthropic Messages 格式。这条路径打磨最久，几个容易被忽略的细节：
  - 同一次 assistant 回合并行发起的多个工具调用，其结果会被合并进**一条** user 消息的多个 `tool_result` 块，而不是拆成多条连续的 user 消息——Anthropic 严格要求 user/assistant 交替，连续同角色消息会被上游直接拒绝（`roles must alternate` 400），这是 agent 类场景（并行工具调用）最容易踩的坑。转换结束后还有一道兜底扫描，合并任何残留的相邻同角色消息。
  - `data:image/...;base64,...` 形式的图片会被识别并拆成 Anthropic 的 `{type:"base64", media_type, data}` 结构，而不是当成普通链接塞进 `{type:"url"}`——Anthropic 的 `url` 源只认可公网可抓取的 http(s) 地址，SillyTavern 发的图基本都是 base64 data URI，两种格式不能混用。
  - `tool_choice` 从 OpenAI 形状（`"auto"` / `"required"` / `{type:"function",...}`）映射到 Anthropic 形状（`{type:"auto"/"any"/"tool", name}`）。
  - extended thinking（`thinking` / `thinking_delta`）映射到社区惯例的 `reasoning_content` 字段，流式、非流式都支持，避免开启思维链的模型的推理过程被直接丢弃。

### 流式转发

- 用原始 Node HTTP response（`reply.hijack()`）直接转发上游 SSE，不经过 Fastify 的 WHATWG ReadableStream 序列化，没有额外缓冲和延迟。
- 设置 `Cache-Control: no-cache, no-transform` 与 `X-Accel-Buffering: no`，防止中间层缓冲造成的卡顿感。
- 剔除上游 `content-encoding` 响应头——`undici` 的 fetch 已经透明解压了 gzip/br/deflate，原样转发这个头会让客户端对已解压内容再解压一次，报 `Z_DATA_ERROR`。
- 客户端断开连接会立即中止上游流并取消底层连接，不会把没人接收的响应继续拉完，白白消耗流量和上游额度。
- 边转发边扫描 SSE 帧提取 token 用量（OpenAI 的尾帧 `usage`、Responses 的 `response.completed`、Anthropic 的 `message_delta`），不用等整个响应结束才能拿到统计。

### 可观测性

- 管理页每秒轮询，展示：发送 / 接收 / 拒绝 / 超时 / 错误请求数，输入 / 输出 / 总 token 消耗，收发字节数，累计连接 / 当前活跃连接 / 峰值并发。
- 实时连接状态显示每个请求当前所处阶段（连接中 / 等待上游 / 流式传输中 / 已完成 / 错误），带耗时和已收发字节。
- 最近请求历史（状态码、耗时、错误信息）。
- 已禁用 / 已失效的代理项目仍会在管理页列出，可随时重新启用或删除；单个模型也能单独删除而不影响整个项目。

### 数据与持久化

- 配置（`gateway-data.json`）、日志（`gateway-logs.json`，上限 500 条）、统计（`gateway-stats.json`）三个文件分离存放，避免核心配置被日志淹没，也方便单独备份/检查配置。
- 统计和日志的落盘做了 500ms 防抖：正常请求不再触发同步 `writeFileSync`；配置变更（增删项目/模型、改密钥）这类低频但必须立即生效的操作仍保持同步写。防抖窗口内的数据在进程退出（含 `SIGINT`/`SIGTERM`）时会强制 flush，只有 `SIGKILL`/强杀才会丢最多几百毫秒的统计，配置文件不受影响。
- 所有落盘都是"写临时文件 + rename"的原子操作，进程被杀或断电不会留下半截的 JSON 文件。
- 并发数用独立计数器而不是活跃请求 Map 的大小——已完成的请求会在 Map 里滞留 30 秒供仪表盘展示，用 Map 大小会把串行请求误算成并发。
- 请求收尾（`requestEnd`）带幂等保护，同一个请求不会被重复计入统计。

### 安全

- 连接密钥（Bearer Token）留空即不鉴权，一旦设置，客户端接口（`/v1/*`）和管理接口（`/api/*`）都需要携带。
- CORS 只放行本机回环来源（`127.0.0.1` / `localhost` / `[::1]`），不反射任意 Origin。
- 管理接口不会明文回传已保存的连接密钥，只返回是否已配置；各项目的上游 API Key 在列表接口里脱敏成 `••••••••`，更新时传空字符串才会清空、省略字段保持原值不变、传脱敏占位符会被忽略——避免管理页把脱敏值误当新密钥回传，覆盖掉真实密钥。

## 千帆示例

- OpenAI 兼容 Token Plan：`https://qianfan.baidubce.com/v2/tokenplan/personal`，Wire API 选 `openai-chat`。
- Anthropic 兼容 Token Plan：`https://qianfan.baidubce.com/anthropic/tokenplan/personal`，Wire API 选 `anthropic`。
- Codex 兼容 Responses Plan：填 `/v1/responses` 端点，Wire API 选 `responses`。

协议由"上游实际接口"决定，不由厂商名称决定。

## 协议细节

- OpenAI Chat：网关向 `<base_url>/chat/completions` 发起请求。
- Anthropic：网关向 `<base_url>/messages` 发起请求。
- Responses：网关向 `<base_url>/responses` 发起请求。
- 上游超时默认 300000ms，可用环境变量 `UPSTREAM_TIMEOUT_MS` 覆盖，对所有 adapter 统一生效；超时对外统一返回 504。超时语义是"无进展超时"，分两段各自计时：等响应头的最长时限，以及响应体相邻字节间的最长空闲时限——流式长响应不会被总时长误杀，上游发完响应头后彻底卡死（不再发任何字节）则会按时中止。
- `POST /api/stats/reset` 清零统计，`POST /api/logs/clear` 清空日志，管理页都有对应按钮。