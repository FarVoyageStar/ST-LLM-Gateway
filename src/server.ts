// ST LLM Gateway 服务入口。
// 职责：对外提供 OpenAI Chat Completions / Responses 兼容接口，按模型别名路由到
// 已配置的上游项目（三种线路协议：openai-chat / responses / anthropic），
// 同时承载管理接口（/api/*）、静态管理页（web/）、请求统计与实时流量采集。
import Fastify from "fastify";
import cors from "@fastify/cors";
import fastifyStatic from "@fastify/static";
import { DB, type UsageDelta } from "./core/db.js";
import { OpenAIChatAdapter } from "./adapters/openai-chat.js";
import { ResponsesAdapter } from "./adapters/responses.js";
import { AnthropicAdapter } from "./adapters/anthropic.js";
import type { Adapter, ChatRequest, WireApi } from "./types.js";
import { join } from "node:path";
import { readFileSync } from "node:fs";
import { timingSafeEqual } from "node:crypto";
import { Readable } from "node:stream";

// 版本号唯一来源是 package.json（单一事实源），运行时读取，杜绝多处手抄导致不同步
const VERSION: string =
  JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8"))?.version ?? "";
const db = new DB();
// bodyLimit 默认仅 1MB，长聊天上下文（长历史 + 大角色卡）的 JSON 易超限被 413 拒绝，放宽到 50MB
const app = Fastify({ logger: true, bodyLimit: 50 * 1024 * 1024 });
// 兼容性加固：部分客户端（含旧版管理页）会给无 body 的 DELETE/POST 请求带上
// content-type: application/json，Fastify 默认对空 JSON body 直接返回 400
// （FST_ERR_CTP_EMPTY_JSON_BODY），导致删除模型/项目、清零统计、清空日志全部失败。
// 覆盖默认解析器：空 body 按 {} 处理，其余情况与原逻辑一致（非法 JSON 仍 400）。
app.addContentTypeParser("application/json", { parseAs: "string" }, (_req, body, done) => {
  if (body === "" || body === undefined) return done(null, {});
  try {
    done(null, JSON.parse(body as string));
  } catch (e: any) {
    e.statusCode = 400;
    done(e);
  }
});
const UPSTREAM_TIMEOUT_MS = Number(process.env.UPSTREAM_TIMEOUT_MS ?? 300000);
// CORS 策略：不反射任意 Origin。管理页是同源访问、SillyTavern/Codex 是服务端调用，
// 都不需要 CORS；仅放行本机回环来源，兼容本机浏览器调试工具。
await app.register(cors, {
  origin: (origin, cb) => {
    if (!origin || /^https?:\/\/(127\.0\.0\.1|localhost|\[::1\])(:\d+)?$/i.test(origin)) cb(null, true);
    else cb(null, false);
  },
});
await app.register(fastifyStatic, { root: join(process.cwd(), "web"), prefix: "/" });

const adapters: Record<WireApi, Adapter> = {
  "openai-chat": new OpenAIChatAdapter(),
  "responses": new ResponsesAdapter(),
  "anthropic": new AnthropicAdapter(),
};

// ===== 统计口径 =====
// 四类结果：ok（2xx）/ rejected（4xx，含鉴权失败与未知模型）/ timeout（408 或中止类异常）/ error（其余）。
type Outcome = "ok" | "rejected" | "timeout" | "error";
function classifyStatus(s?: number): Outcome {
  if (s === 408) return "timeout";
  if (s !== undefined && s >= 200 && s < 300) return "ok";
  if (s !== undefined && s >= 400 && s < 500) return "rejected";
  return "error";
}
function isTimeout(e: any): boolean {
  const m = String(e?.name ?? "") + " " + String(e?.message ?? "");
  return /abort|time\s?out|timed\s?out/i.test(m);
}
function recordOutcome(kind: Outcome) {
  if (kind === "ok") db.statsIncrement({ responses_total: 1 });
  else if (kind === "rejected") db.statsIncrement({ rejected_total: 1 });
  else if (kind === "timeout") db.statsIncrement({ timeout_total: 1 });
  else db.statsIncrement({ error_total: 1 });
}
function countRejected() {
  db.statsIncrement({ rejected_total: 1 });
}

// ===== 用量提取（OpenAI Chat / Responses / Anthropic 三种 usage 形态） =====
function extractUsage(j: any): UsageDelta | null {
  const u = j?.usage ?? j?.response?.usage ?? j?.message?.usage;
  if (!u || typeof u !== "object") return null;
  const prompt = Number(u.prompt_tokens ?? u.input_tokens ?? 0) || 0;
  const completion = Number(u.completion_tokens ?? u.output_tokens ?? 0) || 0;
  const total = Number(u.total_tokens ?? prompt + completion) || 0;
  if (!prompt && !completion && !total) return null;
  return { prompt, completion, total };
}
// 非流式响应的用量嗅探：克隆一份响应体尝试解析 JSON，失败/非 JSON 时静默返回 null。
async function sniffUsage(r: Response): Promise<UsageDelta | null> {
  const ct = (r.headers.get("content-type") || "").toLowerCase();
  if (!ct.includes("json")) return null;
  try {
    const j = await r.clone().json();
    return extractUsage(j);
  } catch {
    return null;
  }
}

// ===== 基础工具 =====
// 常量时间比较：直接 === 逐字节短路会暴露前缀匹配进度（时序侧信道）。
// 长度不同直接判负——密钥长度本身不视为秘密。
function keyEqual(got: string, expected: string) {
  const a = Buffer.from(got);
  const b = Buffer.from(expected);
  return a.length === b.length && timingSafeEqual(a, b);
}
// 从请求头提取 Bearer 令牌并与预期密钥做常量时间比较
function keyMatches(req: any, expected: string) {
  const a = String(req.headers.authorization || "");
  const got = a.startsWith("Bearer ") ? a.slice(7) : "";
  return keyEqual(got, expected);
}
// 对外推理接口（/v1/*）的鉴权：未设置连接密钥时放行；设置后要求 Bearer 匹配。
function auth(req: any, reply: any) {
  const expected = db.getSetting("api_key");
  if (!expected) return true;
  if (!keyMatches(req, expected)) {
    countRejected();
    reply.code(401).send({ error: { message: "Invalid API key", type: "invalid_request_error" } });
    return false;
  }
  return true;
}

// 管理接口 /api/* 的鉴权钩子：设置了连接密钥后同样要求 Bearer 鉴权。
// 管理页前端会把密钥随 authorization 头一起发送；未设置密钥时保持开放（默认仅监听 127.0.0.1）。
app.addHook("onRequest", async (req, reply) => {
  if (!req.url.startsWith("/api/")) return;
  const expected = db.getSetting("api_key");
  if (!expected) return;
  const a = String(req.headers.authorization || "");
  const got = a.startsWith("Bearer ") ? a.slice(7) : "";
  if (got !== expected) {
    countRejected();
    return reply.code(401).send({ error: { message: "Invalid API key", type: "invalid_request_error" } });
  }
});
function cleanBase(s: string) {
  return String(s || "").replace(/\/+$/, "");
}
// 按 base_url 猜测线路协议：路径含 /anthropic 视为 anthropic，否则默认 openai-chat。
function inferWire(url: string): WireApi {
  return /\/anthropic(?:\/|$)/i.test(url) ? "anthropic" : "openai-chat";
}
// 归一化各家 /models 响应（OpenAI 的 data / 部分厂商的 models / 裸数组）为统一列表。
function modelList(j: any) {
  const raw = Array.isArray(j?.data) ? j.data : Array.isArray(j?.models) ? j.models : Array.isArray(j) ? j : [];
  return raw
    .map((m: any) => ({ id: String(m.id ?? m.model ?? m.name), name: String(m.name ?? m.id ?? m.model) }))
    .filter((m: any) => m.id && m.id !== "undefined");
}
function discoveryUrl(base: string) {
  return `${cleanBase(base)}/models`;
}
function discoveryHeaders(key: string, wire: WireApi) {
  const h: Record<string, string> = { accept: "application/json" };
  if (wire === "anthropic") {
    h["x-api-key"] = key;
    h["anthropic-version"] = "2023-06-01";
  } else h.authorization = `Bearer ${key}`;
  return h;
}

// ===== 流式透传 + 实时字节/Token 采集 =====
// hijack 后手工管理响应：边收边转，同时统计入向/出向字节数并扫描 SSE 帧里的 usage。
async function streamResponse(reply: any, response: Response, requestId: string) {
  reply.hijack();
  reply.raw.statusCode = response.status;
  for (const [k, v] of response.headers) {
    const lk = k.toLowerCase();
    // content-encoding 必须剔除：undici fetch 已透明解压 body，
    // 原样转发会让客户端对已解压的字节再做一次 gunzip（Z_DATA_ERROR）。
    if (lk === "content-length" || lk === "connection" || lk === "transfer-encoding" || lk === "content-encoding") continue;
    reply.raw.setHeader(k, v);
  }
  if (!reply.raw.hasHeader("content-type"))
    reply.raw.setHeader("content-type", response.headers.get("content-type") || "text/event-stream; charset=utf-8");
  reply.raw.setHeader("cache-control", "no-cache, no-transform");
  reply.raw.setHeader("x-accel-buffering", "no");
  reply.raw.flushHeaders?.();

  if (!response.body) {
    db.requestEnd(requestId, { phase: "completed", upstream_status: response.status });
    reply.raw.end();
    return;
  }

  db.requestUpdate(requestId, {
    phase: response.ok ? "streaming" : "error",
    upstream_status: response.status,
  });

  const nodeStream = Readable.fromWeb(response.body as any);
  let bytesIn = 0;
  let bytesOut = 0;
  const raw = reply.raw;
  // 客户端中途断开时立刻中止上游流，避免把上游响应拉完白白消耗流量与额度。
  // destroy 会经 Readable.fromWeb 取消底层 reader，undici 随之中止上游连接。
  // raw 的 error 必须兜底吞掉，否则写已断开的 socket 会抛出未捕获异常导致进程崩溃。
  raw.on("error", () => {});
  raw.on("close", () => {
    if (!nodeStream.destroyed) nodeStream.destroy(new Error("client disconnected"));
  });
  const originalWrite = raw.write.bind(raw);
  raw.write = ((chunk: any, ...args: any[]) => {
    const n = Buffer.isBuffer(chunk) ? chunk.length : Buffer.byteLength(String(chunk));
    bytesOut += n;
    db.requestUpdate(requestId, { bytes_out: bytesOut });
    return originalWrite(chunk, ...args);
  }) as any;

  // SSE 帧中若带 usage，尽量采集（OpenAI 流式末尾 / Responses 的 response.completed）。
  let sseBuf = "";
  const scanUsage = (chunk: Buffer) => {
    const s = chunk.toString("utf8");
    if (!s.includes("usage")) return;
    sseBuf += s;
    if (sseBuf.length > 1_000_000) sseBuf = sseBuf.slice(-500_000);
    let idx: number;
    while ((idx = sseBuf.search(/\r?\n\r?\n/)) >= 0) {
      const frame = sseBuf.slice(0, idx);
      sseBuf = sseBuf.slice(idx).replace(/^\r?\n\r?\n/, "");
      const line = frame.split(/\r?\n/).find((l) => l.trimStart().startsWith("data:"));
      if (!line) continue;
      const rawLine = line.trimStart().slice(5).trim();
      if (!rawLine || rawLine === "[DONE]") continue;
      try {
        const e = JSON.parse(rawLine);
        const u = extractUsage(e);
        if (u) db.requestUpdate(requestId, { usage: u });
      } catch {
        /* 忽略无法解析的帧 */
      }
    }
  };

  nodeStream.on("data", (chunk: any) => {
    if (Buffer.isBuffer(chunk)) {
      bytesIn += chunk.length;
      scanUsage(chunk);
    } else {
      bytesIn += Buffer.byteLength(String(chunk));
    }
    db.requestUpdate(requestId, { bytes_in: bytesIn });
  });
  nodeStream.on("error", (err: any) => {
    db.requestEnd(requestId, { phase: "error", error: err?.message ?? String(err), bytes_in: bytesIn, bytes_out: bytesOut });
    raw.destroy(err);
  });
  nodeStream.on("end", () => {
    db.requestEnd(requestId, { phase: "completed", bytes_in: bytesIn, bytes_out: bytesOut });
    raw.end();
  });
  nodeStream.pipe(raw);
}

// ===== 健康检查 =====
app.get("/health", async () => ({ ok: true, service: "st-llm-gateway", version: VERSION }));

// ===== 对外模型列表 =====
app.get("/v1/models", async (req: any, reply: any) => {
  if (!auth(req, reply)) return;
  return {
    object: "list",
    data: db.listModels().map((m) => ({ id: m.public_id, object: "model", created: 0, owned_by: "st-llm-gateway" })),
  };
});

// ===== Chat Completions（OpenAI/Codex 兼容） =====
app.post("/v1/chat/completions", async (req: any, reply: any) => {
  if (!auth(req, reply)) return;
  const body = req.body as ChatRequest;
  // 用 getModelAny 区分"模型不存在"(404) 与"所属项目已禁用"(503)
  const model = db.getModelAny(body.model);
  if (!model) {
    countRejected();
    return reply.code(404).send({ error: { message: `Unknown model: ${body.model}`, type: "invalid_request_error" } });
  }
  const provider = db.getProvider(model.provider_id);
  if (!provider || !provider.enabled) {
    countRejected();
    return reply.code(503).send({ error: { message: "Provider unavailable", type: "server_error" } });
  }
  const adapter = adapters[provider.wire_api];
  if (!adapter?.chat) {
    countRejected();
    return reply.code(400).send({ error: { message: `${provider.wire_api} adapter does not expose Chat Completions`, type: "invalid_request_error" } });
  }
  const t = Date.now();
  const requestId = db.requestStart({
    provider_id: provider.id,
    provider_name: provider.name,
    model: body.model,
    wire_api: provider.wire_api,
    upstream_url: `${provider.base_url}/chat/completions`,
    stream: !!body.stream,
    phase: "connecting",
  });
  try {
    db.requestUpdate(requestId, { phase: "waiting" });
    const r = await adapter.chat(body, model, provider);
    recordOutcome(classifyStatus(r.status));
    db.log(provider.id, body.model, r.status, Date.now() - t, r.ok ? "" : "upstream error");
    const usage = await sniffUsage(r);

    if (body.stream) {
      if (usage) db.requestUpdate(requestId, { usage });
      return streamResponse(reply, r, requestId);
    }

    reply.code(r.status);
    // content-encoding 必须剔除：body 已被 undici 解压，原样转发会导致客户端重复解压失败
    for (const [k, v] of r.headers) {
      const lk = k.toLowerCase();
      if (lk === "content-length" || lk === "content-encoding" || lk === "connection" || lk === "transfer-encoding") continue;
      reply.header(k, v);
    }
    // 非流式转发：先读满响应体再整体发送。非流式 body 通常很小，缓冲代价可忽略
    // （sniffUsage 的 clone().json() 本来就会整段读入内存）；读满后按真实字节数
    // 计入 bytes_in/bytes_out，流量统计对流式 / 非流式保持同一口径；
    // 同时以 Buffer 交给 Fastify 原生发送，自动产出正确的 content-length。
    const buf = Buffer.from(await r.arrayBuffer());
    db.requestEnd(requestId, { phase: r.ok ? "completed" : "error", upstream_status: r.status, usage: usage ?? undefined, bytes_in: buf.length, bytes_out: buf.length });
    return reply.send(buf);
  } catch (e: any) {
    const timeout = isTimeout(e);
    recordOutcome(timeout ? "timeout" : "error");
    const msg = e?.message ?? String(e);
    db.requestEnd(requestId, { phase: "error", error: msg });
    // 超时与上游故障分开计量与返回：超时 504，其余上游异常 502
    db.log(provider.id, body.model, timeout ? 504 : 502, Date.now() - t, msg);
    return reply.code(timeout ? 504 : 502).send({ error: { message: msg, type: timeout ? "timeout_error" : "upstream_error" } });
  }
});

// ===== Responses（Codex CLI 主走此接口） =====
app.post("/v1/responses", async (req: any, reply: any) => {
  if (!auth(req, reply)) return;
  const body: any = req.body;
  // 与 chat/completions 同一口径：先查模型再判断项目启用状态
  const model = db.getModelAny(body.model);
  if (!model) {
    countRejected();
    return reply.code(404).send({ error: { message: `Unknown model: ${body.model}`, type: "invalid_request_error" } });
  }
  const provider = db.getProvider(model.provider_id);
  if (!provider || !provider.enabled) {
    countRejected();
    return reply.code(503).send({ error: { message: "Provider unavailable", type: "server_error" } });
  }
  const adapter = adapters[provider.wire_api];
  if (!adapter?.responses) {
    countRejected();
    return reply.code(400).send({ error: { message: "This project is not configured for OpenAI Responses", type: "invalid_request_error" } });
  }
  const t = Date.now();
  const requestId = db.requestStart({
    provider_id: provider.id,
    provider_name: provider.name,
    model: body.model,
    wire_api: provider.wire_api,
    upstream_url: `${provider.base_url}/responses`,
    stream: !!body.stream,
    phase: "connecting",
  });
  try {
    db.requestUpdate(requestId, { phase: "waiting" });
    const r = await adapter.responses(body, model, provider);
    recordOutcome(classifyStatus(r.status));
    db.log(provider.id, body.model, r.status, Date.now() - t, r.ok ? "" : "upstream error");
    const usage = await sniffUsage(r);
    if (body.stream) {
      if (usage) db.requestUpdate(requestId, { usage });
      return streamResponse(reply, r, requestId);
    }
    reply.code(r.status);
    // content-encoding 必须剔除：body 已被 undici 解压，原样转发会导致客户端重复解压失败
    for (const [k, v] of r.headers) {
      const lk = k.toLowerCase();
      if (lk === "content-length" || lk === "content-encoding" || lk === "connection" || lk === "transfer-encoding") continue;
      reply.header(k, v);
    }
    // 与 chat/completions 同一口径：先读满响应体，按真实字节数统计流量后再转发
    const buf = Buffer.from(await r.arrayBuffer());
    db.requestEnd(requestId, { phase: r.ok ? "completed" : "error", upstream_status: r.status, usage: usage ?? undefined, bytes_in: buf.length, bytes_out: buf.length });
    return reply.send(buf);
  } catch (e: any) {
    const timeout = isTimeout(e);
    recordOutcome(timeout ? "timeout" : "error");
    const msg = e?.message ?? String(e);
    db.requestEnd(requestId, { phase: "error", error: msg });
    // 与 chat/completions 同一口径：超时 504，其余上游异常 502
    return reply.code(timeout ? 504 : 502).send({ error: { message: msg, type: timeout ? "timeout_error" : "upstream_error" } });
  }
});

// ===== 管理接口 =====
app.get("/api/status", async () => ({
  ok: true,
  version: VERSION,
  upstream_timeout_ms: UPSTREAM_TIMEOUT_MS,
  server_time: new Date().toISOString(),
  active_requests: db.activeRequests(),
  recent_logs: db.recentLogs(50),
  stats: db.getStats(),
}));
app.get("/api/diagnostics", async () => ({ ok: true, streaming: "SSE passthrough enabled", timeout_ms: UPSTREAM_TIMEOUT_MS }));

app.get("/api/stats", async () => db.getStats());
app.post("/api/stats/reset", async () => {
  db.resetStats();
  return { ok: true, stats: db.getStats() };
});
app.post("/api/logs/clear", async () => {
  db.clearLogs();
  return { ok: true };
});

// 设置查询只回传密钥是否已配置，不回传明文，避免密钥经管理接口外泄
app.get("/api/settings", async () => ({ api_key_configured: !!db.getSetting("api_key") }));
app.post("/api/settings", async (req: any) => {
  const b: any = req.body || {};
  if (typeof b.api_key === "string") db.setSetting("api_key", b.api_key);
  return { ok: true };
});

// 管理端：返回全部项目（含禁用/失效），密钥脱敏。
app.get("/api/providers", async () => db.listProviders().map((p) => ({ ...p, api_key: p.api_key ? "••••••••" : "", wire_api: p.wire_api })));

// 项目连通性测试 + 模型发现：请求上游 /models，归一化后返回模型列表或失败详情。
app.post("/api/providers/test", async (req: any, reply: any) => {
  const b: any = req.body || {};
  const base = cleanBase(b.base_url);
  const key = String(b.api_key || "");
  const wire = (b.wire_api || inferWire(base)) as WireApi;
  if (!base) return reply.code(400).send({ ok: false, message: "项目 URL 不能为空" });
  try {
    const r = await fetch(discoveryUrl(base), { headers: discoveryHeaders(key, wire) });
    const text = await r.text();
    let j: any = {};
    try {
      j = JSON.parse(text);
    } catch {}
    if (!r.ok) return reply.code(r.status).send({ ok: false, status: r.status, wire_api: wire, message: j?.error?.message ?? j?.message ?? `上游返回 HTTP ${r.status}`, detail: text.slice(0, 1000) });
    return { ok: true, status: r.status, wire_api: wire, models: modelList(j) };
  } catch (e: any) {
    return reply.code(502).send({ ok: false, message: e?.message ?? String(e) });
  }
});

app.post("/api/providers", async (req: any) => {
  const b: any = req.body || {};
  const base = cleanBase(b.base_url);
  return { id: db.createProvider({ ...b, base_url: base, wire_api: b.wire_api || inferWire(base) }) };
});
app.delete("/api/providers/:id", async (req: any) => {
  db.deleteProvider(Number(req.params.id));
  return { ok: true };
});
app.put("/api/providers/:id", async (req: any) => {
  db.updateProvider(Number(req.params.id), req.body || {});
  return { ok: true };
});
app.get("/api/models", async () => db.listAllModels());
app.post("/api/models", async (req: any) => ({ id: db.createModel(req.body || {}) }));
app.delete("/api/models/:id", async (req: any) => {
  db.deleteModel(Number(req.params.id));
  return { ok: true };
});

app.get("/", async (_, reply) => reply.sendFile("index.html"));

const host = process.env.HOST ?? "127.0.0.1";
const port = Number(process.env.PORT ?? 8317);
app
  .listen({ host, port })
  .then(() => app.log.info(`ST LLM Gateway ${host}:${port}`))
  .catch((e) => {
    app.log.error(e);
    process.exit(1);
  });
