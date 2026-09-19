// anthropic 线路适配器：OpenAI Chat 请求 ↔ Anthropic Messages 协议的双向转换。
// 覆盖内容部件（文本 / 图片）、工具调用（tool_calls ↔ tool_use / tool_result）、
// 思维链（thinking → reasoning_content）以及 SSE 流式翻译。
import type { Adapter, ChatRequest, Model, Provider } from "../types.js";
import { headers, joinUrl, upstreamFetch, upstreamErrorResponse } from "../core/http.js";

// 消息内容归一化为 Anthropic 内容块数组。
// 字符串包成 text 块；OpenAI 多模态部件逐一映射：
//   text      → text 块
//   image_url → image 块；data:image/...;base64,... 形式的内联图必须拆成 base64 源
//               （Anthropic 的 url 源只接受可公网抓取的 http(s) 链接，否则上游 400）
function normalizeContent(content: unknown): any[] {
  if (typeof content === "string") return [{ type: "text", text: content }];
  if (Array.isArray(content)) {
    return content.map((x: any) => {
      if (typeof x === "string") return { type: "text", text: x };
      if (x?.type === "text") return x;
      if (x?.type === "image_url") {
        const url: string = x.image_url?.url ?? "";
        const dm = /^data:(image\/[a-zA-Z0-9.+-]+);base64,([\s\S]*)$/.exec(url);
        if (dm) {
          return { type: "image", source: { type: "base64", media_type: dm[1], data: dm[2].replace(/\s+/g, "") } };
        }
        return { type: "image", source: { type: "url", url } };
      }
      return x;
    });
  }
  return [{ type: "text", text: content == null ? "" : JSON.stringify(content) }];
}

// OpenAI Chat 请求体 → Anthropic Messages 请求体。
// 转换规则：
//  1) system / developer 角色抽出到顶层 system 字段（Anthropic 不允许 messages 里出现）
//  2) tool 角色结果 → tool_result 块；assistant.tool_calls → tool_use 块，工具调用历史完整保留
//  3) OpenAI 历史里同一次 assistant 回合并行发起的多个 tool_call，其结果是连续多条 role:"tool"
//     消息；Anthropic 要求 user/assistant 严格交替，必须合并进同一条 user 消息的多个
//     tool_result 块，否则连续 user 消息触发 400 roles must alternate
function toAnthropic(req: ChatRequest, model: Model) {
  const system: any[] = [];
  const messages: any[] = [];
  for (const m of req.messages ?? []) {
    if (m.role === "system" || m.role === "developer") {
      system.push(...normalizeContent(m.content));
      continue;
    }
    if (m.role === "tool") {
      const block = {
        type: "tool_result",
        tool_use_id: m.tool_call_id ?? m.call_id ?? "",
        content: typeof m.content === "string" ? m.content : JSON.stringify(m.content ?? "")
      };
      const last = messages[messages.length - 1];
      if (last?.role === "user" && Array.isArray(last.content) &&
          last.content.every((b: any) => b?.type === "tool_result")) {
        last.content.push(block);
      } else {
        messages.push({ role: "user", content: [block] });
      }
      continue;
    }
    if (m.role === "assistant" && Array.isArray((m as any).tool_calls) && (m as any).tool_calls.length) {
      const content = normalizeContent(m.content).filter((b: any) => b?.type === "text" && b.text);
      for (const tc of (m as any).tool_calls) {
        let input: any = {};
        try { input = JSON.parse(tc?.function?.arguments || "{}"); } catch { /* arguments 非 JSON 时保持空对象 */ }
        content.push({ type: "tool_use", id: tc?.id ?? "", name: tc?.function?.name ?? "", input });
      }
      messages.push({ role: "assistant", content });
      continue;
    }
    messages.push({
      role: m.role === "assistant" ? "assistant" : "user",
      content: normalizeContent(m.content)
    });
  }

  // 兜底：合并残留的相邻同角色消息（如 tool 结果后紧跟 user 文本），
  // 保证送给 Anthropic 的 messages 严格满足 user/assistant 交替
  const merged: any[] = [];
  for (const msg of messages) {
    const last = merged[merged.length - 1];
    if (last && last.role === msg.role) last.content.push(...msg.content);
    else merged.push(msg);
  }

  const body: any = {
    model: model.upstream_model,
    messages: merged,
    max_tokens: req.max_tokens ?? req.max_completion_tokens ?? 4096,
    stream: Boolean(req.stream)
  };
  if (system.length) body.system = system;
  if (req.temperature !== undefined) body.temperature = req.temperature;
  if (req.top_p !== undefined) body.top_p = req.top_p;
  if (req.tools) {
    body.tools = (req.tools as any[]).map((t: any) => t?.function ? ({
      name: t.function.name,
      description: t.function.description,
      input_schema: t.function.parameters ?? { type: "object", properties: {} }
    }) : t);
  }
  if (req.tool_choice) {
    // OpenAI 的 tool_choice（"auto"/"required"/{"type":"function",...}）与 Anthropic
    // （{"type":"auto"|"any"|"none"|"tool"}）结构不同，直接透传会被上游 400
    const tc: any = req.tool_choice;
    if (typeof tc === "string") {
      body.tool_choice = { type: tc === "required" ? "any" : tc };
    } else if (tc?.type === "function" && tc?.function?.name) {
      body.tool_choice = { type: "tool", name: tc.function.name };
    }
  }
  return body;
}

// Anthropic Messages 响应体 → Chat Completions 响应形状。
// 思维链文本映射到 OpenAI 社区惯例的 reasoning_content，供客户端识别展示
// （redacted_thinking 是加密载荷，没有可展示内容，忽略）。
function toChat(body: any, model: string) {
  const blocks = body?.content ?? [];
  const texts = blocks.filter((x:any) => x?.type === "text").map((x:any) => x.text ?? "").join("");
  const thinking = blocks.filter((x:any) => x?.type === "thinking").map((x:any) => x.thinking ?? "").join("");
  const tools = blocks.filter((x:any) => x?.type === "tool_use").map((x:any) => ({
    id: x.id,
    type: "function",
    function: { name: x.name, arguments: JSON.stringify(x.input ?? {}) }
  }));
  return {
    id: body?.id ?? `chatcmpl_${Date.now()}`,
    object: "chat.completion",
    created: Math.floor(Date.now()/1000),
    model,
    choices: [{
      index: 0,
      message: { role: "assistant", content: texts || null, ...(thinking ? { reasoning_content: thinking } : {}), ...(tools.length ? {tool_calls: tools} : {}) },
      finish_reason: tools.length ? "tool_calls" : "stop"
    }],
    usage: body?.usage ? {
      prompt_tokens: body.usage.input_tokens,
      completion_tokens: body.usage.output_tokens,
      total_tokens: (body.usage.input_tokens ?? 0) + (body.usage.output_tokens ?? 0)
    } : undefined
  };
}

export class AnthropicAdapter implements Adapter {
  async chat(req: ChatRequest, model: Model, provider: Provider) {
    const body = toAnthropic(req, model);
    const h = headers(provider);
    // Anthropic 鉴权用 x-api-key + anthropic-version 头，不用 Bearer
    h.set("anthropic-version", "2023-06-01");
    h.delete("authorization");
    if (provider.api_key) h.set("x-api-key", provider.api_key);
    const r = await upstreamFetch(joinUrl(provider.base_url, "messages"), {
      method: "POST", headers: h, body: JSON.stringify(body)
    });
    // 请求级错误（含流式请求）：按 OpenAI 惯例返回 JSON 错误体与上游状态码
    if (!r.ok) return upstreamErrorResponse(r);
    if (req.stream) return translateStream(r, model.public_id);
    const j = await r.json().catch(() => ({}));
    return Response.json(toChat(j, model.public_id));
  }
}

// Anthropic SSE → OpenAI chat.completion.chunk 流式翻译。
// 设计要点：
//  1) pull() 读到无输出帧时必须继续内层循环读上游，否则 ReadableStream 不会再触发
//     pull，流永久挂起；有输出才交还控制权以保持背压
//  2) SSE 帧按空行（\r?\n\r?\n）切分，buffer 中可能残留不完整帧，留待下轮拼接
//  3) message_stop 或上游结束时补发 finish_reason 终帧 + usage + [DONE]，并 cancel 上游 reader
//  4) tool_use 块按 content_block 生命周期翻译：start 发函数名、delta 累积参数 JSON、
//     stop 发完整 arguments；thinking_delta 映射到 reasoning_content
function translateStream(upstream: Response, model: string) {
  if (!upstream.body) return upstream;
  const reader = upstream.body.getReader();
  const decoder = new TextDecoder();
  const encoder = new TextEncoder();
  let buffer = "";
  let closed = false;
  let id = `chatcmpl_${Date.now()}`;
  const created = Math.floor(Date.now() / 1000);
  let finishReason: string | null = null;
  let inputTokens = 0;
  let outputTokens = 0;
  let toolCallIndex = 0;
  // 上游内容块 index → 工具调用累积状态（id / 函数名 / 参数 JSON / 是否已发完整帧）
  const toolBlocks = new Map<number, { id: string; name: string; json: string; emitted: boolean }>();

  const base = () => ({ id, object: "chat.completion.chunk" as const, created, model });

  // 向下游发一帧；流已关闭时丢弃。返回 1 表示本轮有产出（供 pull 判断背压）
  function emit(controller: ReadableStreamDefaultController, obj: any): number {
    if (closed) return 0;
    controller.enqueue(encoder.encode(`data: ${JSON.stringify(obj)}\n\n`));
    return 1;
  }

  // 收尾：补发 finish_reason 终帧（含累计 usage）与 [DONE]，关闭下游并取消上游读取
  function closeStream(controller: ReadableStreamDefaultController) {
    if (closed) return;
    closed = true;
    const final: any = {
      ...base(),
      choices: [{ index: 0, delta: {}, finish_reason: finishReason ?? (toolBlocks.size ? "tool_calls" : "stop") }]
    };
    if (inputTokens || outputTokens) {
      final.usage = {
        prompt_tokens: inputTokens,
        completion_tokens: outputTokens,
        total_tokens: inputTokens + outputTokens
      };
    }
    controller.enqueue(encoder.encode(`data: ${JSON.stringify(final)}\n\n`));
    controller.enqueue(encoder.encode("data: [DONE]\n\n"));
    controller.close();
    reader.cancel().catch(() => {});
  }

  // 翻译单个 SSE 帧；返回向下游发出的帧数（0 表示该帧无需对外输出）
  function processFrame(controller: ReadableStreamDefaultController, part: string): number {
    const lines = part.split(/\r?\n/);
    const event = lines.find(x => x.startsWith("event:"))?.slice(6).trim();
    const line = lines.find(x => x.startsWith("data:"))?.slice(5).trim();
    if (!line) return 0;
    let e: any; try { e = JSON.parse(line); } catch { return 0; }
    const type = e?.type ?? event;

    switch (type) {
      case "message_start": {
        if (e?.message?.id) id = e.message.id;
        if (e?.message?.usage?.input_tokens) inputTokens = e.message.usage.input_tokens;
        return 0;
      }
      case "content_block_start": {
        const block = e?.content_block;
        if (block?.type === "tool_use") {
          toolBlocks.set(e.index, { id: block.id, name: block.name, json: "", emitted: false });
          return emit(controller, {
            ...base(),
            choices: [{
              index: 0,
              delta: { tool_calls: [{ index: toolCallIndex, id: block.id, type: "function", function: { name: block.name, arguments: "" } }] },
              finish_reason: null
            }]
          });
        }
        return 0;
      }
      case "content_block_delta": {
        const d = e?.delta;
        if (d?.type === "text_delta") {
          return emit(controller, {
            ...base(),
            choices: [{ index: 0, delta: { content: d.text ?? "" }, finish_reason: null }]
          });
        }
        // 思维链增量映射到 reasoning_content（OpenAI 社区惯例）；
        // signature_delta 是签名校验数据，无可展示内容，忽略
        if (d?.type === "thinking_delta") {
          return emit(controller, {
            ...base(),
            choices: [{ index: 0, delta: { reasoning_content: d.thinking ?? "" }, finish_reason: null }]
          });
        }
        if (d?.type === "input_json_delta") {
          const blk = toolBlocks.get(e.index);
          if (blk) blk.json += d.partial_json ?? "";
        }
        return 0;
      }
      case "content_block_stop": {
        const blk = toolBlocks.get(e.index);
        if (blk && !blk.emitted) {
          blk.emitted = true;
          const n = emit(controller, {
            ...base(),
            choices: [{
              index: 0,
              delta: { tool_calls: [{ index: toolCallIndex, id: blk.id, type: "function", function: { name: blk.name, arguments: blk.json || "{}" } }] },
              finish_reason: null
            }]
          });
          toolCallIndex++;
          return n;
        }
        return 0;
      }
      case "message_delta": {
        if (e?.usage?.output_tokens) outputTokens = e.usage.output_tokens;
        const reason = e?.delta?.stop_reason;
        finishReason = reason === "tool_use" ? "tool_calls"
          : reason === "max_tokens" ? "length"
          : reason ? "stop" : null;
        return 0;
      }
      case "message_stop": {
        closeStream(controller);
        return 0;
      }
      case "error": {
        const err = e?.error ?? e;
        emit(controller, { error: { message: err?.message ?? "upstream stream error", type: err?.type ?? "upstream_error", code: err?.code ?? null } });
        finishReason = "error";
        closeStream(controller);
        return 1;
      }
      default:
        return 0; // ping / 文本块的 start/stop 等无需翻译
    }
  }

  // 按空行切出完整 SSE 帧逐一翻译；不完整帧留在 buffer 等下轮数据拼接
  function processBuffer(controller: ReadableStreamDefaultController): number {
    const parts = buffer.split(/\r?\n\r?\n/);
    buffer = parts.pop() ?? "";
    let n = 0;
    for (const part of parts) {
      n += processFrame(controller, part);
      if (closed) break;
    }
    return n;
  }

  const stream = new ReadableStream({
    async pull(controller) {
      try {
        while (true) {
          const { value, done } = await reader.read();
          if (done) {
            // 冲刷 decoder 并处理残留在 buffer 中的最后一帧；
            // 上游结束（无论是否发过 message_stop）都要收尾
            buffer += decoder.decode();
            processBuffer(controller);
            closeStream(controller);
            return;
          }
          buffer += decoder.decode(value, { stream: true });
          const emitted = processBuffer(controller);
          if (closed) return;
          // 本轮有输出则交还控制权，保持背压；无输出则继续内层循环读上游
          if (emitted > 0) return;
        }
      } catch (err) {
        controller.error(err);
      }
    },
    cancel() {
      reader.cancel().catch(() => {});
    }
  });
  return new Response(stream, {
    status: upstream.status,
    headers: { "content-type": "text/event-stream", "cache-control": "no-cache" }
  });
}
