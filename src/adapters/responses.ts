// responses 线路适配器：对接 OpenAI Responses 协议（/responses，Codex CLI 等使用）。
// 提供两个方向：
//   chat()       对外 Chat 请求 → 转换（toResponsesInput）后调上游 Responses，响应再
//                转回 Chat 形状（responsesToChat），供只认 Chat 接口的客户端使用
//   responses()  对外 Responses 请求 → 原样透传，仅替换 model 为上游真实模型名
import type { Adapter, ChatRequest, Model, Provider } from "../types.js";
import { headers, joinUrl, upstreamFetch, upstreamErrorResponse } from "../core/http.js";
import { responsesToChat, toResponsesInput } from "../core/normalize.js";

export class ResponsesAdapter implements Adapter {
  async chat(req: ChatRequest, model: Model, provider: Provider) {
    const body: any = {
      model: model.upstream_model,
      input: toResponsesInput(req.messages ?? []),
      stream: Boolean(req.stream)
    };
    // 采样 / 工具 / 输出格式等参数与 Responses 同名，直接透传
    for (const k of ["temperature","top_p","tools","tool_choice","response_format"]) if ((req as any)[k] !== undefined) body[k] = (req as any)[k];
    // max_tokens / max_completion_tokens → Responses 的 max_output_tokens（同名透传会丢失）。
    // 有值才带上，不强制：部分 reasoning 模型的 Responses 端点会直接拒绝这个字段。
    const maxOut = req.max_completion_tokens ?? req.max_tokens;
    if (maxOut !== undefined) body.max_output_tokens = maxOut;
    // Chat 的 reasoning_effort 平铺字段 → Responses 的 reasoning 对象
    if (req.reasoning_effort) body.reasoning = { effort: req.reasoning_effort };

    const r = await upstreamFetch(joinUrl(provider.base_url, "responses"), {
      method: "POST", headers: headers(provider), body: JSON.stringify(body)
    });
    // 请求级错误（含流式请求）：按 OpenAI 惯例返回 JSON 错误体与上游状态码
    if (!r.ok) return upstreamErrorResponse(r);
    if (req.stream) return translateStream(r, model.public_id);
    const j = await r.json().catch(() => ({}));
    return Response.json(responsesToChat(j, model.public_id));
  }

  async responses(body: any, model: Model, provider: Provider) {
    return upstreamFetch(joinUrl(provider.base_url, "responses"), {
      method: "POST", headers: headers(provider),
      body: JSON.stringify({ ...body, model: model.upstream_model })
    });
  }
}

// OpenAI Responses SSE → OpenAI chat.completion.chunk 流式翻译。
// 设计要点：
//  1) pull() 读到无输出帧（response.created / in_progress / output_item.added 等）时必须
//     继续内层循环读上游，否则 ReadableStream 不会再触发 pull，流永久挂起（0 字节输出）；
//     有输出才交还控制权以保持背压
//  2) SSE 帧按空行（\r?\n\r?\n）切分，buffer 中可能残留不完整帧，留待下轮拼接
//  3) response.completed / incomplete → 补发 finish_reason 终帧 + usage + [DONE] 并收尾
//  4) function_call 按 output_item 生命周期翻译：added 发函数名、arguments.delta 增量
//     转发参数；response.failed / error 事件透传为错误帧
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
  let usage: any = null;
  let toolCallIndex = 0;
  const toolItems = new Map<string, number>(); // item_id → tool_calls index

  const base = () => ({ id, object: "chat.completion.chunk" as const, created, model });

  // 向下游发一帧；流已关闭时丢弃。返回 1 表示本轮有产出（供 pull 判断背压）
  function emit(controller: ReadableStreamDefaultController, obj: any): number {
    if (closed) return 0;
    controller.enqueue(encoder.encode(`data: ${JSON.stringify(obj)}\n\n`));
    return 1;
  }

  // 收尾：补发 finish_reason 终帧（含 usage，input/output_tokens → Chat 命名）与
  // [DONE]，关闭下游并取消上游读取
  function closeStream(controller: ReadableStreamDefaultController) {
    if (closed) return;
    closed = true;
    const final: any = {
      ...base(),
      choices: [{ index: 0, delta: {}, finish_reason: finishReason ?? (toolItems.size ? "tool_calls" : "stop") }]
    };
    if (usage) {
      const prompt = Number(usage.input_tokens ?? usage.prompt_tokens ?? 0) || 0;
      const completion = Number(usage.output_tokens ?? usage.completion_tokens ?? 0) || 0;
      if (prompt || completion) {
        final.usage = {
          prompt_tokens: prompt,
          completion_tokens: completion,
          total_tokens: Number(usage.total_tokens ?? prompt + completion) || prompt + completion
        };
      }
    }
    controller.enqueue(encoder.encode(`data: ${JSON.stringify(final)}\n\n`));
    controller.enqueue(encoder.encode("data: [DONE]\n\n"));
    controller.close();
    reader.cancel().catch(() => {});
  }

  // 翻译单个 SSE 帧；返回向下游发出的帧数（0 表示该帧无需对外输出）
  function processFrame(controller: ReadableStreamDefaultController, part: string): number {
    const lines = part.split(/\r?\n/);
    const line = lines.find(x => x.startsWith("data:"));
    if (!line) return 0;
    const raw = line.slice(5).trim();
    if (!raw || raw === "[DONE]") return 0;
    let e: any; try { e = JSON.parse(raw); } catch { return 0; }

    switch (e?.type) {
      case "response.created":
      case "response.in_progress":
        if (e?.response?.id) id = e.response.id;
        return 0;
      case "response.output_item.added": {
        const item = e?.item;
        if (item?.type === "function_call") {
          const idx = toolCallIndex++;
          toolItems.set(item.id ?? item.call_id ?? String(idx), idx);
          return emit(controller, {
            ...base(),
            choices: [{
              index: 0,
              delta: { tool_calls: [{ index: idx, id: item.call_id ?? item.id, type: "function", function: { name: item.name, arguments: "" } }] },
              finish_reason: null
            }]
          });
        }
        return 0;
      }
      case "response.function_call_arguments.delta": {
        const idx = toolItems.get(e.item_id);
        if (idx !== undefined) {
          return emit(controller, {
            ...base(),
            choices: [{
              index: 0,
              delta: { tool_calls: [{ index: idx, function: { arguments: e.delta ?? "" } }] },
              finish_reason: null
            }]
          });
        }
        return 0;
      }
      case "response.output_text.delta": {
        return emit(controller, {
          ...base(),
          choices: [{ index: 0, delta: { content: e.delta ?? "" }, finish_reason: null }]
        });
      }
      case "response.completed": {
        usage = e?.response?.usage ?? usage;
        closeStream(controller);
        return 0;
      }
      case "response.incomplete": {
        usage = e?.response?.usage ?? usage;
        const reason = e?.response?.incomplete_details?.reason;
        finishReason = reason === "max_output_tokens" ? "length" : "stop";
        closeStream(controller);
        return 0;
      }
      case "response.failed":
      case "error": {
        const err = e?.response?.error ?? e?.error ?? e;
        emit(controller, { error: { message: err?.message ?? "upstream stream error", type: err?.code ?? err?.type ?? "upstream_error", code: err?.code ?? null } });
        finishReason = "error";
        closeStream(controller);
        return 1;
      }
      default:
        // output_text.done / content_part.* / output_item.done 等无需翻译
        return 0;
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
            // 上游结束（无论是否发过 response.completed）都要收尾
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
  return new Response(stream, { status: upstream.status, headers: { "content-type": "text/event-stream", "cache-control": "no-cache" } });
}
