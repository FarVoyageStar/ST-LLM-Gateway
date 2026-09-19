// OpenAI Chat Completions 与 Responses 两种请求/响应形状之间的双向转换。
// 供 responses 适配器在「对外 Chat 接口、上游仅支持 Responses」等场景下做协议桥接。
function text(content: unknown): string {
  if (typeof content === "string") return content;
  if (Array.isArray(content)) return content.map((x:any) => typeof x === "string" ? x : x?.text ?? "").join("");
  return content == null ? "" : JSON.stringify(content);
}

// OpenAI Chat messages → Responses input
// 转换规则：
//  1) assistant 消息的文本部件用 output_text（其余角色用 input_text）
//  2) user 图片部件翻成 input_image，保留视觉请求
//  3) assistant.tool_calls → function_call 部件、tool 角色 → function_call_output，
//     保证多轮工具调用历史完整
export function toResponsesInput(messages: any[]): any[] {
  const out: any[] = [];
  for (const m of messages ?? []) {
    if (m.role === "tool") {
      out.push({
        type: "function_call_output",
        call_id: m.tool_call_id ?? m.call_id ?? "",
        output: typeof m.content === "string" ? m.content : JSON.stringify(m.content ?? "")
      });
      continue;
    }
    if (m.role === "assistant" && Array.isArray(m.tool_calls) && m.tool_calls.length) {
      // 先输出文本（若有），再输出函数调用部件
      const t = text(m.content);
      if (t) out.push({ type: "message", role: "assistant", content: [{ type: "output_text", text: t }] });
      for (const tc of m.tool_calls) {
        out.push({
          type: "function_call",
          call_id: tc?.id ?? "",
          name: tc?.function?.name ?? "",
          arguments: tc?.function?.arguments ?? "{}"
        });
      }
      continue;
    }
    const content: any[] = [];
    if (Array.isArray(m.content)) {
      for (const part of m.content) {
        if (typeof part === "string") {
          content.push({ type: m.role === "assistant" ? "output_text" : "input_text", text: part });
        } else if (part?.type === "text") {
          content.push({ type: m.role === "assistant" ? "output_text" : "input_text", text: part.text ?? "" });
        } else if (part?.type === "image_url") {
          content.push({ type: "input_image", image_url: part.image_url?.url ?? "" });
        }
      }
    } else {
      content.push({ type: m.role === "assistant" ? "output_text" : "input_text", text: text(m.content) });
    }
    out.push({ type: "message", role: m.role, content });
  }
  return out;
}

// Responses 响应体 → Chat Completions 响应形状。
// model 字段回显对外 public_id（与 Anthropic 路径一致），而不是上游真实模型名，
// 避免客户端按请求模型匹配响应时困惑。
export function responsesToChat(body: any, publicId?: string) {
  const texts: string[] = [];
  const tools: any[] = [];
  for (const item of body?.output ?? []) {
    if (item.type === "message") {
      for (const c of item.content ?? []) if (c.type === "output_text") texts.push(c.text ?? "");
    }
    if (item.type === "function_call") {
      tools.push({
        id: item.call_id ?? item.id, type: "function",
        function: { name: item.name, arguments: item.arguments ?? "{}" }
      });
    }
  }
  const msg: any = { role: "assistant", content: texts.join("") || null };
  if (tools.length) msg.tool_calls = tools;
  // Responses 的 usage（input_tokens/output_tokens/total_tokens）→ Chat 形状（prompt_tokens/...）
  let usage: any = undefined;
  const u = body?.usage;
  if (u && typeof u === "object") {
    const prompt = Number(u.input_tokens ?? u.prompt_tokens ?? 0) || 0;
    const completion = Number(u.output_tokens ?? u.completion_tokens ?? 0) || 0;
    if (prompt || completion || u.total_tokens) {
      usage = {
        prompt_tokens: prompt,
        completion_tokens: completion,
        total_tokens: Number(u.total_tokens ?? prompt + completion) || prompt + completion
      };
    }
  }
  return {
    id: body?.id ?? `chatcmpl_${Date.now()}`, object: "chat.completion",
    created: Math.floor(Date.now()/1000), model: publicId ?? body?.model ?? "unknown",
    choices: [{ index: 0, message: msg, finish_reason: tools.length ? "tool_calls" : "stop" }],
    usage
  };
}
