// 上游 HTTP 请求的公共工具：URL 拼接、请求头构造、超时控制、错误体规范化。
import type { Provider } from "../types.js";

export function joinUrl(base: string, path: string) {
  return `${base.replace(/\/+$/, "")}/${path.replace(/^\/+/, "")}`;
}

// 按项目配置构造上游请求头：支持自定义 headers、Bearer 密钥及 OpenAI 组织/项目/应用标识。
export function headers(provider: Provider) {
  const h = new Headers({ "content-type": "application/json", ...provider.headers });
  if (provider.api_key) h.set("authorization", `Bearer ${provider.api_key}`);
  if (provider.organization) h.set("OpenAI-Organization", provider.organization);
  if (provider.project_id) h.set("OpenAI-Project", provider.project_id);
  if (provider.app_id) h.set("X-App-Id", provider.app_id);
  return h;
}

// 带超时的上游请求：默认 300 秒（UPSTREAM_TIMEOUT_MS 可覆盖）。
// 超时语义是"无进展超时"，分两段各自计时：
//   1) 响应头阶段：fetch 从发起到拿到响应头的最长等待；
//   2) 响应体阶段：拿到响应头后给 body 套一层 pull 式看门狗，以相邻两个
//      字节块的最长空闲间隔为限，每读出一块数据重置计时器。
// 健康的流式长响应不会被总时长误杀；上游发完头后彻底卡死（不发字节也不断连）
// 会按时中止，abort 错误经 isTimeout 归类为 504。
// 调用方已提供 signal 时尊重调用方，不再套自己的超时信号。
export async function upstreamFetch(url:string, init:RequestInit = {}) {
  const timeoutMs=Number(process.env.UPSTREAM_TIMEOUT_MS??300000);
  const controller=new AbortController();
  let timer=setTimeout(()=>controller.abort(),timeoutMs);
  // 看门狗计时器本身不应阻止进程退出
  timer.unref?.();
  const resetTimer=()=>{
    clearTimeout(timer);
    timer=setTimeout(()=>controller.abort(),timeoutMs);
    timer.unref?.();
  };
  try {
    const r=await fetch(url,{...init,signal:init.signal??controller.signal});
    // 无 body 的响应没有第二阶段，直接收尾；
    // 状态码超出 Response 构造器允许范围（200-599）时不做包裹，仅保留头阶段超时
    if(!r.body||r.status<200||r.status>599){clearTimeout(timer);return r;}
    // body 阶段从完整窗口重新计时（等第一块数据）
    resetTimer();
    // 包一层 pull 式看门狗：每读出一块数据重置空闲计时；流正常结束、读取失败或
    // 被下游取消时清除计时器。pull 由消费方驱动，天然保留背压，不会多拉数据。
    const reader=r.body.getReader();
    const body=new ReadableStream({
      async pull(c){
        try {
          const {done,value}=await reader.read();
          if(done){clearTimeout(timer);c.close();return;}
          resetTimer();
          c.enqueue(value);
        } catch(e) {
          clearTimeout(timer);
          c.error(e);
        }
      },
      cancel(reason){clearTimeout(timer);return reader.cancel(reason);},
    });
    // statusText 清洗为合法 reason-phrase（HTTP/1.1 上游可能带 obs-text），避免构造器抛错
    const statusText=/^[\t\x20-\x7E\x80-\xFF]*$/.test(r.statusText)?r.statusText:"";
    const wrapped=new Response(body,{status:r.status,statusText,headers:r.headers});
    // Response 构造器会在缺 content-type 时自动补 text/plain，恢复原状以免误导下游判断
    if(!r.headers.has("content-type")) wrapped.headers.delete("content-type");
    return wrapped;
  } catch(e) {
    clearTimeout(timer);
    throw e;
  }
}

// 上游错误体规范化为 OpenAI 兼容错误形状（流式请求的请求级错误也按 OpenAI 惯例返回 JSON）
export async function upstreamErrorResponse(r: Response): Promise<Response> {
  const text = await r.text().catch(() => "");
  let j: any = null;
  try { j = JSON.parse(text); } catch { /* 非 JSON 错误体 */ }
  const src = (j?.error && typeof j.error === "object") ? j.error : (j?.message != null ? j : null);
  const message = src?.message ?? (text ? text.slice(0, 500) : `Upstream returned HTTP ${r.status}`);
  const type = src?.type ?? "upstream_error";
  const code = src?.code ?? r.status;
  return Response.json({ error: { message, type, code } }, { status: r.status });
}
