// openai-chat 线路适配器：请求体原样透传，仅把 model 替换为上游真实模型名。
import type { Adapter, ChatRequest, Model, Provider } from "../types.js";
import { headers, joinUrl, upstreamFetch } from "../core/http.js";

export class OpenAIChatAdapter implements Adapter {
  async chat(req: ChatRequest, model: Model, provider: Provider) {
    return upstreamFetch(joinUrl(provider.base_url, "chat/completions"), {
      method: "POST", headers: headers(provider),
      body: JSON.stringify({ ...req, model: model.upstream_model })
    });
  }
}
