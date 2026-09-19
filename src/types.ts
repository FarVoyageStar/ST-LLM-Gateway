// 全局共享类型定义。

// 上游线路协议：
//   openai-chat  OpenAI Chat Completions（/chat/completions）
//   responses    OpenAI Responses（/responses，Codex CLI 使用）
//   anthropic    Anthropic Messages（/v1/messages）
export type WireApi = "openai-chat" | "responses" | "anthropic";

// 上游项目：一个可被网关转发请求的 API 端点（base_url + 密钥 + 线路协议）
export type Provider = {
  id: number;
  name: string;
  base_url: string;
  api_key: string;
  wire_api: WireApi;
  headers?: Record<string,string>;
  organization?: string;
  project_id?: string;
  app_id?: string;
  enabled: boolean;
  created_at: string;
  updated_at: string;
};

// 模型映射：public_id 是对外暴露的别名（客户端按它请求），
// upstream_model 是转发到上游时替换成的真实模型名。
export type Model = {
  id: number;
  provider_id: number;
  public_id: string;
  name: string;
  upstream_model: string;
  enabled: boolean;
};

export type ChatMessage = {
  role: string;
  content: any;
  [key: string]: any;
};

export type ChatRequest = {
  model: string;
  messages?: ChatMessage[];
  stream?: boolean;
  temperature?: number;
  top_p?: number;
  max_tokens?: number;
  max_completion_tokens?: number;
  tools?: any[];
  tool_choice?: any;
  [key: string]: any;
};

// 线路适配器接口：按协议能力实现 chat / responses 之一或两者。
export type Adapter = {
  chat?: (req: ChatRequest, model: Model, provider: Provider) => Promise<Response>;
  responses?: (req: any, model: Model, provider: Provider) => Promise<Response>;
};
