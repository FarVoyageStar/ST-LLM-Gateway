// 存储层：基于 JSON 文件的轻量持久化（Termux 环境不使用原生 SQLite 依赖）。
// 数据分三个文件存放：
//   gateway-data.json   配置（设置 / 项目 / 模型），变更少、同步写、崩溃不丢；
//   gateway-logs.json   请求日志（环形保留最近 MAX_LOGS 条）；
//   gateway-stats.json  累计统计（请求数 / Token / 字节数 / 并发峰值）。
import fs from "node:fs";
import path from "node:path";
import type { Model, Provider, WireApi } from "../types.js";

// 请求生命周期阶段：connecting（已受理）→ waiting（已转发上游）→ streaming（透传中）→ completed / error
export type RequestPhase = "connecting" | "waiting" | "streaming" | "completed" | "error";
export type UsageDelta = { prompt: number; completion: number; total: number };

export type RequestStatus = {
  id: string;
  started_at: string;
  elapsed_ms: number;
  provider_id?: number;
  provider_name?: string;
  model?: string;
  wire_api?: WireApi;
  phase: RequestPhase;
  upstream_status?: number;
  upstream_url?: string;
  bytes_in: number;
  bytes_out: number;
  stream: boolean;
  error?: string;
  usage?: UsageDelta;
};

export type Stats = {
  started_at: string;
  cleared_at: string;
  requests_total: number;
  responses_total: number;
  rejected_total: number;
  timeout_total: number;
  error_total: number;
  token_prompt: number;
  token_completion: number;
  token_total: number;
  bytes_in: number;
  bytes_out: number;
  connections_total: number;
  peak_concurrent: number;
};

export type NumericStats = Partial<
  Record<
    | "requests_total"
    | "responses_total"
    | "rejected_total"
    | "timeout_total"
    | "error_total"
    | "token_prompt"
    | "token_completion"
    | "token_total"
    | "bytes_in"
    | "bytes_out"
    | "connections_total"
    | "peak_concurrent",
    number
  >
>;

type Store = {
  settings: Record<string, string>;
  providers: Provider[];
  models: Model[];
  seq: { provider: number; model: number };
};

const MAX_LOGS = 500;

// 配置 / 日志 / 统计三者分离，避免 gateway-data.json 被海量日志污染。
const cfgFile = path.join(process.cwd(), "gateway-data.json");
const logFile = path.join(process.cwd(), "gateway-logs.json");
const statsFile = path.join(process.cwd(), "gateway-stats.json");

function now() {
  return new Date().toISOString();
}
function readJson<T>(file: string, fallback: T): T {
  try {
    return JSON.parse(fs.readFileSync(file, "utf8")) as T;
  } catch {
    return fallback;
  }
}
// 原子写：先写临时文件再改名，避免写盘中途崩溃留下半个 JSON 导致配置全丢。
// mode 0o600：配置内含连接密钥与各项目 API Key 明文，不依赖进程 umask，
// 显式限制为仅属主可读写（rename 会保留临时文件的权限位）。
function writeJson(file: string, data: unknown) {
  const tmp = file + ".tmp";
  fs.writeFileSync(tmp, JSON.stringify(data, null, 2), { encoding: "utf8", mode: 0o600 });
  fs.renameSync(tmp, file);
}
function defaultStats(): Stats {
  const t = now();
  return {
    started_at: t,
    cleared_at: t,
    requests_total: 0,
    responses_total: 0,
    rejected_total: 0,
    timeout_total: 0,
    error_total: 0,
    token_prompt: 0,
    token_completion: 0,
    token_total: 0,
    bytes_in: 0,
    bytes_out: 0,
    connections_total: 0,
    peak_concurrent: 0,
  };
}

export class DB {
  private data: Store;
  private stats: Stats;
  private logs: any[];
  // done 标记：requestEnd 幂等防护（同一请求重复收尾不再重复计数/扣减并发）
  private active = new Map<string, RequestStatus & { done?: boolean }>();
  // 真实并发数：requestStart +1 / requestEnd -1。
  // 不能用 active.size——完成的请求会在 map 里滞留 30s 供仪表盘展示，会把串行请求也算成并发。
  private concurrent = 0;

  // ===== 统计/日志防抖落盘的状态 =====
  // 每个请求都会触发 requestStart/requestEnd/statsIncrement/log 共 3~4 次整文件
  // writeFileSync，手机闪存较慢、并发高时会阻塞事件循环造成短暂卡顿。统计与日志
  // 允许崩溃时丢失最近几百毫秒的数据，故把 500ms 窗口内的多次修改合并为一次写盘，
  // 并在进程退出前强制 flush（见构造器）。
  private dirtyStats = false;
  private dirtyLogs = false;
  private flushTimer: ReturnType<typeof setTimeout> | null = null;

  constructor() {
    const rawOld = readJson<any>(cfgFile, {});
    this.data = {
      settings: rawOld.settings ?? {},
      providers: rawOld.providers ?? [],
      models: rawOld.models ?? [],
      seq: rawOld.seq ?? { provider: 0, model: 0 },
    };
    // 兼容没有 wire_api 字段的旧配置：按 base_url 推断线路协议
    for (const p of this.data.providers) {
      if (!p.wire_api) p.wire_api = this.inferWire(p.base_url);
    }

    this.stats = { ...defaultStats(), ...readJson<Partial<Stats>>(statsFile, {}) };
    this.logs = readJson<any[]>(logFile, []);

    // 迁移早期写入 gateway-data.json 的日志（启动一次性动作，立即落盘不走访抖）。
    if (Array.isArray(rawOld.logs) && rawOld.logs.length && this.logs.length === 0) {
      this.logs = rawOld.logs;
      writeJson(logFile, this.logs);
    }

    this.persistCfg();

    // 退出前把防抖窗口内未落盘的统计/日志强制写盘。
    // process.exit 会同步执行 'exit' 回调，故 SIGINT/SIGTERM 与 exit 双注册兜底（flush 幂等）。
    // 退出码遵循惯例：128 + 信号编号。
    const flushOnExit = () => this.flush();
    process.once("exit", flushOnExit);
    for (const sig of ["SIGINT", "SIGTERM"] as const) {
      process.once(sig, () => {
        this.flush();
        process.exit(sig === "SIGINT" ? 130 : 143);
      });
    }
  }

  private inferWire(url: string): WireApi {
    return /\/anthropic(?:\/|$)/i.test(url) ? "anthropic" : "openai-chat";
  }

  private persistCfg() {
    // 配置变更极少（仅管理页手动操作），保持同步写，保证立即生效、崩溃不丢配置
    writeJson(cfgFile, this.data);
  }

  private scheduleFlush() {
    if (this.flushTimer) return;
    this.flushTimer = setTimeout(() => {
      this.flushTimer = null;
      this.flush();
    }, 500);
    // 不让防抖计时器本身阻止进程退出
    this.flushTimer.unref?.();
  }

  private flush() {
    if (this.dirtyStats) {
      this.dirtyStats = false;
      writeJson(statsFile, this.stats);
    }
    if (this.dirtyLogs) {
      this.dirtyLogs = false;
      writeJson(logFile, this.logs);
    }
  }

  private persistLogs() {
    this.dirtyLogs = true;
    this.scheduleFlush();
  }
  private persistStats() {
    this.dirtyStats = true;
    this.scheduleFlush();
  }
  private enabledProvider(id: number): boolean {
    const p = this.data.providers.find((x) => x.id === id);
    return !!p && p.enabled;
  }

  // ===== 设置 =====
  getSetting(k: string) {
    return this.data.settings[k] ?? "";
  }
  setSetting(k: string, v: string) {
    this.data.settings[k] = v;
    this.persistCfg();
  }

  // ===== 项目（Provider） =====
  // 管理端返回全部项目，包含已禁用 / 失效的，保证都能被删除或重新启用。
  listProviders() {
    return this.data.providers;
  }
  getProvider(id: number) {
    return this.data.providers.find((p) => p.id === id);
  }
  createProvider(input: any) {
    const id = ++this.data.seq.provider;
    const p: Provider = {
      id,
      name: String(input.name || "Unnamed"),
      base_url: String(input.base_url || "").replace(/\/+$/, ""),
      api_key: String(input.api_key || ""),
      wire_api: (input.wire_api || this.inferWire(String(input.base_url || ""))) as WireApi,
      enabled: input.enabled !== undefined ? Boolean(input.enabled) : true,
      created_at: now(),
      updated_at: now(),
    };
    this.data.providers.push(p);
    this.persistCfg();
    return id;
  }
  updateProvider(id: number, input: any) {
    const p = this.getProvider(id);
    if (!p) throw new Error("Provider not found");
    if (input.name !== undefined) p.name = String(input.name);
    if (input.base_url !== undefined) p.base_url = String(input.base_url).replace(/\/+$/, "");
    // 显式传入即生效（含空字符串=清空密钥）；仅忽略管理页脱敏掩码的回传，防止误覆盖
    if (input.api_key !== undefined) {
      const k = String(input.api_key);
      if (k !== "••••••••") p.api_key = k;
    }
    if (input.wire_api !== undefined) p.wire_api = input.wire_api as WireApi;
    if (input.enabled !== undefined) p.enabled = Boolean(input.enabled);
    p.updated_at = now();
    this.persistCfg();
  }
  // 删除项目时级联删除其下全部模型，避免留下指向不存在项目的孤儿模型
  deleteProvider(id: number) {
    this.data.models = this.data.models.filter((m) => m.provider_id !== id);
    this.data.providers = this.data.providers.filter((p) => p.id !== id);
    this.persistCfg();
  }

  // ===== 模型 =====
  // 对外暴露：仅启用模型且所属项目启用。
  listModels() {
    return this.data.models.filter((m) => m.enabled && this.enabledProvider(m.provider_id));
  }
  // 管理端：全部模型（含失效项目下的模型）。
  listAllModels() {
    return this.data.models;
  }
  getModel(publicId: string) {
    return this.data.models.find(
      (m) => m.enabled && m.public_id === publicId && this.enabledProvider(m.provider_id),
    );
  }
  // 按对外别名查模型，不过滤所属项目的启用状态：把"项目已禁用"（503）与
  // "模型不存在"（404）区分开，保证路由层的 503 Provider unavailable 分支可达
  getModelAny(publicId: string) {
    return this.data.models.find((m) => m.enabled && m.public_id === publicId);
  }
  createModel(input: any) {
    const id = ++this.data.seq.model;
    const upstream = String(input.upstream_model ?? input.name ?? "");
    const publicId = String(input.public_id || `${input.provider_id}:${upstream}`);
    this.data.models.push({
      id,
      provider_id: Number(input.provider_id),
      public_id: publicId,
      name: String(input.name || upstream),
      upstream_model: upstream,
      enabled: true,
    });
    this.persistCfg();
    return id;
  }
  deleteModel(id: number) {
    this.data.models = this.data.models.filter((m) => m.id !== id);
    this.persistCfg();
  }

  // ===== 日志（独立文件，防抖落盘） =====
  log(provider_id: number, model: string, status: number, latency_ms: number, error: string) {
    this.logs.push({ time: now(), provider_id, model, status, latency_ms, error });
    if (this.logs.length > MAX_LOGS) this.logs.splice(0, this.logs.length - MAX_LOGS);
    this.persistLogs();
  }
  recentLogs(n = 50) {
    return this.logs.slice(-n).reverse();
  }
  clearLogs() {
    this.logs = [];
    this.persistLogs();
  }

  // ===== 统计（独立文件，防抖落盘） =====
  // connections_active 是当前在途并发，属于瞬时值，直接读取不写文件。
  getStats() {
    const s: Stats & { connections_active: number } = {
      ...this.stats,
      connections_active: this.concurrent,
    };
    return s;
  }
  resetStats() {
    this.stats = defaultStats();
    this.persistStats();
  }
  statsIncrement(patch: NumericStats) {
    for (const k of Object.keys(patch) as (keyof NumericStats)[]) {
      const v = patch[k];
      if (typeof v === "number") (this.stats as any)[k] += v;
    }
    this.persistStats();
  }
  addTokens(u: UsageDelta) {
    this.stats.token_prompt += u.prompt || 0;
    this.stats.token_completion += u.completion || 0;
    this.stats.token_total += u.total || 0;
    this.persistStats();
  }

  // ===== 活跃请求 =====
  requestStart(x: Partial<RequestStatus>) {
    const id = x.id || `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    const rec: RequestStatus = {
      id,
      started_at: now(),
      elapsed_ms: 0,
      phase: "connecting",
      bytes_in: 0,
      bytes_out: 0,
      stream: false,
      ...x,
    };
    this.active.set(id, rec);
    this.stats.connections_total += 1;
    this.stats.requests_total += 1;
    this.concurrent += 1;
    if (this.concurrent > this.stats.peak_concurrent) {
      this.stats.peak_concurrent = this.concurrent;
    }
    this.persistStats();
    return id;
  }
  requestUpdate(id: string, patch: Partial<RequestStatus>) {
    const r = this.active.get(id);
    if (!r) return;
    Object.assign(r, patch, { elapsed_ms: Date.now() - Date.parse(r.started_at) });
  }
  // 幂等收尾：done 标记保证重复调用不重复计数；完成后记录滞留 30s 供仪表盘展示。
  requestEnd(id: string, patch: Partial<RequestStatus> = {}) {
    const r = this.active.get(id);
    if (!r || r.done) return;
    r.done = true;
    this.concurrent = Math.max(0, this.concurrent - 1);
    Object.assign(r, patch, { elapsed_ms: Date.now() - Date.parse(r.started_at) });
    this.stats.bytes_in += Number(r.bytes_in) || 0;
    this.stats.bytes_out += Number(r.bytes_out) || 0;
    if (r.usage) {
      this.stats.token_prompt += r.usage.prompt || 0;
      this.stats.token_completion += r.usage.completion || 0;
      this.stats.token_total += r.usage.total || 0;
    }
    this.persistStats();
    setTimeout(() => this.active.delete(id), 30000);
  }
  activeRequests() {
    return [...this.active.values()].map((r) => ({
      ...r,
      elapsed_ms: Date.now() - Date.parse(r.started_at),
    }));
  }
}
