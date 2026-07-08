/**
 * Local OpenAI-compatible proxy that translates requests to Cursor's gRPC protocol.
 *
 * Accepts POST /v1/chat/completions in OpenAI format, translates to Cursor's
 * protobuf/HTTP2 Connect protocol, and streams back OpenAI-format SSE.
 *
 * Tool calling uses Cursor's native MCP tool protocol:
 * - OpenAI tool defs → McpToolDefinition in RequestContext
 * - Cursor toolCallStarted/Delta/Completed → OpenAI tool_calls SSE chunks
 * - mcpArgs exec → pause stream, return tool_calls to caller
 * - Follow-up request with tool results → resume bridge with mcpResult
 *
 * HTTP/2 transport is delegated to a Node child process (h2-bridge.mjs)
 * because Bun's node:http2 module is broken.
 */
import { create, fromBinary, fromJson, type JsonValue, toBinary, toJson } from "@bufbuild/protobuf";
import { ValueSchema } from "@bufbuild/protobuf/wkt";
import {
  AgentClientMessageSchema,
  AgentRunRequestSchema,
  AgentServerMessageSchema,
  ClientHeartbeatSchema,
  ConversationActionSchema,
  ConversationStateStructureSchema,
  ConversationStepSchema,
  AgentConversationTurnStructureSchema,
  ConversationTurnStructureSchema,
  AssistantMessageSchema,
  BackgroundShellSpawnResultSchema,
  DeleteResultSchema,
  DeleteRejectedSchema,
  DiagnosticsResultSchema,
  ExecClientMessageSchema,
  FetchErrorSchema,
  FetchResultSchema,
  GetBlobResultSchema,
  GrepErrorSchema,
  GrepResultSchema,
  KvClientMessageSchema,
  LsRejectedSchema,
  LsResultSchema,
  McpErrorSchema,
  McpResultSchema,
  McpSuccessSchema,
  McpTextContentSchema,
  McpToolDefinitionSchema,
  McpToolResultContentItemSchema,
  ModelDetailsSchema,
  ReadRejectedSchema,
  ReadResultSchema,
  RequestContextResultSchema,
  RequestContextSchema,
  RequestContextSuccessSchema,
  SetBlobResultSchema,
  ShellRejectedSchema,
  ShellResultSchema,
  UserMessageActionSchema,
  UserMessageSchema,
  WriteRejectedSchema,
  WriteResultSchema,
  WriteShellStdinErrorSchema,
  WriteShellStdinResultSchema,
  type AgentServerMessage,
  type ConversationStateStructure,
  type ExecServerMessage,
  type KvServerMessage,
  type McpToolDefinition,
} from "./proto/agent_pb";
import { createHash } from "node:crypto";
import { resolve as pathResolve } from "node:path";
import { statSync } from "node:fs";

const CURSOR_API_URL = process.env.CURSOR_API_URL ?? "https://api2.cursor.sh";
export const CONNECT_END_STREAM_FLAG = 0b00000010;
const BRIDGE_PATH = pathResolve(import.meta.dir, "h2-bridge.mjs");

/** Build stamp = mtime (ms) of THIS module's own file. A freshly-rebuilt proxy
 *  has a newer mtime, so it can detect — and evict — an OLDER proxy squatting the
 *  fixed port (a still-running opencode window from before the rebuild). Without
 *  this, `startProxy` adopts ANY healthy cursor-oauth proxy on the port, so a new
 *  window silently reuses stale in-memory plugin code and a fix never takes
 *  effect until every old window is killed. See probeHealth / requestProxyEviction.
 *  Falls back to 0 (never wins an eviction race) if the self-stat fails. */
const BUILD_EPOCH: number = (() => {
  try {
    return Math.floor(statSync(pathResolve(import.meta.dir, "proxy.js")).mtimeMs);
  } catch {
    try {
      // Dev/source run (bun src/proxy.ts): no compiled proxy.js next to us.
      return Math.floor(statSync(new URL(import.meta.url).pathname).mtimeMs);
    } catch {
      return 0;
    }
  }
})();

const SSE_HEADERS = {
  "Content-Type": "text/event-stream",
  "Cache-Control": "no-cache",
  Connection: "keep-alive",
} as const;

// Bun.serve's idleTimeout is hard-capped at 255s (oven-sh/bun#27470) — Bun
// itself, not Cursor or opencode, severs a connection that goes that long
// without an outbound byte. A Cursor turn that thinks silently for longer
// than that (slow tool-result processing, backend load) had its SSE
// connection to opencode killed mid-turn, surfacing to opencode as
// "AI_APICallError: socket connection was closed unexpectedly". opencode
// retries, but the retry replays a checkpoint/blobStore pairing Cursor's
// server may already have moved past — one more path to "Blob not found".
// Pinging well under Bun's ceiling keeps the connection looking alive for the
// whole duration of a legitimately slow turn.
let SSE_KEEPALIVE_MS = 20_000;
export function setSseKeepAliveMsForTest(ms: number): void {
  SSE_KEEPALIVE_MS = ms;
}

// 2026-07-07: a turn hung silently for 2h36m — no tool call, no error, no
// data — recovering only when something outside this process (the network
// layer) finally reaped a dead connection. Root cause: h2-bridge.mjs's own
// kill-watchdog resets on ANY stdin write, including the clientHeartbeat
// ping above every 5s, so it never measured "has Cursor responded," only
// "are we still writing" — which is always true. That watchdog is left
// alone here: it correctly keeps the bridge alive across a *different*,
// legitimate wait (a paused bridge sitting in activeBridges between a
// tool_calls turn ending and opencode sending back a slow tool result, e.g.
// a long-running bash command or a human answering the `question` tool —
// which can and should take far longer than this).
//
// This timer instead lives at the application level, scoped only to "we are
// still expecting Cursor to produce its own output for this turn" (inside
// createBridgeStreamResponse / collectFullResponse, before any terminal
// state). It resets ONLY on a genuine parsed AgentServerMessage — never on
// our own outbound heartbeat. Empirically (2026-07-07 live probe against the
// real backend: 7,033 messages / 75s trace, max gap 3.9s at startup, p99
// 127ms, zero server-sent HeartbeatUpdate frames), a healthy turn never goes
// quiet for more than a few seconds. 90s leaves generous margin above that
// noise floor while still bounding the worst case to under two minutes
// instead of two and a half hours.
let GENERATION_STALL_MS = 90_000;
export function setGenerationStallMsForTest(ms: number): void {
  GENERATION_STALL_MS = ms;
}

interface OpenAIToolCall {
  id: string;
  type: "function";
  function: { name: string; arguments: string };
}

/** A single element in an OpenAI multi-part content array. */
interface ContentPart {
  type: string;
  text?: string;
}

interface OpenAIMessage {
  role: "system" | "user" | "assistant" | "tool";
  content: string | null | ContentPart[];
  tool_call_id?: string;
  tool_calls?: OpenAIToolCall[];
}

interface OpenAIToolDef {
  type: "function";
  function: {
    name: string;
    description?: string;
    parameters?: Record<string, unknown>;
  };
}

interface ChatCompletionRequest {
  model: string;
  messages: OpenAIMessage[];
  stream?: boolean;
  temperature?: number;
  max_tokens?: number;
  tools?: OpenAIToolDef[];
  tool_choice?: unknown;
}


interface CursorRequestPayload {
  requestBytes: Uint8Array;
  blobStore: Map<string, Uint8Array>;
  mcpTools: McpToolDefinition[];
}

/** A pending tool execution waiting for results from the caller. */
interface PendingExec {
  execId: string;
  execMsgId: number;
  toolCallId: string;
  toolName: string;
  /** Decoded arguments JSON string for SSE tool_calls emission. */
  decodedArgs: string;
}

/** A bridge kept alive across requests for tool result continuation. */
interface ActiveBridge {
  bridge: StreamBridge;
  heartbeatTimer: NodeJS.Timeout;
  blobStore: Map<string, Uint8Array>;
  mcpTools: McpToolDefinition[];
  pendingExecs: PendingExec[];
}

// Active bridges keyed by a session token (derived from conversation state).
// When tool_calls are returned, the bridge stays alive. The next request
// with tool results looks up the bridge and sends mcpResult messages.
const activeBridges = new Map<string, ActiveBridge>();

interface StoredConversation {
  conversationId: string;
  checkpoint: Uint8Array | null;
  blobStore: Map<string, Uint8Array>;
  lastAccessMs: number;
}

const conversationStates = new Map<string, StoredConversation>();
const CONVERSATION_TTL_MS = 30 * 60 * 1000; // 30 minutes

function getOrCreateStoredConversation(convKey: string): StoredConversation {
  let stored = conversationStates.get(convKey);
  if (!stored) {
    stored = {
      conversationId: crypto.randomUUID(),
      checkpoint: null,
      blobStore: new Map(),
      lastAccessMs: Date.now(),
    };
    conversationStates.set(convKey, stored);
  }
  return stored;
}

export function getStoredConversationIdForTest(convKey: string): string {
  return getOrCreateStoredConversation(convKey).conversationId;
}

/** Discard local state for a conversation we can no longer trust: Cursor
 *  rejected the last turn (any Connect end-stream error — "Blob not found" or
 *  otherwise), or the client disconnected before a turn reached a clean
 *  terminal state. The next request for this convKey mints a fresh
 *  conversationId (see getOrCreateStoredConversation) instead of replaying a
 *  checkpoint/blobStore pairing Cursor's server may have already moved past.
 *  Turns any Blob-not-found-class error into a one-turn hiccup instead of a
 *  conversation wedged for the rest of the proxy process's lifetime. */
function poisonStoredConversation(convKey: string): void {
  conversationStates.delete(convKey);
}

export function evictStoredConversationForTest(convKey: string): void {
  poisonStoredConversation(convKey);
}

function evictStaleConversations(): void {
  const now = Date.now();
  for (const [key, stored] of conversationStates) {
    if (now - stored.lastAccessMs > CONVERSATION_TTL_MS) {
      conversationStates.delete(key);
    }
  }
}

/** Length-prefix a message: [4-byte BE length][payload] */
function lpEncode(data: Uint8Array): Buffer {
  const buf = Buffer.alloc(4 + data.length);
  buf.writeUInt32BE(data.length, 0);
  buf.set(data, 4);
  return buf;
}

/** Connect protocol frame: [1-byte flags][4-byte BE length][payload] */
function frameConnectMessage(data: Uint8Array, flags = 0): Buffer {
  const frame = Buffer.alloc(5 + data.length);
  frame[0] = flags;
  frame.writeUInt32BE(data.length, 1);
  frame.set(data, 5);
  return frame;
}

/**
 * Spawn the Node H2 bridge and return read/write handles.
 * The bridge uses length-prefixed framing on stdin/stdout.
 */
interface SpawnBridgeOptions {
  accessToken: string;
  rpcPath: string;
  url?: string;
  /** When true, use application/proto for unary RPCs instead of Connect streaming. */
  unary?: boolean;
}

/** Narrow view of a spawned bridge used by response-streaming code. Omits
 *  `proc` (only `callCursorUnaryRpc`'s own timeout guard needs direct process
 *  control, and it closes over its own `spawnBridge()` result directly).
 *  Exported so tests can drive `createBridgeStreamResponseForTest` /
 *  `collectFullResponseForTest` with a fake bridge — no real child process or
 *  H2 connection required. */
export interface StreamBridge {
  write: (data: Uint8Array) => void;
  end: () => void;
  onData: (cb: (chunk: Buffer) => void) => void;
  onClose: (cb: (code: number) => void) => void;
  /** True while the bridge subprocess is still running. */
  get alive(): boolean;
  /** Kill the underlying bridge subprocess immediately. */
  kill: () => void;
}

function spawnBridge(options: SpawnBridgeOptions): {
  proc: ReturnType<typeof Bun.spawn>;
  write: (data: Uint8Array) => void;
  end: () => void;
  onData: (cb: (chunk: Buffer) => void) => void;
  onClose: (cb: (code: number) => void) => void;
  /** True while the bridge subprocess is still running. */
  get alive(): boolean;
  kill: () => void;
} {
  const proc = Bun.spawn(["node", BRIDGE_PATH], {
    stdin: "pipe",
    stdout: "pipe",
    stderr: "ignore",
  });

  const config = JSON.stringify({
    accessToken: options.accessToken,
    url: options.url ?? CURSOR_API_URL,
    path: options.rpcPath,
    unary: options.unary ?? false,
  });
  proc.stdin.write(lpEncode(new TextEncoder().encode(config)));

  const cbs = {
    data: null as ((chunk: Buffer) => void) | null,
    close: null as ((code: number) => void) | null,
  };

  // Track exit state so late onClose registrations fire immediately.
  let exited = false;
  let exitCode = 1;

  (async () => {
    const reader = proc.stdout.getReader();
    let pending = Buffer.alloc(0);

    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        pending = Buffer.concat([pending, Buffer.from(value)]);

        while (pending.length >= 4) {
          const len = pending.readUInt32BE(0);
          if (pending.length < 4 + len) break;
          const payload = pending.subarray(4, 4 + len);
          pending = pending.subarray(4 + len);
          cbs.data?.(Buffer.from(payload));
        }
      }
    } catch {
      // Stream ended
    }

    const code = await proc.exited ?? 1;
    exited = true;
    exitCode = code;
    cbs.close?.(code);
  })();

  return {
    proc,
    get alive() { return !exited; },
    write(data) {
      try { proc.stdin.write(lpEncode(data)); } catch {}
    },
    end() {
      try {
        proc.stdin.write(lpEncode(new Uint8Array(0)));
        proc.stdin.end();
      } catch {}
    },
    onData(cb) { cbs.data = cb; },
    onClose(cb) {
      if (exited) {
        // Process already exited — invoke immediately so streams don't hang.
        queueMicrotask(() => cb(exitCode));
      } else {
        cbs.close = cb;
      }
    },
    kill() {
      try { proc.kill(); } catch {}
    },
  };
}

interface CursorUnaryRpcOptions {
  accessToken: string;
  rpcPath: string;
  requestBody: Uint8Array;
  url?: string;
  timeoutMs?: number;
}

export async function callCursorUnaryRpc(
  options: CursorUnaryRpcOptions,
 ): Promise<{ body: Uint8Array; exitCode: number; timedOut: boolean }> {
  const bridge = spawnBridge({
    accessToken: options.accessToken,
    rpcPath: options.rpcPath,
    url: options.url,
    unary: true,
  });
  const chunks: Buffer[] = [];
  const { promise, resolve } = Promise.withResolvers<{
    body: Uint8Array;
    exitCode: number;
    timedOut: boolean;
  }>();
  let timedOut = false;
  const timeoutMs = options.timeoutMs ?? 5_000;
  const timeout = timeoutMs > 0
    ? setTimeout(() => {
        timedOut = true;
        try { bridge.proc.kill(); } catch {}
      }, timeoutMs)
    : undefined;

  bridge.onData((chunk) => {
    chunks.push(Buffer.from(chunk));
  });
  bridge.onClose((exitCode) => {
    if (timeout) clearTimeout(timeout);
    resolve({
      body: Buffer.concat(chunks),
      exitCode,
      timedOut,
    });
  });

  // Unary: send raw protobuf body (no Connect framing)
  bridge.write(options.requestBody);
  bridge.end();

  return promise;
}

// Proxy singleton anchored on globalThis, NOT module scope. opencode can load a
// plugin more than once (auto-discovery from BOTH ~/.config/opencode/plugin/ and
// plugins/, plus any npm `plugin[]` entry) — each load is a SEPARATE module copy
// with its own module-scoped variables. A module-scoped singleton would then let
// two copies each try to Bun.serve the same fixed port: the first binds, the
// second hits EADDRINUSE. Storing the server on globalThis makes every copy in
// the same process share one instance, so the second load is a no-op reuse.
interface CursorProxyGlobal {
  server?: ReturnType<typeof Bun.serve>;
  port?: number;
}
const PROXY_GLOBAL_KEY = "__cursorOauthProxy__";
function proxyGlobal(): CursorProxyGlobal {
  const g = globalThis as unknown as Record<string, CursorProxyGlobal | undefined>;
  return (g[PROXY_GLOBAL_KEY] ??= {});
}

let proxyAccessTokenProvider: (() => Promise<string>) | undefined;
let proxyModels: Array<{ id: string; name: string }> = [];

function buildOpenAIModelList(models: ReadonlyArray<{ id: string; name: string }>): Array<{
  id: string;
  object: "model";
  created: number;
  owned_by: string;
}> {
  return models.map((model) => ({
    id: model.id,
    object: "model",
    created: 0,
    owned_by: "cursor",
  }));
}

export function getProxyPort(): number | undefined {
  return proxyGlobal().port;
}

export async function startProxy(
  getAccessToken: () => Promise<string>,
  models: ReadonlyArray<{ id: string; name: string }> = [],
): Promise<number> {
  proxyAccessTokenProvider = getAccessToken;
  proxyModels = models.map((model) => ({
    id: model.id,
    name: model.name,
  }));

  const state = proxyGlobal();
  if (state.server && state.port) return state.port;

  // Fixed port when CURSOR_PROXY_PORT is set (standalone mode); 0 = ephemeral
  // (plugin/loader mode, where the port is handed back to opencode in-process).
  const fixedPort = process.env.CURSOR_PROXY_PORT
    ? Number(process.env.CURSOR_PROXY_PORT)
    : 0;

  // Reuse an already-running cursor-oauth proxy on the same fixed port (another
  // opencode window, a standalone `serve`, or a second copy of this plugin). The
  // probe retries briefly: a racing starter may have bound the socket a
  // millisecond ago but not yet wired its fetch handler, in which case /health
  // answers Bun's default until the handler attaches. Only adopt on a CONFIRMED
  // cursor-oauth health response — AND only if it is not OLDER than us. If the
  // running proxy predates this build (stale opencode window from before a
  // rebuild), evict it and bind ourselves so the fix actually takes effect.
  if (fixedPort) {
    const health = await probeHealthWithRetry(fixedPort);
    if (health.ok) {
      if (health.buildEpoch >= BUILD_EPOCH) {
        state.port = fixedPort;
        state.server = undefined; // we don't own it; never stop() someone else's server
        return fixedPort;
      }
      // Running proxy is older → take over the port.
      const evicted = await requestProxyEviction(fixedPort);
      if (!evicted) {
        try {
          const fetchHandler = makeFetchHandler();
          state.server = Bun.serve({ port: fixedPort, idleTimeout: 255, fetch: fetchHandler });
          const rebound = state.server.port;
          if (rebound) {
            state.port = rebound;
            return rebound;
          }
        } catch {
          // still genuinely occupied; fall through to optimistic adopt
        }
        // It refused or wouldn't let go; adopt it rather than leave opencode with
        // no provider. (Worst case: still stale, same as before this guard.)
        console.error(
          `[cursor-oauth] older proxy on ${fixedPort} did not release; adopting it`,
        );
        state.port = fixedPort;
        state.server = undefined;
        return fixedPort;
      }
      // Port is free now; fall through to bind our fresh server below.
    }
  }

  const fetchHandler = makeFetchHandler();
  try {
    state.server = Bun.serve({ port: fixedPort, idleTimeout: 255, fetch: fetchHandler });
  } catch (err: any) {
    const inUse =
      err?.code === "EADDRINUSE" || /in use|EADDRINUSE/i.test(String(err?.message));
    if (fixedPort && inUse) {
      // Lost the startup race: someone bound the port between our probe and serve.
      // If it's a healthy cursor-oauth proxy that is not older than us, adopt it;
      // if it's older, evict and retry the bind once.
      const health = await probeHealthWithRetry(fixedPort);
      if (health.ok && health.buildEpoch >= BUILD_EPOCH) {
        state.port = fixedPort;
        state.server = undefined;
        return fixedPort;
      }
      if (health.ok && health.buildEpoch < BUILD_EPOCH && (await requestProxyEviction(fixedPort))) {
        try {
          state.server = Bun.serve({ port: fixedPort, idleTimeout: 255, fetch: fetchHandler });
          const rebound = state.server.port;
          if (rebound) {
            state.port = rebound;
            return rebound;
          }
        } catch {
          // fall through to ephemeral
        }
      }
      // The port is held by something that is NOT our proxy (a foreign squatter,
      // or a half-dead bind that never answers /health). Rather than hard-fail
      // the plugin — which leaves opencode with no Cursor provider at all — bind
      // an EPHEMERAL port instead. The bound port flows into the provider block,
      // so this window still works; it just doesn't share the fixed port.
      console.error(
        `[cursor-oauth] port ${fixedPort} is held by a non-cursor-oauth process; ` +
        `falling back to an ephemeral port for this instance`,
      );
      state.server = Bun.serve({ port: 0, idleTimeout: 255, fetch: fetchHandler });
    } else {
      throw err;
    }
  }

  const bound = state.server.port;
  if (!bound) throw new Error("Failed to bind proxy to a port");
  state.port = bound;
  return bound;
}

interface ProxyHealth {
  /** True iff the port answered a cursor-oauth /health. */
  ok: boolean;
  /** The running proxy's build stamp (0 if it predates the stamp field). */
  buildEpoch: number;
}

/** GET /health on a candidate port. Reports whether it's a live cursor-oauth
 *  proxy and, if so, its build stamp. */
async function probeHealth(port: number): Promise<ProxyHealth> {
  try {
    const res = await fetch(`http://127.0.0.1:${port}/health`, {
      signal: AbortSignal.timeout(1500),
    });
    if (!res.ok) return { ok: false, buildEpoch: 0 };
    const body = (await res.json().catch(() => null)) as
      | { proxy?: string; buildEpoch?: number }
      | null;
    if (body?.proxy !== "cursor-oauth") return { ok: false, buildEpoch: 0 };
    return { ok: true, buildEpoch: typeof body.buildEpoch === "number" ? body.buildEpoch : 0 };
  } catch {
    return { ok: false, buildEpoch: 0 };
  }
}

/**
 * probeHealth with a few quick retries. Covers the narrow race where a competing
 * starter has just bound the socket but not yet attached its fetch handler (so
 * /health momentarily returns Bun's default page). Returns the confirmed health
 * as soon as any attempt succeeds; a non-ok health if none do within the budget.
 */
async function probeHealthWithRetry(
  port: number,
  attempts = 3,
  delayMs = 150,
): Promise<ProxyHealth> {
  let last: ProxyHealth = { ok: false, buildEpoch: 0 };
  for (let i = 0; i < attempts; i++) {
    last = await probeHealth(port);
    if (last.ok) return last;
    if (i < attempts - 1) await new Promise((r) => setTimeout(r, delayMs));
  }
  return last;
}

/** Ask an OLDER proxy on `port` to release it (POST /shutdown with our epoch).
 *  Returns true once the port is free (the old proxy stopped and no cursor-oauth
 *  /health answers anymore). Best-effort: false if it refused or didn't let go. */
async function requestProxyEviction(port: number): Promise<boolean> {
  try {
    const res = await fetch(`http://127.0.0.1:${port}/shutdown`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ buildEpoch: BUILD_EPOCH }),
      signal: AbortSignal.timeout(1500),
    });
    if (!res.ok) return false;
  } catch {
    return false;
  }
  // Poll until the old server has actually stopped answering (its setTimeout
  // stop + socket teardown take a beat).
  for (let i = 0; i < 20; i++) {
    await new Promise((r) => setTimeout(r, 100));
    if (!(await probeHealth(port)).ok) return true;
  }
  return false;
}

/** Build the request handler (shared by fresh server + reuse path). */
function makeFetchHandler() {
  return async (req: Request): Promise<Response> => {
    {
      const url = new URL(req.url);

      if (req.method === "GET" && url.pathname === "/health") {
        return new Response(
          JSON.stringify({ proxy: "cursor-oauth", models: proxyModels.length, buildEpoch: BUILD_EPOCH }),
          { headers: { "Content-Type": "application/json" } },
        );
      }

      // Control endpoint: a NEWER proxy asks this (older) one to release the port.
      // Authorized ONLY if the caller's buildEpoch is strictly greater than ours,
      // so a stale opencode window can't evict a fresh one, and randoms can't kill
      // the proxy. On success we stop our server; the caller then binds the port.
      if (req.method === "POST" && url.pathname === "/shutdown") {
        let callerEpoch = 0;
        try {
          const body = (await req.json().catch(() => null)) as { buildEpoch?: number } | null;
          callerEpoch = typeof body?.buildEpoch === "number" ? body.buildEpoch : 0;
        } catch { /* callerEpoch stays 0 */ }
        if (callerEpoch > BUILD_EPOCH) {
          console.error(
            `[cursor-oauth] evicted by newer build (${callerEpoch} > ${BUILD_EPOCH}); releasing port`,
          );
          // Defer stop so this response flushes first.
          setTimeout(() => stopProxy(), 50);
          return new Response(JSON.stringify({ ok: true, releasing: true }), {
            headers: { "Content-Type": "application/json" },
          });
        }
        return new Response(
          JSON.stringify({ ok: false, buildEpoch: BUILD_EPOCH, reason: "caller not newer" }),
          { status: 409, headers: { "Content-Type": "application/json" } },
        );
      }

      if (req.method === "GET" && url.pathname === "/v1/models") {
        return new Response(
          JSON.stringify({
            object: "list",
            data: buildOpenAIModelList(proxyModels),
          }),
          { headers: { "Content-Type": "application/json" } },
        );
      }

      if (req.method === "POST" && url.pathname === "/v1/chat/completions") {
        const reqStart = Date.now();
        try {
          const body = (await req.json()) as ChatCompletionRequest;
          if (!proxyAccessTokenProvider) {
            throw new Error("Cursor proxy access token provider not configured");
          }
          const accessToken = await proxyAccessTokenProvider();
          if (process.env.CURSOR_PROXY_DEBUG) {
            const sysLen = body.messages.filter((m) => m.role === "system")
              .reduce((n, m) => n + (typeof m.content === "string" ? m.content.length : 0), 0);
            console.error(
              `[proxy] REQ model=${body.model} stream=${body.stream} msgs=${body.messages.length} ` +
              `sys=${sysLen}ch tools=${body.tools?.length ?? 0} tokenWait=${Date.now() - reqStart}ms`,
            );
          }
          const resp = await handleChatCompletion(body, accessToken);
          if (process.env.CURSOR_PROXY_DEBUG) {
            console.error(`[proxy] RESP headers-sent in ${Date.now() - reqStart}ms (streaming continues after)`);
          }
          return resp;
        } catch (err) {
          const message = err instanceof Error ? err.message : String(err);
          return new Response(
            JSON.stringify({
              error: { message, type: "server_error", code: "internal_error" },
            }),
            { status: 500, headers: { "Content-Type": "application/json" } },
          );
        }
      }

      return new Response("Not Found", { status: 404 });
    }
  };
}

export function stopProxy(): void {
  const state = proxyGlobal();
  if (state.server) {
    state.server.stop();
    state.server = undefined;
    state.port = undefined;
    proxyAccessTokenProvider = undefined;
    proxyModels = [];
  }
  // Clean up any lingering bridges
  for (const active of activeBridges.values()) {
    clearInterval(active.heartbeatTimer);
    active.bridge.end();
  }
  activeBridges.clear();
  conversationStates.clear();
}

function handleChatCompletion(
  body: ChatCompletionRequest,
  accessToken: string,
): Response | Promise<Response> {
  const { systemPrompt, userText, turns, toolResults } = parseMessages(body.messages);
  const modelId = body.model;
  const tools = body.tools ?? [];

  if (!userText && toolResults.length === 0) {
    return new Response(
      JSON.stringify({
        error: {
          message: "No user message found",
          type: "invalid_request_error",
        },
      }),
      { status: 400, headers: { "Content-Type": "application/json" } },
    );
  }

  // bridgeKey: model-specific, for active tool-call bridges
  // convKey: model-independent, for conversation state that survives model switches
  const bridgeKey = deriveBridgeKey(modelId, body.messages);
  const convKey = deriveConversationKey(body.messages);
  const activeBridge = activeBridges.get(bridgeKey);

  if (activeBridge && toolResults.length > 0) {
    activeBridges.delete(bridgeKey);

    if (activeBridge.bridge.alive) {
      // Resume the live bridge with tool results
      return handleToolResultResume(activeBridge, toolResults, modelId, bridgeKey, convKey);
    }

    // Bridge died (timeout, server disconnect, etc.).
    // Clean up and fall through to start a fresh bridge.
    clearInterval(activeBridge.heartbeatTimer);
    activeBridge.bridge.end();
  }

  // Clean up stale bridge if present
  if (activeBridge && activeBridges.has(bridgeKey)) {
    clearInterval(activeBridge.heartbeatTimer);
    activeBridge.bridge.end();
    activeBridges.delete(bridgeKey);
  }

  const stored = getOrCreateStoredConversation(convKey);
  stored.lastAccessMs = Date.now();
  evictStaleConversations();

  // Build the request. When tool results are present but the bridge died,
  // we must still include the last user text so Cursor has context.
  const mcpTools = buildMcpToolDefinitions(tools);
  const effectiveUserText = userText || (toolResults.length > 0
    ? toolResults.map((r) => r.content).join("\n")
    : "");
  const payload = buildCursorRequest(
    modelId, systemPrompt, effectiveUserText, turns,
    stored.conversationId, stored.checkpoint, stored.blobStore,
  );
  payload.mcpTools = mcpTools;

  if (body.stream === false) {
    return handleNonStreamingResponse(payload, accessToken, modelId, convKey);
  }
  return handleStreamingResponse(payload, accessToken, modelId, bridgeKey, convKey);
}

interface ToolResultInfo {
  toolCallId: string;
  content: string;
}

interface ParsedMessages {
  systemPrompt: string;
  userText: string;
  turns: Array<{ userText: string; assistantText: string }>;
  toolResults: ToolResultInfo[];
}

/** Normalize OpenAI message content to a plain string. */
function textContent(content: OpenAIMessage["content"]): string {
  if (content == null) return "";
  if (typeof content === "string") return content;
  return content
    .filter((p) => p.type === "text" && p.text)
    .map((p) => p.text!)
    .join("\n");
}

function parseMessages(messages: OpenAIMessage[]): ParsedMessages {
  let systemPrompt = "You are a helpful assistant.";
  const pairs: Array<{ userText: string; assistantText: string }> = [];
  const toolResults: ToolResultInfo[] = [];

  // Collect system messages
  const systemParts = messages
    .filter((m) => m.role === "system")
    .map((m) => textContent(m.content));
  if (systemParts.length > 0) {
    systemPrompt = systemParts.join("\n");
  }

  // Separate tool results from conversation turns
  const nonSystem = messages.filter((m) => m.role !== "system");
  let pendingUser = "";

  for (const msg of nonSystem) {
    if (msg.role === "tool") {
      toolResults.push({
        toolCallId: msg.tool_call_id ?? "",
        content: textContent(msg.content),
      });
    } else if (msg.role === "user") {
      if (pendingUser) {
        pairs.push({ userText: pendingUser, assistantText: "" });
      }
      pendingUser = textContent(msg.content);
    } else if (msg.role === "assistant") {
      // Skip assistant messages that are just tool_calls with no text
      const text = textContent(msg.content);
      if (pendingUser) {
        pairs.push({ userText: pendingUser, assistantText: text });
        pendingUser = "";
      }
    }
  }

  let lastUserText = "";
  if (pendingUser) {
    lastUserText = pendingUser;
  } else if (pairs.length > 0 && toolResults.length === 0) {
    const last = pairs.pop()!;
    lastUserText = last.userText;
  }

  return { systemPrompt, userText: lastUserText, turns: pairs, toolResults };
}

/** Convert OpenAI tool definitions to Cursor's MCP tool protobuf format. */
function buildMcpToolDefinitions(tools: OpenAIToolDef[]): McpToolDefinition[] {
  return tools.map((t) => {
    const fn = t.function;
    const jsonSchema: JsonValue =
      fn.parameters && typeof fn.parameters === "object"
        ? (fn.parameters as JsonValue)
        : { type: "object", properties: {}, required: [] };
    const inputSchema = toBinary(ValueSchema, fromJson(ValueSchema, jsonSchema));
    return create(McpToolDefinitionSchema, {
      name: fn.name,
      description: fn.description || "",
      providerIdentifier: "opencode",
      toolName: fn.name,
      inputSchema,
    });
  });
}

/** Decode a Cursor MCP arg value (protobuf Value bytes) to a JS value. */
function decodeMcpArgValue(value: Uint8Array): unknown {
  try {
    const parsed = fromBinary(ValueSchema, value);
    return toJson(ValueSchema, parsed);
  } catch {}
  return new TextDecoder().decode(value);
}

/** Decode a map of MCP arg values. */
function decodeMcpArgsMap(args: Record<string, Uint8Array>): Record<string, unknown> {
  const decoded: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(args)) {
    decoded[key] = decodeMcpArgValue(value);
  }
  return decoded;
}

function buildCursorRequest(
  modelId: string,
  systemPrompt: string,
  userText: string,
  turns: Array<{ userText: string; assistantText: string }>,
  conversationId: string,
  checkpoint: Uint8Array | null,
  existingBlobStore?: Map<string, Uint8Array>,
): CursorRequestPayload {
  const blobStore = new Map<string, Uint8Array>(existingBlobStore ?? []);

  // System prompt → blob store (Cursor requests it back via KV handshake)
  const systemJson = JSON.stringify({ role: "system", content: systemPrompt });
  const systemBytes = new TextEncoder().encode(systemJson);
  const systemBlobId = storeBlob(blobStore, systemBytes);

  if (process.env.CURSOR_PROXY_DEBUG_BLOB) {
    console.error(
      `[blob] buildRequest: systemBlobId=${Buffer.from(systemBlobId).toString("hex").slice(0, 16)}` +
        ` checkpoint=${checkpoint ? "YES" : "no"} incomingStore=${(existingBlobStore?.size ?? 0)} sysPromptLen=${systemPrompt.length}`,
    );
  }

  let conversationState;
  if (checkpoint) {
    conversationState = fromBinary(ConversationStateStructureSchema, checkpoint);
  } else {
    const turnBytes: Uint8Array[] = [];
    for (const turn of turns) {
      const userMsg = create(UserMessageSchema, {
        text: turn.userText,
        messageId: crypto.randomUUID(),
      });
      const userMsgBytes = toBinary(UserMessageSchema, userMsg);
      const userMsgBlobId = storeBlob(blobStore, userMsgBytes);

      const stepBytes: Uint8Array[] = [];
      if (turn.assistantText) {
        const step = create(ConversationStepSchema, {
          message: {
            case: "assistantMessage",
            value: create(AssistantMessageSchema, { text: turn.assistantText }),
          },
        });
        stepBytes.push(storeBlob(blobStore, toBinary(ConversationStepSchema, step)));
      }

      const agentTurn = create(AgentConversationTurnStructureSchema, {
        userMessage: userMsgBlobId,
        steps: stepBytes,
      });
      const turnStructure = create(ConversationTurnStructureSchema, {
        turn: { case: "agentConversationTurn", value: agentTurn },
      });
      turnBytes.push(storeBlob(blobStore, toBinary(ConversationTurnStructureSchema, turnStructure)));
    }

    conversationState = create(ConversationStateStructureSchema, {
      rootPromptMessagesJson: [systemBlobId],
      turns: turnBytes,
      todos: [],
      pendingToolCalls: [],
      previousWorkspaceUris: [],
      fileStates: {},
      fileStatesV2: {},
      summaryArchives: [],
      turnTimings: [],
      subagentStates: {},
      selfSummaryCount: 0,
      readPaths: [],
    });
  }

  const userMessage = create(UserMessageSchema, {
    text: userText,
    messageId: crypto.randomUUID(),
  });
  const action = create(ConversationActionSchema, {
    action: {
      case: "userMessageAction",
      value: create(UserMessageActionSchema, { userMessage }),
    },
  });

  const modelDetails = create(ModelDetailsSchema, {
    modelId,
    displayModelId: modelId,
    displayName: modelId,
  });

  const runRequest = create(AgentRunRequestSchema, {
    conversationState,
    action,
    modelDetails,
    conversationId,
  });

  const clientMessage = create(AgentClientMessageSchema, {
    message: { case: "runRequest", value: runRequest },
  });

  return {
    requestBytes: toBinary(AgentClientMessageSchema, clientMessage),
    blobStore,
    mcpTools: [],
  };
}

function storeBlob(blobStore: Map<string, Uint8Array>, bytes: Uint8Array): Uint8Array {
  const blobId = new Uint8Array(createHash("sha256").update(bytes).digest());
  blobStore.set(Buffer.from(blobId).toString("hex"), bytes);
  return blobId;
}

/** @internal Test-only. */
export const buildCursorRequestForTest = buildCursorRequest;

function parseConnectEndStream(data: Uint8Array): Error | null {
  try {
    const payload = JSON.parse(new TextDecoder().decode(data));
    const error = payload?.error;
    if (error) {
      const code = error.code ?? "unknown";
      const message = error.message ?? "Unknown error";
      return new Error(`Connect error ${code}: ${message}`);
    }
    return null;
  } catch {
    return new Error("Failed to parse Connect end stream");
  }
}

function makeHeartbeatBytes(): Uint8Array {
  const heartbeat = create(AgentClientMessageSchema, {
    message: {
      case: "clientHeartbeat",
      value: create(ClientHeartbeatSchema, {}),
    },
  });
  return frameConnectMessage(toBinary(AgentClientMessageSchema, heartbeat));
}

/**
 * Create a stateful parser for Connect protocol frames.
 * Handles buffering partial data across chunks.
 */
function createConnectFrameParser(
  onMessage: (bytes: Uint8Array) => void,
  onEndStream: (bytes: Uint8Array) => void,
): (incoming: Buffer) => void {
  let pending = Buffer.alloc(0);
  return (incoming: Buffer) => {
    pending = Buffer.concat([pending, incoming]);
    while (pending.length >= 5) {
      const flags = pending[0]!;
      const msgLen = pending.readUInt32BE(1);
      if (pending.length < 5 + msgLen) break;
      const messageBytes = pending.subarray(5, 5 + msgLen);
      pending = pending.subarray(5 + msgLen);
      if (flags & CONNECT_END_STREAM_FLAG) {
        onEndStream(messageBytes);
      } else {
        onMessage(messageBytes);
      }
    }
  };
}

const THINKING_TAG_NAMES = ['think', 'thinking', 'reasoning', 'thought', 'think_intent'];
const MAX_THINKING_TAG_LEN = 16; // </think_intent> is 15 chars

/**
 * Strip thinking tags from streamed text, routing tagged content to reasoning.
 * Buffers partial tags across chunk boundaries.
 */
function createThinkingTagFilter(): {
  process(text: string): { content: string; reasoning: string };
  flush(): { content: string; reasoning: string };
} {
  let buffer = '';
  let inThinking = false;

  return {
    process(text: string) {
      const input = buffer + text;
      buffer = '';
      let content = '';
      let reasoning = '';
      let lastIdx = 0;

      const re = new RegExp(`<(/?)(?:${THINKING_TAG_NAMES.join('|')})\\s*>`, 'gi');
      let match: RegExpExecArray | null;
      while ((match = re.exec(input)) !== null) {
        const before = input.slice(lastIdx, match.index);
        if (inThinking) reasoning += before;
        else content += before;
        inThinking = match[1] !== '/';
        lastIdx = re.lastIndex;
      }

      const rest = input.slice(lastIdx);
      // Buffer a trailing '<' that could be the start of a thinking tag.
      const ltPos = rest.lastIndexOf('<');
      if (ltPos >= 0 && rest.length - ltPos < MAX_THINKING_TAG_LEN && /^<\/?[a-z_]*$/i.test(rest.slice(ltPos))) {
        buffer = rest.slice(ltPos);
        const before = rest.slice(0, ltPos);
        if (inThinking) reasoning += before;
        else content += before;
      } else {
        if (inThinking) reasoning += rest;
        else content += rest;
      }

      return { content, reasoning };
    },
    flush() {
      const b = buffer;
      buffer = '';
      if (!b) return { content: '', reasoning: '' };
      return inThinking ? { content: '', reasoning: b } : { content: b, reasoning: '' };
    },
  };
}

interface StreamState {
  toolCallIndex: number;
  pendingExecs: PendingExec[];
  outputTokens: number;
  totalTokens: number;
}

function computeUsage(state: StreamState) {
  const completion_tokens = state.outputTokens;
  const total_tokens = state.totalTokens || completion_tokens;
  const prompt_tokens = Math.max(0, total_tokens - completion_tokens);
  return { prompt_tokens, completion_tokens, total_tokens };
}

function processServerMessage(
  msg: AgentServerMessage,
  blobStore: Map<string, Uint8Array>,
  mcpTools: McpToolDefinition[],
  sendFrame: (data: Uint8Array) => void,
  state: StreamState,
  onText: (text: string, isThinking?: boolean) => void,
  onMcpExec: (exec: PendingExec) => void,
  onCheckpoint?: (checkpointBytes: Uint8Array) => void,
): void {
  const msgCase = msg.message.case;

  if (msgCase === "interactionUpdate") {
    handleInteractionUpdate(msg.message.value, state, onText);
  } else if (msgCase === "kvServerMessage") {
    handleKvMessage(msg.message.value as KvServerMessage, blobStore, sendFrame);
  } else if (msgCase === "execServerMessage") {
    handleExecMessage(
      msg.message.value as ExecServerMessage,
      mcpTools,
      sendFrame,
      onMcpExec,
    );
  } else if (msgCase === "conversationCheckpointUpdate") {
    const stateStructure = msg.message.value as ConversationStateStructure;
    if (stateStructure.tokenDetails) {
      state.totalTokens = stateStructure.tokenDetails.usedTokens;
    }
    if (onCheckpoint) {
      onCheckpoint(toBinary(ConversationStateStructureSchema, stateStructure));
    }
  }
}

function handleInteractionUpdate(
  update: any,
  state: StreamState,
  onText: (text: string, isThinking?: boolean) => void,
): void {
  const updateCase = update.message?.case;

  if (updateCase === "textDelta") {
    const delta = update.message.value.text || "";
    if (delta) onText(delta, false);
  } else if (updateCase === "thinkingDelta") {
    const delta = update.message.value.text || "";
    if (delta) onText(delta, true);
  } else if (updateCase === "tokenDelta") {
    state.outputTokens += update.message.value.tokens ?? 0;
  }
  // toolCallStarted, partialToolCall, toolCallDelta, toolCallCompleted
  // are intentionally ignored. MCP tool calls flow through the exec
  // message path (mcpArgs → mcpResult), not interaction updates.
}

/** Send a KV client response back to Cursor. */
function sendKvResponse(
  kvMsg: KvServerMessage,
  messageCase: string,
  value: unknown,
  sendFrame: (data: Uint8Array) => void,
): void {
  const response = create(KvClientMessageSchema, {
    id: kvMsg.id,
    message: { case: messageCase as any, value: value as any },
  });
  const clientMsg = create(AgentClientMessageSchema, {
    message: { case: "kvClientMessage", value: response },
  });
  sendFrame(frameConnectMessage(toBinary(AgentClientMessageSchema, clientMsg)));
}

function handleKvMessage(
  kvMsg: KvServerMessage,
  blobStore: Map<string, Uint8Array>,
  sendFrame: (data: Uint8Array) => void,
): void {
  const kvCase = kvMsg.message.case;

  if (kvCase === "getBlobArgs") {
    const blobId = kvMsg.message.value.blobId;
    const blobIdKey = Buffer.from(blobId).toString("hex");
    const blobData = blobStore.get(blobIdKey);
    if (process.env.CURSOR_PROXY_DEBUG_BLOB) {
      console.error(
        `[blob] getBlob ${blobIdKey.slice(0, 16)} → ${blobData ? `HIT (${blobData.length}b)` : "MISS"}` +
          ` | store has ${blobStore.size} blob(s): [${[...blobStore.keys()].map((k) => k.slice(0, 16)).join(", ")}]`,
      );
    }
    sendKvResponse(
      kvMsg, "getBlobResult",
      create(GetBlobResultSchema, blobData ? { blobData } : {}),
      sendFrame,
    );
  } else if (kvCase === "setBlobArgs") {
    const { blobId, blobData } = kvMsg.message.value;
    blobStore.set(Buffer.from(blobId).toString("hex"), blobData);
    if (process.env.CURSOR_PROXY_DEBUG_BLOB) {
      console.error(`[blob] setBlob ${Buffer.from(blobId).toString("hex").slice(0, 16)} (${blobData.length}b)`);
    }
    sendKvResponse(
      kvMsg, "setBlobResult",
      create(SetBlobResultSchema, {}),
      sendFrame,
    );
  }
}

function handleExecMessage(
  execMsg: ExecServerMessage,
  mcpTools: McpToolDefinition[],
  sendFrame: (data: Uint8Array) => void,
  onMcpExec: (exec: PendingExec) => void,
): void {
  const execCase = execMsg.message.case;

  if (execCase === "requestContextArgs") {
    const requestContext = create(RequestContextSchema, {
      rules: [],
      repositoryInfo: [],
      tools: mcpTools,
      gitRepos: [],
      projectLayouts: [],
      mcpInstructions: [],
      fileContents: {},
      customSubagents: [],
    });
    const result = create(RequestContextResultSchema, {
      result: {
        case: "success",
        value: create(RequestContextSuccessSchema, { requestContext }),
      },
    });
    sendExecResult(execMsg, "requestContextResult", result, sendFrame);
    return;
  }

  if (execCase === "mcpArgs") {
    const mcpArgs = execMsg.message.value;
    const decoded = decodeMcpArgsMap(mcpArgs.args ?? {});
    onMcpExec({
      execId: execMsg.execId,
      execMsgId: execMsg.id,
      toolCallId: mcpArgs.toolCallId || crypto.randomUUID(),
      toolName: mcpArgs.toolName || mcpArgs.name,
      decodedArgs: JSON.stringify(decoded),
    });
    return;
  }

  // --- Reject native Cursor tools ---
  // The model tries these first. We must respond with rejection/error
  // so it falls back to our MCP tools (registered via RequestContext).
  const REJECT_REASON = "Tool not available in this environment. Use the MCP tools provided instead.";

  if (execCase === "readArgs") {
    const args = execMsg.message.value;
    const result = create(ReadResultSchema, {
      result: { case: "rejected", value: create(ReadRejectedSchema, { path: args.path, reason: REJECT_REASON }) },
    });
    sendExecResult(execMsg, "readResult", result, sendFrame);
    return;
  }
  if (execCase === "lsArgs") {
    const args = execMsg.message.value;
    const result = create(LsResultSchema, {
      result: { case: "rejected", value: create(LsRejectedSchema, { path: args.path, reason: REJECT_REASON }) },
    });
    sendExecResult(execMsg, "lsResult", result, sendFrame);
    return;
  }
  if (execCase === "grepArgs") {
    const result = create(GrepResultSchema, {
      result: { case: "error", value: create(GrepErrorSchema, { error: REJECT_REASON }) },
    });
    sendExecResult(execMsg, "grepResult", result, sendFrame);
    return;
  }
  if (execCase === "writeArgs") {
    const args = execMsg.message.value;
    const result = create(WriteResultSchema, {
      result: { case: "rejected", value: create(WriteRejectedSchema, { path: args.path, reason: REJECT_REASON }) },
    });
    sendExecResult(execMsg, "writeResult", result, sendFrame);
    return;
  }
  if (execCase === "deleteArgs") {
    const args = execMsg.message.value;
    const result = create(DeleteResultSchema, {
      result: { case: "rejected", value: create(DeleteRejectedSchema, { path: args.path, reason: REJECT_REASON }) },
    });
    sendExecResult(execMsg, "deleteResult", result, sendFrame);
    return;
  }
  if (execCase === "shellArgs" || execCase === "shellStreamArgs") {
    const args = execMsg.message.value;
    const result = create(ShellResultSchema, {
      result: {
        case: "rejected",
        value: create(ShellRejectedSchema, {
          command: args.command ?? "",
          workingDirectory: args.workingDirectory ?? "",
          reason: REJECT_REASON,
          isReadonly: false,
        }),
      },
    });
    sendExecResult(execMsg, "shellResult", result, sendFrame);
    return;
  }
  if (execCase === "backgroundShellSpawnArgs") {
    const args = execMsg.message.value;
    const result = create(BackgroundShellSpawnResultSchema, {
      result: {
        case: "rejected",
        value: create(ShellRejectedSchema, {
          command: args.command ?? "",
          workingDirectory: args.workingDirectory ?? "",
          reason: REJECT_REASON,
          isReadonly: false,
        }),
      },
    });
    sendExecResult(execMsg, "backgroundShellSpawnResult", result, sendFrame);
    return;
  }
  if (execCase === "writeShellStdinArgs") {
    const result = create(WriteShellStdinResultSchema, {
      result: { case: "error", value: create(WriteShellStdinErrorSchema, { error: REJECT_REASON }) },
    });
    sendExecResult(execMsg, "writeShellStdinResult", result, sendFrame);
    return;
  }
  if (execCase === "fetchArgs") {
    const args = execMsg.message.value;
    const result = create(FetchResultSchema, {
      result: { case: "error", value: create(FetchErrorSchema, { url: args.url ?? "", error: REJECT_REASON }) },
    });
    sendExecResult(execMsg, "fetchResult", result, sendFrame);
    return;
  }
  if (execCase === "diagnosticsArgs") {
    const result = create(DiagnosticsResultSchema, {});
    sendExecResult(execMsg, "diagnosticsResult", result, sendFrame);
    return;
  }

  // MCP resource/screen/computer exec types
  const miscCaseMap: Record<string, string> = {
    listMcpResourcesExecArgs: "listMcpResourcesExecResult",
    readMcpResourceExecArgs: "readMcpResourceExecResult",
    recordScreenArgs: "recordScreenResult",
    computerUseArgs: "computerUseResult",
  };
  const resultCase = miscCaseMap[execCase as string];
  if (resultCase) {
    sendExecResult(execMsg, resultCase, create(McpResultSchema, {}), sendFrame);
    return;
  }

  // Unknown exec type — log and ignore
  console.error(`[proxy] unhandled exec: ${execCase}`);
}

/** Send an exec client message back to Cursor. */
function sendExecResult(
  execMsg: ExecServerMessage,
  messageCase: string,
  value: unknown,
  sendFrame: (data: Uint8Array) => void,
): void {
  const execClientMessage = create(ExecClientMessageSchema, {
    id: execMsg.id,
    execId: execMsg.execId,
    message: { case: messageCase as any, value: value as any },
  });
  const clientMessage = create(AgentClientMessageSchema, {
    message: { case: "execClientMessage", value: execClientMessage },
  });
  sendFrame(frameConnectMessage(toBinary(AgentClientMessageSchema, clientMessage)));
}

/** Derive a key for active bridge lookup (tool-call continuations). Model-specific. */
function deriveBridgeKey(modelId: string, messages: OpenAIMessage[]): string {
  const firstUserMsg = messages.find((m) => m.role === "user");
  const firstUserText = firstUserMsg ? textContent(firstUserMsg.content) : "";
  return createHash("sha256")
    .update(`bridge:${modelId}:${systemPromptFingerprint(messages)}:${firstUserText.slice(0, 200)}`)
    .digest("hex")
    .slice(0, 16);
}

/**
 * Fingerprint the system prompt so conversations with the SAME first user
 * message but DIFFERENT system prompts don't collide onto one server-side
 * Cursor conversation. The classic collision: opencode's title-generation
 * side-call replays the session's first user message under a tiny "Generate a
 * title" system prompt. Keyed on first-user-text alone, that shared the main
 * chat's conversation key — so the title request hit a server
 * conversation whose root-prompt blob it never sent, yielding
 * "Connect error internal: Blob not found". Within a real chat the system
 * prompt is stable across turns, so folding it in preserves context reuse.
 */
function systemPromptFingerprint(messages: OpenAIMessage[]): string {
  const systemPrompt = messages
    .filter((m) => m.role === "system")
    .map((m) => textContent(m.content))
    .join("\n");
  return createHash("sha256").update(systemPrompt).digest("hex").slice(0, 16);
}

/** Derive a key for conversation state. Model-independent so context survives
 *  model switches, but system-prompt-aware so a title-gen side-call (same first
 *  user message, different system prompt) gets its own conversation instead of
 *  colliding onto the chat's server-side state (which triggers "Blob not found").
 */
export function deriveConversationKey(messages: OpenAIMessage[]): string {
  const firstUserMsg = messages.find((m) => m.role === "user");
  const firstUserText = firstUserMsg ? textContent(firstUserMsg.content) : "";
  return createHash("sha256")
    .update(`conv:${systemPromptFingerprint(messages)}:${firstUserText.slice(0, 200)}`)
    .digest("hex")
    .slice(0, 16);
}

/** Create an SSE streaming Response that reads from a live bridge. */
function createBridgeStreamResponse(
  bridge: StreamBridge,
  heartbeatTimer: NodeJS.Timeout,
  blobStore: Map<string, Uint8Array>,
  mcpTools: McpToolDefinition[],
  modelId: string,
  bridgeKey: string,
  convKey: string,
): Response {
  const completionId = `chatcmpl-${crypto.randomUUID().replace(/-/g, "").slice(0, 28)}`;
  const created = Math.floor(Date.now() / 1000);
  let closed = false;
  let keepAliveTimer: ReturnType<typeof setInterval> | undefined;
  let stallTimer: ReturnType<typeof setTimeout> | undefined;

  const cleanupTimers = () => {
    clearInterval(heartbeatTimer);
    if (keepAliveTimer) clearInterval(keepAliveTimer);
    if (stallTimer) clearTimeout(stallTimer);
  };

  const stream = new ReadableStream({
    start(controller) {
      const encoder = new TextEncoder();
      const sendSSE = (data: object) => {
        if (closed) return;
        controller.enqueue(encoder.encode(`data: ${JSON.stringify(data)}\n\n`));
      };
      const sendDone = () => {
        if (closed) return;
        controller.enqueue(encoder.encode("data: [DONE]\n\n"));
      };
      const closeController = () => {
        if (closed) return;
        closed = true;
        controller.close();
      };

      // Bun's SSE connection to opencode has its own idle ceiling, separate
      // from the Cursor-facing heartbeat below — see SSE_KEEPALIVE_MS.
      keepAliveTimer = setInterval(() => {
        if (closed) return;
        try {
          controller.enqueue(encoder.encode(": keep-alive\n\n"));
        } catch {
          // Controller already erroring/closing; cancel() will clean up.
        }
      }, SSE_KEEPALIVE_MS);

      const makeChunk = (
        delta: Record<string, unknown>,
        finishReason: string | null = null,
      ) => ({
        id: completionId,
        object: "chat.completion.chunk",
        created,
        model: modelId,
        choices: [{ index: 0, delta, finish_reason: finishReason }],
      });

      const makeUsageChunk = () => {
        const { prompt_tokens, completion_tokens, total_tokens } = computeUsage(state);
        return {
          id: completionId,
          object: "chat.completion.chunk",
          created,
          model: modelId,
          choices: [],
          usage: { prompt_tokens, completion_tokens, total_tokens },
        };
      };

      const state: StreamState = {
        toolCallIndex: 0,
        pendingExecs: [],
        outputTokens: 0,
        totalTokens: 0,
      };
      const tagFilter = createThinkingTagFilter();

      let mcpExecReceived = false;

      // Cursor has gone silent for this turn: no content, no tool call, no
      // stop, nothing — for GENERATION_STALL_MS straight. Treat it exactly
      // like a Connect end-stream error (see the endStreamBytes handler
      // below): poison the stored conversation so the next attempt mints a
      // fresh one, kill the bridge, and surface a real error instead of
      // hanging until something outside this process notices.
      const onStall = () => {
        if (closed) return;
        poisonStoredConversation(convKey);
        const flushed = tagFilter.flush();
        if (flushed.reasoning) sendSSE(makeChunk({ reasoning_content: flushed.reasoning }));
        if (flushed.content) sendSSE(makeChunk({ content: flushed.content }));
        sendSSE(makeChunk({
          content: `\n[Error: Cursor produced no response for ${GENERATION_STALL_MS / 1000}s — treating the connection as dead]`,
        }));
        sendSSE(makeChunk({}, "stop"));
        sendSSE(makeUsageChunk());
        sendDone();
        closeController();
        cleanupTimers();
        bridge.kill();
        activeBridges.delete(bridgeKey);
      };
      const resetStallTimer = () => {
        if (stallTimer) clearTimeout(stallTimer);
        stallTimer = setTimeout(onStall, GENERATION_STALL_MS);
      };
      resetStallTimer();

      const processChunk = createConnectFrameParser(
        (messageBytes) => {
          if (closed) return;
          resetStallTimer();
          try {
            const serverMessage = fromBinary(
              AgentServerMessageSchema,
              messageBytes,
            );
            if (process.env.CURSOR_PROXY_DEBUG_STALL) {
              console.error(`[stall-probe] t=${Date.now()} case=${serverMessage.message.case} bytes=${messageBytes.length}`);
            }
            processServerMessage(
              serverMessage,
              blobStore,
              mcpTools,
              (data) => bridge.write(data),
              state,
              (text, isThinking) => {
                if (isThinking) {
                  sendSSE(makeChunk({ reasoning_content: text }));
                } else {
                  const { content, reasoning } = tagFilter.process(text);
                  if (reasoning) sendSSE(makeChunk({ reasoning_content: reasoning }));
                  if (content) sendSSE(makeChunk({ content }));
                }
              },
              // onMcpExec — the model wants to execute a tool.
              (exec) => {
                state.pendingExecs.push(exec);
                mcpExecReceived = true;

                const flushed = tagFilter.flush();
                if (flushed.reasoning) sendSSE(makeChunk({ reasoning_content: flushed.reasoning }));
                if (flushed.content) sendSSE(makeChunk({ content: flushed.content }));

                const toolCallIndex = state.toolCallIndex++;
                sendSSE(makeChunk({
                  tool_calls: [{
                    index: toolCallIndex,
                    id: exec.toolCallId,
                    type: "function",
                    function: {
                      name: exec.toolName,
                      arguments: exec.decodedArgs,
                    },
                  }],
                }));

                // Keep the bridge alive for tool result continuation.
                activeBridges.set(bridgeKey, {
                  bridge,
                  heartbeatTimer,
                  blobStore,
                  mcpTools,
                  pendingExecs: state.pendingExecs,
                });

                sendSSE(makeChunk({}, "tool_calls"));
                sendDone();
                closeController();
              },
              (checkpointBytes) => {
                const stored = conversationStates.get(convKey);
                if (stored) {
                  stored.checkpoint = checkpointBytes;
                  stored.lastAccessMs = Date.now();
                }
              },
            );
          } catch {
            // Skip unparseable messages
          }
        },
        (endStreamBytes) => {
          const endError = parseConnectEndStream(endStreamBytes);
          if (endError) {
            poisonStoredConversation(convKey);
            sendSSE(makeChunk({ content: `\n[Error: ${endError.message}]` }));
          }
        },
      );

      bridge.onData(processChunk);

      bridge.onClose((code) => {
        // If onStall() already tore this turn down (killed the bridge itself
        // to force this very callback), there's nothing left to do — timers
        // are already cleared and the SSE response is already closed.
        if (closed) return;
        cleanupTimers();
        const stored = conversationStates.get(convKey);
        if (stored) {
          for (const [k, v] of blobStore) stored.blobStore.set(k, v);
          stored.lastAccessMs = Date.now();
        }
        if (!mcpExecReceived) {
          const flushed = tagFilter.flush();
          if (flushed.reasoning) sendSSE(makeChunk({ reasoning_content: flushed.reasoning }));
          if (flushed.content) sendSSE(makeChunk({ content: flushed.content }));
          sendSSE(makeChunk({}, "stop"));
          sendSSE(makeUsageChunk());
          sendDone();
          closeController();
        } else if (code !== 0) {
          // Bridge died while tool calls are pending (timeout, crash, etc.).
          // Close the SSE stream so the client doesn't hang forever.
          sendSSE(makeChunk({ content: "\n[Error: bridge connection lost]" }));
          sendSSE(makeChunk({}, "stop"));
          sendSSE(makeUsageChunk());
          sendDone();
          closeController();
          // Remove stale entry so the next request doesn't try to resume it.
          activeBridges.delete(bridgeKey);
        }
      });
    },
    cancel() {
      // The client (opencode) disconnected, or Bun's idleTimeout severed the
      // connection, before we reached a clean terminal state — `closed` is
      // only true here if neither the tool_calls path nor the stop/error path
      // ran first (both set it before this could matter). Whatever Cursor was
      // doing for this turn is now unobservable to us: stop leaking the
      // bridge + timers, and don't let the next attempt trust local state
      // Cursor's server may have already moved past.
      if (closed) return;
      closed = true;
      cleanupTimers();
      bridge.kill();
      poisonStoredConversation(convKey);
    },
  });

  return new Response(stream, { headers: SSE_HEADERS });
}

/** @internal Test-only. */
export const createBridgeStreamResponseForTest = createBridgeStreamResponse;

/** Spawn a bridge, send the initial request frame, and start heartbeat. */
function startBridge(
  accessToken: string,
  requestBytes: Uint8Array,
): { bridge: ReturnType<typeof spawnBridge>; heartbeatTimer: NodeJS.Timeout } {
  const bridge = spawnBridge({
    accessToken,
    rpcPath: "/agent.v1.AgentService/Run",
  });
  bridge.write(frameConnectMessage(requestBytes));
  const heartbeatTimer = setInterval(() => bridge.write(makeHeartbeatBytes()), 5_000);
  return { bridge, heartbeatTimer };
}

function handleStreamingResponse(
  payload: CursorRequestPayload,
  accessToken: string,
  modelId: string,
  bridgeKey: string,
  convKey: string,
): Response {
  const { bridge, heartbeatTimer } = startBridge(accessToken, payload.requestBytes);
  return createBridgeStreamResponse(
    bridge, heartbeatTimer,
    payload.blobStore, payload.mcpTools,
    modelId, bridgeKey, convKey,
  );
}

/** Resume a paused bridge by sending MCP results and continuing to stream. */
function handleToolResultResume(
  active: ActiveBridge,
  toolResults: ToolResultInfo[],
  modelId: string,
  bridgeKey: string,
  convKey: string,
): Response {
  const { bridge, heartbeatTimer, blobStore, mcpTools, pendingExecs } = active;

  // Send mcpResult for each pending exec that has a matching tool result
  for (const exec of pendingExecs) {
    const result = toolResults.find(
      (r) => r.toolCallId === exec.toolCallId,
    );
    const mcpResult = result
      ? create(McpResultSchema, {
          result: {
            case: "success",
            value: create(McpSuccessSchema, {
              content: [
                create(McpToolResultContentItemSchema, {
                  content: {
                    case: "text",
                    value: create(McpTextContentSchema, { text: result.content }),
                  },
                }),
              ],
              isError: false,
            }),
          },
        })
      : create(McpResultSchema, {
          result: {
            case: "error",
            value: create(McpErrorSchema, { error: "Tool result not provided" }),
          },
        });

    const execClientMessage = create(ExecClientMessageSchema, {
      id: exec.execMsgId,
      execId: exec.execId,
      message: {
        case: "mcpResult" as any,
        value: mcpResult as any,
      },
    });

    const clientMessage = create(AgentClientMessageSchema, {
      message: { case: "execClientMessage", value: execClientMessage },
    });

    bridge.write(
      frameConnectMessage(toBinary(AgentClientMessageSchema, clientMessage)),
    );
  }

  return createBridgeStreamResponse(
    bridge, heartbeatTimer,
    blobStore, mcpTools,
    modelId, bridgeKey, convKey,
  );
}

async function handleNonStreamingResponse(
  payload: CursorRequestPayload,
  accessToken: string,
  modelId: string,
  convKey: string,
): Promise<Response> {
  const completionId = `chatcmpl-${crypto.randomUUID().replace(/-/g, "").slice(0, 28)}`;
  const created = Math.floor(Date.now() / 1000);
  const { text, usage } = await collectFullResponse(payload, accessToken, convKey);

  return new Response(
    JSON.stringify({
      id: completionId,
      object: "chat.completion",
      created,
      model: modelId,
      choices: [
        {
          index: 0,
          message: { role: "assistant", content: text },
          finish_reason: "stop",
        },
      ],
      usage,
    }),
    { headers: { "Content-Type": "application/json" } },
  );
}

interface CollectedResponse {
  text: string;
  usage: { prompt_tokens: number; completion_tokens: number; total_tokens: number };
}

async function collectFullResponse(
  payload: CursorRequestPayload,
  accessToken: string,
  convKey: string,
  bridgeOverride?: { bridge: StreamBridge; heartbeatTimer: NodeJS.Timeout },
): Promise<CollectedResponse> {
  const { promise, resolve } = Promise.withResolvers<CollectedResponse>();
  let fullText = "";

  const { bridge, heartbeatTimer } = bridgeOverride ?? startBridge(accessToken, payload.requestBytes);

  const state: StreamState = {
    toolCallIndex: 0,
    pendingExecs: [],
    outputTokens: 0,
    totalTokens: 0,
  };
  const tagFilter = createThinkingTagFilter();
  let endStreamError: Error | null = null;
  let stalled = false;
  let stallTimer: ReturnType<typeof setTimeout> | undefined;

  // See the matching comment in createBridgeStreamResponse / the
  // GENERATION_STALL_MS definition above: this covers the non-streaming path
  // (title generation, model discovery) with the same fix — reset only by a
  // genuine parsed AgentServerMessage, never by our own outbound heartbeat.
  const resetStallTimer = () => {
    if (stallTimer) clearTimeout(stallTimer);
    stallTimer = setTimeout(() => {
      stalled = true;
      poisonStoredConversation(convKey);
      bridge.kill();
      const flushed = tagFilter.flush();
      fullText += flushed.content;
      if (!fullText) {
        fullText = `[Error: Cursor produced no response for ${GENERATION_STALL_MS / 1000}s — treating the connection as dead]`;
      }
      resolve({ text: fullText, usage: computeUsage(state) });
    }, GENERATION_STALL_MS);
  };
  resetStallTimer();

  bridge.onData(createConnectFrameParser(
    (messageBytes) => {
      if (stalled) return;
      resetStallTimer();
      try {
        const serverMessage = fromBinary(
          AgentServerMessageSchema,
          messageBytes,
        );
        if (process.env.CURSOR_PROXY_DEBUG_STALL) {
          console.error(`[stall-probe] t=${Date.now()} case=${serverMessage.message.case} bytes=${messageBytes.length}`);
        }
        processServerMessage(
          serverMessage,
          payload.blobStore,
          payload.mcpTools,
          (data) => bridge.write(data),
          state,
          (text, isThinking) => {
            if (isThinking) return;
            const { content } = tagFilter.process(text);
            fullText += content;
          },
          () => {},
          (checkpointBytes) => {
            const stored = conversationStates.get(convKey);
            if (stored) {
              stored.checkpoint = checkpointBytes;
              stored.lastAccessMs = Date.now();
            }
          },
        );
      } catch {
        // Skip
      }
    },
    (endStreamBytes) => {
      endStreamError = parseConnectEndStream(endStreamBytes);
    },
  ));

  bridge.onClose(() => {
    // onStall's own timeout already resolved the promise and killed the
    // bridge itself (to force this callback) — nothing left to do.
    if (stalled) return;
    if (stallTimer) clearTimeout(stallTimer);
    clearInterval(heartbeatTimer);
    const flushed = tagFilter.flush();
    fullText += flushed.content;

    // Surface a real upstream error instead of silently returning an empty
    // body (which would otherwise become a blank title / empty completion).
    if (endStreamError) {
      // Cursor rejected this conversation's state (e.g. "Blob not found").
      // Poison it so the next turn mints a fresh conversationId instead of
      // repeating the same mismatch forever.
      poisonStoredConversation(convKey);
      if (!fullText) {
        if (process.env.CURSOR_PROXY_DEBUG_BLOB) {
          console.error(`[blob] end-stream error became content: ${endStreamError.message}`);
        }
        fullText = `[Error: ${endStreamError.message}]`;
      }
    } else {
      const stored = conversationStates.get(convKey);
      if (stored) {
        for (const [k, v] of payload.blobStore) stored.blobStore.set(k, v);
        stored.lastAccessMs = Date.now();
      }
    }

    const usage = computeUsage(state);
    resolve({
      text: fullText,
      usage,
    });
  });

  return promise;
}

/** @internal Test-only. */
export const collectFullResponseForTest = collectFullResponse;
