import http from "node:http";
import http2 from "node:http2";
import type { AddressInfo } from "node:net";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import type { StreamBridge } from "../src/proxy";
import {
  AgentClientMessageSchema,
  AgentServerMessageSchema,
  ConversationStepSchema,
  ConversationTurnStructureSchema,
  UserMessageSchema,
  GetUsableModelsResponseSchema,
  ModelDetailsSchema,
} from "../src/proto/agent_pb";

type DiscoveryMode = "success" | "empty" | "auth-error" | "transient-empty";

interface TestModules {
  startProxy: typeof import("../src/proxy").startProxy;
  stopProxy: typeof import("../src/proxy").stopProxy;
  getProxyPort: typeof import("../src/proxy").getProxyPort;
  deriveConversationKey: typeof import("../src/proxy").deriveConversationKey;
  getStoredConversationIdForTest: typeof import("../src/proxy").getStoredConversationIdForTest;
  evictStoredConversationForTest: typeof import("../src/proxy").evictStoredConversationForTest;
  buildCursorRequestForTest: typeof import("../src/proxy").buildCursorRequestForTest;
  createBridgeStreamResponseForTest: typeof import("../src/proxy").createBridgeStreamResponseForTest;
  collectFullResponseForTest: typeof import("../src/proxy").collectFullResponseForTest;
  setSseKeepAliveMsForTest: typeof import("../src/proxy").setSseKeepAliveMsForTest;
  setGenerationStallMsForTest: typeof import("../src/proxy").setGenerationStallMsForTest;
  CONNECT_END_STREAM_FLAG: typeof import("../src/proxy").CONNECT_END_STREAM_FLAG;
  generateCursorAuthParams: typeof import("../src/auth").generateCursorAuthParams;
  getTokenExpiry: typeof import("../src/auth").getTokenExpiry;
  CursorAuthPlugin: typeof import("../src/index").CursorAuthPlugin;
  getCursorModels: typeof import("../src/models").getCursorModels;
  clearModelCache: typeof import("../src/models").clearModelCache;
  buildProviderBlock: typeof import("../src/standalone").buildProviderBlock;
}

interface TestCursorBackend {
  apiUrl: string;
  refreshUrl: string;
  setDiscoveryMode: (mode: DiscoveryMode) => void;
  setDiscoveredModels: (models: Array<{ id: string; name: string; reasoning?: boolean }>) => void;
  resetObservations: () => void;
  getDiscoveryAuthHeaders: () => string[];
  getDiscoveryRequestBodies: () => Uint8Array[];
  getRefreshAuthHeaders: () => string[];
  close: () => Promise<void>;
}

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

function assertEqual<T>(actual: T, expected: T, message: string): void {
  if (actual !== expected) {
    throw new Error(`${message}: expected ${String(expected)}, got ${String(actual)}`);
  }
}

function assertArrayEqual(
  actual: readonly string[],
  expected: readonly string[],
  message: string,
): void {
  const actualJson = JSON.stringify(actual);
  const expectedJson = JSON.stringify(expected);
  if (actualJson !== expectedJson) {
    throw new Error(`${message}: expected ${expectedJson}, got ${actualJson}`);
  }
}

function makeJwt(expiresAtSeconds: number): string {
  const header = btoa(JSON.stringify({ alg: "HS256", typ: "JWT" }));
  const payload = btoa(JSON.stringify({ exp: expiresAtSeconds }));
  return `${header}.${payload}.fakesig`;
}

function frameConnectUnaryMessage(payload: Uint8Array): Buffer {
  const frame = Buffer.alloc(5 + payload.length);
  frame[0] = 0;
  frame.writeUInt32BE(payload.length, 1);
  frame.set(payload, 5);
  return frame;
}

async function createTestCursorBackend(): Promise<TestCursorBackend> {
  let discoveryMode: DiscoveryMode = "success";
  let transientFailuresRemaining = 0;
  let discoveredModels: Array<{ id: string; name: string; reasoning?: boolean }> = [
    { id: "composer-2", name: "Composer 2", reasoning: true },
  ];
  const discoveryAuthHeaders: string[] = [];
  const discoveryRequestBodies: Uint8Array[] = [];
  const refreshAuthHeaders: string[] = [];

  const refreshServer = http.createServer((req, res) => {
    if (req.method !== "POST" || req.url !== "/auth/exchange_user_api_key") {
      res.writeHead(404);
      res.end("not found");
      return;
    }

    const authHeader = req.headers.authorization ?? "";
    refreshAuthHeaders.push(authHeader);

    if (authHeader !== "Bearer valid-refresh") {
      res.writeHead(401, { "Content-Type": "text/plain" });
      res.end("bad refresh token");
      return;
    }

    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(
      JSON.stringify({
        accessToken: makeJwt(Math.floor(Date.now() / 1000) + 3600),
        refreshToken: "valid-refresh",
      }),
    );
  });
  await new Promise<void>((resolve) => refreshServer.listen(0, "127.0.0.1", resolve));
  const refreshPort = (refreshServer.address() as AddressInfo).port;

  const apiServer = http2.createServer();
  apiServer.on("stream", (stream, headers) => {
    const path = String(headers[":path"] ?? "");
    const authHeader = String(headers.authorization ?? "");
    if (path === "/agent.v1.AgentService/Run") {
      stream.respond({
        ":status": 200,
        "content-type": "application/connect+proto",
      });
      stream.end();
      return;
    }

    const chunks: Buffer[] = [];

    stream.on("data", (chunk) => {
      chunks.push(Buffer.from(chunk));
    });
    stream.on("end", () => {
      if (path === "/agent.v1.AgentService/GetUsableModels") {
        discoveryAuthHeaders.push(authHeader);
        discoveryRequestBodies.push(new Uint8Array(Buffer.concat(chunks)));

        if (discoveryMode === "auth-error") {
          stream.respond({
            ":status": 401,
            "content-type": "application/json",
          });
          stream.end(
            JSON.stringify({ code: "unauthenticated", message: "expired token" }),
          );
          return;
        }

        const shouldReturnEmpty = discoveryMode === "empty" || transientFailuresRemaining > 0;
        if (transientFailuresRemaining > 0) transientFailuresRemaining -= 1;
        const responseBody = shouldReturnEmpty
          ? frameConnectUnaryMessage(new Uint8Array())
          : frameConnectUnaryMessage(
              toBinary(
                GetUsableModelsResponseSchema,
                create(GetUsableModelsResponseSchema, {
                  models: discoveredModels.map((model) =>
                    create(ModelDetailsSchema, {
                      modelId: model.id,
                      displayModelId: model.id,
                      displayName: model.name,
                      displayNameShort: model.name,
                      aliases: [],
                    }),
                  ),
                }),
              ),
            );
        stream.respond({
          ":status": 200,
          "content-type": "application/connect+proto",
        });
        stream.end(responseBody);
        return;
      }

      stream.respond({ ":status": 404 });
      stream.end();
    });
  });
  await new Promise<void>((resolve) => apiServer.listen(0, "127.0.0.1", resolve));
  const apiPort = (apiServer.address() as AddressInfo).port;

  return {
    apiUrl: `http://127.0.0.1:${apiPort}`,
    refreshUrl: `http://127.0.0.1:${refreshPort}/auth/exchange_user_api_key`,
    setDiscoveryMode(mode) {
      discoveryMode = mode;
      transientFailuresRemaining = mode === "transient-empty" ? 1 : 0;
    },
    setDiscoveredModels(models) {
      discoveredModels = models;
    },
    resetObservations() {
      discoveryAuthHeaders.length = 0;
      discoveryRequestBodies.length = 0;
      refreshAuthHeaders.length = 0;
    },
    getDiscoveryAuthHeaders() {
      return [...discoveryAuthHeaders];
    },
    getDiscoveryRequestBodies() {
      return discoveryRequestBodies.map((body) => new Uint8Array(body));
    },
    getRefreshAuthHeaders() {
      return [...refreshAuthHeaders];
    },
    async close() {
      await Promise.all([
        new Promise<void>((resolve, reject) =>
          apiServer.close((error) => (error ? reject(error) : resolve())),
        ),
        new Promise<void>((resolve, reject) =>
          refreshServer.close((error) => (error ? reject(error) : resolve())),
        ),
      ]);
    },
  };
}

async function loadModules(): Promise<TestModules> {
  const proxy = await import("../src/proxy");
  const auth = await import("../src/auth");
  const index = await import("../src/index");
  const models = await import("../src/models");
  const standalone = await import("../src/standalone");
  return {
    startProxy: proxy.startProxy,
    stopProxy: proxy.stopProxy,
    getProxyPort: proxy.getProxyPort,
    deriveConversationKey: proxy.deriveConversationKey,
    getStoredConversationIdForTest: proxy.getStoredConversationIdForTest,
    evictStoredConversationForTest: proxy.evictStoredConversationForTest,
    buildCursorRequestForTest: proxy.buildCursorRequestForTest,
    createBridgeStreamResponseForTest: proxy.createBridgeStreamResponseForTest,
    collectFullResponseForTest: proxy.collectFullResponseForTest,
    setSseKeepAliveMsForTest: proxy.setSseKeepAliveMsForTest,
    setGenerationStallMsForTest: proxy.setGenerationStallMsForTest,
    CONNECT_END_STREAM_FLAG: proxy.CONNECT_END_STREAM_FLAG,
    generateCursorAuthParams: auth.generateCursorAuthParams,
    getTokenExpiry: auth.getTokenExpiry,
    CursorAuthPlugin: index.CursorAuthPlugin,
    getCursorModels: models.getCursorModels,
    clearModelCache: models.clearModelCache,
    buildProviderBlock: standalone.buildProviderBlock,
  };
}

function decodePriorTurnFromBlobStore(
  modules: TestModules,
  turns: Array<{ userText: string; assistantText: string }>,
): { stateTurns: number; userText: string; assistantText: string; storeSize: number } {
  const payload = modules.buildCursorRequestForTest(
    "composer-2.5",
    "system prompt",
    "current user",
    turns,
    "11111111-1111-4111-8111-111111111111",
    null,
  );
  const client = fromBinary(AgentClientMessageSchema, payload.requestBytes);
  assert(client.message.case === "runRequest", "Expected runRequest client message");
  const state = client.message.value.conversationState;
  assert(state, "Expected conversation state");
  assertEqual(state.turns.length, turns.length, "Expected serialized prior turn count");
  const turnId = state.turns[0];
  assert(turnId, "Expected first prior-turn blob id");
  const turnBlob = payload.blobStore.get(Buffer.from(turnId).toString("hex"));
  assert(turnBlob, "Expected prior turn blob to be stored for KV getBlob");
  const turn = fromBinary(ConversationTurnStructureSchema, turnBlob);
  assert(turn.turn.case === "agentConversationTurn", "Expected agent conversation turn");
  const agentTurn = turn.turn.value;
  const userBlob = payload.blobStore.get(Buffer.from(agentTurn.userMessage).toString("hex"));
  assert(userBlob, "Expected nested prior user-message blob to be stored");
  const user = fromBinary(UserMessageSchema, userBlob);
  let assistantText = "";
  const stepId = agentTurn.steps[0];
  if (stepId) {
    const stepBlob = payload.blobStore.get(Buffer.from(stepId).toString("hex"));
    assert(stepBlob, "Expected nested prior assistant-step blob to be stored");
    const step = fromBinary(ConversationStepSchema, stepBlob);
    assert(step.message.case === "assistantMessage", "Expected assistant-message step");
    assistantText = step.message.value.text;
  }
  return { stateTurns: state.turns.length, userText: user.text, assistantText, storeSize: payload.blobStore.size };
}

async function testProxyStartStop(modules: TestModules) {
  console.log("[test] Starting proxy...");
  const port = await modules.startProxy(async () => "test-token");
  console.log(`[test] Proxy started on port ${port}`);

  if (port < 1) {
    throw new Error(`Expected a valid port number, got ${port}`);
  }
  if (modules.getProxyPort() !== port) {
    throw new Error("getProxyPort() mismatch");
  }

  const modelsRes = await fetch(`http://localhost:${port}/v1/models`);
  if (!modelsRes.ok) {
    throw new Error(`/v1/models returned ${modelsRes.status}`);
  }
  const modelsBody = await modelsRes.json();
  if (modelsBody.object !== "list") {
    throw new Error(`Expected object=list, got ${modelsBody.object}`);
  }
  if (!Array.isArray(modelsBody.data) || modelsBody.data.length !== 0) {
    throw new Error(`Expected empty model list data array, got ${JSON.stringify(modelsBody.data)}`);
  }
  console.log("[test] /v1/models OK");

  const badRes = await fetch(`http://localhost:${port}/v1/chat/completions`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ model: "test", messages: [] }),
  });
  if (badRes.status !== 400) {
    throw new Error(`Expected 400 for missing user message, got ${badRes.status}`);
  }
  const badBody = await badRes.json();
  if (!badBody.error?.message?.includes("No user message")) {
    throw new Error(`Expected 'No user message' error, got: ${badBody.error?.message}`);
  }
  console.log("[test] Missing user message validation OK");

  const notFoundRes = await fetch(`http://localhost:${port}/unknown`);
  if (notFoundRes.status !== 404) {
    throw new Error(`Expected 404, got ${notFoundRes.status}`);
  }
  console.log("[test] 404 handling OK");

  modules.stopProxy();
  if (modules.getProxyPort() !== undefined) {
    throw new Error("Proxy port should be undefined after stop");
  }
  console.log("[test] Proxy stop OK");
}

async function testAuthParams(modules: TestModules) {
  console.log("[test] Generating auth params...");
  const params = await modules.generateCursorAuthParams();

  if (!params.verifier || !params.challenge || !params.uuid || !params.loginUrl) {
    throw new Error("Missing auth params");
  }
  if (!params.loginUrl.includes("cursor.com/loginDeepControl")) {
    throw new Error(`Unexpected login URL: ${params.loginUrl}`);
  }
  if (!params.loginUrl.includes(params.uuid)) {
    throw new Error("Login URL missing UUID");
  }

  const data = new TextEncoder().encode(params.verifier);
  const hashBuffer = await crypto.subtle.digest("SHA-256", data);
  const expectedChallenge = Buffer.from(hashBuffer).toString("base64url");
  if (params.challenge !== expectedChallenge) {
    throw new Error(
      `PKCE challenge mismatch: expected ${expectedChallenge}, got ${params.challenge}`,
    );
  }

  console.log("[test] Auth params OK");
}

async function testTokenExpiry(modules: TestModules) {
  console.log("[test] Testing token expiry parsing...");

  const futureExp = Math.floor(Date.now() / 1000) + 7200;
  const fakeJwt = makeJwt(futureExp);

  const expiry = modules.getTokenExpiry(fakeJwt);
  const expectedMin = futureExp * 1000 - 5 * 60 * 1000 - 1000;
  const expectedMax = futureExp * 1000 - 5 * 60 * 1000 + 1000;

  if (expiry < expectedMin || expiry > expectedMax) {
    throw new Error(`Token expiry ${expiry} out of expected range [${expectedMin}, ${expectedMax}]`);
  }

  const fallbackExpiry = modules.getTokenExpiry("not-a-jwt");
  const now = Date.now();
  const expectedFallback = now + 3600 * 1000;
  if (Math.abs(fallbackExpiry - expectedFallback) > 5000) {
    throw new Error(
      `Fallback expiry off by ${Math.abs(fallbackExpiry - expectedFallback)}ms, expected ~1h from now`,
    );
  }

  console.log("[test] Token expiry OK");
}

async function testPluginShape(modules: TestModules) {
  console.log("[test] Checking plugin export shape...");

  const fakeInput = {
    client: { auth: { set: async () => {} } },
  } as any;
  const hooks = await modules.CursorAuthPlugin(fakeInput);

  if (!hooks.auth) {
    throw new Error("Plugin hooks missing 'auth'");
  }
  if (hooks.auth.provider !== "cursor") {
    throw new Error(`Expected provider 'cursor', got '${hooks.auth.provider}'`);
  }
  if (typeof hooks.auth.loader !== "function") {
    throw new Error("Plugin hooks.auth.loader is not a function");
  }
  if (!Array.isArray(hooks.auth.methods) || hooks.auth.methods.length === 0) {
    throw new Error("Plugin hooks.auth.methods missing or empty");
  }
  if (hooks.auth.methods[0].type !== "oauth") {
    throw new Error(`Expected method type 'oauth', got '${hooks.auth.methods[0].type}'`);
  }
  if (typeof hooks.auth.methods[0].authorize !== "function") {
    throw new Error("Plugin auth method missing authorize function");
  }

  console.log("[test] Plugin shape OK");
}

async function testArrayContentParsing(modules: TestModules) {
  console.log("[test] Testing array content (plan-mode) parsing...");
  const port = await modules.startProxy(async () => "test-token");

  const res = await fetch(`http://localhost:${port}/v1/chat/completions`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      model: "test",
      stream: false,
      messages: [
        {
          role: "system",
          content: [
            { type: "text", text: "You are a helpful assistant." },
            { type: "text", text: "Plan mode is active." },
          ],
        },
        {
          role: "user",
          content: [
            { type: "text", text: "lazy-load recharts" },
            { type: "text", text: "work on a plan" },
          ],
        },
      ],
    }),
  });

  if (res.status === 400) {
    const body = await res.json();
    if (body.error?.message?.includes("No user message")) {
      throw new Error(
        "Array content not normalized — plan mode messages lost",
      );
    }
  }

  modules.stopProxy();
  console.log("[test] Array content parsing OK");
}

async function testExpiredTokenRefreshBeforeDiscovery(
  modules: TestModules,
  backend: TestCursorBackend,
) {
  console.log("[test] Testing refresh-before-discovery...");
  modules.clearModelCache();
  backend.resetObservations();
  backend.setDiscoveryMode("success");
  backend.setDiscoveredModels([
    { id: "fresh-model", name: "Fresh Model", reasoning: true },
  ]);

  let authState = {
    type: "oauth" as const,
    access: "expired-access",
    refresh: "valid-refresh",
    expires: Date.now() - 10_000,
  };
  const writes: Array<{ access: string; refresh: string; expires: number }> = [];
  const hooks = await modules.CursorAuthPlugin({
    client: {
      auth: {
        set: async ({ body }: any) => {
          writes.push(body);
          authState = body;
        },
      },
    },
  } as any);
  const provider = { models: {} as Record<string, unknown> } as any;

  await hooks.auth!.loader(async () => authState, provider);

  assertEqual(writes.length, 1, "Expected refreshed auth to be persisted once");
  assert(
    writes[0]?.access && writes[0].access !== "expired-access",
    "Expected refreshed access token to replace the expired token",
  );
  assertArrayEqual(
    backend.getRefreshAuthHeaders(),
    ["Bearer valid-refresh"],
    "Expected refresh endpoint to be called with the stored refresh token",
  );
  assert(
    backend.getDiscoveryAuthHeaders().every((header) => header === `Bearer ${writes[0]?.access}`),
    `Expected discovery to use the refreshed token, got ${JSON.stringify(backend.getDiscoveryAuthHeaders())}`,
  );
  assertArrayEqual(
    Object.keys(provider.models),
    ["fresh-model"],
    "Expected provider models to come from successful discovery",
  );

  modules.stopProxy();
  console.log("[test] Refresh-before-discovery OK");
}

async function testDiscoveryFallbackAndSuccess(
  modules: TestModules,
  backend: TestCursorBackend,
) {
  console.log("[test] Testing discovery fallback and success...");

  const authState = {
    type: "oauth" as const,
    access: makeJwt(Math.floor(Date.now() / 1000) + 3600),
    refresh: "valid-refresh",
    expires: Date.now() + 3_600_000,
  };
  const hooks = await modules.CursorAuthPlugin({
    client: {
      auth: {
        set: async () => {},
      },
    },
  } as any);
  const provider = { models: { stale: { id: "stale" } } } as any;

  // Failed discovery should fall back to hardcoded models
  modules.clearModelCache();
  backend.setDiscoveryMode("empty");
  const degradedConfig = await hooks.auth!.loader(async () => authState, provider);
  assert(
    Object.keys(provider.models).length > 0,
    "Expected fallback models to be registered when discovery fails",
  );
  assert(
    !("stale" in provider.models),
    "Expected stale models to be replaced",
  );
  const degradedModelsRes = await fetch(`${degradedConfig.baseURL}/models`);
  assertEqual(degradedModelsRes.status, 200, "Expected degraded /v1/models to succeed");
  const degradedModelsBody = await degradedModelsRes.json();
  assert(
    degradedModelsBody.data.length > 0,
    "Expected proxy /v1/models to expose fallback models",
  );

  // Successful discovery should replace with real models
  modules.clearModelCache();
  backend.setDiscoveryMode("success");
  backend.setDiscoveredModels([
    { id: "real-model-a", name: "Real Model A" },
    { id: "real-model-b", name: "Real Model B", reasoning: true },
  ]);
  const discoveredConfig = await hooks.auth!.loader(async () => authState, provider);
  assertArrayEqual(
    Object.keys(provider.models).sort(),
    ["real-model-a", "real-model-b"],
    "Expected successful discovery to replace fallback models",
  );
  const discoveredModelsRes = await fetch(`${discoveredConfig.baseURL}/models`);
  assertEqual(discoveredModelsRes.status, 200, "Expected discovered /v1/models to succeed");
  const discoveredModelsBody = await discoveredModelsRes.json();
  assertArrayEqual(
    discoveredModelsBody.data.map((model: { id: string }) => model.id).sort(),
    ["real-model-a", "real-model-b"],
    "Expected proxy /v1/models to expose discovered models",
  );

  modules.stopProxy();
  console.log("[test] Discovery fallback and success OK");
}

async function testDiscoveryRetriesTransientFailure(
  modules: TestModules,
  backend: TestCursorBackend,
) {
  console.log("[test] Testing discovery retry after transient failure...");
  modules.clearModelCache();
  backend.resetObservations();
  backend.setDiscoveryMode("transient-empty");
  backend.setDiscoveredModels([
    { id: "composer-2.5", name: "Composer 2.5", reasoning: true },
  ]);

  const models = await modules.getCursorModels(makeJwt(Math.floor(Date.now() / 1000) + 3600));
  assertArrayEqual(
    models.map((model) => model.id),
    ["composer-2.5"],
    "Expected discovery to retry and return the second successful response",
  );
  assertEqual(
    backend.getDiscoveryRequestBodies().length,
    2,
    "Expected transient discovery failure to be retried once",
  );
  console.log("[test] Discovery retry after transient failure OK");
}

async function testProviderAllowListFallbackWarns(modules: TestModules) {
  console.log("[test] Testing provider allow-list fallback warning...");
  const prevAllowList = process.env.CURSOR_MODELS;
  process.env.CURSOR_MODELS = "composer-2.5";
  const messages: string[] = [];
  const originalError = console.error;
  console.error = (...args: unknown[]) => messages.push(args.map(String).join(" "));
  try {
    const provider = modules.buildProviderBlock(12345, [
      { id: "composer-1.5", name: "Composer 1.5" },
    ]) as { models?: Record<string, unknown> };
    assertArrayEqual(
      Object.keys(provider.models ?? {}),
      ["composer-1.5"],
      "Expected provider fallback to keep a non-empty model list",
    );
    assert(
      messages.some((message) => message.includes("CURSOR_MODELS") && message.includes("composer-2.5")),
      `Expected allow-list fallback warning, got ${JSON.stringify(messages)}`,
    );
  } finally {
    console.error = originalError;
    if (prevAllowList === undefined) delete process.env.CURSOR_MODELS;
    else process.env.CURSOR_MODELS = prevAllowList;
  }
  console.log("[test] Provider allow-list fallback warning OK");
}

async function testFixedPortReuse(modules: TestModules) {
  // Regression: EADDRINUSE "Failed to start server. Is port 32125 in use?".
  // A second startProxy call on the SAME fixed port must REUSE the running
  // proxy (confirmed via /health), not try to re-bind and throw. This is the
  // crash the plugin surfaced whenever more than one opencode instance loaded.
  console.log("[test] Testing fixed-port reuse (no EADDRINUSE)...");
  const prev = process.env.CURSOR_PROXY_PORT;
  // Pick a high, almost-certainly-free fixed port for the test.
  const fixed = 39217;
  process.env.CURSOR_PROXY_PORT = String(fixed);
  try {
    const first = await modules.startProxy(async () => "test-token");
    assertEqual(first, fixed, "First start should bind the requested fixed port");

    // Simulate a SECOND, independent starter by clearing the in-process global
    // singleton, then starting again on the same fixed port. Without the
    // health-reuse path this second call throws EADDRINUSE.
    const g = globalThis as unknown as Record<string, unknown>;
    const saved = g["__cursorOauthProxy__"];
    g["__cursorOauthProxy__"] = {}; // force the "fresh module copy" code path

    const second = await modules.startProxy(async () => "test-token");
    assertEqual(second, fixed, "Second start must REUSE the fixed port, not throw");

    // Restore the real singleton so stopProxy() below tears down the server.
    g["__cursorOauthProxy__"] = saved;

    const health = await fetch(`http://127.0.0.1:${fixed}/health`);
    assertEqual(health.status, 200, "Reused proxy /health should answer 200");
    const body = (await health.json()) as { proxy?: string };
    assertEqual(body.proxy, "cursor-oauth", "/health must identify as cursor-oauth");

    modules.stopProxy();
    console.log("[test] Fixed-port reuse OK");
  } finally {
    if (prev === undefined) delete process.env.CURSOR_PROXY_PORT;
    else process.env.CURSOR_PROXY_PORT = prev;
  }
}

async function testConversationKeyIsolation(modules: TestModules) {
  // Regression: session title showed "[Error: Connect error internal: Blob not
  // found]". opencode's title-generation replays the session's FIRST user
  // message under a tiny "Generate a title" system prompt. deriveConversationKey
  // keyed on first-user-text ALONE, so the title call collided onto the chat's
  // deterministic conversationId and hit a server-side conversation whose
  // root-prompt blob it never sent. Distinct system prompts must yield distinct
  // conversation keys.
  console.log("[test] Testing conversation-key isolation (title vs chat)...");
  const firstUser = "help me refactor the auth module and fix the failing tests";

  const chat = [
    { role: "system", content: "You are opencode, a large coding agent. [huge system prompt...]" },
    { role: "user", content: firstUser },
  ] as any;
  const title = [
    { role: "system", content: "Generate a short title for this conversation. Reply with the title only." },
    { role: "user", content: firstUser },
  ] as any;

  const chatKey = modules.deriveConversationKey(chat);
  const titleKey = modules.deriveConversationKey(title);
  assert(
    chatKey !== titleKey,
    `Title and chat must not share a conversation key (both got ${chatKey})`,
  );

  // Same system prompt + same first user message across follow-up turns MUST
  // still map to one conversation (preserves server-side context reuse).
  const turn1 = [
    { role: "system", content: "You are opencode, a large coding agent. [huge system prompt...]" },
    { role: "user", content: firstUser },
  ] as any;
  const turn2 = [
    { role: "system", content: "You are opencode, a large coding agent. [huge system prompt...]" },
    { role: "user", content: firstUser },
    { role: "assistant", content: "Sure, let's start." },
    { role: "user", content: "now run the tests" },
  ] as any;
  assertEqual(
    modules.deriveConversationKey(turn1),
    modules.deriveConversationKey(turn2),
    "Follow-up turns in the same chat must keep one conversation key",
  );

  console.log("[test] Conversation-key isolation OK");
}

async function testTitlePriorTurnBlobsAreStored(modules: TestModules) {
  console.log("[test] Testing title-shaped prior-turn blob storage...");
  const decoded = decodePriorTurnFromBlobStore(modules, [
    { userText: "Generate a title for this conversation:\n", assistantText: "" },
  ]);
  assertEqual(decoded.stateTurns, 1, "Title-shaped request should serialize one prior turn");
  assertEqual(
    decoded.userText,
    "Generate a title for this conversation:\n",
    "Stored prior-turn user blob should decode to the synthetic title prompt",
  );
  assert(decoded.storeSize >= 3, "Expected system, prior user, and prior turn blobs in the store");
  console.log("[test] Title-shaped prior-turn blob storage OK");
}

async function testCheckpointlessContinuedTurnBlobsAreStored(modules: TestModules) {
  console.log("[test] Testing checkpointless continued-turn blob storage...");
  const decoded = decodePriorTurnFromBlobStore(modules, [
    { userText: "inspect the repo", assistantText: "I will inspect it with a tool call." },
  ]);
  assertEqual(decoded.stateTurns, 1, "Continued request should serialize one historical turn");
  assertEqual(
    decoded.userText,
    "inspect the repo",
    "Stored historical user-message blob should decode to turn 1 user text",
  );
  assertEqual(
    decoded.assistantText,
    "I will inspect it with a tool call.",
    "Stored historical assistant-step blob should decode to turn 1 assistant text",
  );
  console.log("[test] Checkpointless continued-turn blob storage OK");
}

async function testConversationIdScopedToStoredConversationLifetime(modules: TestModules) {
  // Regression: the shared proxy evicts idle StoredConversation entries after a
  // TTL sweep triggered by any opencode window. A deterministic ID derived from
  // convKey meant the next request recreated an empty local blobStore/checkpoint
  // under the SAME Cursor-side conversation ID. Cursor still had advanced
  // server-side state for that ID, asked for blobs the new local store did not
  // have, and returned "Blob not found". IDs must be stable only while the
  // StoredConversation entry survives, then freshly minted after eviction.
  console.log("[test] Testing conversation-ID StoredConversation lifetime scope...");
  const convKey = "fixedconvkey1234";

  const a = modules.getStoredConversationIdForTest(convKey);
  const b = modules.getStoredConversationIdForTest(convKey);
  assertEqual(a, b, "conversationId must stay stable while StoredConversation is alive");

  assert(
    /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(a),
    `conversationId must be a v4-shaped UUID, got ${a}`,
  );

  modules.evictStoredConversationForTest(convKey);
  const afterEviction = modules.getStoredConversationIdForTest(convKey);
  assert(
    afterEviction !== a,
    `An evicted StoredConversation must mint a fresh conversation ID (both got ${a}) — ` +
      `that is the "Blob not found" regression`,
  );

  const script =
    `const p = await import("${new URL("../src/proxy.ts", import.meta.url).pathname}");` +
    `process.stdout.write(p.getStoredConversationIdForTest(${JSON.stringify(convKey)}));`;
  const proc = Bun.spawn(["bun", "-e", script], { stdout: "pipe", stderr: "pipe" });
  const otherProcessId = (await new Response(proc.stdout).text()).trim();
  const code = await proc.exited;
  assert(
    code === 0 && otherProcessId.length > 0,
    `failed to derive conversationId in a second process (exit ${code}, err: ${await new Response(proc.stderr).text()})`,
  );
  assert(
    otherProcessId !== a,
    `A restarted proxy must NOT reuse the conversation ID (both got ${a}) — ` +
      `that is the "Blob not found" regression`,
  );

  console.log("[test] Conversation-ID StoredConversation lifetime scope OK");
}

/** Build a fake StreamBridge for testing response-streaming code without a
 *  real child process or H2 connection. Captures the onData/onClose
 *  callbacks so a test can drive them directly. */
function makeFakeStreamBridge(): {
  bridge: StreamBridge;
  killed: () => boolean;
  triggerData: (bytes: Uint8Array) => void;
  triggerClose: (code: number) => void;
} {
  let onDataCb: ((chunk: Buffer) => void) | null = null;
  let onCloseCb: ((code: number) => void) | null = null;
  let killedFlag = false;
  const bridge: StreamBridge = {
    write() {},
    end() {},
    onData(cb) {
      onDataCb = cb;
    },
    onClose(cb) {
      onCloseCb = cb;
    },
    get alive() {
      return !killedFlag;
    },
    kill() {
      killedFlag = true;
    },
  };
  return {
    bridge,
    killed: () => killedFlag,
    triggerData: (bytes) => onDataCb?.(Buffer.from(bytes)),
    triggerClose: (code) => onCloseCb?.(code),
  };
}

/** Frame a Connect end-stream error matching proxy.ts's wire format:
 *  [1-byte flags with CONNECT_END_STREAM_FLAG set][4-byte BE length][JSON]. */
function frameConnectEndStreamError(modules: TestModules, message: string): Buffer {
  const payload = Buffer.from(JSON.stringify({ error: { code: "internal", message } }), "utf8");
  const frame = Buffer.alloc(5 + payload.length);
  frame[0] = modules.CONNECT_END_STREAM_FLAG;
  frame.writeUInt32BE(payload.length, 1);
  frame.set(payload, 5);
  return frame;
}

/** Frame a minimal, valid (non-end-stream) AgentServerMessage — a stand-in
 *  for genuine Cursor activity in the generation-stall regression tests. No
 *  oneof case is set, so it's a safe no-op through processServerMessage;
 *  only its arrival (and the resulting timer reset) is under test. */
function frameGenericServerMessage(): Buffer {
  const msg = create(AgentServerMessageSchema, {});
  const payload = toBinary(AgentServerMessageSchema, msg);
  const frame = Buffer.alloc(5 + payload.length);
  frame[0] = 0; // no CONNECT_END_STREAM_FLAG — an ordinary data frame
  frame.writeUInt32BE(payload.length, 1);
  frame.set(payload, 5);
  return frame;
}

async function readSseText(response: Response): Promise<string> {
  const reader = response.body!.getReader();
  const decoder = new TextDecoder();
  let text = "";
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    text += decoder.decode(value, { stream: true });
  }
  return text;
}

async function testEndStreamErrorPoisonsConversationStreaming(modules: TestModules) {
  // Regression: the 5th "Blob not found" mechanism. Bun.serve's idleTimeout
  // is hard-capped at 255s (oven-sh/bun#27470) — a Cursor turn that thinks
  // silently for longer than that gets its SSE connection to opencode killed
  // by Bun itself, opencode retries, and the retry replayed a
  // checkpoint/blobStore pairing Cursor's server had already moved past —
  // surfacing as a fresh "Blob not found". More generally: ANY Connect
  // end-stream error means local state no longer matches what Cursor's
  // server expects, so it must not be replayed on the next turn.
  console.log("[test] Testing end-stream error poisons conversation (streaming)...");
  const convKey = "streaming-poison-test-key";
  const bridgeKey = "streaming-poison-bridge-key";
  const before = modules.getStoredConversationIdForTest(convKey);

  const fake = makeFakeStreamBridge();
  const heartbeatTimer = setInterval(() => {}, 60_000);
  const response = modules.createBridgeStreamResponseForTest(
    fake.bridge,
    heartbeatTimer,
    new Map(),
    [],
    "composer-2.5",
    bridgeKey,
    convKey,
  );

  fake.triggerData(frameConnectEndStreamError(modules, "Blob not found"));
  fake.triggerClose(0);

  const text = await readSseText(response);
  assert(
    text.includes("Blob not found"),
    `Expected the end-stream error to surface as SSE content, got: ${text}`,
  );

  const after = modules.getStoredConversationIdForTest(convKey);
  assert(
    after !== before,
    `A Connect end-stream error must poison the StoredConversation (both got ${before}) — ` +
      `otherwise the next turn replays the same mismatched state forever`,
  );

  console.log("[test] End-stream error poisons conversation (streaming) OK");
}

async function testEndStreamErrorPoisonsConversationNonStreaming(modules: TestModules) {
  console.log("[test] Testing end-stream error poisons conversation (non-streaming)...");
  const convKey = "non-streaming-poison-test-key";
  const before = modules.getStoredConversationIdForTest(convKey);

  const fake = makeFakeStreamBridge();
  const heartbeatTimer = setInterval(() => {}, 60_000);
  const resultPromise = modules.collectFullResponseForTest(
    { requestBytes: new Uint8Array(), blobStore: new Map(), mcpTools: [] },
    "test-token",
    convKey,
    { bridge: fake.bridge, heartbeatTimer },
  );

  fake.triggerData(frameConnectEndStreamError(modules, "Blob not found"));
  fake.triggerClose(0);

  const result = await resultPromise;
  assert(
    result.text.includes("Blob not found"),
    `Expected the end-stream error surfaced as content, got: ${result.text}`,
  );

  const after = modules.getStoredConversationIdForTest(convKey);
  assert(
    after !== before,
    `A Connect end-stream error must poison the StoredConversation (both got ${before})`,
  );

  console.log("[test] End-stream error poisons conversation (non-streaming) OK");
}

async function testCancelBeforeTerminalStateKillsBridgeAndPoisons(modules: TestModules) {
  // Regression: opencode (or Bun's own idleTimeout) can tear down the SSE
  // response before a turn ever reaches a clean terminal state (no
  // tool_calls, no stop). Without a cancel() handler, the bridge subprocess +
  // heartbeat leaked forever, and the next retry replayed local state Cursor's
  // server had no reason to still honor.
  console.log("[test] Testing stream cancel before terminal state kills bridge + poisons...");
  const convKey = "cancel-poison-test-key";
  const bridgeKey = "cancel-poison-bridge-key";
  const before = modules.getStoredConversationIdForTest(convKey);

  const fake = makeFakeStreamBridge();
  const heartbeatTimer = setInterval(() => {}, 60_000);
  const response = modules.createBridgeStreamResponseForTest(
    fake.bridge,
    heartbeatTimer,
    new Map(),
    [],
    "composer-2.5",
    bridgeKey,
    convKey,
  );

  // Nothing has streamed yet — simulate the client/Bun giving up mid-turn.
  await response.body!.cancel();

  assert(fake.killed(), "cancel() before a terminal state must kill the bridge subprocess");

  const after = modules.getStoredConversationIdForTest(convKey);
  assert(
    after !== before,
    `A cancelled mid-flight turn must poison the StoredConversation (both got ${before}) — ` +
      `the next retry must not replay state Cursor's server may have moved past`,
  );

  console.log("[test] Stream cancel before terminal state OK");
}

async function testKeepAlivePingsDuringSilence(modules: TestModules) {
  // Regression: the underlying trigger for the 5th mechanism above. Bun.serve
  // enforces idleTimeout=255 (its hard-capped max) on the SSE connection to
  // opencode; a Cursor turn silent for longer than that got disconnected by
  // Bun itself. Ping well under that ceiling so Bun always sees activity.
  console.log("[test] Testing SSE keep-alive pings during Cursor silence...");
  modules.setSseKeepAliveMsForTest(15);
  const heartbeatTimer = setInterval(() => {}, 60_000);
  try {
    const convKey = "keepalive-test-key";
    const bridgeKey = "keepalive-test-bridge-key";
    const fake = makeFakeStreamBridge();
    const response = modules.createBridgeStreamResponseForTest(
      fake.bridge,
      heartbeatTimer,
      new Map(),
      [],
      "composer-2.5",
      bridgeKey,
      convKey,
    );

    // Cursor stays silent (no triggerData) — only the keep-alive timer should
    // produce output.
    const reader = response.body!.getReader();
    const decoder = new TextDecoder();
    try {
      const { value } = await Promise.race([
        reader.read(),
        new Promise<{ value?: Uint8Array }>((resolve) => setTimeout(() => resolve({}), 1_000)),
      ]);
      assert(value, "Expected the keep-alive timer to produce a chunk within 1s");
      const text = decoder.decode(value);
      assert(
        text.includes(": keep-alive"),
        `Expected an SSE keep-alive comment, got: ${JSON.stringify(text)}`,
      );
    } finally {
      await reader.cancel();
    }

    console.log("[test] SSE keep-alive pings OK");
  } finally {
    clearInterval(heartbeatTimer);
    modules.setSseKeepAliveMsForTest(20_000);
  }
}

async function testGenerationStallPoisonsAndKillsBridgeStreaming(modules: TestModules) {
  // Regression: 2026-07-07, a turn hung silently for 2h36m — Cursor sent one
  // partial sentence then went completely dark (no tool call, no error, no
  // more data). Root cause: h2-bridge.mjs's own kill-watchdog resets on ANY
  // stdin write, including our own outbound clientHeartbeat every 5s, so it
  // never actually measured "has Cursor responded" — only "are we still
  // writing," which is always true. This app-level timer is the fix: reset
  // only by genuine inbound AgentServerMessages, so real silence gets caught.
  console.log("[test] Testing generation stall poisons + kills bridge (streaming)...");
  modules.setGenerationStallMsForTest(25);
  const heartbeatTimer = setInterval(() => {}, 60_000);
  try {
    const convKey = "generation-stall-streaming-test-key";
    const bridgeKey = "generation-stall-streaming-bridge-key";
    const before = modules.getStoredConversationIdForTest(convKey);

    const fake = makeFakeStreamBridge();
    const response = modules.createBridgeStreamResponseForTest(
      fake.bridge,
      heartbeatTimer,
      new Map(),
      [],
      "composer-2.5",
      bridgeKey,
      convKey,
    );

    // Cursor never sends anything — no triggerData, no triggerClose. The
    // stall timer alone must notice and tear the turn down.
    const text = await readSseText(response);

    assert(
      text.includes("no response") && text.includes("25s"),
      `Expected a stall error mentioning the timeout, got: ${text}`,
    );
    assert(
      text.includes('"finish_reason":"stop"'),
      `Expected a clean stop after the stall, got: ${text}`,
    );
    assert(fake.killed(), "A generation stall must kill the bridge subprocess");

    const after = modules.getStoredConversationIdForTest(convKey);
    assert(
      after !== before,
      `A generation stall must poison the StoredConversation (both got ${before}) — ` +
        `otherwise the next retry replays state Cursor never acknowledged`,
    );

    console.log("[test] Generation stall poisons + kills bridge (streaming) OK");
  } finally {
    clearInterval(heartbeatTimer);
    modules.setGenerationStallMsForTest(90_000);
  }
}

async function testGenerationStallResolvesWithErrorNonStreaming(modules: TestModules) {
  console.log("[test] Testing generation stall resolves with error (non-streaming)...");
  modules.setGenerationStallMsForTest(25);
  const heartbeatTimer = setInterval(() => {}, 60_000);
  try {
    const convKey = "generation-stall-non-streaming-test-key";
    const before = modules.getStoredConversationIdForTest(convKey);

    const fake = makeFakeStreamBridge();
    const resultPromise = modules.collectFullResponseForTest(
      { requestBytes: new Uint8Array(), blobStore: new Map(), mcpTools: [] },
      "test-token",
      convKey,
      { bridge: fake.bridge, heartbeatTimer },
    );

    // Cursor never sends anything (this is the code path title-gen and
    // model-discovery calls use — the same path that hung in the real
    // incident's OTHER concurrent call).
    const result = await resultPromise;

    assert(
      result.text.includes("no response"),
      `Expected a stall error in the resolved text, got: ${result.text}`,
    );
    assert(fake.killed(), "A generation stall must kill the bridge subprocess");

    const after = modules.getStoredConversationIdForTest(convKey);
    assert(
      after !== before,
      `A generation stall must poison the StoredConversation (both got ${before})`,
    );

    console.log("[test] Generation stall resolves with error (non-streaming) OK");
  } finally {
    clearInterval(heartbeatTimer);
    modules.setGenerationStallMsForTest(90_000);
  }
}

async function testGenuineActivityPreventsFalseGenerationStall(modules: TestModules) {
  // Regression-of-the-regression: the fix must reset on genuine Cursor
  // activity, not just paper over the old bug with a slightly-less-wrong
  // fixed budget. A legitimately slow-but-alive turn (real frames arriving
  // slower than the old bug's masking heartbeat, but well inside the stall
  // window of each other) must survive well past the nominal stall duration
  // — and must still be caught once genuine activity actually stops.
  console.log("[test] Testing genuine activity prevents a false generation-stall kill...");
  modules.setGenerationStallMsForTest(25);
  const heartbeatTimer = setInterval(() => {}, 60_000);
  try {
    const convKey = "generation-stall-activity-test-key";
    const bridgeKey = "generation-stall-activity-bridge-key";
    const fake = makeFakeStreamBridge();
    modules.createBridgeStreamResponseForTest(
      fake.bridge,
      heartbeatTimer,
      new Map(),
      [],
      "composer-2.5",
      bridgeKey,
      convKey,
    );

    // Real frames every 5ms — well inside the 25ms stall window — for
    // 200ms total: 8x the nominal stall budget.
    const pings = setInterval(() => fake.triggerData(frameGenericServerMessage()), 5);
    await new Promise((resolve) => setTimeout(resolve, 200));
    clearInterval(pings);

    assert(!fake.killed(), "Genuine periodic activity must not be treated as a stall");

    // Now let it actually go quiet and confirm the timer still fires.
    await new Promise((resolve) => setTimeout(resolve, 300));
    assert(fake.killed(), "Once genuine activity stops, the stall timer must still fire");

    console.log("[test] Genuine activity prevents false generation-stall kill OK");
  } finally {
    clearInterval(heartbeatTimer);
    modules.setGenerationStallMsForTest(90_000);
  }
}

async function testProxyVersionGuardEvictsOlder(modules: TestModules) {
  // Regression: a fresh plugin build must be able to TAKE OVER the fixed port
  // from an OLDER cursor-oauth proxy still running there (a stale opencode window
  // from before a rebuild). Before the version guard, startProxy adopted ANY
  // healthy cursor-oauth proxy on the port, so new plugin code never loaded until
  // every old window was killed — the "restart didn't help" trap. Here we stand
  // up a fake OLDER proxy (buildEpoch=1) and confirm the real startProxy evicts
  // it via POST /shutdown and binds the port itself.
  console.log("[test] Testing proxy version guard (evict older)...");
  const PORT = 39160;

  let shutdownCalledWithEpoch = -1;
  let released = false;
  let fake: ReturnType<typeof Bun.serve> | null = null;
  fake = Bun.serve({
    port: PORT,
    idleTimeout: 30,
    fetch: async (req) => {
      const url = new URL(req.url);
      if (url.pathname === "/health") {
        if (released) return new Response("gone", { status: 503 });
        return new Response(JSON.stringify({ proxy: "cursor-oauth", models: 0, buildEpoch: 1 }), {
          headers: { "Content-Type": "application/json" },
        });
      }
      if (req.method === "POST" && url.pathname === "/shutdown") {
        const body = (await req.json().catch(() => null)) as { buildEpoch?: number } | null;
        shutdownCalledWithEpoch = body?.buildEpoch ?? -1;
        released = true;
        // Actually release the socket, as the real proxy's stopProxy() does, so
        // the reclaiming proxy can bind. Defer so this response flushes first.
        setTimeout(() => { try { fake?.stop(true); } catch { /* noop */ } }, 20);
        return new Response(JSON.stringify({ ok: true, releasing: true }), {
          headers: { "Content-Type": "application/json" },
        });
      }
      return new Response("not found", { status: 404 });
    },
  });

  const prevPort = process.env.CURSOR_PROXY_PORT;
  process.env.CURSOR_PROXY_PORT = String(PORT);
  try {
    // Real startProxy: its BUILD_EPOCH (source mtime) is far newer than 1, so it
    // must evict the fake via POST /shutdown, wait for the port to free, then
    // bind it. It must NOT simply adopt the epoch=1 proxy.
    const boundPort = await modules.startProxy(async () => "test-token", [
      { id: "composer-2.5", name: "composer-2.5" },
    ]);

    assert(
      shutdownCalledWithEpoch > 1,
      `startProxy must POST /shutdown with its (newer) epoch; got ${shutdownCalledWithEpoch}`,
    );
    assertEqual(boundPort, PORT, "startProxy should bind the fixed port after evicting the older proxy");

    // The port must now be served by OUR proxy (real /v1/models works).
    const res = await fetch(`http://127.0.0.1:${PORT}/v1/models`);
    assert(res.ok, `/v1/models on reclaimed port returned ${res.status}`);
    const body = (await res.json()) as { object?: string };
    assertEqual(body.object, "list", "reclaimed proxy must serve /v1/models");

    modules.stopProxy();
  } finally {
    try { fake.stop(true); } catch { /* already stopped */ }
    if (prevPort === undefined) delete process.env.CURSOR_PROXY_PORT;
    else process.env.CURSOR_PROXY_PORT = prevPort;
  }

  console.log("[test] Proxy version guard OK");
}

function proxyState(): { server?: ReturnType<typeof Bun.serve>; port?: number } {
  const globals = globalThis as unknown as Record<string, { server?: ReturnType<typeof Bun.serve>; port?: number } | undefined>;
  return globals.__cursorOauthProxy__ ?? {};
}

async function testSlowDrainEvictionBindsAfterPollTimeout(modules: TestModules) {
  console.log("[test] Testing slow-drain eviction retry-bind...");
  const PORT = 39161;

  let shutdownCalledWithEpoch = -1;
  let healthAfterShutdown = 0;
  let fake: ReturnType<typeof Bun.serve> | null = null;
  fake = Bun.serve({
    port: PORT,
    idleTimeout: 30,
    fetch: async (req) => {
      const url = new URL(req.url);
      if (url.pathname === "/health") {
        if (shutdownCalledWithEpoch > 0) {
          healthAfterShutdown += 1;
          if (healthAfterShutdown === 20) {
            try { fake?.stop(); } catch { /* noop */ }
          }
        }
        return new Response(JSON.stringify({ proxy: "cursor-oauth", models: 0, buildEpoch: 1 }), {
          headers: { "Content-Type": "application/json" },
        });
      }
      if (req.method === "POST" && url.pathname === "/shutdown") {
        const body = (await req.json().catch(() => null)) as { buildEpoch?: number } | null;
        shutdownCalledWithEpoch = body?.buildEpoch ?? -1;
        return new Response(JSON.stringify({ ok: true, releasing: true }), {
          headers: { "Content-Type": "application/json" },
        });
      }
      return new Response("not found", { status: 404 });
    },
  });

  const prevPort = process.env.CURSOR_PROXY_PORT;
  process.env.CURSOR_PROXY_PORT = String(PORT);
  try {
    const boundPort = await modules.startProxy(async () => "test-token", [
      { id: "composer-2.5", name: "composer-2.5" },
    ]);

    assert(shutdownCalledWithEpoch > 1, `startProxy must request shutdown; got ${shutdownCalledWithEpoch}`);
    assertEqual(boundPort, PORT, "startProxy should bind the fixed port after delayed release");
    assert(proxyState().server, "startProxy must own a real server after slow-drain eviction, not phantom-adopt");

    modules.stopProxy();
  } finally {
    try { fake?.stop(); } catch { /* already stopped */ }
    if (prevPort === undefined) delete process.env.CURSOR_PROXY_PORT;
    else process.env.CURSOR_PROXY_PORT = prevPort;
  }

  console.log("[test] Slow-drain eviction retry-bind OK");
}

async function testStuckEvictionStillAdopts(modules: TestModules) {
  console.log("[test] Testing stuck eviction fallback adopt...");
  const PORT = 39162;

  let shutdownCalledWithEpoch = -1;
  const fake = Bun.serve({
    port: PORT,
    idleTimeout: 30,
    fetch: async (req) => {
      const url = new URL(req.url);
      if (url.pathname === "/health") {
        return new Response(JSON.stringify({ proxy: "cursor-oauth", models: 0, buildEpoch: 1 }), {
          headers: { "Content-Type": "application/json" },
        });
      }
      if (req.method === "POST" && url.pathname === "/shutdown") {
        const body = (await req.json().catch(() => null)) as { buildEpoch?: number } | null;
        shutdownCalledWithEpoch = body?.buildEpoch ?? -1;
        return new Response(JSON.stringify({ ok: true, releasing: true }), {
          headers: { "Content-Type": "application/json" },
        });
      }
      return new Response("not found", { status: 404 });
    },
  });

  const prevPort = process.env.CURSOR_PROXY_PORT;
  process.env.CURSOR_PROXY_PORT = String(PORT);
  try {
    const boundPort = await modules.startProxy(async () => "test-token", [
      { id: "composer-2.5", name: "composer-2.5" },
    ]);

    assert(shutdownCalledWithEpoch > 1, `startProxy must request shutdown; got ${shutdownCalledWithEpoch}`);
    assertEqual(boundPort, PORT, "startProxy should keep the fixed port when stuck proxy is adopted");
    assertEqual(proxyState().server, undefined, "stuck old proxy should still use optimistic adopt fallback");

    modules.stopProxy();
  } finally {
    try { fake.stop(); } catch { /* already stopped */ }
    if (prevPort === undefined) delete process.env.CURSOR_PROXY_PORT;
    else process.env.CURSOR_PROXY_PORT = prevPort;
  }

  console.log("[test] Stuck eviction fallback adopt OK");
}

async function main() {
  // Hermetic tests: never adopt a live cursor-oauth proxy (e.g. a running
  // opencode window on CURSOR_PROXY_PORT). Force ephemeral ports so startProxy
  // always binds its OWN server with the test's model list — otherwise
  // /v1/models returns the live catalog and the empty-list assertion fails.
  // (testProxyVersionGuardEvictsOlder sets/restores it locally.)
  delete process.env.CURSOR_PROXY_PORT;

  const backend = await createTestCursorBackend();
  process.env.CURSOR_API_URL = backend.apiUrl;
  process.env.CURSOR_REFRESH_URL = backend.refreshUrl;

  const modules = await loadModules();

  try {
    await testProxyStartStop(modules);
    await testFixedPortReuse(modules);
    await testConversationKeyIsolation(modules);
    await testTitlePriorTurnBlobsAreStored(modules);
    await testCheckpointlessContinuedTurnBlobsAreStored(modules);
    await testConversationIdScopedToStoredConversationLifetime(modules);
    await testEndStreamErrorPoisonsConversationStreaming(modules);
    await testEndStreamErrorPoisonsConversationNonStreaming(modules);
    await testCancelBeforeTerminalStateKillsBridgeAndPoisons(modules);
    await testKeepAlivePingsDuringSilence(modules);
    await testGenerationStallPoisonsAndKillsBridgeStreaming(modules);
    await testGenerationStallResolvesWithErrorNonStreaming(modules);
    await testGenuineActivityPreventsFalseGenerationStall(modules);
    await testProxyVersionGuardEvictsOlder(modules);
    await testSlowDrainEvictionBindsAfterPollTimeout(modules);
    await testStuckEvictionStillAdopts(modules);
    await testAuthParams(modules);
    await testTokenExpiry(modules);
    await testPluginShape(modules);
    await testArrayContentParsing(modules);
    await testExpiredTokenRefreshBeforeDiscovery(modules, backend);
    await testDiscoveryFallbackAndSuccess(modules, backend);
    await testDiscoveryRetriesTransientFailure(modules, backend);
    await testProviderAllowListFallbackWarns(modules);
    console.log("\n✓ All smoke tests passed");
    process.exitCode = 0;
  } catch (err) {
    console.error("\n✗ Smoke test failed:", err);
    process.exitCode = 1;
  } finally {
    modules.stopProxy();
    await backend.close();
  }
}

main();
