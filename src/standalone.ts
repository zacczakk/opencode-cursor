/**
 * Cursor direct-gRPC proxy — shared core used by BOTH the standalone CLI and the
 * opencode plugin. Sidesteps opencode's broken provider promotion (1.17.8+) by
 * running the Cursor gRPC→OpenAI proxy on a FIXED port; opencode reaches it via a
 * STATIC `@ai-sdk/openai-compatible` provider (always promotes). Talks DIRECTLY
 * to api2.cursor.sh over HTTP/2 — no `cursor-agent` per-request spawn.
 *
 * Auth: borrows the access/refresh tokens `cursor-agent` already stored in the
 * macOS Keychain (services `cursor-access-token` / `cursor-refresh-token`,
 * account `cursor-user`). No second OAuth login. Reading a token ≠ paying the
 * spawn tax — the latency win is intact.
 *
 * CLI usage:
 *   CURSOR_PROXY_PORT=32125 bun src/standalone.ts serve
 *   bun src/standalone.ts gen-config            # emit opencode.json provider block
 *
 * NOTE: Keychain read is macOS-only. Linux/Windows need a different token source.
 */
import { getCursorModels } from "./models";
import { getProxyPort, startProxy } from "./proxy";
import { getTokenExpiry, refreshCursorToken } from "./auth";

export const KEYCHAIN_ACCOUNT = "cursor-user";
export const KEYCHAIN_ACCESS_SERVICE = "cursor-access-token";
export const KEYCHAIN_REFRESH_SERVICE = "cursor-refresh-token";
export const PROVIDER_ID = "cursor-oauth";
export const DEFAULT_PORT = 32125; // avoid @rama_nigg/open-cursor's 32124

/** Read a secret from the macOS Keychain. Returns null if unavailable. */
async function readKeychain(service: string): Promise<string | null> {
  if (process.platform !== "darwin") return null;
  const proc = Bun.spawn(
    ["security", "find-generic-password", "-s", service, "-a", KEYCHAIN_ACCOUNT, "-w"],
    { stdout: "pipe", stderr: "ignore" },
  );
  const out = (await new Response(proc.stdout).text()).trim();
  const code = await proc.exited;
  return code === 0 && out.length > 0 ? out : null;
}

export interface TokenState {
  access: string;
  refresh: string;
  expires: number;
}

/** Load tokens from the Keychain (the login cursor-agent already performed). */
export async function loadTokens(): Promise<TokenState> {
  const access = await readKeychain(KEYCHAIN_ACCESS_SERVICE);
  const refresh = await readKeychain(KEYCHAIN_REFRESH_SERVICE);
  if (!access || !refresh) {
    throw new Error(
      `Cursor tokens not found in Keychain (services ${KEYCHAIN_ACCESS_SERVICE}/` +
      `${KEYCHAIN_REFRESH_SERVICE}, account ${KEYCHAIN_ACCOUNT}). ` +
      `Run 'cursor-agent login' first.`,
    );
  }
  return { access, refresh, expires: getTokenExpiry(access) };
}

/**
 * Build a token provider for startProxy: returns a valid access token, refreshing
 * via the refresh token when the current one is within the expiry safety margin.
 */
export function makeTokenProvider(initial: TokenState): () => Promise<string> {
  let state = initial;
  let refreshing: Promise<void> | null = null;

  return async () => {
    if (Date.now() < state.expires) return state.access;
    if (!refreshing) {
      refreshing = (async () => {
        try {
          const creds = await refreshCursorToken(state.refresh);
          state = { access: creds.access, refresh: creds.refresh, expires: creds.expires };
        } finally {
          refreshing = null;
        }
      })();
    }
    await refreshing;
    return state.access;
  };
}

/**
 * Start the proxy on the fixed port using Keychain tokens. Idempotent (startProxy
 * returns the existing port if already running). Returns the bound port + the
 * discovered model list, so callers can build a provider block.
 */
export async function startCursorProxy(
  port: number = DEFAULT_PORT,
): Promise<{ port: number; models: Array<{ id: string; name: string }> }> {
  if (!process.env.CURSOR_PROXY_PORT) {
    process.env.CURSOR_PROXY_PORT = String(port);
  }
  const tokens = await loadTokens();
  const getAccessToken = makeTokenProvider(tokens);
  const discovered = await getCursorModels(tokens.access);
  const models = discovered.map((m) => ({ id: m.id, name: m.name }));
  const boundPort = await startProxy(getAccessToken, models);
  return { port: boundPort, models };
}

/** Build a static `@ai-sdk/openai-compatible` provider block for opencode config. */
export function buildProviderBlock(
  port: number,
  models: Array<{ id: string; name: string }>,
): Record<string, unknown> {
  return {
    name: "Cursor (OAuth, direct)",
    npm: "@ai-sdk/openai-compatible",
    options: { baseURL: `http://127.0.0.1:${port}/v1`, apiKey: "unused" },
    models: Object.fromEntries(models.map((m) => [m.id, { name: m.name }])),
  };
}

async function serve(): Promise<void> {
  const { port, models } = await startCursorProxy(
    process.env.CURSOR_PROXY_PORT ? Number(process.env.CURSOR_PROXY_PORT) : DEFAULT_PORT,
  );
  console.error(`[cursor-oauth] proxy listening on http://127.0.0.1:${port}/v1`);
  console.error(`[cursor-oauth] ${models.length} models; auth via Keychain (refresh enabled)`);
  console.error(`[cursor-oauth] provider '${PROVIDER_ID}' — see: gen-config`);
  await new Promise<never>(() => {}); // keep alive
}

/** Emit a static opencode.json provider block (no loader, always promotes). */
async function genConfig(): Promise<void> {
  const tokens = await loadTokens();
  const discovered = await getCursorModels(tokens.access);
  const port = process.env.CURSOR_PROXY_PORT ? Number(process.env.CURSOR_PROXY_PORT) : DEFAULT_PORT;
  const block = {
    provider: {
      [PROVIDER_ID]: buildProviderBlock(
        port,
        discovered.map((m) => ({ id: m.id, name: m.name })),
      ),
    },
  };
  console.log(JSON.stringify(block, null, 2));
}

// Only run the CLI when executed directly (not when imported by the plugin).
if (import.meta.main) {
  const cmd = process.argv[2] ?? "serve";
  const main = cmd === "gen-config" ? genConfig : serve;
  main().catch((err) => {
    console.error(`[cursor-oauth] ${err instanceof Error ? err.message : String(err)}`);
    process.exit(1);
  });
}

export { getProxyPort };
