/**
 * Standalone proxy entrypoint — sidesteps opencode's broken provider promotion
 * on 1.17.8+ by running the Cursor gRPC→OpenAI proxy as a plain process on a
 * FIXED port. opencode then talks to it via a STATIC `@ai-sdk/openai-compatible`
 * provider in opencode.json (which always promotes), exactly like the
 * @rama_nigg/open-cursor trick — but talking DIRECTLY to api2.cursor.sh over
 * HTTP/2, with no `cursor-agent` per-request spawn.
 *
 * Auth: borrows the access/refresh tokens that `cursor-agent` already stored in
 * the macOS Keychain (services `cursor-access-token` / `cursor-refresh-token`,
 * account `cursor-user`). No second OAuth login. Reading a token file is not the
 * same as paying the spawn tax — the latency win is intact.
 *
 * Usage:
 *   CURSOR_PROXY_PORT=32125 bun src/standalone.ts serve
 *   bun src/standalone.ts gen-config            # emit opencode.json provider block
 *
 * NOTE: Keychain read is macOS-only. Linux/Windows would need a different token
 * source (documented as a follow-up).
 */
import { getCursorModels, type CursorModel } from "./models";
import { getProxyPort, startProxy } from "./proxy";
import { getTokenExpiry, refreshCursorToken } from "./auth";

const KEYCHAIN_ACCOUNT = "cursor-user";
const KEYCHAIN_ACCESS_SERVICE = "cursor-access-token";
const KEYCHAIN_REFRESH_SERVICE = "cursor-refresh-token";
const PROVIDER_ID = "cursor-oauth";
const DEFAULT_PORT = 32125; // avoid @rama_nigg/open-cursor's 32124

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

interface TokenState {
  access: string;
  refresh: string;
  expires: number;
}

/** Load tokens from the Keychain (the login cursor-agent already performed). */
async function loadTokens(): Promise<TokenState> {
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
function makeTokenProvider(initial: TokenState): () => Promise<string> {
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

async function serve(): Promise<void> {
  if (!process.env.CURSOR_PROXY_PORT) {
    process.env.CURSOR_PROXY_PORT = String(DEFAULT_PORT);
  }
  const tokens = await loadTokens();
  const getAccessToken = makeTokenProvider(tokens);
  const models = await getCursorModels(tokens.access);
  const port = await startProxy(getAccessToken, models.map((m) => ({ id: m.id, name: m.name })));

  console.error(`[cursor-oauth] proxy listening on http://127.0.0.1:${port}/v1`);
  console.error(`[cursor-oauth] ${models.length} models; auth via Keychain (refresh enabled)`);
  console.error(`[cursor-oauth] add provider '${PROVIDER_ID}' to opencode.json — see: gen-config`);

  // Keep the process alive.
  await new Promise<never>(() => {});
}

/** Emit a static opencode.json provider block (no loader, always promotes). */
async function genConfig(): Promise<void> {
  const tokens = await loadTokens();
  const models = await getCursorModels(tokens.access);
  const port = process.env.CURSOR_PROXY_PORT ?? String(DEFAULT_PORT);

  const block = {
    provider: {
      [PROVIDER_ID]: {
        name: "Cursor (OAuth, direct)",
        npm: "@ai-sdk/openai-compatible",
        options: { baseURL: `http://127.0.0.1:${port}/v1` },
        models: Object.fromEntries(
          models.map((m) => [m.id, { name: m.name }]),
        ),
      },
    },
  };
  console.log(JSON.stringify(block, null, 2));
}

const cmd = process.argv[2] ?? "serve";
const main = cmd === "gen-config" ? genConfig : serve;
main().catch((err) => {
  console.error(`[cursor-oauth] ${err instanceof Error ? err.message : String(err)}`);
  process.exit(1);
});

export { getProxyPort };
