/**
 * OpenCode plugin: Cursor models via DIRECT gRPC (no cursor-agent spawn).
 *
 * The WORKING replacement for the dead auth-loader plugin (./index.ts). On
 * opencode 1.17.8+ a provider registered through the `auth.loader` hook never
 * gets promoted into the resolved set, so the loader never fires. We sidestep it:
 *
 *   1. On load, start the Cursor gRPC→OpenAI proxy on a FIXED port
 *      (CURSOR_PROXY_PORT, default 32125), authenticated with the tokens
 *      cursor-agent already stored in the macOS Keychain.
 *   2. In the `config` hook, inject a STATIC `@ai-sdk/openai-compatible`
 *      provider (display name "Cursor") pointed at that proxy. That provider
 *      type ALWAYS promotes, so `cursor-oauth/<model>` resolves with no loader.
 *
 * Every turn hits a persistent HTTP/2 connection to api2.cursor.sh instead of
 * spawning a fresh cursor-agent process — measured ~2-6x faster warm in the TUI.
 *
 * Env:
 *   CURSOR_PROXY_PORT   fixed proxy port (default 32125)
 *   CURSOR_MODELS       comma-separated model ids to SHOW in the selector
 *                       (default "composer-2.5"; set "" to show all discovered)
 *
 * Register in opencode.json: { "plugin": ["opencode-cursor-oauth"] }
 * (macOS only for now — Keychain token read.)
 */
import type { Hooks, Plugin, PluginInput } from "@opencode-ai/plugin";
import {
  DEFAULT_PORT,
  PROVIDER_ID,
  buildProviderBlock,
  startCursorProxy,
} from "./standalone";

// Default the selector to composer-2.5 only. A user can override (or widen to
// all) by exporting CURSOR_MODELS before launching opencode.
if (process.env.CURSOR_MODELS === undefined) {
  process.env.CURSOR_MODELS = "composer-2.5";
}

const PORT = process.env.CURSOR_PROXY_PORT ? Number(process.env.CURSOR_PROXY_PORT) : DEFAULT_PORT;

export const CursorDirectPlugin: Plugin = async (
  _input: PluginInput,
): Promise<Hooks> => {
  // Start the proxy once at load. startCursorProxy reuses an existing healthy
  // proxy on the port (second opencode window, standalone serve). Failures
  // (e.g. missing Keychain tokens) are tolerated so opencode still boots.
  let port = PORT;
  let models: Array<{ id: string; name: string }> = [];
  let started = false;
  try {
    const res = await startCursorProxy(PORT);
    port = res.port;
    models = res.models;
    started = true;
  } catch (err) {
    console.error(
      `[cursor-oauth] proxy not started: ${err instanceof Error ? err.message : String(err)}`,
    );
  }

  return {
    /**
     * Inject the static provider so models promote without a loader.
     * buildProviderBlock applies the CURSOR_MODELS allow-list.
     */
    async config(config: any) {
      if (!started) return;
      config.provider = config.provider ?? {};
      if (!config.provider[PROVIDER_ID]) {
        config.provider[PROVIDER_ID] = buildProviderBlock(port, models);
      } else {
        // User has their own block — keep it, but ensure it can reach the proxy.
        const existing = config.provider[PROVIDER_ID];
        existing.options = existing.options ?? {};
        existing.options.baseURL ??= `http://127.0.0.1:${port}/v1`;
        existing.options.apiKey ??= "unused";
      }
    },
  };
};

export default CursorDirectPlugin;
