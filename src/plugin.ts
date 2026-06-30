/**
 * OpenCode plugin: Cursor models via DIRECT gRPC (no cursor-agent spawn).
 *
 * This is the WORKING replacement for the dead auth-loader plugin (./index.ts).
 * On opencode 1.17.8+ a provider registered through the `auth.loader` hook never
 * gets promoted into the resolved provider set, so the loader never fires. We
 * sidestep that entirely:
 *
 *   1. On plugin load, start the Cursor gRPC→OpenAI proxy on a FIXED port
 *      (CURSOR_PROXY_PORT, default 32125), authenticated with the tokens
 *      cursor-agent already stored in the macOS Keychain.
 *   2. In the `config` hook, inject a STATIC `@ai-sdk/openai-compatible`
 *      provider pointed at that proxy. That provider type ALWAYS promotes, so
 *      `cursor-oauth/<model>` resolves with no loader involved.
 *
 * Result: models appear automatically (no hand-edited opencode.json), and every
 * turn hits a persistent HTTP/2 connection to api2.cursor.sh instead of spawning
 * a fresh cursor-agent process. Warm latency ~2-3x better than the CLI bridge.
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

const PORT = process.env.CURSOR_PROXY_PORT ? Number(process.env.CURSOR_PROXY_PORT) : DEFAULT_PORT;

export const CursorDirectPlugin: Plugin = async (
  _input: PluginInput,
): Promise<Hooks> => {
  // Start the proxy once at load. startCursorProxy is idempotent: if another
  // workspace/instance already bound the port, startProxy returns it. We tolerate
  // failures (e.g. missing Keychain tokens) so opencode still boots — the provider
  // just won't have a live backend until `cursor-agent login` is run.
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
     * Merge (don't overwrite) any existing provider config the user has.
     */
    async config(config: any) {
      if (!started) return;
      config.provider = config.provider ?? {};
      // Respect a user-defined cursor-oauth block if present; otherwise inject.
      if (!config.provider[PROVIDER_ID]) {
        config.provider[PROVIDER_ID] = buildProviderBlock(port, models);
      } else {
        // Keep user's settings but ensure baseURL points at the live port.
        const existing = config.provider[PROVIDER_ID];
        existing.options = existing.options ?? {};
        existing.options.baseURL ??= `http://127.0.0.1:${port}/v1`;
        existing.options.apiKey ??= "unused";
        if (!existing.models || Object.keys(existing.models).length === 0) {
          existing.models = Object.fromEntries(models.map((m) => [m.id, { name: m.name }]));
        }
      }
    },
  };
};

export default CursorDirectPlugin;
