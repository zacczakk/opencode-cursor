# opencode-cursor-oauth

OpenCode plugin for Cursor models — **direct gRPC, no `cursor-agent` spawn**.
Talks straight to `api2.cursor.sh` over a persistent HTTP/2 connection, so every
turn skips the per-request process spawn that the `cursor-agent` CLI bridges pay.
Measured **~2-6x faster** than a `cursor-agent`-backed bridge in the OpenCode TUI.

> Fork note: upstream registered its provider through OpenCode's `auth.loader`
> hook. On OpenCode 1.17.8+ that provider never gets promoted into the resolved
> provider set, so the loader never fires and zero models appear. This fork
> sidesteps that entirely — see [How it works](#how-it-works).

## Requirements

- [OpenCode](https://opencode.ai) 1.17.8+
- [Bun](https://bun.sh) (OpenCode's plugin runtime)
- [Node.js](https://nodejs.org) >= 18 for the HTTP/2 bridge process
- Active [Cursor](https://cursor.com) subscription, logged in via the Cursor
  agent CLI: `cursor-agent login`
- **macOS** (auth reads Cursor's tokens from the Keychain — see [Auth](#auth))

## Install

This fork is used from a local build (not published to npm). Build it, then
symlink the plugin into OpenCode's plugin directory:

```sh
git clone https://github.com/ephraimduncan/opencode-cursor.git
cd opencode-cursor
git checkout fix/static-provider-fixed-port
bun install && bun run build

ln -sf "$PWD/dist/plugin.js" ~/.config/opencode/plugin/cursor-oauth.js
```

Add the plugin name to `~/.config/opencode/opencode.json`:

```jsonc
{
  "$schema": "https://opencode.ai/config.json",
  "plugin": ["cursor-oauth"]
}
```

That's all. **No provider block, no `opencode auth login`.** The plugin starts
the proxy on load and injects a static `@ai-sdk/openai-compatible` provider
(display name **Cursor**) via the `config` hook — that provider type always
promotes, so models resolve without the broken loader path.

## Auth

The plugin **borrows the tokens the Cursor agent CLI already stored** in the
macOS Keychain (`cursor-access-token` / `cursor-refresh-token`, account
`cursor-user`). Log in once with the CLI and the plugin reuses it:

```sh
cursor-agent login
```

Reading a token from the Keychain is not the same as spawning `cursor-agent`
per request — the latency win is fully intact. The access token is refreshed
automatically via the stored refresh token when it nears expiry.

> Linux/Windows are not yet supported (Keychain-only token read). The proxy and
> gRPC engine are cross-platform; only the token source needs a fallback.

## Use

Start OpenCode, pick the **Cursor** provider, select **Composer 2.5**.

By default only Composer 2.5 is shown. To change or widen the list:

```sh
# show a specific set
CURSOR_MODELS="composer-2.5,composer-2.5-fast,gpt-5.2" opencode

# show ALL discovered Cursor models
CURSOR_MODELS="" opencode
```

The proxy always *serves* every discovered model; `CURSOR_MODELS` only filters
what the selector advertises.

## Configuration

| Env var | Default | Effect |
|---|---|---|
| `CURSOR_PROXY_PORT` | `32125` | Fixed port the proxy binds / reuses |
| `CURSOR_MODELS` | `composer-2.5` | Comma-separated model ids shown in the selector; `""` = all |
| `CURSOR_PROXY_DEBUG` | unset | Log per-request model/size/timing to stderr |

Running two OpenCode windows is fine: the second detects the first's healthy
proxy via `GET /health` and reuses it instead of erroring.

## Standalone (no OpenCode)

The proxy can run on its own as an OpenAI-compatible endpoint:

```sh
CURSOR_PROXY_PORT=32125 bun src/standalone.ts serve      # run the proxy
bun src/standalone.ts gen-config                          # print an opencode.json provider block
```

## How it works

1. **Auth** — reads Cursor OAuth tokens from the macOS Keychain (put there by
   `cursor-agent login`); refreshes on expiry.
2. **Model discovery** — queries Cursor's gRPC API for available models.
3. **Local proxy** — translates `POST /v1/chat/completions` into Cursor's
   protobuf/HTTP/2 Connect protocol on a **fixed port**.
4. **Static provider injection** — the plugin's `config` hook registers a
   standard `@ai-sdk/openai-compatible` provider pointed at the proxy, so it
   promotes without OpenCode's broken `auth.loader` path.
5. **Native tool routing** — rejects Cursor's built-in filesystem/shell tools
   and exposes OpenCode's tool surface via Cursor MCP instead.

HTTP/2 transport runs through a Node child process (`h2-bridge.mjs`) because
Bun's `node:http2` support is not reliable against Cursor's API.

## Architecture

```
OpenCode  -->  /v1/chat/completions  -->  Bun.serve (proxy, fixed port)
                                               |
                                     Node child process (h2-bridge.mjs)
                                               |
                                      HTTP/2 Connect stream
                                               |
                                     api2.cursor.sh gRPC
                                       /agent.v1.AgentService/Run
```

### Tool call flow

```
1. Cursor model receives OpenAI tools via RequestContext (as MCP tool defs)
2. Model tries native tools (readArgs, shellArgs, etc.)
3. Proxy rejects each with typed error (ReadRejected, ShellRejected, etc.)
4. Model falls back to MCP tool -> mcpArgs exec message
5. Proxy emits OpenAI tool_calls SSE chunk, pauses H2 stream
6. OpenCode executes tool, sends result in follow-up request
7. Proxy resumes H2 stream with mcpResult, streams continuation
```

## Note on latency

The proxy is fast (single-digit seconds for a completion). If OpenCode turns
feel slow, the dominant cost is usually the **payload OpenCode sends** — the
full agent system prompt plus the resolved tool registry — which the model
must process upstream. A leaner agent profile (fewer tools, shorter prompt)
reduces that. This is an OpenCode-side concern, independent of the transport.

## Develop locally

```sh
bun install
bun run build
bun test/smoke.ts
```

## Requirements recap

- OpenCode 1.17.8+, Bun, Node >= 18, macOS, Cursor subscription (`cursor-agent login`).
</content>
