# orient-orchestrator

The orchestrator for [Orient](https://github.com/tungsten-united/project-description): the only service the phone talks to. It owns sessions, route state and stale-result dropping, calls each model in order, and streams guidance back over SSE.

It implements [`docs/contracts.md`](https://github.com/tungsten-united/project-description/blob/main/docs/contracts.md).

## Model

- **Client:** one phone from Start to Stop. It holds the token and the event stream (`/v1/clients/{id}/…`).
- **Session:** one spoken action, such as going to the counter. The phone sends audio, the orchestrator runs speech-to-text, then Jev picks the action. A different action from the current session starts a new session, with a new `sessionId`, a new generation, an empty frame buffer and no previous output. The same action keeps the current session.
- **Frame buffer:** each session keeps its last `NAV_FRAMES` (5) frames. The navigation engine gets all of them, oldest first, as repeated `frames` parts.
- **Worker:** one per client. It evaluates the newest frame, validates the navigation answer against the route, and compares the result with the session's previous output. It sends `guidance` only when the output changed (or after `REPEAT_MS` of the same output); otherwise it sends a quiet `heartbeat`.

## Run

```sh
cargo run                        # listens on 0.0.0.0:8000
NAV_URL=http://gpu-box:8001 cargo run
cargo test
```

## Debug locally

See the server's workflow live, without a phone or real models:

```sh
cargo run --example fakes    # fake navigation engine on :8001, fake speech-to-text on :8002
STT_URL=http://localhost:8002/stt DEBUG_PAGE=1 TRACE_PATH=trace.jsonl cargo run
open http://localhost:8000/debug
```

The page drives the real API. You can start a client, "say" a destination (the fake speech-to-text turns the audio bytes back into text), send frames by hand or on a timer, choose what the fake navigation engine answers, and press Stop or Retry. Next to the controls it shows the live events, coloured per session, with generation changes and ignored late events. It also shows the run trace, so you can see which outputs were spoken, which were kept quiet, and which were dropped. `/debug` exists only when `DEBUG_PAGE` is set.

## Deploy to Cloudflare

The same API runs as a Worker with one Durable Object per client. SQLite-backed Durable Objects work on the free plan.

```bash
npx wrangler login
npx wrangler secret put TYPESAFE_API_KEY   # optional
npx wrangler deploy --var NAV_URL:https://your-vla --var STT_URL:https://your-stt/stt
```

Or put the settings from the table below in `[vars]` in `wrangler.toml`. `NAV_URL` and `STT_URL` must be reachable from the internet. `wrangler dev` runs it locally on :8787.

## Configuration

| Env var | Default | Purpose |
| --- | --- | --- |
| `BIND` | `0.0.0.0:8000` | Listen address |
| `NAV_URL` | `http://localhost:8001` | Navigation engine (VLA), `POST /v1/navigate` |
| `NAV_ENGINE` | `vla` | Engine name shown in debug and trace |
| `TYPESAFE_API_KEY` | empty | Key for TypeSafe's Jev, used by the Command step. Server only. Empty: keyword matching |
| `JEV_URL` | `https://api.typesafe.ai/v1/systemone` | System One endpoint |
| `JEV_MODEL` | `jev-latest` | Jev model |
| `JEV_MIN_CONFIDENCE` | `0.5` | Below this, the command is treated as unclear and the user is asked again |
| `STT_URL` | empty | Speech-to-text service, `POST` with an `audio` part, returns `{"transcript"}`. Required: utterances return 503 without it |
| `NAV_FRAMES` | `5` | Frames per navigation call |
| `ROUTE_PATH` | built-in `src/route.json` | Route definition |
| `MAX_INPUT_AGE_MS` | `3000` | Reject older input |
| `MIN_CONFIDENCE` | `0.5` | Below this, the VLA answer becomes `wait` |
| `REPEAT_MS` | `7000` | The worker repeats unchanged guidance after this |
| `TRACE_PATH` | unset | Also append the run trace as JSON lines to this file |
| `ALLOW_ORIGINS` | `*` | CORS origins, comma separated |
| `DEBUG_PAGE` | unset | Serve the local debug page at `/debug` |

## Layout

- `src/client.rs`: clients and sessions, the API operations, SSE events, and the worker. Shared by both hosts.
- `src/server.rs`, `src/main.rs`: native axum server.
- `src/cloudflare.rs`, `wrangler.toml`: Cloudflare Worker plus one Durable Object per client.
- `tests/e2e.rs`: end-to-end test over HTTP and SSE, against the native server or any deployment (`E2E_BASE`).
- `src/pipeline.rs`: model calls (STT, Jev, VLA), route validation, the worker's comparison rule, and sentence templates.
- `src/route.json`: placeholder Itnig route. Replace it once S01 freezes the real route.

## How Jev is used

[Jev](https://docs.typesafe.ai/introduction) is a decision model. It takes a `state` and typed questions (Choice, Score, Noul) and returns calibrated answers. It does not generate text.

- **Command:** one Choice question per utterance. The state is `{spokenRequest, sessionPhase}`. The options are each destination ID plus `cancel` and `unsupported`. If `confidence` is below `JEV_MIN_CONFIDENCE`, the user is asked again.
- **Worker:** compares outputs in code. If that proves too rigid, this is a natural Noul question ("should the user be told something now?").
- **Sentences:** fixed templates, because Jev doesn't write sentences.

## Known limits

- The Jev call has not been run against the live API yet. The request and answer shapes come from the TypeSafe quickstart, and the parsing is unit-tested against them.
- 429 and 529 from TypeSafe are not retried. With a 3 s budget, the user is asked again instead.
- STT is any HTTP service returning `{"transcript": ...}` (open point 1).
- The first navigation calls of a session carry fewer than 5 frames.
- Sessions live in memory in one process and never expire. On Cloudflare each client lives in its own Durable Object's memory; if Cloudflare evicts it (no open event stream for a while, or a deploy), the phone gets 404 and must start a new client.
- On Cloudflare, `ROUTE_PATH`, `TRACE_PATH` and `DEBUG_PAGE` are not available: the route is the built-in one and the trace is only served by `GET .../trace`.
- The server checks audio and frame sizes in bytes only. The phone enforces `maxAudioMs` and `maxFrameEdgePx`.
- No rate limiting (`429`) yet.
