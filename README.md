# orient-orchestrator

The orchestrator for [Orient](https://github.com/tungsten-united/project-description): the only service the phone talks to. It owns sessions, route state and stale-result dropping, calls each model in order, and streams guidance back over SSE.

It implements [`docs/contracts.md`](https://github.com/tungsten-united/project-description/blob/main/docs/contracts.md).

## Run

```sh
cargo run                        # listens on 0.0.0.0:8000
NAV_URL=http://gpu-box:8001 cargo run
cargo test
```

## Configuration

| Env var | Default | Purpose |
| --- | --- | --- |
| `BIND` | `0.0.0.0:8000` | Listen address |
| `NAV_URL` | `http://localhost:8001` | Navigation engine (VLA), `POST /v1/navigate` |
| `NAV_ENGINE` | `vla` | Engine name shown in debug and trace |
| `JEV_BASE_URL` | empty | Jev API for the command and writer LLMs. Empty: keyword matching and sentence templates |
| `JEV_API_KEY` | empty | Jev API key. Server only |
| `JEV_MODEL` | empty | Model name sent to Jev |
| `STT_URL` | empty | Speech-to-text service. Empty: the phone must send a `transcript` |
| `ROUTE_PATH` | built-in `src/route.json` | Route definition |
| `MAX_INPUT_AGE_MS` | `3000` | Reject older input |
| `MIN_CONFIDENCE` | `0.5` | Below this, the VLA answer becomes `wait` |
| `REPEAT_MS` | `7000` | Decider repeats unchanged guidance after this |
| `TRACE_PATH` | unset | Also append the run trace as JSON lines to this file |
| `ALLOW_ORIGINS` | `*` | CORS origins, comma separated |

## Layout

- `src/main.rs`: HTTP API, sessions, SSE, the navigation loop, and the integration test.
- `src/pipeline.rs`: model calls (STT, Jev, VLA), route validation, the rules decider, and sentence templates.
- `src/route.json`: placeholder Itnig route. Replace it once S01 freezes the real route.

## Known limits

- The Jev adapter assumes an OpenAI-compatible `/chat/completions` endpoint with JSON mode. That is a guess until the Jev docs are in. Only `Pipeline::jev` changes when they are.
- STT is any HTTP service returning `{"transcript": ...}` (open point 1).
- Sessions live in memory in one process and never expire.
- The server checks audio and frame sizes in bytes only. The phone enforces `maxAudioMs` and `maxFrameEdgePx`.
- No rate limiting (`429`) yet.
