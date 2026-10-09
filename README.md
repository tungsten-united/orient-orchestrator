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
| `TYPESAFE_API_KEY` | empty | Key for TypeSafe's Jev, used by the Command step. Server only. Empty: keyword matching |
| `JEV_URL` | `https://api.typesafe.ai/v1/systemone` | System One endpoint |
| `JEV_MODEL` | `jev-latest` | Jev model |
| `JEV_MIN_CONFIDENCE` | `0.5` | Below this, the command is treated as unclear and the user is asked again |
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

## How Jev is used

[Jev](https://docs.typesafe.ai/introduction) is a decision model. It takes a `state` and typed questions (Choice, Score, Noul) and returns calibrated answers. It does not generate text.

- **Command:** one Choice question per utterance. The state is `{spokenRequest, sessionPhase}`. The options are each destination ID plus `cancel` and `unsupported`. If `confidence` is below `JEV_MIN_CONFIDENCE`, the user is asked again.
- **Utterance decider:** rules for now. If the rules prove too rigid, this is a natural Noul question ("should the user be told something now?").
- **Utterance writer:** fixed sentence templates, because Jev doesn't write sentences.

## Known limits

- The Jev call has not been run against the live API yet. The request and answer shapes come from the TypeSafe quickstart, and the parsing is unit-tested against them.
- 429 and 529 from TypeSafe are not retried. With a 3 s budget, the user is asked again instead.
- STT is any HTTP service returning `{"transcript": ...}` (open point 1).
- Sessions live in memory in one process and never expire.
- The server checks audio and frame sizes in bytes only. The phone enforces `maxAudioMs` and `maxFrameEdgePx`.
- No rate limiting (`429`) yet.
