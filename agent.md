# orient-orchestrator: agent guidance

The backend for Orient, a voice-first indoor guidance prototype for blind and low-vision people. This service is the only thing the phone talks to. Product scope, constraints and the Definition of Done live in [project-description/agent.md](https://github.com/tungsten-united/project-description/blob/main/agent.md). It is a supervised hackathon prototype, not proven safe navigation: never invent route progress.

## Source of truth

[project-description/docs/contracts.md](https://github.com/tungsten-united/project-description/blob/main/docs/contracts.md) defines every HTTP shape, SSE event and model call. [docs/architecture.md](https://github.com/tungsten-united/project-description/blob/main/docs/architecture.md) has the diagrams. To change behaviour, update the contract first, then this code, then tell the web app owner (`apps/web` in project-description). The web app currently lags behind the contract.

## Commands

```bash
cargo run                                   # 0.0.0.0:8000; env vars in README.md
cargo test                                  # unit tests + one end-to-end test
cargo test validate_only                    # tests whose name matches
cargo test --test e2e                       # just the end-to-end test (HTTP + SSE)
cargo clippy --all-targets && cargo fmt     # keep both clean
cargo run --example fakes                   # fake STT + VLA; with DEBUG_PAGE=1 see /debug (README)
```

The end-to-end test also runs against a deployment: `E2E_BASE=https://... cargo test --test e2e`. The fakes then bind :8101 (navigation) and :8102 (STT), so point the deployment's `NAV_URL` and `STT_URL` there.

Without `TYPESAFE_API_KEY`, the command step falls back to keyword matching on the route aliases. `STT_URL` is required: utterances return 503 without it. The end-to-end test starts fake STT and navigation servers on random ports, so it needs no network.

## Architecture

`src/client.rs` holds the state, the API operations (`utterance`, `frames`, `stop`, `retry`, `trace`, the SSE `events` stream) and the worker, with no HTTP framework. `src/pipeline.rs` holds the model calls and pure decision rules, and never touches state. `src/server.rs` (axum, run by `src/main.rs`) only parses requests, calls `client`, and writes responses. Behaviour changes go in `client.rs`. `src/route.json` is a placeholder route until S01 freezes the real one.

**Two levels of state.** A `Client` is one phone from Start to Stop. It owns the token, the open SSE streams (`Events`), and the `generation`. A `Session` is one spoken action (one destination). It owns the route step, a buffer of the last `NAV_FRAMES` (5) frames, and the previous navigation output. When speech-to-text plus Jev yields a different destination, `start_session` replaces the session. The same destination keeps it.

**`generation` is the stale-result guard.** `Client::reset` increments it. In-flight model calls are not aborted: they finish and their results are dropped. Stop, retry, errors and every new session call it, so the first session already runs at generation 2. Every async step snapshots the generation before awaiting a model and re-checks it after. If it changed, the result is logged as `dropped` and never emitted. Keep this pattern in any new async path.

**Locking.** Each client is an `Arc<std::sync::Mutex<Client>>`. Never hold the guard across an `.await`: snapshot what you need, release, await, then re-lock and re-check the generation.

**Flow:**

1. `utterance` checks the request, then spawns `handle_utterance`: STT, then `pipeline.command` (one Jev Choice question), then start or keep a session, cancel, or `needs_input`.
2. `frames` pushes each frame into the session buffer via `submit_frame`. Only the newest frame is evaluated (`pending`).
3. A single `worker` task per client and generation (`Client.worker` holds its generation) loops over pending frames. A worker from an older generation exits at its next turn. `evaluate` sends the whole buffer to the VLA, then `pipeline.validate` turns the answer into an `Output`. The step can only move to the next step on the route. Anything else becomes `wait`, and low confidence becomes an uncertain `wait`.
4. `pipeline.should_speak` compares the output with the session's previous output. A different output becomes a `guidance` event with a template sentence. The same output becomes a quiet `heartbeat`, except for a reminder after `REPEAT_MS`.

**Jev** (TypeSafe, `docs.typesafe.ai`) is a decision model. It answers typed questions (Choice, Score, Noul) and never generates text. That is why sentences are templates and the speak-or-stay-quiet rule is code. The Jev call has not been run against the live API yet.

**Idempotency and ordering:** responses are cached by `requestId`. `sequence` must increase per client. Input older than `maxInputAgeMs` is rejected. Stop and retry accept any generation.

Deliberate shortcuts are marked with `ponytail:` comments: in-memory clients, a generic STT endpoint. Known limits are listed in README.md.
