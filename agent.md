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
cargo run --example fakes                   # fake STT + navigation on :8001; with DEBUG_PAGE=1 see /debug (README)
```

CI (`.github/workflows/ci.yml`) runs `cargo fmt --check`, clippy with `-D warnings`, and `cargo test` on every push and PR. Keep all three green.

The end-to-end test also runs against a deployment: `E2E_BASE=https://... cargo test --test e2e`. The fakes then bind :8101 (navigation) and :8102 (ElevenLabs), so point the deployment's `NAV_URL` and `ELEVENLABS_URL` there.

Without `TYPESAFE_API_KEY`, the command step falls back to keyword matching on the route aliases. Speech to text is ElevenLabs Scribe (contracts.md section 4): inputs return 503 without `ELEVENLABS_API_KEY`, unless `ELEVENLABS_URL` points at fakes. The end-to-end test starts fake STT and navigation servers on random ports, so it needs no network.

## Navigation model and deployment

**The navigation engine (nav-engine) does not run in this service.** The orchestrator calls nav-api (Cloud Run, `europe-southwest1`) at `NAV_URL` (`.../api/v1`): `POST /maps/{NAV_MAP_ID}/localize` and `POST /maps/{NAV_MAP_ID}/route` (contracts.md section 2), with nav-api's own token as `NAV_API_TOKEN` (Secret Manager `nav-api-token`). Each `localize` call uploads up to 4 frames (`NAV_FRAMES`), which makes it the main source of network egress cost and latency: it has a 4 s timeout, `route` 2 s (`pipeline.rs`), and three failures in a row end guidance with an error. A call takes about 100 ms, so the phone sets the pace: it sends the next frame `limits.frameGapMs` (`FRAME_GAP_MS`) after each upload, at most `limits.maxFrameEdgePx` (`MAX_FRAME_EDGE_PX`) wide. The first frame after nav-infer restarts can get a 503 while MegaLoc loads. `NAV_TRUST` (default `observed`) is the edge trust sent to `route`: Itnig has no verified edges yet. Destination ids in `route.json` are map node ids, passed through unchecked: a wrong one is a 404 from `route`. Do not assume the model shares a network, region or credentials with the orchestrator, and don't move work to it without asking. Speech in and out is ElevenLabs (`ELEVENLABS_URL`, default the real API).

**Staging runs on Cloud Run and is released by hand only** (`.github/workflows/deploy-staging.yml`, `workflow_dispatch`). Never add an automatic deploy. One image (`Dockerfile`) holds the server and the fakes (`examples/fakes.rs`, `--command fakes`). Staging uses the real `NAV_URL` (nav-api) and ElevenLabs API by default; `fake_nav` and `fake_speech` switch each to the fakes for one release. Because clients live in memory, the server service must keep `--max-instances 1`, `--no-cpu-throttling` (the worker runs between requests) and a long `--timeout` (SSE). Staging also sets `TRACE_STDOUT`, so every trace entry, stamped with `commit`, `version` and `routeId`, lands in Cloud Logging: look there first when a developer reports a problem (queries in README.md, "Session logs"). Staging is used only by developers, so traces may hold transcripts. The debug page (`DEBUG_PAGE`) is on in staging; its fake navigation URL defaults to the server's `NAV_URL`. Setup: README.md, "Staging on Cloud Run". Test the image locally with Podman (there is no Docker on the dev machine): `podman build -t orient-orchestrator:local .`, then `podman run --rm -p 8080:8080 orient-orchestrator:local`. The Podman VM is arm64 while CI builds amd64.

## Credentials and accounts

Never print, log or commit a key. Read secrets from `.env` or Secret Manager inside a subshell and pass them through stdin or env, as below.

**Local runs.** `.env` (gitignored, never committed) holds `ELEVENLABS_API_KEY` and `ELEVENLABS_VOICE_ID`. The server does not read `.env` itself; load it into the shell first:

```bash
set -a; source .env; set +a
cargo run --example fakes                          # fake navigation on :8001
NAV_URL=http://localhost:8001 DEBUG_PAGE=1 cargo run   # real ElevenLabs: leave ELEVENLABS_URL unset
```

The debug page's Say box sends typed text as fake audio, which only the fakes understand. To test real Scribe, send a recorded clip (`ffmpeg -f avfoundation -i ":0" -t 3 -c:a libopus say.webm`) as the `audio` part of `POST /inputs`.

**ElevenLabs account.** It is on the free plan, verified live on 2026-10-10 (Scribe 370 ms, Flash first byte about 0.4 s):

- Free accounts cannot use library voices through the API: ElevenLabs answers `402 paid_plan_required`, and `GET /speech` returns 503. Only built-in (`premade`) voices work. The voice in use is Lily, `pFZP5JQG7iQjIQuC4Bku`. The final voice is the S06 owner's call; a library voice needs at least $5 of credits.
- The key lacks the `user_read` permission, so it cannot read the account's quota (`/v1/user/subscription`).

**Google Cloud (`tungsten-united`, project number 613464313064, region `europe-west1`).** Shared with tungsten-united/project-description, which created it with its `infra/gcp/setup.sh`. Reuse these resources, don't create parallel ones:

| Resource | Name | Notes |
| --- | --- | --- |
| Workload Identity pool / provider | `github` / `github-oidc` | Attribute condition limits which GitHub repos may sign in |
| Deploy service account | `orient-deployer` | `run.admin`, `artifactregistry.writer`, may act as the runtime account only |
| Runtime service account | `orient-orchestrator` | `secretmanager.secretAccessor` and `logging.logWriter` on the project |
| Artifact Registry | `orient` (Docker, `europe-west1`) | |
| Secret | `ELEVENLABS_API_KEY` | The ElevenLabs key. Created from `.env` |
| Secret | `TYPESAFE_API_KEY` | The TypeSafe (Jev) key, mounted on every release |

The developer account has `roles/editor`. It can create secrets and versions, but cannot read secret values or change IAM. Granting this repo access to `orient-deployer` (the provider condition and a `workloadIdentityUser` binding) needs a project owner; the commands are in the README section "Staging on Cloud Run". Rotate the ElevenLabs key by adding a version, which staging picks up as `latest` on its next release:

```bash
set -a; source .env; set +a
printf '%s' "$ELEVENLABS_API_KEY" | gcloud secrets versions add ELEVENLABS_API_KEY --data-file=- --project tungsten-united
```

**GitHub environment `staging`.** Variables only, no secrets (Workload Identity Federation needs no key): `GCP_PROJECT_ID`, `GCP_REGION`, `GCP_WIF_PROVIDER`, `GCP_DEPLOY_SA`, `GCP_RUNTIME_SA`, `ELEVENLABS_SECRET` (the secret's name, `ELEVENLABS_API_KEY`), `ELEVENLABS_VOICE_ID`, `NAV_URL` (nav-api's `.../api/v1`), `NAV_API_SECRET` (`nav-api-token`) and `ALLOW_ORIGINS` (`https://orient.harshdeepsingh.dev`, the web app). Set them with `gh variable set <NAME> --env staging`. The real ElevenLabs key is used only on releases with `fake_speech` unticked; `fake_nav` is separate.

## Architecture

`src/client.rs` holds the state, the API operations (`input`, `frames`, `stop`, `retry`, `trace`, the SSE `events` stream) and the worker, with no HTTP framework. `src/pipeline.rs` holds the model calls and pure decision rules, and never touches state. `src/server.rs` (axum, run by `src/main.rs`) only parses requests, calls `client`, and writes responses. Behaviour changes go in `client.rs`. `src/route.json` lists the destinations: node ids on nav-api's `itnig` map.

**Two levels of state.** A `Client` is one phone from Start to Stop. It owns the token, the open SSE streams (`Events`), and the `generation`. A `Session` is one spoken action (one destination). It owns the user's last confirmed map node (carried into the next session), a buffer of the last `NAV_FRAMES` (4) frames, and the previous navigation output. When speech-to-text plus Jev yields a different destination, `start_session` replaces the session. The same destination keeps it.

**`generation` is the stale-result guard.** `Client::reset` increments it. In-flight model calls are not aborted: they finish and their results are dropped. Stop, retry, errors and every new session call it, so the first session already runs at generation 2. Every async step snapshots the generation before awaiting a model and re-checks it after. If it changed, the result is logged as `dropped` and never emitted. Keep this pattern in any new async path.

**Locking.** Each client is an `Arc<std::sync::Mutex<Client>>`. Never hold the guard across an `.await`: snapshot what you need, release, await, then re-lock and re-check the generation.

**Flow:**

1. `input` checks the request, then spawns `handle_input`: STT, then `pipeline.command` (one Jev Choice question), then start or keep a session, cancel, or `needs_input`.
2. `frames` pushes each frame into the session buffer via `submit_frame`. Only the newest frame is evaluated (`pending`).
3. A single `worker` task per client and generation (`Client.worker` holds its generation) loops over pending frames. A worker from an older generation exits at its next turn. `evaluate` runs the navigation loop (contracts.md section 2, `advance` in `client.rs`): `localize` with the frames not sent yet and their `motion`; locating, `NAV_VOTE_K` of the last `NAV_VOTE_N` votes (or a `confirmed` result) place the user, then one `route`; following, the hop's target reached on `NAV_VOTE_K` votes, the next hop's output spoken; `NAV_VOTE_K` votes for another node (only a `confirmed` result casts one) or `NAV_LOST_MS` with no result pointing at the hop (`lost`, or another node leading) start over. `pipeline.hop_output` turns a hop into an `Output` (`turn` for turn_left/right/around, else `continue`, instruction cut to 240 characters).
4. `pipeline.should_speak` compares the output with the session's previous output. The first output and `arrived` are always spoken. A different output goes to Jev (`pipeline.worth_saying`), which can keep it quiet; without a TypeSafe key it is spoken. Spoken outputs become a `guidance` event with the hop's `instruction` or a template sentence. The same output becomes a quiet `heartbeat`; nothing is repeated on a timer. Asking for the same destination again clears the previous output, so the next output is spoken.
5. `GET /speech` (`client::speech`) turns any text the phone speaks into ElevenLabs Flash audio, with that text echoed in the `X-Speech-Text` header, streamed through and cached in memory by text once a stream completes. Failures are a 503, and the phone falls back to browser TTS, so speech never blocks guidance.

**Jev** (TypeSafe, `docs.typesafe.ai`) is a decision model. It answers typed questions (Choice, Score, Noul) and never generates text. That is why sentences come from the path or templates, and Jev only answers speak or quiet. The Jev call has not been run against the live API yet.

**Idempotency and ordering:** responses are cached by `requestId`. `sequence` must increase per client. Input older than `maxInputAgeMs` is rejected. Stop and retry accept any generation.

Deliberate shortcuts are marked with `ponytail:` comments, for example in-memory clients. Known limits are listed in README.md.
