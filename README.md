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
cargo run --example fakes    # fake navigation engine and speech-to-text, both on :8001
STT_URL=http://localhost:8001/stt DEBUG_PAGE=1 TRACE_PATH=trace.jsonl cargo run
open http://localhost:8000/debug
```

The page drives the real API. You can start a client, "say" a destination (the fake speech-to-text turns the audio bytes back into text), send frames by hand or on a timer, choose what the fake navigation engine answers, and press Stop or Retry. Next to the controls it shows the live events, coloured per session, with generation changes and ignored late events. It also shows the run trace, so you can see which outputs were spoken, which were kept quiet, and which were dropped. `/debug` exists only when `DEBUG_PAGE` is set.

## Staging on Cloud Run

Staging is released by hand: **Actions > Deploy staging > Run workflow**, then pick a branch or tag. The workflow runs the CI checks, builds one image (`Dockerfile`: the server, plus the fakes), pushes it to Artifact Registry, and deploys two Cloud Run services:

- `orient-fakes-staging`: the fake navigation engine and STT. Only when the **fakes** box is ticked (the default).
- `orient-orchestrator-staging`: the server with `DEBUG_PAGE=1`, pointed at the fakes, or at the `NAV_URL` and `STT_URL` of the `staging` environment when the box is unticked. Share `<url>/debug` with the team. `/v1/health` shows the deployed commit.

Both services are public. Anyone with the URL can use the debug page and change the fake answers. Each release drops the clients in memory: connected phones get 404 and start a new client.

The server runs with one instance (clients live in memory), `--no-cpu-throttling` (the frame worker runs between requests) and a 60 min timeout (SSE). It scales to zero when idle, which keeps it within the free tier but loses live clients after about 15 idle minutes.

### One-time setup

```bash
PROJECT=your-project REGION=europe-west1 REPO=tungsten-united/orient-orchestrator
gcloud services enable run.googleapis.com artifactregistry.googleapis.com iamcredentials.googleapis.com --project $PROJECT
gcloud artifacts repositories create orient --repository-format docker --location $REGION --project $PROJECT

# Deploy identity for GitHub Actions, without a stored key (Workload Identity Federation).
gcloud iam service-accounts create github-deploy --project $PROJECT
SA=github-deploy@$PROJECT.iam.gserviceaccount.com
for role in roles/run.admin roles/artifactregistry.writer roles/iam.serviceAccountUser; do
  gcloud projects add-iam-policy-binding $PROJECT --member serviceAccount:$SA --role $role
done
gcloud iam workload-identity-pools create github --location global --project $PROJECT
gcloud iam workload-identity-pools providers create-oidc github --location global --project $PROJECT \
  --workload-identity-pool github --issuer-uri https://token.actions.githubusercontent.com \
  --attribute-mapping google.subject=assertion.sub,attribute.repository=assertion.repository \
  --attribute-condition "assertion.repository=='$REPO'"
POOL=$(gcloud iam workload-identity-pools describe github --location global --project $PROJECT --format 'value(name)')
gcloud iam service-accounts add-iam-policy-binding $SA --project $PROJECT --role roles/iam.workloadIdentityUser \
  --member "principalSet://iam.googleapis.com/$POOL/attribute.repository/$REPO"
echo "GCP_WIF_PROVIDER=$POOL/providers/github"
```

Then, in GitHub, create the environment `staging` (Settings > Environments) with these variables:

| Variable | Value |
| --- | --- |
| `GCP_PROJECT_ID`, `GCP_REGION` | as above |
| `GCP_WIF_PROVIDER` | printed by the last command |
| `GCP_DEPLOY_SA` | `github-deploy@<project>.iam.gserviceaccount.com` |
| `GCP_RUNTIME_SA` | service account the services run as. The deploy account needs `roles/iam.serviceAccountUser` on it |
| `NAV_URL`, `STT_URL` | optional: the real models, for releases without fakes |
| `ALLOW_ORIGINS` | optional: the web app's staging origin (default `*`) |
| `TYPESAFE_SECRET` | optional: name of a Secret Manager secret holding the TypeSafe key. Grant the Cloud Run runtime service account `roles/secretmanager.secretAccessor` on it |

### Session logs

Staging prints every trace entry to Cloud Logging. Each entry has `clientId`, `sessionId`, `requestId`, `kind`, the step's outcome and timings, and which app produced it: `commit`, `version` and `routeId`. Find them in Logs Explorer, or:

```bash
# One session
gcloud logging read 'resource.labels.service_name="orient-orchestrator-staging" AND jsonPayload.sessionId="<id>"' \
  --project tungsten-united --order asc --format 'value(jsonPayload)'
# Everything one release produced
gcloud logging read 'resource.labels.service_name="orient-orchestrator-staging" AND jsonPayload.commit="<sha>"' \
  --project tungsten-united --freshness 7d --format json
```

Logs are kept 30 days.

To require approval before each release, add required reviewers to the `staging` environment. If your organization blocks public services (`allUsers`), `--allow-unauthenticated` fails; ask an org admin, or put the services behind IAP.

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
| `DEBUG_PAGE` | unset | Serve the debug page at `/debug` (local runs and staging) |
| `GIT_SHA` | unset | Commit shown by `/v1/health` and stamped on every trace entry; set by the staging deploy |
| `TRACE_STDOUT` | unset | Also print every trace entry as one JSON line (stored by Cloud Logging on Cloud Run); on in staging |

## Layout

- `Dockerfile`, `.github/workflows/`: CI checks on every push, and the manual staging release.
- `examples/fakes.rs`: fake navigation engine and STT, for local runs and staging.
- `src/client.rs`: clients and sessions, the API operations, SSE events, and the worker.
- `src/server.rs`, `src/main.rs`: axum HTTP server.
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
- Sessions live in memory in one process and never expire.
- The server checks audio and frame sizes in bytes only. The phone enforces `maxAudioMs` and `maxFrameEdgePx`.
- No rate limiting (`429`) yet.
