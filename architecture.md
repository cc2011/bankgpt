# BankGPT — Architecture (Car-Purchase Durable Workflow)

Architecture reference for **BankGPT**, a car-buying workflow built on **Hatchet durable tasks** with **event-driven human-in-the-loop (HITL)** pauses. FastAPI serves the public/mock API; a Hatchet worker runs the durable orchestrator and its child tasks; a **local Ollama LLM** performs car research; dealer and bank calls are mocked.

The durable task **only orchestrates and waits**. All I/O (LLM calls, mock APIs) lives in child tasks, keeping the orchestrator deterministic and safely replayable.

---

## Components

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              EXTERNAL                                        │
│  ┌──────────────────────┐              ┌──────────────────────┐           │
│  │  Buyer / UI client     │              │  Dealer + Bank portals │           │
│  │  (starts purchase)     │              │  (mock event senders)  │           │
│  └───────────┬────────────┘              └───────────┬──────────┘           │
└──────────────┼─────────────────────────────────────────┼────────────────────┘
               │                                         │
               ▼                                         ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         PUBLIC LAYER (FastAPI — main.py)                     │
│                                                                              │
│   POST /purchase/start        POST /mock/dealer-accept                       │
│   (UUID + trigger buy-car)    POST /mock/bank-approve                        │
│                               POST /mock/bank-deny                           │
│                                                                              │
│   Validates JSON · creates purchase_id · pushes Hatchet events              │
└───────────────┬──────────────────────────────────────────────┬─────────────┘
                │ aio_run_no_wait(buy_car)        event.push(scope=purchase:{id})
                ▼                                                ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                              HATCHET ENGINE                                    │
│                              (durable state + event router)                    │
│                                                                                │
│   ┌──────────────────────────────────────────────────────────────────────┐ │
│   │  car-buyer-worker (worker.py)                                          │ │
│   │                                                                        │ │
│   │   buy-car  (durable_task orchestrator)                                 │ │
│   │     ├─ spawn batch: research-car ║ get-dealer-offer   (parallel)       │ │
│   │     ├─ wait_for_event: dealer:offer:accepted          (HITL 1)         │ │
│   │     ├─ spawn: submit-loan-app                                          │ │
│   │     └─ wait_for_event: bank:loan:approved             (HITL 2)         │ │
│   │                                                                        │ │
│   │   children:  research-car   get-dealer-offer   submit-loan-app         │ │
│   └───────────────┬──────────────────────────────────────────────────────┘ │
└───────────────────┼────────────────────────────────────────────────────────┘
                    │ research-car
                    ▼
          ┌───────────────────────┐
          │  Ollama (local LLM)     │
          │  deepseek-r1:1.5b       │
          │  3-bullet car tips      │
          └───────────────────────┘
```

`get-dealer-offer` and `submit-loan-app` are **mock** integrations (no external bank/dealer system); they compute deterministic-ish results in-process.

---

## Key idea: durable orchestration + event waits (HITL)

The `buy-car` durable task drives the whole purchase but performs **no direct I/O**. It spawns child tasks for work and **suspends on `aio_wait_for_event`** at each human checkpoint. The worker slot is freed while suspended — the workflow resumes only when a matching event arrives.

Events are routed by **scope** so each purchase resumes independently:

```
scope = f"purchase:{purchase_id}"        # purchase_scope() in worker.py
filter = f"input.purchase_id == '{purchase_id}'"
lookback_window = 1 hour
```

FastAPI's `/mock/*` endpoints push these events; the durable task is the consumer.

```
  FastAPI (/mock/dealer-accept)              buy-car durable task
       │                                            │
       │  event.push("dealer:offer:accepted",       │  (suspended on
       │     {purchase_id, agreed_price},            │   aio_wait_for_event)
       │      scope="purchase:<id>")                 │
       │───────────────────► Hatchet ───────────────►│  resumes
       │                                            │  reads agreed_price
       ▼                                            ▼
```

**Why this works:** Hatchet persists durable-task state. A pause can last seconds or hours; on the matching scoped event the orchestrator resumes exactly where it left off. No polling loop, no Redis, no extra messaging layer.

---

## System flow (high level)

```
  BUYER              FASTAPI            HATCHET           WORKER            OLLAMA
   │                    │                  │                 │                 │
   │ 1. POST            │                  │                 │                 │
   │   /purchase/start  │                  │                 │                 │
   │───────────────────►│                  │                 │                 │
   │                    │ 2. trigger       │                 │                 │
   │                    │   buy-car        │                 │                 │
   │                    │ (aio_run_no_wait)│                 │                 │
   │                    │─────────────────►│                 │                 │
   │ 3. { purchase_id } │                  │ 4. dispatch     │                 │
   │◄───────────────────│                  │────────────────►│                 │
   │                    │                  │                 │ 5. research +   │
   │                    │                  │                 │   dealer offer  │
   │                    │                  │                 │────────────────►│
   │                    │                  │                 │◄── 3 tips ──────│
   │                    │                  │   (suspend on dealer:offer:accepted)│
   │                    │                  │                 │                 │
   │ 6. POST            │                  │                 │                 │
   │   /mock/dealer-    │ 7. event.push    │                 │                 │
   │      accept        │─────────────────►│ 8. resume ─────►│                 │
   │───────────────────►│                  │                 │ 9. submit-loan  │
   │                    │                  │   (suspend on bank:loan:approved)  │
   │ 10. POST           │                  │                 │                 │
   │   /mock/bank-      │ 11. event.push   │                 │                 │
   │      approve|deny  │─────────────────►│ 12. resume ────►│                 │
   │───────────────────►│                  │                 │ 13. complete /  │
   │                    │                  │                 │     deny        │
   ▼                    ▼                  ▼                 ▼                 ▼
```

---

## Step-by-step flow

### Phase A — Start purchase (short HTTP, connection closes)

```
Step 1   Buyer sends purchase request
         POST /purchase/start
         Body: { buyer_name, car_type, max_budget }

Step 2   FastAPI validates + assigns ID
         • parse_json_body() validates against StartPurchaseRequest
         • purchase_id = uuid4()

Step 3   Trigger durable workflow (fire-and-forget)
         worker.buy_car.aio_run_no_wait(BuyCarInput(...))
         • FastAPI does NOT wait for the workflow

Step 4   Return immediately
         { status: "started", purchase_id, message: "...call /mock/dealer-accept" }
         • POST connection closes here
```

### Phase B — Research + dealer offer (parallel child tasks)

```
Step 5   buy-car spawns a batch (aio_run_many):
           • research-car      → Ollama deepseek-r1:1.5b, 3 concise tips
                                  (temperature 0.2, num_predict 200 to cap tokens)
           • get-dealer-offer  → mock: offer_price = budget * (1 + 5–15% markup)

Step 6   Orchestrator collects both results and logs the dealer offer.
```

### Phase C — HITL 1: buyer accepts dealer price

```
Step 7   buy-car suspends on aio_wait_for_event("dealer:offer:accepted")
           scope = purchase:<id>,  filter input.purchase_id == '<id>'

Step 8   Buyer/dealer portal → POST /mock/dealer-accept { purchase_id, agreed_price }
         FastAPI → hatchet.event.push("dealer:offer:accepted", {...}, scope=...)

Step 9   Workflow resumes; agreed_price taken from the event
         (falls back to dealer offer_price if absent).
```

### Phase D — Submit loan application (child task)

```
Step 10  down_payment = 20% of agreed_price
         buy-car runs submit-loan-app (mock bank):
           • application_id = LOAN-<first 8 of purchase_id, upper>
           • amortized monthly payment over 60 months, base APR 6.7%
```

### Phase E — HITL 2: bank approves or denies

```
Step 11  buy-car suspends on aio_wait_for_event("bank:loan:approved")

Step 12  Bank system → one of:
           POST /mock/bank-approve { purchase_id, final_apr }   → approved=true
           POST /mock/bank-deny    { purchase_id, reason }      → approved=false
         (Both push the SAME event name "bank:loan:approved"; the approved flag differs.)

Step 13  Workflow resumes:
           • approved=false → return { status: "loan_denied", purchase_id }
           • approved=true  → return full purchase summary (car, price, loan, APR,
                              research_summary, research_model)
```

---

## Tasks and their roles

```
┌─────────────────────┬──────────────┬────────────────────────────────────────┐
│ Task                │ Type         │ Role                                     │
├─────────────────────┼──────────────┼────────────────────────────────────────┤
│ buy-car             │ durable_task │ Orchestrator: spawns children, waits on  │
│                     │              │ HITL events. No direct I/O. 3-min timeout│
├─────────────────────┼──────────────┼────────────────────────────────────────┤
│ research-car        │ task         │ Ollama (deepseek-r1:1.5b) → 3 buying tips│
├─────────────────────┼──────────────┼────────────────────────────────────────┤
│ get-dealer-offer    │ task         │ Mock dealer: price 5–15% above budget    │
├─────────────────────┼──────────────┼────────────────────────────────────────┤
│ submit-loan-app     │ task         │ Mock bank: app ID + amortized payment     │
└─────────────────────┴──────────────┴────────────────────────────────────────┘
```

---

## Events

```
┌──────────────────────────┬─────────────────────────┬──────────────────────────┐
│ Event                    │ Pushed by               │ Payload                  │
├──────────────────────────┼─────────────────────────┼──────────────────────────┤
│ dealer:offer:accepted    │ POST /mock/dealer-accept │ purchase_id, agreed_price│
├──────────────────────────┼─────────────────────────┼──────────────────────────┤
│ bank:loan:approved       │ POST /mock/bank-approve  │ purchase_id, approved,   │
│ (approved=true)          │                         │ final_apr                │
├──────────────────────────┼─────────────────────────┼──────────────────────────┤
│ bank:loan:approved       │ POST /mock/bank-deny     │ purchase_id, approved,   │
│ (approved=false)         │                         │ reason                   │
└──────────────────────────┴─────────────────────────┴──────────────────────────┘

All events are scoped to purchase:<purchase_id> and filtered by purchase_id,
so concurrent purchases never cross-trigger.
```

---

## Configuration

```
config.py        Loads .env via python-dotenv; requires HATCHET_CLIENT_TOKEN
                 (raises ValueError if missing)
worker.py        LOCAL_MODEL = "deepseek-r1:1.5b"  (swap to llama3.2, mistral, ...)
                 EVENT_LOOKBACK = 1 hour
                 execution_timeout = 3 minutes (buy-car)
                 Ollama AsyncClient → http://127.0.0.1:11434 (must be running)
.env             HATCHET_CLIENT_TOKEN (required)
                 HATCHET_CLIENT_TLS_STRATEGY=none (required for local Hatchet)
```

Dependencies (`requirements.txt`): fastapi, uvicorn[standard], hatchet-sdk, ollama,
python-dotenv, pydantic.

---

## Setup

This stack has four moving parts that all run locally: the **Hatchet engine**
(Docker), the **Ollama** LLM server, the **worker**, and the **FastAPI** API.

### 1. Prerequisites

```
• Python 3.11+
• Docker (for the local self-hosted Hatchet engine)
• Ollama (local LLM runtime)  — https://ollama.com/download
```

### 2. Python environment + dependencies

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Local Hatchet engine (self-hosted, replaces Hatchet Cloud)

```bash
# Start Postgres + hatchet-lite (dashboard on :8888, gRPC on :7077)
docker compose -f docker-compose.hatchet.yml up -d

# Open the dashboard, create a tenant, generate an API token:
#   http://localhost:8888   (login: admin@example.com / Admin123!!)
#   Settings → API Tokens → generate
```

Copy `.env.example` to `.env` and paste the token. Local Hatchet runs gRPC
without TLS, so the TLS strategy must be disabled:

```bash
cp .env.example .env
# then edit .env:
#   HATCHET_CLIENT_TOKEN="<token from the local dashboard>"
#   HATCHET_CLIENT_TLS_STRATEGY=none
```

### 4. Ollama (powers the `research-car` task)

The `research-car` task calls a local Ollama server at `127.0.0.1:11434`. If
Ollama isn't running (or the model isn't pulled), `buy-car` fails at its first
step with `ConnectionError: Failed to connect to Ollama`.

```bash
# Install (macOS): brew install ollama   — or download from ollama.com/download

# Start the Ollama server (leave running, or use the macOS app)
ollama serve

# Pull the model used by worker.py (LOCAL_MODEL = "deepseek-r1:1.5b")
ollama pull deepseek-r1:1.5b

# Verify it responds
curl http://127.0.0.1:11434/api/tags
```

To use a different model, change `LOCAL_MODEL` in `worker.py` (e.g. `llama3.2`,
`mistral`) and `ollama pull` that model.

---

## Running

With the Hatchet engine and Ollama both up:

```bash
# Terminal 1 — start the Hatchet worker (registers all tasks)
source venv/bin/activate
python worker.py

# Terminal 2 — start the API
source venv/bin/activate
python main.py          # uvicorn on 127.0.0.1:8001

# Drive the workflow
curl -X POST 127.0.0.1:8001/purchase/start \
  -d '{"buyer_name":"Ada","car_type":"2024 Toyota Camry Hybrid","max_budget":32000}'
# → { purchase_id }

curl -X POST 127.0.0.1:8001/mock/dealer-accept \
  -d '{"purchase_id":"<id>","agreed_price":31000}'

curl -X POST 127.0.0.1:8001/mock/bank-approve \
  -d '{"purchase_id":"<id>","final_apr":6.5}'
```

> Ports at a glance: Hatchet dashboard `:8888`, Hatchet gRPC `:7077`,
> FastAPI `:8001`, Ollama `:11434`.

---

## Summary

1. **POST /purchase/start** triggers the `buy-car` durable task fire-and-forget and returns a `purchase_id`.
2. **buy-car** spawns `research-car` (Ollama) and `get-dealer-offer` in parallel, then **suspends** awaiting human input.
3. **HITL events** (`dealer:offer:accepted`, `bank:loan:approved`), scoped per purchase, resume the workflow.
4. **All I/O lives in child tasks**; the durable orchestrator stays deterministic and replay-safe.
5. **No Redis / no polling** — Hatchet's durable state + scoped event waits are the only coordination mechanism.

**Rule:** FastAPI starts work and pushes events. Hatchet persists durable state and routes events. The `buy-car` orchestrator waits; child tasks do the work.
