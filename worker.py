"""
Buy-a-Car workflow using Hatchet durable tasks + event waits.

Flow:
  buy-car (durable orchestrator)
    ├── spawn: research-car        → Ollama (local LLM), returns compact summary
    ├── spawn: get-dealer-offer    → mock dealer API, returns initial price
    ├── wait_for_event: dealer:offer:accepted  ← human/dealer confirms (HITL)
    ├── spawn: submit-loan-app     → mock bank API, returns application ID
    └── wait_for_event: bank:loan:approved     ← bank approves (HITL)

Determinism rule: all I/O lives in child tasks; the durable task only
orchestrates and waits — no direct DB or API calls.
"""

import random
from datetime import timedelta
from ollama import AsyncClient
from hatchet_sdk import Hatchet, Context, DurableContext
from pydantic import BaseModel
import config  # noqa: F401 — loads .env and validates keys

hatchet = Hatchet()
ollama = AsyncClient()
LOCAL_MODEL = "deepseek-r1:1.5b"  # swap to llama3.2, mistral, etc. as needed
EVENT_LOOKBACK = timedelta(hours=1)


def purchase_scope(purchase_id: str) -> str:
    return f"purchase:{purchase_id}"


def _child_output(result: dict, task_name: str) -> dict:
    return result.get(task_name) or result

# ── Input / Output models ─────────────────────────────────────────────────────

class BuyCarInput(BaseModel):
    purchase_id: str          # caller-generated UUID for event routing
    buyer_name: str
    car_type: str             # e.g. "2024 Toyota Camry Hybrid"
    max_budget: int           # USD

class ResearchInput(BaseModel):
    car_type: str
    max_budget: int

class DealerOfferInput(BaseModel):
    car_type: str
    max_budget: int

class LoanInput(BaseModel):
    purchase_id: str
    buyer_name: str
    car_price: int
    down_payment: int

# ── Child task: research car via Ollama ──────────────────────────────────────
# Token minimization: tight system prompt forces exactly 3 bullets; no fluff.

@hatchet.task(name="research-car", input_validator=ResearchInput)
async def research_car(input: ResearchInput, ctx: Context) -> dict:
    ctx.log(f"Researching: {input.car_type} (budget ${input.max_budget}) via {LOCAL_MODEL}")

    response = await ollama.chat(
        model=LOCAL_MODEL,
        options={"temperature": 0.2, "num_predict": 200},  # cap output tokens
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a concise car-buying advisor. "
                    "Reply with exactly 3 bullet points. No extra text."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Give me 3 key buying tips for a {input.car_type} "
                    f"with a ${input.max_budget:,} budget. "
                    "Include typical price range, must-check features, and one negotiation lever."
                ),
            },
        ],
    )

    message = response["message"]
    summary = (message.get("content") or "").strip()
    if not summary:
        summary = (message.get("thinking") or "").strip()
    ctx.log("Research done.")
    if summary:
        ctx.log(summary)
    else:
        ctx.log("Ollama returned no text content.")
    return {"summary": summary, "model": LOCAL_MODEL}


# ── Child task: mock dealer offer ────────────────────────────────────────────

@hatchet.task(name="get-dealer-offer", input_validator=DealerOfferInput)
async def get_dealer_offer(input: DealerOfferInput, ctx: Context) -> dict:
    ctx.log(f"Calling dealer API for {input.car_type}")

    # Mock: dealer always comes in 5-15% above budget for negotiation room
    markup = random.uniform(0.05, 0.15)
    offer_price = int(input.max_budget * (1 + markup))
    dealer_name = "Sunrise Auto Group"

    ctx.log(f"Dealer offer: ${offer_price:,} from {dealer_name}")
    return {
        "dealer_name": dealer_name,
        "offer_price": offer_price,
        "msrp": int(offer_price * 0.97),
    }


# ── Child task: mock loan application ────────────────────────────────────────

@hatchet.task(name="submit-loan-app", input_validator=LoanInput)
async def submit_loan_app(input: LoanInput, ctx: Context) -> dict:
    ctx.log(f"Submitting loan for {input.buyer_name}: ${input.car_price:,}")

    # Mock: bank returns an application ID instantly; approval is async (event wait)
    app_id = f"LOAN-{input.purchase_id[:8].upper()}"
    monthly_rate = 0.067 / 12
    loan_amount = input.car_price - input.down_payment
    months = 60
    monthly_payment = loan_amount * (monthly_rate * (1 + monthly_rate) ** months) / (
        (1 + monthly_rate) ** months - 1
    )

    ctx.log(f"Application submitted: {app_id}, est. payment ${monthly_payment:.0f}/mo")
    return {
        "application_id": app_id,
        "loan_amount": loan_amount,
        "estimated_monthly_payment": round(monthly_payment, 2),
        "term_months": months,
        "apr": 6.7,
    }


# ── Durable orchestrator: buy-car ─────────────────────────────────────────────
# All I/O is in children. This function is deterministic and only orchestrates.

@hatchet.durable_task(
    name="buy-car",
    input_validator=BuyCarInput,
    execution_timeout=timedelta(minutes=3),
)
async def buy_car(input: BuyCarInput, ctx: DurableContext) -> dict:
    ctx.log(f"[{input.purchase_id}] Starting car purchase for {input.buyer_name}")
    scope = purchase_scope(input.purchase_id)

    # ── Step 1: Research + dealer offer (single durable spawn batch) ──────────
    ctx.log("Spawning research + dealer offer...")
    spawn_results = await research_car._workflow.aio_run_many([
        research_car.create_bulk_run_item(input=ResearchInput(
            car_type=input.car_type,
            max_budget=input.max_budget,
        )),
        get_dealer_offer.create_bulk_run_item(input=DealerOfferInput(
            car_type=input.car_type,
            max_budget=input.max_budget,
        )),
    ])
    research_result = _child_output(spawn_results[0], "research-car")
    dealer_result = _child_output(spawn_results[1], "get-dealer-offer")

    ctx.log(f"Research summary ready. Dealer offer: ${dealer_result['offer_price']:,}")

    # ── Step 2: Human-in-the-loop — buyer reviews & accepts dealer price ──────
    # Push dealer:offer:accepted from your UI/test script with:
    #   {"purchase_id": "<id>", "agreed_price": <int>}
    ctx.log("Waiting for buyer to accept dealer offer (event: dealer:offer:accepted)...")
    offer_event = await ctx.aio_wait_for_event(
        "dealer:offer:accepted",
        f"input.purchase_id == '{input.purchase_id}'",
        scope=scope,
        lookback_window=EVENT_LOOKBACK,
    )
    agreed_price = offer_event.get("agreed_price", dealer_result["offer_price"])
    ctx.log(f"Offer accepted at ${agreed_price:,}")

    # ── Step 3: Submit loan application ──────────────────────────────────────
    down_payment = int(agreed_price * 0.20)  # assume 20% down
    loan_result = await submit_loan_app.aio_run(LoanInput(
        purchase_id=input.purchase_id,
        buyer_name=input.buyer_name,
        car_price=agreed_price,
        down_payment=down_payment,
    ))
    ctx.log(f"Loan app submitted: {loan_result['application_id']}")

    # ── Step 4: Human-in-the-loop — bank approves loan ────────────────────────
    # Push bank:loan:approved from your bank system/test script with:
    #   {"purchase_id": "<id>", "approved": true, "final_apr": 6.5}
    ctx.log("Waiting for bank loan approval (event: bank:loan:approved)...")
    loan_event = await ctx.aio_wait_for_event(
        "bank:loan:approved",
        f"input.purchase_id == '{input.purchase_id}'",
        scope=scope,
        lookback_window=EVENT_LOOKBACK,
    )

    if not loan_event.get("approved", False):
        ctx.log("Loan denied.")
        return {"status": "loan_denied", "purchase_id": input.purchase_id}

    final_apr = loan_event.get("final_apr", loan_result["apr"])
    ctx.log(f"Loan approved at {final_apr}% APR. Purchase complete!")

    return {
        "status": "purchase_complete",
        "purchase_id": input.purchase_id,
        "buyer": input.buyer_name,
        "car": input.car_type,
        "agreed_price": agreed_price,
        "down_payment": down_payment,
        "loan": {
            "application_id": loan_result["application_id"],
            "loan_amount": loan_result["loan_amount"],
            "monthly_payment": loan_result["estimated_monthly_payment"],
            "term_months": loan_result["term_months"],
            "apr": final_apr,
        },
        "research_summary": research_result["summary"],
        "research_model": research_result["model"],
    }


# ── Worker entrypoint ─────────────────────────────────────────────────────────

TASKS = [buy_car, research_car, get_dealer_offer, submit_loan_app]

def main():
    worker = hatchet.worker("car-buyer-worker", workflows=TASKS)
    worker.start()

if __name__ == "__main__":
    main()
