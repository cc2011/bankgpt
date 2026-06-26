"""
FastAPI HTTP layer for the car-buying workflow.

Endpoints:
  POST /purchase/start          — kick off the buy-car durable workflow
  POST /mock/dealer-accept      — simulate buyer accepting dealer price (HITL event 1)
  POST /mock/bank-approve       — simulate bank approving the loan (HITL event 2)
  POST /mock/bank-deny          — simulate bank denying the loan
  GET  /purchase/{purchase_id}  — (optional) check run status via Hatchet
"""

import json
import uuid
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ValidationError
from hatchet_sdk import Hatchet
import worker  # registers all tasks with hatchet on import
from worker import purchase_scope

hatchet = Hatchet()
app = FastAPI(title="BankGPT — Car Purchase API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def parse_json_body(request: Request, model: type[BaseModel]) -> BaseModel:
    """Parse JSON body even when curl omits Content-Type: application/json."""
    try:
        body = await request.json()
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=422, detail=f"Invalid JSON body: {e}") from e
    try:
        return model.model_validate(body)
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=e.errors()) from e


# ── Request models ────────────────────────────────────────────────────────────

class StartPurchaseRequest(BaseModel):
    buyer_name: str
    car_type: str
    max_budget: int

class DealerAcceptRequest(BaseModel):
    purchase_id: str
    agreed_price: int               # negotiated final price

class BankApproveRequest(BaseModel):
    purchase_id: str
    final_apr: float = 6.5          # bank's actual approved rate

class BankDenyRequest(BaseModel):
    purchase_id: str
    reason: str = "credit score too low"


# ── Routes ────────────────────────────────────────────────────────────────────

@app.post("/purchase/start")
async def start_purchase(request: Request):
    """Kick off the full car-buying durable workflow."""
    req = await parse_json_body(request, StartPurchaseRequest)
    purchase_id = str(uuid.uuid4())

    run = await worker.buy_car.aio_run_no_wait(
        worker.BuyCarInput(
            purchase_id=purchase_id,
            buyer_name=req.buyer_name,
            car_type=req.car_type,
            max_budget=req.max_budget,
        )
    )

    return {
        "status": "started",
        "purchase_id": purchase_id,
        "message": (
            "Workflow started. It will pause at the dealer offer step. "
            "Call POST /mock/dealer-accept to continue."
        ),
    }


@app.post("/mock/dealer-accept")
async def mock_dealer_accept(request: Request):
    """
    Simulate the buyer (or dealer portal) accepting a final price.
    This unblocks the durable task at its first event wait.
    """
    req = await parse_json_body(request, DealerAcceptRequest)
    try:
        hatchet.event.push(
            "dealer:offer:accepted",
            {
                "purchase_id": req.purchase_id,
                "agreed_price": req.agreed_price,
            },
            scope=purchase_scope(req.purchase_id),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to push event: {e}")

    return {
        "status": "event_sent",
        "event": "dealer:offer:accepted",
        "purchase_id": req.purchase_id,
        "agreed_price": req.agreed_price,
        "next": "Call POST /mock/bank-approve (or /mock/bank-deny) once loan is reviewed.",
    }


@app.post("/mock/bank-approve")
async def mock_bank_approve(request: Request):
    """
    Simulate the bank approving the loan.
    This unblocks the durable task at its second event wait.
    """
    req = await parse_json_body(request, BankApproveRequest)
    try:
        hatchet.event.push(
            "bank:loan:approved",
            {
                "purchase_id": req.purchase_id,
                "approved": True,
                "final_apr": req.final_apr,
            },
            scope=purchase_scope(req.purchase_id),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to push event: {e}")

    return {
        "status": "event_sent",
        "event": "bank:loan:approved",
        "purchase_id": req.purchase_id,
        "approved": True,
        "final_apr": req.final_apr,
    }


@app.post("/mock/bank-deny")
async def mock_bank_deny(request: Request):
    """Simulate the bank denying the loan."""
    req = await parse_json_body(request, BankDenyRequest)
    try:
        hatchet.event.push(
            "bank:loan:approved",
            {
                "purchase_id": req.purchase_id,
                "approved": False,
                "reason": req.reason,
            },
            scope=purchase_scope(req.purchase_id),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to push event: {e}")

    return {
        "status": "event_sent",
        "event": "bank:loan:approved",
        "purchase_id": req.purchase_id,
        "approved": False,
    }


# ── Dev entrypoint ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    # Port 8000 is commonly taken by other local services (e.g. Plane in Docker),
    # which causes confusing 404s. Use 8001 for BankGPT.
    uvicorn.run("main:app", host="127.0.0.1", port=8001, reload=True)
