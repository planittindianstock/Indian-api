from datetime import datetime
from typing import Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware

from db import SignalStore
from schemas import HealthResponse, SignalIngestPayload, SignalResponse
from settings import settings

app = FastAPI(title=settings.APP_NAME, version=settings.APP_VERSION)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_store: Optional[SignalStore] = None


def get_store() -> SignalStore:
    global _store
    if _store is None:
        _store = SignalStore()
    return _store


def require_auth(authorization: str = Header(default="")) -> None:
    expected = settings.SIGNAL_API_KEY.strip()
    if not expected:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="SIGNAL_API_KEY not configured")
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing bearer token")
    token = authorization.removeprefix("Bearer ").strip()
    if token != expected:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(ok=True, service=settings.APP_NAME, timestamp=datetime.utcnow())


@app.post("/signals", response_model=SignalResponse)
def ingest_signal(
    payload: SignalIngestPayload,
    _auth: None = Depends(require_auth),
    store: SignalStore = Depends(get_store),
):
    if payload.signal_type == "BUY" and not (payload.stop_loss < payload.entry_price < payload.target_price):
        return SignalResponse(ok=False, status="rejected", reason="invalid_price_structure_for_buy")
    if payload.signal_type == "SELL" and not (payload.target_price < payload.entry_price < payload.stop_loss):
        return SignalResponse(ok=False, status="rejected", reason="invalid_price_structure_for_sell")

    outcome = store.upsert_signal(payload.model_dump())
    if outcome["inserted"]:
        return SignalResponse(ok=True, status="inserted", signal_id=outcome["signal_id"])
    return SignalResponse(ok=True, status="updated", signal_id=outcome["signal_id"])


@app.get("/signals/latest")
def get_latest_signals(
    limit: int = Query(default=50, ge=1, le=settings.MAX_PAGE_SIZE),
    _auth: None = Depends(require_auth),
    store: SignalStore = Depends(get_store),
):
    return {"ok": True, "count": limit, "items": store.latest_signals(limit=limit)}


@app.get("/signals/history")
def get_signal_history(
    symbol: Optional[str] = Query(default=None),
    timeframe: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=settings.MAX_PAGE_SIZE),
    _auth: None = Depends(require_auth),
    store: SignalStore = Depends(get_store),
):
    return {"ok": True, "items": store.signal_history(symbol=symbol, timeframe=timeframe, limit=limit)}
