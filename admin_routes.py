from datetime import datetime
from typing import Dict

from fastapi import APIRouter, Depends, Header, HTTPException, Request, status

try:
    from render_api.schemas import GenerateSignalRequest, WorkerControlRequest
    from render_api.settings import settings
    from render_api.worker_controller import WorkerController
except ImportError:
    from schemas import GenerateSignalRequest, WorkerControlRequest
    from settings import settings
    from worker_controller import WorkerController

router = APIRouter(prefix="", tags=["Admin"])


def require_admin_auth(authorization: str = Header(default="")) -> None:
    expected = settings.ADMIN_API_KEY.strip()
    if not expected:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="ADMIN_API_KEY not configured")
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing bearer token")
    token = authorization.removeprefix("Bearer ").strip()
    if token != expected:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")


def get_controller(request: Request) -> WorkerController:
    controller = getattr(request.app.state, "worker_controller", None)
    if controller is None:
        raise HTTPException(status_code=500, detail="Worker controller not initialized")
    return controller


@router.get("/ai/status")
async def ai_status(
    _auth: None = Depends(require_admin_auth),
    controller: WorkerController = Depends(get_controller),
):
    return {
        "ok": True,
        "timestamp": datetime.utcnow().isoformat(),
        "ai_advisor_enabled": bool(settings.ENABLE_AI_ADVISOR),
        "workers": controller.status(),
        "store": controller.store.stats(),
    }


@router.post("/ai/retrain")
async def ai_retrain(
    payload: Dict | None = None,
    _auth: None = Depends(require_admin_auth),
    controller: WorkerController = Depends(get_controller),
):
    limit = 100
    if isinstance(payload, dict):
        raw_limit = payload.get("limit")
        if isinstance(raw_limit, int) and 1 <= raw_limit <= 5000:
            limit = raw_limit
    result = await controller.retrain_once(limit=limit)
    return {"ok": True, "result": result}


@router.post("/worker/start-stop")
async def worker_start_stop(
    payload: WorkerControlRequest,
    _auth: None = Depends(require_admin_auth),
    controller: WorkerController = Depends(get_controller),
):
    workers = payload.workers or ["generator", "labeler"]
    if payload.action.value == "start":
        status_data = await controller.start_workers(
            workers=workers,
            symbols=payload.symbols,
            interval_seconds=payload.interval_seconds,
        )
    else:
        status_data = await controller.stop_workers(workers=workers)
    return {"ok": True, "workers": status_data}


@router.post("/signals/generate")
async def generate_signal_now(
    payload: GenerateSignalRequest,
    _auth: None = Depends(require_admin_auth),
    controller: WorkerController = Depends(get_controller),
):
    signal = await controller.generate_once(symbol=payload.symbol, timeframe=payload.timeframe)
    return {"ok": True, "item": signal}
