import asyncio
from typing import Dict, List, Optional

try:
    from render_api.db import SignalStore
    from render_api.settings import settings
except ImportError:
    from db import SignalStore
    from settings import settings


class WorkerController:
    def __init__(self, store: SignalStore):
        self.store = store
        self.generator_task: Optional[asyncio.Task] = None
        self.labeler_task: Optional[asyncio.Task] = None
        self.symbols: List[str] = list(settings.SIGNAL_SCAN_SYMBOLS)
        self.interval_seconds: int = int(settings.SIGNAL_SCAN_INTERVAL_SECONDS)
        self._lock = asyncio.Lock()
        self.runtime_workers_enabled: bool = bool(settings.ENABLE_RUNTIME_WORKERS)

    def status(self) -> Dict:
        return {
            "generator_running": self.generator_task is not None and not self.generator_task.done(),
            "labeler_running": self.labeler_task is not None and not self.labeler_task.done(),
            "symbols": self.symbols,
            "interval_seconds": self.interval_seconds,
            "runtime_workers_enabled": self.runtime_workers_enabled,
            "mode": "runtime_workers" if self.runtime_workers_enabled else "receiver_only",
        }

    async def start_workers(self, workers: List[str], symbols: Optional[List[str]] = None, interval_seconds: Optional[int] = None) -> Dict:
        if not self.runtime_workers_enabled:
            raise RuntimeError(
                "Runtime workers are disabled for this deployment. "
                "Keep render_api as receiver-only and run signal workers on your local machine."
            )
        async with self._lock:
            if symbols is not None:
                self.symbols = [s.strip().upper() for s in symbols if s.strip()]
            if interval_seconds is not None:
                self.interval_seconds = interval_seconds

            if "generator" in workers and (self.generator_task is None or self.generator_task.done()):
                self.generator_task = asyncio.create_task(self._generator_loop(), name="generator_worker")
            if "labeler" in workers and (self.labeler_task is None or self.labeler_task.done()):
                self.labeler_task = asyncio.create_task(self._labeler_loop(), name="labeler_worker")
            return self.status()

    async def stop_workers(self, workers: List[str]) -> Dict:
        if not self.runtime_workers_enabled:
            return self.status()
        async with self._lock:
            tasks = []
            if "generator" in workers and self.generator_task is not None:
                self.generator_task.cancel()
                tasks.append(self.generator_task)
                self.generator_task = None
            if "labeler" in workers and self.labeler_task is not None:
                self.labeler_task.cancel()
                tasks.append(self.labeler_task)
                self.labeler_task = None
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            return self.status()

    async def generate_once(self, symbol: str, timeframe: str = "1d") -> Dict:
        if not self.runtime_workers_enabled:
            raise RuntimeError(
                "Signal generation is disabled on render_api receiver-only deployment. "
                "Generate signals from local engine and POST to /signals."
            )
        raise RuntimeError("Runtime worker engine integration is not configured in this deployment.")

    async def retrain_once(self, limit: int = 100) -> Dict:
        if not self.runtime_workers_enabled:
            stats = self.store.stats()
            return {"labeled_now": 0, **stats}
        labeled = await asyncio.to_thread(self._run_labeling_cycle, limit)
        stats = self.store.stats()
        return {"labeled_now": labeled, **stats}

    async def _generator_loop(self) -> None:
        if not self.runtime_workers_enabled:
            return
        try:
            while True:
                for symbol in self.symbols:
                    try:
                        payload = await self.generate_once(symbol, "1d")
                        self.store.upsert_event(payload)
                    except Exception:
                        continue
                await asyncio.sleep(self.interval_seconds)
        except asyncio.CancelledError:
            return

    async def _labeler_loop(self) -> None:
        if not self.runtime_workers_enabled:
            return
        try:
            while True:
                self._run_labeling_cycle(limit=200)
                await asyncio.sleep(max(20, self.interval_seconds // 2))
        except asyncio.CancelledError:
            return

    def _run_labeling_cycle(self, limit: int = 100) -> int:
        # Receiver-only deployment does not have market-data access/model runtime.
        # Labels can be written by an external/local process via shared DB if desired.
        return 0
