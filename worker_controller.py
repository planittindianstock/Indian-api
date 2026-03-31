import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd

from core.signal_engine import LocalSignalEngine
from data.fetcher import DataFetcher
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
        self.fetcher = DataFetcher()
        self._lock = asyncio.Lock()

    def status(self) -> Dict:
        return {
            "generator_running": self.generator_task is not None and not self.generator_task.done(),
            "labeler_running": self.labeler_task is not None and not self.labeler_task.done(),
            "symbols": self.symbols,
            "interval_seconds": self.interval_seconds,
        }

    async def start_workers(self, workers: List[str], symbols: Optional[List[str]] = None, interval_seconds: Optional[int] = None) -> Dict:
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
        engine = LocalSignalEngine()
        # Prevent recursion into this receiver API in render deployment.
        engine.api_client.base_url = ""
        engine.api_client.api_key = ""
        payload = await asyncio.to_thread(engine.generate_signal, symbol, timeframe)
        self.store.upsert_event(payload)
        return payload

    async def retrain_once(self, limit: int = 100) -> Dict:
        labeled = await asyncio.to_thread(self._run_labeling_cycle, limit)
        stats = self.store.stats()
        return {"labeled_now": labeled, **stats}

    async def _generator_loop(self) -> None:
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
        try:
            while True:
                self._run_labeling_cycle(limit=200)
                await asyncio.sleep(max(20, self.interval_seconds // 2))
        except asyncio.CancelledError:
            return

    def _run_labeling_cycle(self, limit: int = 100) -> int:
        events = self.store.unlabeled_events(limit=limit)
        labeled_count = 0
        for event in events:
            try:
                symbol = str(event.get("symbol", "")).strip().upper()
                signal_type = str(event.get("signal_type", "HOLD"))
                entry = float(event.get("entry_price", 0))
                event_ts = pd.to_datetime(event.get("timestamp")).to_pydatetime()
                if not symbol or entry <= 0:
                    continue

                start = (event_ts - timedelta(days=2)).strftime("%Y-%m-%d")
                end = (event_ts + timedelta(days=settings.LABEL_LOOKAHEAD_DAYS + 5)).strftime("%Y-%m-%d")
                df = self.fetcher.fetch_stock(symbol, start, end)
                df = self.fetcher.prepare_data(df)
                if df is None or df.empty:
                    continue

                next_rows = df[df.index > event_ts]
                if next_rows.empty:
                    continue
                next_close = float(next_rows.iloc[0]["Close"])
                ret_pct = ((next_close - entry) / entry) * 100.0

                if signal_type == "BUY":
                    label = "CORRECT" if ret_pct > 0 else "WRONG" if ret_pct < 0 else "FLAT"
                elif signal_type == "SELL":
                    label = "CORRECT" if ret_pct < 0 else "WRONG" if ret_pct > 0 else "FLAT"
                else:
                    label = "FLAT"

                self.store.label_event(event["idempotency_key"], label, ret_pct, label_source="render_labeler")
                labeled_count += 1
            except Exception:
                continue
        return labeled_count
