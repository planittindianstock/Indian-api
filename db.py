from datetime import datetime
from typing import Dict, List, Optional

from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.collection import Collection

try:
    from render_api.settings import settings
except ImportError:
    from settings import settings


class SignalStore:
    def __init__(self):
        if not settings.MONGODB_URI:
            raise RuntimeError("MONGODB_URI is required for render_api")
        self.client = MongoClient(settings.MONGODB_URI)
        self.collection: Collection = self.client[settings.MONGODB_DB_NAME][settings.MONGODB_SIGNALS_COLLECTION]
        self.events: Collection = self.client[settings.MONGODB_DB_NAME][settings.MONGODB_SIGNAL_EVENTS_COLLECTION]
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        self.collection.create_index([("idempotency_key", ASCENDING)], unique=True, name="uniq_idempotency_key")
        self.collection.create_index([("symbol", ASCENDING), ("timeframe", ASCENDING), ("timestamp", DESCENDING)], name="symbol_tf_ts_idx")
        self.collection.create_index([("timestamp", DESCENDING)], name="ts_desc_idx")
        self.events.create_index([("idempotency_key", ASCENDING)], unique=True, name="events_uniq_idempotency_key")
        self.events.create_index([("symbol", ASCENDING), ("timeframe", ASCENDING), ("timestamp", DESCENDING)], name="events_symbol_tf_ts_idx")
        self.events.create_index([("labeled", ASCENDING), ("timestamp", DESCENDING)], name="events_labeled_ts_idx")

    def upsert_signal(self, payload: Dict) -> Dict:
        now = datetime.utcnow()
        payload = dict(payload)
        payload.setdefault("received_at", now)
        result = self.collection.update_one(
            {"idempotency_key": payload["idempotency_key"]},
            {"$set": payload, "$setOnInsert": {"created_at": now}},
            upsert=True,
        )
        doc = self.collection.find_one({"idempotency_key": payload["idempotency_key"]}, {"_id": 1})
        return {
            "inserted": bool(result.upserted_id),
            "updated": bool(result.matched_count and result.modified_count >= 0),
            "signal_id": str(doc["_id"]) if doc else None,
        }

    def latest_signals(self, limit: int = 50) -> List[Dict]:
        cursor = self.collection.find({}, {"_id": 0}).sort("timestamp", DESCENDING).limit(limit)
        return list(cursor)

    def signal_history(self, symbol: Optional[str], timeframe: Optional[str], limit: int = 100) -> List[Dict]:
        query: Dict = {}
        if symbol:
            query["symbol"] = symbol.upper()
        if timeframe:
            query["timeframe"] = timeframe
        cursor = self.collection.find(query, {"_id": 0}).sort("timestamp", DESCENDING).limit(limit)
        return list(cursor)

    def upsert_event(self, payload: Dict) -> None:
        now = datetime.utcnow()
        doc = dict(payload)
        doc.setdefault("created_at", now.isoformat())
        doc.setdefault("labeled", False)
        self.events.update_one(
            {"idempotency_key": doc["idempotency_key"]},
            {"$set": doc, "$setOnInsert": {"inserted_at": now.isoformat()}},
            upsert=True,
        )

    def latest_events(self, limit: int = 100) -> List[Dict]:
        return list(self.events.find({}, {"_id": 0}).sort("timestamp", DESCENDING).limit(limit))

    def unlabeled_events(self, limit: int = 200) -> List[Dict]:
        return list(self.events.find({"labeled": False}, {"_id": 0}).sort("timestamp", DESCENDING).limit(limit))

    def label_event(self, idempotency_key: str, label_direction: str, label_return_pct: float, label_source: str = "worker") -> None:
        self.events.update_one(
            {"idempotency_key": idempotency_key},
            {
                "$set": {
                    "labeled": True,
                    "label_direction": label_direction,
                    "label_return_pct": float(label_return_pct),
                    "labeled_at": datetime.utcnow().isoformat(),
                    "label_source": label_source,
                }
            },
            upsert=False,
        )

    def stats(self) -> Dict:
        total = self.events.count_documents({})
        labeled = self.events.count_documents({"labeled": True})
        unlabeled = total - labeled
        return {"total_events": total, "labeled_events": labeled, "unlabeled_events": unlabeled}
