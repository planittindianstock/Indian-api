from datetime import datetime
from enum import Enum
from typing import Any, Dict, Literal, Optional

from pydantic import BaseModel, Field, field_validator


class SignalType(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"


class SignalIngestPayload(BaseModel):
    symbol: str = Field(min_length=1, max_length=30)
    timeframe: str = Field(min_length=1, max_length=10)
    signal_type: SignalType
    entry_price: float = Field(gt=0)
    stop_loss: float = Field(gt=0)
    target_price: float = Field(gt=0)
    confidence_score: float = Field(ge=0, le=1)
    strategy_breakdown: Dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime
    idempotency_key: str = Field(min_length=16, max_length=128)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        return value.strip().upper()


class SignalResponse(BaseModel):
    ok: bool
    status: Literal["inserted", "updated", "rejected"]
    signal_id: Optional[str] = None
    reason: Optional[str] = None


class HealthResponse(BaseModel):
    ok: bool
    service: str
    timestamp: datetime
