from dataclasses import dataclass


@dataclass
class Tick:
    exchange: str
    price: float
    volume: float    # 24h volume or per-trade size — used for composite weighting
    timestamp: float # unix seconds (time.time())
