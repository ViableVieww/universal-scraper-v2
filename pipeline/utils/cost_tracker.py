from __future__ import annotations

import logging
from collections import defaultdict

from pipeline.constants import API_COSTS

logger = logging.getLogger("pipeline")


class CostTracker:
    def __init__(self, max_cost: float | None = None) -> None:
        self.max_cost = max_cost
        self._totals: dict[str, float] = defaultdict(float)
        self._counts: dict[str, int] = defaultdict(int)

    def record_call(self, service: str) -> None:
        cost = API_COSTS.get(service, 0.0)
        self._totals[service] += cost
        self._counts[service] += 1

    @property
    def total_cost(self) -> float:
        return sum(self._totals.values())

    @property
    def counts(self) -> dict[str, int]:
        return dict(self._counts)

    def ceiling_reached(self) -> bool:
        if self.max_cost is None:
            return False
        reached = self.total_cost >= self.max_cost
        if reached:
            logger.warning(
                "Cost ceiling reached: $%.4f >= $%.4f",
                self.total_cost,
                self.max_cost,
            )
        return reached

    def summary(self) -> dict:
        return {
            "total_cost_usd": round(self.total_cost, 4),
            "calls": dict(self._counts),
            "cost_by_service": {k: round(v, 4) for k, v in self._totals.items()},
        }
