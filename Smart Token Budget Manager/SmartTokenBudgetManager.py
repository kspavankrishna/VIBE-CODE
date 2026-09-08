import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional, Dict, List
import hashlib


@dataclass
class TokenUsage:
    timestamp: float
    model: str
    input_tokens: int
    output_tokens: int
    cost: float
    user_id: str = "default"
    project_id: str = "default"


@dataclass
class BudgetConfig:
    hourly_limit: Optional[int] = None
    daily_limit: Optional[int] = None
    monthly_limit: Optional[int] = None
    per_request_limit: int = 100000
    model_costs: Dict[str, Dict[str, float]] = field(default_factory=lambda: {
        "gpt-4": {"input": 0.03, "output": 0.06},
        "claude-opus": {"input": 0.015, "output": 0.075},
        "claude-sonnet": {"input": 0.003, "output": 0.015},
    })


class SmartTokenBudgetManager:
    def __init__(self, config: BudgetConfig):
        self.config = config
        self.usage_log: List[TokenUsage] = []
        self.alerts = []
        self.blocked_requests = 0

    def check_before_request(self, model: str, estimated_tokens: int,
                           user_id: str = "default") -> tuple[bool, Optional[str]]:
        """Check if request fits within budget constraints."""
        if estimated_tokens > self.config.per_request_limit:
            return False, f"Request {estimated_tokens} exceeds per-request limit"

        now = time.time()
        hour_ago = now - 3600
        day_ago = now - 86400
        month_ago = now - 2592000

        if self.config.hourly_limit:
            hour_tokens = sum(u.input_tokens + u.output_tokens for u in self.usage_log
                            if u.timestamp > hour_ago and u.user_id == user_id)
            if hour_tokens + estimated_tokens > self.config.hourly_limit:
                return False, f"Would exceed hourly limit ({hour_tokens} + {estimated_tokens})"

        if self.config.daily_limit:
            day_tokens = sum(u.input_tokens + u.output_tokens for u in self.usage_log
                           if u.timestamp > day_ago and u.user_id == user_id)
            if day_tokens + estimated_tokens > self.config.daily_limit:
                return False, f"Would exceed daily limit ({day_tokens} + {estimated_tokens})"

        if self.config.monthly_limit:
            month_tokens = sum(u.input_tokens + u.output_tokens for u in self.usage_log
                             if u.timestamp > month_ago and u.user_id == user_id)
            if month_tokens + estimated_tokens > self.config.monthly_limit:
                return False, f"Would exceed monthly limit ({month_tokens} + {estimated_tokens})"

        return True, None

    def record_usage(self, model: str, input_tokens: int, output_tokens: int,
                    user_id: str = "default", project_id: str = "default"):
        """Record API call after completion."""
        model_rates = self.config.model_costs.get(model, {"input": 0.001, "output": 0.002})
        cost = (input_tokens * model_rates["input"] + output_tokens * model_rates["output"]) / 1000

        usage = TokenUsage(
            timestamp=time.time(),
            model=model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost=cost,
            user_id=user_id,
            project_id=project_id
        )
        self.usage_log.append(usage)

        if cost > 1.0:
            self.alerts.append(f"High-cost request: ${cost:.3f} from {user_id}")

    def get_stats(self, hours: int = 24, user_id: Optional[str] = None) -> Dict:
        """Get usage statistics over time window."""
        cutoff = time.time() - (hours * 3600)
        filtered = [u for u in self.usage_log if u.timestamp > cutoff]
        if user_id:
            filtered = [u for u in filtered if u.user_id == user_id]

        total_input = sum(u.input_tokens for u in filtered)
        total_output = sum(u.output_tokens for u in filtered)
        total_cost = sum(u.cost for u in filtered)
        avg_cost = total_cost / len(filtered) if filtered else 0

        return {
            "period_hours": hours,
            "requests": len(filtered),
            "total_input_tokens": total_input,
            "total_output_tokens": total_output,
            "total_tokens": total_input + total_output,
            "total_cost": f"${total_cost:.3f}",
            "avg_cost_per_request": f"${avg_cost:.3f}",
            "top_model": max((u.model for u in filtered), key=lambda m: sum(u.output_tokens for u in filtered if u.model == m)) if filtered else None
        }

    def predict_cost(self, model: str, prompt_length: int, max_tokens: int = 500) -> float:
        """Estimate cost before making API call."""
        rates = self.config.model_costs.get(model, {"input": 0.001, "output": 0.002})
        estimated_input = prompt_length // 4
        estimated_output = max_tokens
        return ((estimated_input * rates["input"]) + (estimated_output * rates["output"])) / 1000


"""
================================================================================
EXPLANATION
This manages token budgets for LLM APIs—the core problem for developers shipping AI features. It enforces hourly, daily, and monthly limits per user, predicts costs before API calls, tracks actual spending with detailed stats, and blocks requests that exceed limits. Use it as middleware for any LLM service. The trick: timestamps let you slice usage by time window and user without complex databases. Store this in production systems where cost control matters. Drop it into FastAPI handlers or client libraries that hit Claude, GPT, or any tokenized API. Prevents runaway bills.
================================================================================
"""
