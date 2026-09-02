"""longtrader-sdk: Tier-2 thin wrapper over generated Connect stubs.

Hand-written surface stays minimal (session handling + overflow policies);
all trading semantics live in the generated contract under
``longtrader_sdk/proto`` (created by ``just sdk-generate``, never hand-edited).
"""

from .ports import MarketPort, OverflowPolicy, TradingPort
from .session import Session

__all__ = ["Session", "OverflowPolicy", "TradingPort", "MarketPort"]
__version__ = "0.1.0"
