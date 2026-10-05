"""longtrader-sdk: Tier-2 thin wrapper over generated Connect stubs.

Hand-written surface stays minimal (session handling + overflow policies);
all trading semantics live in the generated contract under
``longtrader_sdk/proto`` (created by ``just sdk-generate``, never hand-edited).
"""

from .ports import (
    DEFAULT_OVERFLOW,
    MarketPort,
    OverflowPolicy,
    SessionMarketPort,
    SessionTradingPort,
    TradingPort,
    is_sequence_gap,
    overflow_policy_for_channel,
)
from .session import (
    ACTIVE,
    ATTACHED,
    DISCONNECTED,
    GRACEFUL_SHUTDOWN,
    KILL_SWITCH_TRIPPED,
    SYNCING,
    SYNC_IN_PROGRESS,
    TERMINAL_STATES,
    ConnectError,
    Session,
    from_decimal,
    to_decimal,
)

__all__ = [
    "Session",
    "OverflowPolicy",
    "TradingPort",
    "MarketPort",
    "SessionTradingPort",
    "SessionMarketPort",
    "DEFAULT_OVERFLOW",
    "overflow_policy_for_channel",
    "is_sequence_gap",
    "ConnectError",
    "DISCONNECTED",
    "ATTACHED",
    "SYNCING",
    "ACTIVE",
    "KILL_SWITCH_TRIPPED",
    "GRACEFUL_SHUTDOWN",
    "TERMINAL_STATES",
    "SYNC_IN_PROGRESS",
    "to_decimal",
    "from_decimal",
]
__version__ = "0.2.0"
