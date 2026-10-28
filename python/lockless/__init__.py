"""lockless: Wait-free and lock-free concurrency & DSP primitives for Python."""

from __future__ import annotations

from ._cabi import (
    Closed,
    Empty,
    Full,
    LocklessError,
    drain_deferred_decrefs,
)
from .channels import (
    Channel,
    RendezvousChannel,
)
from .maps import (
    BroadcastRing,
    BroadcastSubscriber,
    ConcurrentDict,
    ConcurrentDictSnapshot,
    Ctrie,
    LaggedError,
    SkipListMap,
    SkipListSet,
    Subscriber,
)
from .queues import (
    BQueue128,
    MpmcConsumer,
    MpmcProducer,
    MpmcQueue,
    SpscQueue,
)
from .ratelimit import (
    LeakyBucket,
    TokenBucket,
)
from .streambuffer import (
    StreamBuffer,
    StreamReadReservation,
    StreamWriteReservation,
)
from .taskpool import (
    TaskPool,
)

__version__ = "0.1.0"

__all__ = [
    "__version__",
    # Exceptions
    "LocklessError",
    "Empty",
    "Full",
    "Closed",
    "LaggedError",
    # Queues
    "BQueue128",
    "SpscQueue",
    "MpmcQueue",
    "MpmcProducer",
    "MpmcConsumer",
    # Zero-copy byte stream ring
    "StreamBuffer",
    "StreamWriteReservation",
    "StreamReadReservation",
    # Broadcast & Associative / Ordered Containers
    "BroadcastRing",
    "Subscriber",
    "BroadcastSubscriber",
    "ConcurrentDict",
    "ConcurrentDictSnapshot",
    "Ctrie",
    "SkipListMap",
    "SkipListSet",
    # Channels
    "Channel",
    "RendezvousChannel",
    # Work-Stealing Pool
    "TaskPool",
    # Rate Limiters
    "TokenBucket",
    "LeakyBucket",
    # Diagnostics
    "drain_deferred_decrefs",
]
