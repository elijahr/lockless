"""Lock-free TokenBucket and LeakyBucket rate limiters backed by the Nim C-ABI."""

from __future__ import annotations

from ._cabi import (
    Closed,
    cdll,
    ffi,
    lib,
)


class TokenBucket:
    """Lock-free CAS token bucket rate limiter (`TokenBucket`).

    Parameters
    ----------
    capacity : int
        Maximum burst capacity in tokens (must be ``>= 1``).
    refill_rate : int
        Steady-state token refill rate in tokens per second (must be ``>= 1``).
    """

    __slots__ = ("_destroyed", "_ptr")

    def __init__(self, capacity: int, refill_rate: int) -> None:
        if (
            not isinstance(capacity, int)
            or isinstance(capacity, bool)
            or not isinstance(refill_rate, int)
            or isinstance(refill_rate, bool)
        ):
            raise TypeError("capacity and refill_rate must be integers")
        if capacity <= 0 or refill_rate <= 0:
            raise ValueError(
                f"TokenBucket capacity and refill_rate must be positive, got ({capacity}, {refill_rate})"
            )
        ptr = lib.lf_token_bucket_create(capacity, refill_rate)
        if ptr == ffi.NULL:
            raise ValueError(
                f"Failed to create TokenBucket(capacity={capacity}, refill_rate={refill_rate})"
            )
        self._ptr = ptr
        self._destroyed = False

    def _check_alive(self) -> None:
        if self._destroyed or self._ptr == ffi.NULL:
            raise Closed("TokenBucket has been destroyed")

    @property
    def capacity(self) -> int:
        self._check_alive()
        return int(lib.lf_token_bucket_capacity(self._ptr))

    @property
    def refill_rate(self) -> int:
        self._check_alive()
        return int(lib.lf_token_bucket_refill_rate(self._ptr))

    rate = refill_rate

    @property
    def available(self) -> int:
        self._check_alive()
        return int(lib.lf_token_bucket_available(self._ptr))

    def try_acquire(self, tokens: int = 1) -> bool:
        """Attempts to acquire ``tokens`` immediately without waiting."""
        self._check_alive()
        if not isinstance(tokens, int) or isinstance(tokens, bool) or tokens <= 0:
            raise ValueError(f"tokens must be a positive integer, got {tokens}")
        return bool(lib.lf_token_bucket_try_acquire(self._ptr, tokens))

    try_consume = try_acquire

    def acquire(self, tokens: int = 1, timeout: float | None = None) -> bool:
        """Acquires ``tokens``, waiting up to ``timeout`` seconds (or indefinitely if ``None``)."""
        self._check_alive()
        if not isinstance(tokens, int) or isinstance(tokens, bool) or tokens <= 0:
            raise ValueError(f"tokens must be a positive integer, got {tokens}")
        if timeout is None:
            max_wait_ns = -1
        else:
            if timeout < 0:
                raise ValueError("timeout must be non-negative")
            max_wait_ns = int(timeout * 1_000_000_000)
        addr = int(ffi.cast("uintptr_t", self._ptr))
        return bool(cdll.lf_token_bucket_acquire_timeout(addr, tokens, max_wait_ns))

    acquire_timeout = acquire

    def reset(self) -> None:
        """Resets the bucket back to full burst capacity."""
        self._check_alive()
        lib.lf_token_bucket_reset(self._ptr, self.capacity)

    def destroy(self) -> None:
        if not self._destroyed and self._ptr != ffi.NULL:
            ptr = self._ptr
            self._ptr = ffi.NULL
            self._destroyed = True
            lib.lf_token_bucket_destroy(ptr)

    close = destroy

    def __del__(self) -> None:
        try:
            self.destroy()
        except Exception:
            pass


class LeakyBucket:
    """Lock-free GCRA leaky bucket rate meter (`LeakyBucket`).

    Parameters
    ----------
    capacity : int
        Maximum queue capacity in units (must be ``>= 1``).
    leak_rate : int
        Steady-state drain rate in units per second (must be ``>= 1``).
    """

    __slots__ = ("_capacity", "_destroyed", "_ptr")

    def __init__(self, capacity: int, leak_rate: int) -> None:
        if (
            not isinstance(capacity, int)
            or isinstance(capacity, bool)
            or not isinstance(leak_rate, int)
            or isinstance(leak_rate, bool)
        ):
            raise TypeError("capacity and leak_rate must be integers")
        if capacity <= 0 or leak_rate <= 0:
            raise ValueError(
                f"LeakyBucket capacity and leak_rate must be positive, got ({capacity}, {leak_rate})"
            )
        burst_tolerance_ns = ((capacity - 1) * 1_000_000_000) // leak_rate
        ptr = lib.lf_leaky_bucket_create(burst_tolerance_ns, leak_rate)
        if ptr == ffi.NULL:
            raise ValueError(
                f"Failed to create LeakyBucket(capacity={capacity}, leak_rate={leak_rate})"
            )
        self._ptr = ptr
        self._capacity = capacity
        self._destroyed = False

    def _check_alive(self) -> None:
        if self._destroyed or self._ptr == ffi.NULL:
            raise Closed("LeakyBucket has been destroyed")

    @property
    def capacity(self) -> int:
        self._check_alive()
        return self._capacity

    @property
    def leak_rate(self) -> int:
        self._check_alive()
        return int(lib.lf_leaky_bucket_leak_rate(self._ptr))

    rate = leak_rate

    @property
    def water_level_ns(self) -> int:
        self._check_alive()
        return int(lib.lf_leaky_bucket_water_level(self._ptr))

    @property
    def burst_tolerance_ns(self) -> int:
        self._check_alive()
        return int(lib.lf_leaky_bucket_burst_tolerance_ns(self._ptr))

    def try_acquire(self, units: int = 1) -> bool:
        """Attempts to pour ``units`` into the leaky bucket without waiting."""
        self._check_alive()
        if not isinstance(units, int) or isinstance(units, bool) or units <= 0:
            raise ValueError(f"units must be a positive integer, got {units}")
        if units > self._capacity:
            return False
        return bool(lib.lf_leaky_bucket_try_consume(self._ptr, units))

    try_consume = try_acquire

    def acquire(self, units: int = 1, timeout: float | None = None) -> bool:
        """Acquires ``units``, waiting up to ``timeout`` seconds (or indefinitely if ``None``)."""
        self._check_alive()
        if not isinstance(units, int) or isinstance(units, bool) or units <= 0:
            raise ValueError(f"units must be a positive integer, got {units}")
        if units > self._capacity:
            return False
        if timeout is None:
            max_wait_ns = -1
        else:
            if timeout < 0:
                raise ValueError("timeout must be non-negative")
            max_wait_ns = int(timeout * 1_000_000_000)
        addr = int(ffi.cast("uintptr_t", self._ptr))
        return bool(cdll.lf_leaky_bucket_consume_timeout(addr, units, max_wait_ns))

    acquire_timeout = acquire

    def reset(self) -> None:
        """Empties the leaky bucket back to a zero water level."""
        self._check_alive()
        lib.lf_leaky_bucket_reset(self._ptr)

    def destroy(self) -> None:
        if not self._destroyed and self._ptr != ffi.NULL:
            ptr = self._ptr
            self._ptr = ffi.NULL
            self._destroyed = True
            lib.lf_leaky_bucket_destroy(ptr)

    close = destroy

    def __del__(self) -> None:
        try:
            self.destroy()
        except Exception:
            pass
