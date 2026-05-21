"""Deterministic regression tests for issue #200.

These tests PROVE that capacity is released on transaction failures.
"""

import pytest
from src.queue_capacity import QueueCapacityLimiter


@pytest.mark.asyncio
async def test_rollback_on_failure():
    """ACCEPTANCE CRITERION #1: Rollback on transaction failure."""
    limiter = QueueCapacityLimiter(max_capacity=5)
    initial = limiter.current_capacity

    try:
        async with limiter.transactional_enqueue("task_falha") as reserved:
            if reserved:
                raise Exception("Falha simulada na transação")
    except Exception:
        pass

    final = limiter.current_capacity
    assert final == initial, f"Vazamento! {initial} -> {final}"
    print(f"✅ OK: Capacidade restaurada ({initial} -> {final})")


@pytest.mark.asyncio
async def test_reject_when_full():
    """ACCEPTANCE CRITERION #2: Queue limiter rejects when full."""
    limiter = QueueCapacityLimiter(max_capacity=1)

    assert await limiter.reserve("t1") is True
    assert limiter.current_capacity == 1

    assert await limiter.reserve("t2") is False
    assert limiter.current_capacity == 1

    await limiter.release("t1")
    assert limiter.current_capacity == 0


@pytest.mark.asyncio
async def test_sanitized_logs(caplog):
    """ACCEPTANCE CRITERION #3: Logs don't expose private data."""
    import logging
    caplog.set_level(logging.INFO)

    limiter = QueueCapacityLimiter(max_capacity=5)
    task_id = "customer_credit_card_4111111111111111"

    await limiter.reserve(task_id)
    await limiter.release(task_id)

    for record in caplog.records:
        assert task_id not in record.message, f"Raw task ID leaked!"
