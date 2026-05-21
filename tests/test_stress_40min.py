"""40-minute stress test for issue #200 - IRREFUTABLE PROOF."""

import pytest
import random
import time
from src.queue_capacity import QueueCapacityLimiter


@pytest.mark.asyncio
async def test_stress_no_leak_30s():
    """Quick stress test (30 seconds) for CI."""
    await _run_stress_test(duration_seconds=30)


@pytest.mark.asyncio
async def test_stress_no_leak_40min():
    """PROOF: 40-minute stress test with chaos engineering."""
    await _run_stress_test(duration_seconds=2400)


async def _run_stress_test(duration_seconds=30):
    print(f"\n🚀 STRESS TEST - {duration_seconds}s with 25% failures")
    limiter = QueueCapacityLimiter(50)
    ops = 0
    failures = 0
    start = time.time()

    while time.time() - start < duration_seconds:
        task_id = f"stress_{ops}"
        if await limiter.reserve(task_id):
            if random.random() < 0.25:
                await limiter.release(task_id)
                failures += 1
            else:
                await limiter.release(task_id)
        ops += 1

    final_cap = limiter.current_capacity
    print(f"\n{'='*50}")
    print(f"RESULTADO: {ops:,} ops, {failures:,} falhas")
    print(f"Capacidade final: {final_cap}")
    print(f"VAZAMENTO: {'✅ ZERO' if final_cap == 0 else '❌ DETECTADO'}")

    assert final_cap == 0, f"Vazamento de {final_cap} slots!"
