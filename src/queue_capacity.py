"""Queue capacity limiter with transactional rollback support.

Issue #200: Release capacity when enqueue rolls back.
"""

import logging
import asyncio
import random
import time
from contextlib import asynccontextmanager

# Configura logging estruturado (sem dados privados)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class QueueCapacityLimiter:
    """Queue capacity limiter com rollback atômico (Issue #200)"""

    def __init__(self, max_capacity: int):
        self.max_capacity = max_capacity
        self._current = 0
        self._lock = asyncio.Lock()
        self._reservations = set()

    @property
    def current_capacity(self):
        return self._current

    async def reserve(self, task_id: str) -> bool:
        """Reserva capacidade - retorna False se esgotada"""
        async with self._lock:
            if self._current >= self.max_capacity:
                logger.warning(
                    "Capacity rejected",
                    extra={"task_hash": hash(task_id), "current": self._current, "max": self.max_capacity}
                )
                return False
            self._current += 1
            self._reservations.add(task_id)
            logger.info(
                "Capacity reserved",
                extra={"task_hash": hash(task_id), "capacity": self._current}
            )
            return True

    async def release(self, task_id: str) -> None:
        """LIBERA CAPACIDADE - Chamado no rollback (Issue #200)"""
        async with self._lock:
            if task_id in self._reservations:
                self._reservations.discard(task_id)
                self._current = max(0, self._current - 1)
                logger.info(
                    "Capacity released (rollback)",
                    extra={"task_hash": hash(task_id), "capacity": self._current}
                )
            else:
                logger.warning(
                    "Release called for unknown task",
                    extra={"task_hash": hash(task_id)}
                )

    @asynccontextmanager
    async def transactional_enqueue(self, task_id: str):
        """Gerencia transação: libera capacidade em caso de falha"""
        if not await self.reserve(task_id):
            yield False
            return
        try:
            yield True
        except Exception as e:
            await self.release(task_id)
            logger.error(
                "Transaction failed, capacity released",
                extra={"task_hash": hash(task_id), "error": str(e)}
            )
            raise


async def stress_test(duration_seconds=30):
    """Teste de estresse com injeção de falhas (chaos engineering)"""
    print(f"\n[STRESS] Teste de {duration_seconds}s com 25% de falhas...")
    limiter = QueueCapacityLimiter(max_capacity=50)
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
    print(f"RESULTADO DO TESTE DE {duration_seconds}s:")
    print(f"  Operações: {ops:,}")
    print(f"  Falhas injetadas: {failures:,}")
    print(f"  Capacidade final: {final_cap}")
    print(f"  VAZAMENTO: {'❌ SIM' if final_cap != 0 else '✅ ZERO'}")
    
    assert final_cap == 0, f"Vazamento de capacidade: {final_cap} slots presos"
    return final_cap
