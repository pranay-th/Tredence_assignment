import asyncio
from typing import Dict, Set

class Broadcaster:
    def __init__(self):
        self.queues: Dict[str, Set[asyncio.Queue]] = {}

    async def subscribe(self, run_id: str) -> asyncio.Queue:
        q = asyncio.Queue()
        self.queues.setdefault(run_id, set()).add(q)
        return q

    def unsubscribe(self, run_id: str, q: asyncio.Queue):
        if run_id in self.queues:
            self.queues[run_id].discard(q)
            if not self.queues[run_id]:
                del self.queues[run_id]

    async def publish(self, run_id: str, message: str):
        qs = list(self.queues.get(run_id, []))
        for q in qs:
            await q.put(message)


broadcaster = Broadcaster()
