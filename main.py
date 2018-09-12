import asyncio

import aiopg

from disjoint_set import AsyncDisjointSet
from db.wrapper import load_objects_ids, load_relations, write_components
from db.credentials import PG_DSN


async def build_disjoint_set():
    async with aiopg.create_pool(PG_DSN) as pool:
        ds = AsyncDisjointSet()
        await ds.build(
            objects_ids=load_objects_ids(pool=pool),
            relations=load_relations(pool=pool)
        )
        await write_components(ds.get_components(), pool=pool)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(build_disjoint_set())
