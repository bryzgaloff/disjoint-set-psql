from operator import itemgetter
import asyncio

from .utils import PoolCtxManagerProxy
from .credentials import PG_DSN


def _build_data_loader(query, extractor=lambda x: x, dsn=PG_DSN):
    async def data_loader(*, pool=None):
        async with PoolCtxManagerProxy(pool, dsn) as pool:
            async with pool.acquire() as connection:
                async with connection.cursor() as cursor:
                    await cursor.execute(query)
                    async for row in cursor:
                        yield extractor(row)
    return data_loader


load_objects_ids = \
    _build_data_loader('SELECT id FROM objects', extractor=itemgetter(0))
load_relations = \
    _build_data_loader('SELECT object_id, relative_ids FROM relations')


async def write_components(components, *, pool=None, dsn=PG_DSN):
    async with PoolCtxManagerProxy(pool, dsn) as pool:
        await asyncio.gather(*(
            _insert_component(pool, component) for component in components
        ))


async def _insert_component(pool, component):
    async with pool.acquire() as connection:
        async with connection.cursor() as cursor:
            await cursor.execute(
                'INSERT INTO components (object_ids) VALUES (%s)', (component,)
            )
