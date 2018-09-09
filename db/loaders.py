from operator import itemgetter

import aiopg

from .credentials import PG_DSN


def _build_data_loader(query, extractor=lambda x: x):
    async def data_loader(*, pool=None, dsn=PG_DSN):
        async with _PoolCtxManagerProxy(pool, dsn) as pool:
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


class _PoolCtxManagerProxy:
    """ Wrapper class for correct handling of aiopg.Pool as context manager """

    def __init__(self, pool=None, dsn=None):
        assert pool is not None or dsn is not None
        self._new_pool_created = pool is None
        self._pool = pool or aiopg.create_pool(dsn)

    async def __aenter__(self):
        return await self._pool.__aenter__()

    async def __aexit__(self, *args):
        # if pool was created in __init__ method, close it
        if self._new_pool_created:
            await self._pool.__aexit__(*args)
        # else leave pool not closed to let it be reused
