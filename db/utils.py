import aiopg


class PoolCtxManagerProxy:
    """ Wrapper class for handling shared aiopg.Pool instances. """

    def __init__(self, pool=None, dsn=None):
        assert pool is not None or dsn is not None
        self._new_pool_created = pool is None
        if self._new_pool_created:
            self._pool_ctx_mgr = aiopg.create_pool(dsn)
        else:
            self._pool = pool

    async def __aenter__(self):
        if self._new_pool_created:
            self._pool = await self._pool_ctx_mgr.__aenter__()
        return self._pool

    async def __aexit__(self, *args):
        # if pool was created in __init__ method, close it
        if self._new_pool_created:
            await self._pool_ctx_mgr.__aexit__(*args)
        # else leave pool not closed to let it be reused
