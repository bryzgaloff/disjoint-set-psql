import unittest
import asyncio

import aiopg
from db.credentials import PG_DSN
from db.utils import CursorCtxManager


class CursorCtxManagerTestCase(unittest.TestCase):
    @staticmethod
    def _test_coro(coro):
        asyncio.get_event_loop().run_until_complete(coro)

    def test_shared_pool(self):
        async def _test():
            async with aiopg.create_pool(PG_DSN) as pool:
                ctx_mgr_1 = CursorCtxManager(pool)
                ctx_mgr_2 = CursorCtxManager(pool)
                async with ctx_mgr_1:
                    async with ctx_mgr_2:
                        self.assertIs(ctx_mgr_1._pool, ctx_mgr_2._pool)
                    self.assertFalse(ctx_mgr_2._pool.closed)
                self.assertFalse(ctx_mgr_1._pool.closed)
            self.assertTrue(pool.closed)
        self._test_coro(_test())

    def test_non_shared_pool(self):
        async def _test():
            ctx_mgr_1 = CursorCtxManager(dsn=PG_DSN)
            ctx_mgr_2 = CursorCtxManager(dsn=PG_DSN)
            async with ctx_mgr_1:
                async with ctx_mgr_2:
                    self.assertTrue(ctx_mgr_1._pool is not ctx_mgr_2._pool)
                self.assertTrue(ctx_mgr_2._pool.closed)
                self.assertFalse(ctx_mgr_1._pool.closed)
            self.assertTrue(ctx_mgr_1._pool.closed)
        self._test_coro(_test())

    def test_nested_shared_pool(self):
        async def _test():
            outer_ctx_mgr = CursorCtxManager(dsn=PG_DSN)
            async with outer_ctx_mgr:
                inner_ctx_mgr = CursorCtxManager(outer_ctx_mgr._pool)
                async with inner_ctx_mgr:
                    self.assertIs(inner_ctx_mgr._pool, outer_ctx_mgr._pool)
                self.assertFalse(outer_ctx_mgr._pool.closed)
            self.assertTrue(outer_ctx_mgr._pool.closed)
        self._test_coro(_test())


if __name__ == '__main__':
    unittest.main()
