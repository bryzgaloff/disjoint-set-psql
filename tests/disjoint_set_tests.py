import unittest
import asyncio

from disjoint_set import AsyncDisjointSet


class DisjointSetTestCase(unittest.TestCase):
    @staticmethod
    async def _objects():
        for object_id in range(1, 10):
            yield object_id

    @staticmethod
    async def _relations():
        yield 3, (4, 7, 1)
        yield 4, (9, )
        yield 3, (2, 1)
        yield 6, (8, )

    def test_disjoint_set(self):
        async def _test(dj):
            await dj.build(self._objects(), self._relations())
            components = set(map(tuple, dj.get_components()))
            self.assertSetEqual(
                {
                    (1, 2, 3, 4, 7, 9),
                    (5,),
                    (6, 8)
                },
                components
            )

        test_coro = _test(AsyncDisjointSet())
        asyncio.get_event_loop().run_until_complete(test_coro)


if __name__ == '__main__':
    unittest.main()
