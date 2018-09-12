import unittest
import asyncio
from random import random
from collections import defaultdict

from disjoint_set import AsyncDisjointSet


class DisjointSetTestCase(unittest.TestCase):
    DEFAULT_OBJECT_COUNT = 500
    DEFAULT_EDGE_PROB = 0.005
    DEFAULT_DUP_PROB = 0.1
    DEFAULT_SPLIT_PROB = 0.1

    @staticmethod
    def _generate_objects(objects_count=DEFAULT_OBJECT_COUNT):
        yield from range(objects_count)

    @staticmethod
    def _generate_relations(
            object_count=DEFAULT_OBJECT_COUNT,
            edge_prob=DEFAULT_EDGE_PROB,
            dup_prob=DEFAULT_DUP_PROB,
            split_prob=DEFAULT_SPLIT_PROB
    ):
        """
        :param int object_count: total number of objects
        :param float edge_prob: probability of edge creation
        :param float dup_prob: probability of edge duplication
        :param float split_prob:
            probability of splitting relations list into multiple rows
        """
        for object_id in range(object_count):
            relations = []
            for other_object_id in range(object_id + 1, object_count):
                if random() < edge_prob:
                    while True:
                        relations.append(other_object_id)
                        if random() >= dup_prob:
                            break
            i = 0
            while i < len(relations):
                i += 1
                if random() < split_prob:
                    yield object_id, relations[:i]
                    relations = relations[i:]
                    i = 0
            if relations:
                yield object_id, relations

    @classmethod
    def _find_components_dfs(cls, objects, relations):
        deduplicated_relations = defaultdict(set)
        for object_id, edges in relations:
            deduplicated_relations[object_id].update(edges)
            for other_object_id in edges:
                deduplicated_relations[other_object_id].add(object_id)

        components = []
        non_visited = set(objects)
        while non_visited:
            component = set()
            element = non_visited.pop()
            cls._dfs(element, deduplicated_relations, component, non_visited)
            components.append(component)

        return components

    @classmethod
    def _dfs(cls, element, relations, component, non_visited):
        component.add(element)
        non_visited.discard(element)
        for other_element in relations[element]:
            if other_element in non_visited:
                cls._dfs(other_element, relations, component, non_visited)

    @staticmethod
    async def _aiter(iterable):
        for item in iterable:
            yield item

    def test_disjoint_set(self):
        objects = list(self._generate_objects())
        relations = list(self._generate_relations())
        components_dfs = self._find_components_dfs(objects, relations)

        async def _built_disjoint_set(ds):
            await ds.build(self._aiter(objects), self._aiter(relations))
            return ds.get_components()
        test_coro = _built_disjoint_set(AsyncDisjointSet())
        components_ds = asyncio.get_event_loop().run_until_complete(test_coro)

        self.assertSetEqual(
            set(map(frozenset, components_dfs)),
            set(map(frozenset, components_ds))
        )


if __name__ == '__main__':
    unittest.main()
