from operator import itemgetter
from itertools import groupby


class AsyncDisjointSet:
    """ Disjoint-set implementation with asynchronous input. """

    def __init__(self):
        self._subtrees = {}  # initially empty disjoint-set

    async def build(self, objects_ids, relations):
        await self.make_sets(objects_ids)
        async for object_id, relations_list in relations:
            for other_object_id in relations_list:
                self.union(object_id, other_object_id)

    async def make_sets(self, objects_ids):
        """
        Initialize dict of pointers to roots with every element pointing to
        itself (meaning every node is initially isolated).
        """
        self._subtrees = {
            object_id: DisjointSetSubtree(root_id=object_id, size=1)
            async for object_id in objects_ids
        }

    def find(self, object_id):
        """ Finds subtree containing the given element. """
        subtree = self._subtrees[object_id]
        if object_id != subtree.root_id:
            root_subtree = self.find(subtree.root_id)
            subtree.root_id = root_subtree.root_id  # path compression
        return subtree

    def union(self, x, y):
        x_subtree, y_subtree = self.find(x), self.find(y)
        if x_subtree == y_subtree:
            return
        if x_subtree.size < y_subtree.size:  # union by size
            x_subtree, y_subtree = y_subtree, x_subtree
        y_subtree.root_id = x_subtree.root_id
        x_subtree.size = x_subtree.size + y_subtree.size

    def get_components(self):
        """ Result of building disjoint-set to find connected components. """
        grouped = _sort_and_groupby(
            (
                (subtree.root_id, object_id)
                for object_id, subtree in self._subtrees.items()
            ),
            key=itemgetter(0)
        )
        for root_id, pairs in grouped:
            yield list(map(itemgetter(1), pairs))


class DisjointSetSubtree:
    __slots__ = 'root_id', 'size'

    def __init__(self, root_id, size):
        self.root_id = root_id
        self.size = size


def _sort_and_groupby(iterable, key):
    sorted_ = sorted(iterable, key=key)
    return groupby(sorted_, key=key)
