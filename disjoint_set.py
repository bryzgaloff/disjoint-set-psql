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
            subtree.update(root_subtree)  # path compression
            subtree = root_subtree
        return subtree

    def union(self, x, y):
        x_subtree, y_subtree = self.find(x), self.find(y)
        if x_subtree == y_subtree:
            return
        if x_subtree.size < y_subtree.size:  # union by size
            x_subtree, y_subtree = y_subtree, x_subtree
        x_subtree.size = x_subtree.size + y_subtree.size
        y_subtree.update(x_subtree)

    def get_components(self):
        """ Result of building disjoint-set to find connected components. """
        grouped = _sort_and_groupby(
            self._subtrees.keys(),
            key=lambda object_id: self.find(object_id).root_id
        )
        for root_id, component in grouped:
            yield list(component)


class DisjointSetSubtree:
    __slots__ = 'root_id', 'size'

    def __init__(self, root_id, size):
        self.root_id = root_id
        self.size = size

    def update(self, other):
        self.root_id, self.size = other.root_id, other.size

    def __repr__(self):
        return f'root={self.root_id}, size={self.size}'


def _sort_and_groupby(iterable, key):
    sorted_ = sorted(iterable, key=key)
    return groupby(sorted_, key=key)
