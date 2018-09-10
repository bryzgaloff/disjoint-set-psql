from operator import itemgetter

from .utils import CursorCtxManager
from .credentials import PG_DSN


def _build_data_loader(query, extractor=lambda x: x):
    async def data_loader(*, pool=None, dsn=PG_DSN):
        async with CursorCtxManager(pool, dsn) as cursor:
            await cursor.execute(query)
            async for row in cursor:
                yield extractor(row)
    return data_loader


load_objects_ids = \
    _build_data_loader('SELECT id FROM objects', extractor=itemgetter(0))
load_relations = \
    _build_data_loader('SELECT object_id, relative_ids FROM relations')


async def write_components(components, *, pool=None, dsn=PG_DSN):
    async with CursorCtxManager(pool, dsn) as cursor:
        for component in components:
            await cursor.execute(
                'INSERT INTO components (object_ids) VALUES (%s)', (component, )
            )
