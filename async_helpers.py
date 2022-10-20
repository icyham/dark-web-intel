from typing import Any, AsyncGenerator

import aiostream.stream as stream
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_scan
from elasticsearch_dsl import Search
from elasticsearch_dsl.aggs import Agg
from elasticsearch_dsl.search import get_connection


async def multi_ascan(search: Search, n_slices: int) -> AsyncGenerator[Any, None]:
    search = search.params(raise_on_error=False)
    if n_slices < 2:
        async for i in ascan(search):
            yield i
        return

    async def gen(id):
        s = search.params(scroll="10m").extra(slice={"id": id, "max": n_slices})
        return ascan(s)

    combined = stream.merge(*[await gen(i) for i in range(n_slices)])
    async with combined.stream() as streamer:
        async for i in streamer:
            yield i


async def ascan(search: Search) -> AsyncGenerator[Any, None]:
    es: AsyncElasticsearch = get_connection(search._using)  # type: ignore

    async for hit in async_scan(
        es, query=search.to_dict(), index=search._index, **search._params
    ):
        yield search._get_result(hit)


async def aexecute(search: Search, ignore_cache=False) -> Any:
    es: AsyncElasticsearch = get_connection(search._using)  # type: ignore

    if ignore_cache or not hasattr(search, "_response"):
        search._response = search._response_class(
            search,
            await es.search(
                index=search._index, body=search.to_dict(), **search._params  # type: ignore
            ),
        )
    return search._response


async def acount(search: Search) -> int:
    es: AsyncElasticsearch = get_connection(search._using)  # type: ignore

    if hasattr(search, "_response") and search._response.hits.total.relation == "eq":
        return search._response.hits.total.value  # type: ignore

    return (
        await es.count(
            index=search._index, body=search.to_dict(count=True), **search._params
        )
    )["count"]


class ElasticAggregation:
    def __init__(
        self,
        s: Search,
        sources: list[dict[str, Agg]],
        aggs: dict[str, Agg] = {},
        batch_size=100,
    ):
        self.s: Search = s.extra(track_total_hits=False)[:0]  # type: ignore
        self.sources = sources
        self.aggs = aggs
        self.batch_size = batch_size
        self.after_key = None

    async def batch(self):
        kwargs = {"after": self.after_key} if self.after_key is not None else {}
        self.s.aggs.bucket(
            "composite",
            "composite",
            sources=self.sources,
            aggs=self.aggs,
            size=self.batch_size,
            **kwargs,
        )
        res = await aexecute(self.s, ignore_cache=True)

        comp = res.aggs.composite
        self.after_key = comp.after_key if comp.buckets else None

        return comp.buckets

    async def scan(self):
        while batch := await self.batch():
            for d in batch:
                yield d
