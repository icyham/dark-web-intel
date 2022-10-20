import asyncio

from elasticsearch_dsl import Search

from async_helpers import ascan
from es_connector import ElasticBaseConnector


async def upload_smth(conn: ElasticBaseConnector, doc):
    pass


async def parse_smth(conn: ElasticBaseConnector):
    s = Search(using=conn.es, index="msg").filter(
        "terms", type=["dark_web", "telegram"]
    )
    async for doc in ascan(s):
        # If you want to use it as a dict
        # d = doc.to_dict()
        # by default it is used like this
        print(doc.contents.default)
        await upload_smth(conn, doc)

        break


async def do_smth_else():
    pass


async def main():
    conn = ElasticBaseConnector.from_settings(
        hostname="127.0.0.1",
        port=9200,
    )
    await asyncio.gather(parse_smth(conn), do_smth_else())


if __name__ == "__main__":
    asyncio.run(main())
