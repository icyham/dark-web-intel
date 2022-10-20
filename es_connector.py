# import ssl
from configparser import SectionProxy
from typing import Any, AsyncIterable, Iterable

from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_streaming_bulk

# from elasticsearch.connection.http_urllib3 import create_ssl_context


class ElasticBaseConnector:
    def __init__(self, es: AsyncElasticsearch):
        self.es = es

    @classmethod
    def from_settings(
        cls,
        hostname: str | None = None,
        port: int | str | None = None,
        login: str | None = None,
        password: str | None = None,
        use_ssl: bool | None = None,
        verify_certs: bool | None = None,
        ca_certs: Any = None,
        opaque_id: str | None = None,
    ):
        hostname = hostname or "localhost"
        port = port or 9200
        http_auth = (login or "", password or "")

        scheme = "http"
        # ssl_context = None
        if use_ssl:
            scheme = "https"
            verify_certs = True if verify_certs is None else verify_certs
            # if ca_certs is None:
            #     ssl_context = create_ssl_context()
            #     ssl_context.check_hostname = False
            #     ssl_context.verify_mode = ssl.CERT_NONE

        return cls(
            AsyncElasticsearch(
                [
                    dict(
                        host=hostname,
                        port=port,
                        http_auth=http_auth,
                        scheme=scheme,
                        verify_certs=verify_certs,
                        ca_certs=ca_certs or None,
                    )
                ],
                timeout=500,
                max_retries=3,
                retry_on_timeout=True,
                maxsize=32,
                opaque_id=opaque_id,
            )
        )

    @classmethod
    def from_configs(cls, es_configs: SectionProxy):
        return cls.from_settings(
            hostname=es_configs.get("host"),
            port=es_configs.getint("port"),
            login=es_configs.get("login"),
            password=es_configs.get("password"),
            use_ssl=es_configs.getboolean("use_ssl"),
            verify_certs=es_configs.getboolean("verify_certs"),
            ca_certs=es_configs.get("ca_certs"),
            opaque_id=es_configs.get("opaque_id"),
        )

    async def update_by_generator(self, generator: Iterable[Any] | AsyncIterable[Any]):
        return [
            info
            async for success, info in async_streaming_bulk(
                self.es,
                generator,
                raise_on_exception=False,
                max_retries=2,
            )
            if not success
        ]
