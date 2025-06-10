from logging import Logger
from aiohttp.web import Response
from aiohttp.web_app import Application

from functools import partial

from config import AppConfig
from proxy_view import ProxyView
from web_service import WebService


class APIGateway(WebService):
    def __init__(self, config: AppConfig, logger: Logger) -> None:
        self.config = config
        super().__init__(logger=logger, host=self.config.host, port=self.config.port)

    async def proxy_view_factory(self, request):
        return await ProxyView(
            request,
            monolith_server=self.config.monolith_service,
            gradual_migration=self.config.gradual_migration,
            logger=self.logger,
            migration_config=self.config.migration_config,
        )

    def add_routes(self, app: Application) -> None:
        app.router.add_route(
            method="get",
            path="/health",
            handler=(lambda _: Response(text='{"status": true}', content_type="application/json")),
        )
        app.router.add_route(method="*", path="/{path:.*?}", handler=self.proxy_view_factory)
