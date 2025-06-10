from aiohttp.web import View, Response, Request
from aiohttp import ClientSession
from typing import Optional

from urllib.parse import urljoin, urlparse

import random

from logging import Logger
import re


class ProxyView(View):
    def __init__(
        self,
        request: Request,
        monolith_server: str,
        gradual_migration: bool,
        logger: Logger,
        migration_config: Optional[dict] = None,
    ) -> None:
        super().__init__(request)
        self.monolith_server = monolith_server
        self.migration_config = migration_config
        self.gradual_migration = gradual_migration
        self.logger = logger

    async def get(self) -> Response:
        self.logger.info("Got GET request")
        return await self._proxy_method()

    async def post(self):
        self.logger.info("Got POST request")
        data = await self.request.read()
        self.logger.debug("Got data from request: %s", data and data.decode())
        return await self._proxy_method(data=data)

    async def _proxy_method(self, data: Optional[bytes] = None) -> Response:
        request_url = self.request.url
        self.logger.info("The original path was: %s", request_url)

        self.logger.debug("Trying to get configuration for proxy")
        if not self.gradual_migration:
            self.logger.info("MIGRATION IS DISABLED. PROXY REQUEST TO MONOLITH")
            return await self._make_request(
                host=self.monolith_server,
                path=self.request.path,
                method=self.request.method,
                data=data,
            )

        component_name = self._extract_component_name(self.request.path)
        if component_name is None:
            self.logger.info("COMPONENT NAME WASN'T FOUND. PROXY REQUEST TO MONOLITH")
            return await self._make_request(
                host=self.monolith_server,
                path=self.request.path,
                method=self.request.method,
                data=data,
            )

        service_configuration = self.migration_config.get(component_name)
        if not service_configuration:
            self.logger.info("LOGIC WAS NOT MIGRATED YET. COMPONENT NAME WAS %s", component_name)
            return await self._make_request(
                host=self.monolith_server,
                path=self.request.path,
                method=self.request.method,
                data=data,
            )

        probability = random.random() * 100
        if probability > service_configuration["migration_percent"]:
            self.logger.info(
                "GOT probability %s. COMPONENT NAME WAS %s. MIGRATION PERCENT WAS %s. REQUEST TO MONOLITH SERVICE",
                probability,
                component_name,
                service_configuration["migration_percent"],
            )
            return await self._make_request(
                host=self.monolith_server,
                path=self.request.path,
                method=self.request.method,
                data=data,
            )

        self.logger.info(
            "GOT probability %s. COMPONENT NAME WAS %s. MIGRATION PERCENT WAS %s. REQUEST TO MICROSERVICE",
            probability,
            component_name,
            service_configuration["migration_percent"],
        )
        return await self._make_request(
            host=service_configuration["microservice_url"],
            path=self.request.path,
            method=self.request.method,
            data=data,
        )

    def _extract_component_name(self, url_path: str) -> Optional[str]:
        match = re.search(r"/api/(\w+)", url_path)
        if match:
            return match.group(1)
        return None

    async def _make_request(self, host: str, path: str, method: str, data: Optional[bytes] = None):
        request_url = urljoin(host, path)
        async with ClientSession() as session:
            if method.upper() == "POST":
                async with session.post(request_url, data=data) as response:
                    if 200 <= response.status < 300:
                        data = await response.text()
                        return Response(text=data, content_type="application/json", status=response.status)
                    else:
                        self.logger.error(f"Error: Received status code {response.status}")
                        return Response(
                            text=f"Error: Received status code {response.status}",
                            status=response.status,
                        )
            if method.upper() == "GET":
                async with session.get(request_url, ssl=False, params=(urlparse(self.request.url)).query) as response:
                    if 200 <= response.status < 300:
                        data = await response.text()
                        return Response(text=data, content_type="application/json", status=response.status)
                    else:
                        self.logger.error("Error: Received status code %s", response.status)
                        return Response(
                            text=f"Error: Received status code {response.status}",
                            status=response.status,
                        )
            else:
                self.logger.error("Unsupported HTTP method")
                return Response(
                    text="Unsupported HTTP method",
                    status=500,
                )
