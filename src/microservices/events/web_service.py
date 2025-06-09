from abc import ABC, abstractmethod
from logging import Logger
import asyncio
import signal

from functools import partial
from aiohttp.web_app import Application

from aiohttp.web import TCPSite, Response
from aiohttp.web_runner import AppRunner
import signal
import asyncio
import sys


SUCCESSFULL_EXIT_CODE = 0
ERROR_CODE = 1


class WebService(ABC):
    def __init__(self, logger: Logger, host: str, port: int) -> None:
        self.logger = logger
        self.host = host
        self.port = port

        self.__app = None
        self.__app_runner = None

        self.loop = self.init_async_loop(self.__shutdown_handler)
        self.__shutdown_signal = asyncio.Future()

    @property
    def name(self) -> str:
        return self.__class__.__qualname__

    def init_async_loop(self, signal_handler):
        loop = asyncio.new_event_loop()
        self.logger.info("Initializing signals to stop")
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, partial(signal_handler, sig))
        asyncio.set_event_loop(loop)
        return loop

    async def on_start(self) -> None:
        await self.__start_app()

    async def on_stop(self) -> None:
        await self.__stop_app()

    async def _start_tcp_site(self, runner: AppRunner) -> None:
        self.logger.info("Initializing site with runner on host %s and port %s", self.host, self.port)
        site = TCPSite(runner=runner, host=self.host, port=self.port, ssl_context=None)
        await site.start()

    @abstractmethod
    def add_routes(self, app: Application) -> None:
        raise NotImplementedError

    async def init_routes_logic(self) -> Application:
        self.logger.info("Creating application...")
        app = Application()
        self.logger.info("Add routing logic")
        self.add_routes(app)
        self.__app = app
        return app

    async def __start_app(self) -> None:
        self.logger.info("Got such app_config: %s", self.config)
        self.logger.info("Initializing AppRunner....")
        app_runner = AppRunner(
            await self.init_routes_logic(),
        )
        await app_runner.setup()
        try:
            self.logger.info("Starting tcp site...")
            await self._start_tcp_site(runner=app_runner)
        except Exception:
            await app_runner.cleanup()
            raise
        self.__app_runner = app_runner

    async def __stop_app(self) -> None:
        try:
            await self.__app_runner.cleanup()
        finally:
            self.__app_runner = None
            self.__app = None

    def __shutdown_handler(self, sig: int) -> None:
        self.logger.info("Received exit signal")
        self.__shutdown_signal.set_exception(Exception(f"Shutdown handler called with signal {sig}"))

    def start(self) -> None:
        self.logger.info("Starting service %s ....", self.name)
        exit_code = SUCCESSFULL_EXIT_CODE
        try:
            self.loop.run_until_complete(self.on_start())
            self.loop.run_until_complete(self.__shutdown_signal)
        except Exception:
            self.logger.exception("Service stopped due an error")
            exit_code = ERROR_CODE
        finally:
            self.stop(exit_code)

    def stop(self, exit_code: int) -> None:
        self.logger.info("Stopping service")
        try:
            self.loop.run_until_complete(self.on_stop())
            self.loop.close()
        except Exception:
            exit_code = ERROR_CODE
        finally:
            sys.exit(exit_code)
