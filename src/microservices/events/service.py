from logging import Logger
from aiohttp.web import Response
from aiohttp.web_app import Application


from config import AppConfig
from kafka_events_handler import KafkaEventsHandlerView
from web_service import WebService

from apache_kafka.aiokafka import KafkaConsumer, KafkaProducer


class EventsService(WebService):
    def __init__(self, config: AppConfig, logger: Logger) -> None:
        self.config = config
        super().__init__(logger=logger, host=self.config.host, port=self.config.port)

        self.kafka_consumer: KafkaConsumer = KafkaConsumer(
            config=config.kafka_consumer,
            loop=self.loop,
            subscribers=None,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )

        self.kafka_producer: KafkaProducer = KafkaProducer(
            config=config.kafka_producer,
            loop=self.loop,
        )

    async def on_start(self) -> None:
        await self.kafka_consumer.start()
        await self.kafka_producer.start()
        await super().on_start()

    async def on_stop(self) -> None:
        await self.kafka_consumer.stop()
        await self.kafka_producer.stop()
        await super().on_stop()

    async def kafka_events_handler_view_factory(self, request):
        return await KafkaEventsHandlerView(
            request,
            logger=self.logger,
            kafka_producer=self.kafka_producer,
            kafka_consumer=self.kafka_consumer
        )

    def add_routes(self, app: Application) -> None:
        app.router.add_route(
            method="get",
            path="/api/events/health",
            handler=(lambda _: Response(text='{"status": true}', content_type="application/json")),
        )
        app.router.add_route(method="post", path="/{path:.*?}", handler=self.kafka_events_handler_view_factory)

