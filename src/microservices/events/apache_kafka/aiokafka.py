from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRecord, TopicPartition
import asyncio
from contextlib import suppress
import inspect
import logging
from typing import Any, Callable, Coroutine, Optional, Union

from config import ConsumerConfig, ProducerConfig

logger = logging.getLogger(__name__)

KafkaMessage = ConsumerRecord
OnMessageCallback = Union[
    Callable[[KafkaMessage], Coroutine], Callable[[KafkaMessage | list[KafkaMessage], Any], Coroutine]
]

class KafkaProducer:
    def __init__(self, config: ProducerConfig, loop: asyncio.AbstractEventLoop, **kwargs) -> None:
        self._config = config
        self._loop = loop
        kwargs.update(
            {
                "security_protocol": config.security_protocol,
                "acks": config.acks,
                "compression_type": config.compression_type,
                # "max_batch_size": config.max_batch_size,
                # "linger_ms": config.linger_ms,
                # "max_request_size": config.max_request_size,
            }
        )
        self._client = AIOKafkaProducer(bootstrap_servers=config.servers, loop=loop, **kwargs)

    async def start(self) -> None:
        await self._client.start()

    async def stop(self) -> None:
        await self._client.stop()

    async def check_is_alive(self) -> bool:
        await self._client.partitions_for(self._config.producer_topics[0])
        return True

    async def send(
        self,
        value: Optional[bytes] = None,
        key: Optional[bytes] = None,
        partition: Optional[int] = None,
        timestamp_ms: Optional[int] = None,
        topic: Optional[str] = None,
        wait: bool = True,
    ) -> Optional[asyncio.Future]:
        if topic is None:
            raise RuntimeError("no kafka topic for producer provided")

        if wait:
            return await self._client.send_and_wait(
                topic=topic, value=value, key=key, partition=partition, timestamp_ms=timestamp_ms
            )
        else:
            return await self._client.send(
                topic=topic, value=value, key=key, partition=partition, timestamp_ms=timestamp_ms
            )


class KafkaConsumer:
    def __init__(
        self,
        config: ConsumerConfig,
        loop: asyncio.AbstractEventLoop,
        subscribers: Optional[list[OnMessageCallback]] = None,
        **kwargs,
    ) -> None:
        self.in_stopping = False
        self._config = config
        self._loop = loop
        self._callback_based: bool = bool(subscribers)
        kwargs.update(
            {
                "security_protocol": config.security_protocol,
            }
        )
        logger.info("Kafka consumer config: %s", kwargs)
        self._client = AIOKafkaConsumer(
            *config.consumer_topics,
            bootstrap_servers=config.servers,
            group_id=config.consumer_group,
            loop=loop,
            **kwargs,
        )
        self._task: Optional[asyncio.Future] = None

        self._callbacks: set[OnMessageCallback] = set()
        if subscribers:
            self.register(*subscribers)

    async def start(self) -> None:
        await self._client.start()
        if self._callback_based:
            self._task = self._loop.create_task(self._consume())

    async def stop(self, wait_future=True) -> None:
        await self._client.stop()
        if self._task and wait_future:
            await asyncio.wait_for(self._task, 10)

    async def check_is_alive(self) -> bool:
        if self._task and self._task.done():
            return False

        return bool(await self._client.topics())

    def register(self, *subscribers: OnMessageCallback) -> None:
        for subscriber in subscribers:
            if inspect.iscoroutinefunction(subscriber):
                self._callbacks.add(subscriber)
            else:
                raise ValueError("subscriber should be coroutine function")

    async def commit(self, **kwargs):
        await self._client.commit(**kwargs)

    def stop_consume(self):
        self.in_stopping = True

    async def _consume(self) -> None:
        while True:
            try:
                if self.in_stopping:
                    return
                message = await self._client.getone()
                coros = [callback(message) for callback in self._callbacks]
                await asyncio.gather(*coros)
            except Exception:
                logger.exception("Error while consuming message")
                logger.info("KafkaConsumer stopped")
                with suppress(Exception):
                    await self._client.stop()
                return
            
    async def get_one_message(self, topic: str) -> KafkaMessage:
        if self.in_stopping:
            return
        if topic not in self._config.consumer_topics:
            return
        
        partitions_ids = self._client.partitions_for_topic(topic)
        return await self._client.getone(*[TopicPartition(topic=topic, partition=partition_id) for partition_id in partitions_ids])