from aiohttp.web import View, Response, Request
from apache_kafka.aiokafka import KafkaConsumer, KafkaProducer
from typing import Optional

from datetime import datetime, timezone

from logging import Logger
import re
import json


class KafkaEventsHandlerView(View):
    def __init__(
        self,
        request: Request,
        logger: Logger,
        kafka_consumer:KafkaConsumer,
        kafka_producer: KafkaProducer
    ) -> None:
        super().__init__(request)
        self.logger = logger
        self.kafka_consumer = kafka_consumer
        self.kafka_producer = kafka_producer

    async def post(self):
        self.logger.info("Got POST request")
        data = await self.request.read()
        self.logger.debug("Got data from request: %s", data and data.decode())
        return await self.process_kafka_message(event=json.loads(data.decode()))

    async def process_kafka_message(self, event: dict) -> Response:
        event_type = self._extract_event_type_name(self.request.path)

        kafka_event = self._prepare_kafka_event(event, event_type)
        
        topic_name = f"{event_type}-events"
        await self.kafka_producer.send(value=kafka_event, topic=topic_name, wait=True)
        kafka_message = await self.kafka_consumer.get_one_message(topic_name)

        response_result = {
            "status": "success",
            "partition": kafka_message.partition,
            "offset": kafka_message.offset,
            "event": json.loads(kafka_message.value.decode())
        }

        return Response(text=json.dumps(response_result), content_type="application/json", status=201)


    def _extract_event_type_name(self, url_path: str) -> Optional[str]:
        match = re.search(r"/api/events/(\w+)", url_path)
        if match:
            return match.group(1)
        return None
    
    def _prepare_kafka_event(self, request_data: dict, event_type: str) -> bytes:
        id_field = f"{event_type}_id"
        id_value = request_data.get(id_field)

        action = request_data.get("action")

        timestamp = request_data.get("timestamp", datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'))

        event = {
            "id": f"{event_type}-{id_value}-{action}",
            "type": event_type,
            "timestamp": timestamp,
            "payload": request_data
        }

        return json.dumps(event).encode()


