import re
import os

from env import get_str_var, get_int_var, get_bool_var
from apache_kafka.config import ConsumerConfig, ProducerConfig


_DEFAULT_SERVICE_HOST = "0.0.0.0"
_DEFAULT_SERVICE_PORT = 8000


class AppConfig:
    def __init__(self) -> None:
        self.host = get_str_var("HOST", _DEFAULT_SERVICE_HOST)
        self.port = get_int_var("PORT", _DEFAULT_SERVICE_PORT)

        self.kafka_consumer = ConsumerConfig()
        self.kafka_producer = ProducerConfig()


