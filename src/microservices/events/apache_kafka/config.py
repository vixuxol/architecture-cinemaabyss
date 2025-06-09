import attr
from typing import Optional, Sequence

import env


HOSTS_KEY = "KAFKA_HOSTS"
PORT_KEY = "KAFKA_PORT"
PRODUCER_TOPICS_KEY = "KAFKA_PRODUCER_TOPIC"
ACKS_KEY = "KAFKA_PRODUCER_ACKS"
SECURITY_PROTOCOL_KEY = "KAFKA_SECURITY_PROTOCOL"
COMPRESSION_TYPE_KEY = "KAFKA_COMPRESSION_TYPE"
CONSUMER_TOPICS_KEY = "KAFKA_CONSUMER_TOPICS"
CONSUMER_GROUP_KEY = "KAFKA_CONSUMER_GROUP"

def _get_hosts_config() -> Sequence[str]:
    raw_hosts = env.parse_env_var(HOSTS_KEY, parser=str)
    port = env.parse_env_var(PORT_KEY, parser=int)
    hosts = []
    for raw_host in raw_hosts.split(","):
        host = raw_host if ":" in raw_host else f"{raw_host}:{port}"
        hosts.append(host)
    return hosts
    

def _get_servers_config():
    raw_hosts = env.parse_env_var(HOSTS_KEY, parser=str)
    port = env.parse_env_var(PORT_KEY, parser=int)
    hosts = []
    for raw_host in raw_hosts.split(","):
        host = raw_host if ":" in raw_host else f"{raw_host}:{port}"
        hosts.append(host)
    return ",".join(hosts)


def _get_consumer_topics_config():
    consumer_topics = env.parse_env_var(CONSUMER_TOPICS_KEY, parser=str)
    return [topic.strip() for topic in consumer_topics.split(",")]

def _get_consumer_group_config():
    consumer_group = env.parse_env_var(CONSUMER_GROUP_KEY, default=None, parser=str, raise_error=False)
    return consumer_group

def _get_producer_topics_config() -> str:
    producer_topics = env.parse_env_var(PRODUCER_TOPICS_KEY, raise_error=True, parser=str)
    return [topic.strip() for topic in producer_topics.split(",")]

def _get_acks_config() -> int:
    return env.get_int_var(ACKS_KEY, 1)

def _get_security_protocol_config() -> str:
    return env.get_str_var(SECURITY_PROTOCOL_KEY, "PLAINTEXT")

def _get_compression_type_config() -> str:
    return env.parse_env_var(COMPRESSION_TYPE_KEY, default=None, raise_error=False)


@attr.s
class BaseKafkaConfig:
    hosts = attr.ib(type=Sequence[str], factory=_get_hosts_config)
    servers = attr.ib(type=str, factory=_get_servers_config)
    security_protocol = attr.ib(type=Optional[str], factory=_get_security_protocol_config)


@attr.s
class ProducerConfig(BaseKafkaConfig):
    producer_topics = attr.ib(type=Optional[str], factory=_get_producer_topics_config)
    acks = attr.ib(type=int, factory=_get_acks_config)
    compression_type = attr.ib(type=Optional[str], factory=_get_compression_type_config)

@attr.s
class ConsumerConfig(BaseKafkaConfig):
    consumer_topics = attr.ib(
        type=Sequence[str],
        converter=lambda x: [y.strip() for y in x.split(",")] if isinstance(x, str) else x,
        factory=_get_consumer_topics_config,
    )
    consumer_group = attr.ib(type=Optional[str], factory=_get_consumer_group_config)