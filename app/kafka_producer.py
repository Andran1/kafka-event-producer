from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from .config import settings

class KafkaProducerService:
    def __init__(self):
        self.schema_registry_client = SchemaRegistryClient({"url": settings.SCHEMA_REGISTRY_URL})
        self.schema_cache = {}

        producer_conf = {
            "bootstrap.servers": settings.KAFKA_BROKERS,
            "key.serializer": StringSerializer("utf_8"),
            "acks": settings.KAFKA_ACKS,
            "enable.idempotence": True,
            "retries": settings.KAFKA_RETRIES,
            "linger.ms": settings.KAFKA_LINGER_MS,
        }
        self.producer = SerializingProducer(producer_conf)

    def get_avro_serializer(self, event_type: str) -> AvroSerializer:
        if event_type not in self.schema_cache:
            subject = f"{event_type}-value"
            schema_meta = self.schema_registry_client.get_latest_version(subject)
            schema_str = schema_meta.schema.schema_str
            avro_serializer = AvroSerializer(self.schema_registry_client, schema_str)
            self.schema_cache[event_type] = avro_serializer
        return self.schema_cache[event_type]

    def produce_event(self, topic: str, key: str, value: dict):
        serializer = self.get_avro_serializer(topic)
        serialized_value = serializer(value, None)

        def delivery_report(err, msg):
            if err:
                print(f"Delivery failed for record {msg.key()}: {err}")
            else:
                print(f"Record produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

        self.producer.produce(
            topic=topic,
            key=key,
            value=serialized_value,
            on_delivery=delivery_report
        )
        self.producer.flush()
