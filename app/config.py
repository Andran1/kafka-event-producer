import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    KAFKA_BROKERS: str = os.getenv("KAFKA_BROKERS", "localhost:9092")
    SCHEMA_REGISTRY_URL: str = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    KAFKA_ACKS: str = os.getenv("KAFKA_ACKS", "all")
    KAFKA_RETRIES: int = int(os.getenv("KAFKA_RETRIES", 5))
    KAFKA_LINGER_MS: int = int(os.getenv("KAFKA_LINGER_MS", 5))

settings = Settings()