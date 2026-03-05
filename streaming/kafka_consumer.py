import json
import os
from pathlib import Path

from kafka import KafkaConsumer


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "crypto_prices_raw")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "crypto-debug-consumer")
OUTPUT_FILE = Path(os.getenv("CONSUMER_OUTPUT_FILE", "/tmp/crypto_prices_raw.jsonl"))


def main() -> None:
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with OUTPUT_FILE.open("a", encoding="utf-8") as file:
        for message in consumer:
            event = message.value
            file.write(json.dumps(event) + "\n")
            file.flush()
            print(f"Consumed event: {event}")


if __name__ == "__main__":
    main()
