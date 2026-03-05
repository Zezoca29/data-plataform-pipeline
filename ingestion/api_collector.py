import json
import os
import time
from dataclasses import dataclass
from typing import Dict, Iterable

import requests
from kafka import KafkaProducer


COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"


@dataclass
class CollectorConfig:
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kafka_topic: str = os.getenv("KAFKA_TOPIC", "crypto_prices_raw")
    symbols: str = os.getenv("CRYPTO_SYMBOLS", "bitcoin,ethereum,solana")
    quote_currency: str = os.getenv("QUOTE_CURRENCY", "usd")
    polling_interval_seconds: int = int(os.getenv("POLLING_INTERVAL_SECONDS", "30"))


class CryptoCollector:
    def __init__(self, config: CollectorConfig):
        self.config = config
        self.producer = KafkaProducer(
            bootstrap_servers=config.kafka_bootstrap_servers,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            key_serializer=lambda value: value.encode("utf-8"),
        )

    def fetch_prices(self) -> Dict[str, Dict[str, float]]:
        params = {
            "ids": self.config.symbols,
            "vs_currencies": self.config.quote_currency,
            "include_24hr_change": "true",
            "include_last_updated_at": "true",
        }
        response = requests.get(COINGECKO_URL, params=params, timeout=10)
        response.raise_for_status()
        return response.json()

    def build_messages(self, prices: Dict[str, Dict[str, float]]) -> Iterable[Dict]:
        collected_at = int(time.time())
        for symbol, payload in prices.items():
            message = {
                "symbol": symbol,
                "quote_currency": self.config.quote_currency,
                "price": payload.get(self.config.quote_currency),
                "change_24h": payload.get(f"{self.config.quote_currency}_24h_change"),
                "source_last_updated_at": payload.get("last_updated_at"),
                "collected_at": collected_at,
            }
            yield message

    def run_forever(self) -> None:
        while True:
            try:
                prices = self.fetch_prices()
                for message in self.build_messages(prices):
                    self.producer.send(
                        self.config.kafka_topic,
                        key=message["symbol"],
                        value=message,
                    )
                self.producer.flush()
                print(f"Sent {len(prices)} records to topic={self.config.kafka_topic}")
            except Exception as exc:
                print(f"Collector error: {exc}")
            time.sleep(self.config.polling_interval_seconds)


if __name__ == "__main__":
    collector = CryptoCollector(CollectorConfig())
    collector.run_forever()
