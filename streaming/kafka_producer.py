"""Producer dedicado para publicar eventos de preço no Kafka.

Pode ser executado isoladamente para testar a camada de streaming.
"""

from ingestion.api_collector import CollectorConfig, CryptoCollector


if __name__ == "__main__":
    CryptoCollector(CollectorConfig()).run_forever()
