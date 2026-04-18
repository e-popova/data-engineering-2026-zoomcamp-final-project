import asyncio
import json
import logging
import os
from datetime import datetime, timezone

import websockets
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC = 'klines'
SYMBOLS = 'btcusdt', 'ethusdt', 'bnbusdt', 'solusdt', 'xrpusdt', 'dogeusdt', 'adausdt', 'avaxusdt', 'dotusdt', 'maticusdt'

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def create_topic() -> None:
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    try:
        admin.create_topics([
            NewTopic(
                name=TOPIC,
                num_partitions=len(SYMBOLS),  # по партиции на символ
                replication_factor=1,
            )
        ])
        logger.info(f"Topic '{TOPIC}' created with {len(SYMBOLS)} partitions")
    except TopicAlreadyExistsError:
        logger.info(f"Topic '{TOPIC}' already exists")
    finally:
        admin.close()


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=str.encode,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        compression_type="gzip",
        linger_ms=100,  # батчим каждые 100ms
        acks="all",     # ждём подтверждения от всех реплик
    )


def build_ws_url(symbols: list[str]) -> str:
    """Binance combined stream URL для нескольких символов"""
    streams = "/".join(f"{s}@kline_1s" for s in symbols)
    return f"wss://stream.binance.com:9443/stream?streams={streams}"


def parse_kline(raw: str) -> dict | None:
    """
    Парсим сообщение от Binance.
    Возвращаем None если свеча ещё не закрылась.
    """
    data = json.loads(raw)
    kline = data["data"]["k"]

    # Binance шлёт обновления каждую секунду пока свеча открыта.
    # Нас интересует только финальная закрытая свеча.
    if not kline["x"]:
        return None

    return {
        "symbol":      data["data"]["s"],
        "open_time":   kline["t"],   # unix ms
        "close_time":  kline["T"],   # unix ms
        "open":        kline["o"],
        "high":        kline["h"],
        "low":         kline["l"],
        "close":       kline["c"],
        "volume":      kline["v"],
        "trades":      kline["n"],
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


async def consume(producer: KafkaProducer) -> None:
    url = build_ws_url(SYMBOLS)
    logger.info(f"Connecting to Binance WebSocket...")
    logger.info(f"Subscribed to {len(SYMBOLS)} symbols: {', '.join(SYMBOLS)}")

    # websockets.connect в цикле — автоматический reconnect при обрыве
    async for websocket in websockets.connect(url, ping_interval=20):
        try:
            async for raw in websocket:
                message = parse_kline(raw)

                if message is None:
                    continue  # свеча ещё не закрылась

                producer.send(
                    topic=TOPIC,
                    key=message["symbol"],  # key = symbol → одна монета всегда в одну партицию
                    value=message,
                )

                logger.info(
                    f"{message['symbol']} | "
                    f"close={message['close']} | "
                    f"volume={message['volume']} | "
                    f"trades={message['trades']}"
                )

        except websockets.ConnectionClosed as e:
            logger.warning(f"Connection closed ({e}), reconnecting...")
            continue

        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            continue


async def main() -> None:
    create_topic()
    producer = make_producer()

    try:
        await consume(producer)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        logger.info("Flushing remaining messages...")
        producer.flush()
        producer.close()
        logger.info("Done")


if __name__ == "__main__":
    asyncio.run(main())