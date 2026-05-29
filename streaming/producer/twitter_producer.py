from kafka import KafkaProducer
import csv
import json
import time
from typing import Dict, Iterable, Optional

TOPIC_NAME = "tweets_topic"
BOOTSTRAP_SERVERS = ["localhost:29092"] 
CSV_PATH = "data/raw/Tweets.csv"        
SLEEP_SECONDS = 0.05                
MAX_MESSAGES: Optional[int] = None   


def create_producer():
    producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=5,
    request_timeout_ms=60000,
    max_block_ms=120000,
    metadata_max_age_ms=10000,
)
    return producer


def iter_tweets(csv_path: str) -> Iterable[Dict[str, str]]:
    """
    CSV dosyasını satır satır okur ve her satırı dict olarak yield eder.
    Burada DictReader header'lardan kolon isimlerini otomatik kullanıyor.
    """
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row


def build_message(row: Dict[str, str]) -> Dict[str, str]:
    """
    Kafka message payload'u (value) için, işimize yarayan alanları seçiyoruz.
    HDFS/Hive şeman ile uyumlu olacak şekilde alan adlarını kullanıyoruz.
    """
    return {
        "tweet_id": row.get("tweet_id"),
        "airline": row.get("airline"),
        "airline_sentiment": row.get("airline_sentiment"),
        "airline_sentiment_confidence": row.get("airline_sentiment_confidence"),
        "text": row.get("text"),
        "tweet_created": row.get("tweet_created"),
        "user_timezone": row.get("user_timezone"),
    }


def build_key(row: Dict[str, str]) -> str:
    """
    Kafka mesaj key'i: airline bazlı.
    Böylece ileride partitioning / key-based routing gösterebilirsin.
    """
    airline = row.get("airline") or "unknown"
    return airline


def main() -> None:
    producer = create_producer()
    print(f"[producer] Connected to Kafka at {BOOTSTRAP_SERVERS}")
    print(f"[producer] Reading CSV from {CSV_PATH}")

    sent = 0

    for row in iter_tweets(CSV_PATH):
        key = build_key(row)
        value = build_message(row)

        producer.send(
            TOPIC_NAME,
            key=key,
            value=value,
        )
        sent += 1

        if sent % 100 == 0:
            print(f"[producer] Sent {sent} messages so far...")

        if MAX_MESSAGES is not None and sent >= MAX_MESSAGES:
            print(f"[producer] Reached MAX_MESSAGES = {MAX_MESSAGES}, stopping.")
            break

        time.sleep(SLEEP_SECONDS)

    producer.flush()
    print(f"[producer] Finished. Total messages sent: {sent}")


if __name__ == "__main__":
    main()
