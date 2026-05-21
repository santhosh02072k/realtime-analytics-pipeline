"""
Event Producer - Simulates user engagement events for a content platform
Produces events to Apache Kafka topic: user_events

Events simulated:
- view: user viewed a piece of content
- like: user liked a piece of content
- share: user shared a piece of content
- comment: user commented on a piece of content
"""

import json
import random
import time
import logging
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'user_events'
EVENTS_PER_SECOND = 2

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Sample data
USERS = [f"user_{i:04d}" for i in range(1, 201)]
CONTENT = [f"video_{i:04d}" for i in range(1, 101)]
ACTIONS = ['view', 'like', 'share', 'comment']
ACTION_WEIGHTS = [0.6, 0.25, 0.10, 0.05]  # view is most common
PLATFORMS = ['mobile', 'web', 'tablet']
REGIONS = ['UAE', 'KSA', 'Egypt', 'Kuwait', 'Bahrain']


def create_producer():
    """Create and return a Kafka producer with retry logic."""
    retries = 5
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3
            )
            logger.info(f"Connected to Kafka at {KAFKA_BROKER}")
            return producer
        except KafkaError as e:
            logger.warning(f"Attempt {attempt + 1}/{retries} failed: {e}")
            time.sleep(3)
    raise Exception("Failed to connect to Kafka after multiple attempts")


def generate_event():
    """Generate a realistic user engagement event."""
    user_id = random.choice(USERS)
    content_id = random.choice(CONTENT)
    action = random.choices(ACTIONS, weights=ACTION_WEIGHTS)[0]

    event = {
        'event_id': f"evt_{int(time.time() * 1000)}_{random.randint(1000, 9999)}",
        'user_id': user_id,
        'content_id': content_id,
        'action': action,
        'platform': random.choice(PLATFORMS),
        'region': random.choice(REGIONS),
        'session_id': f"sess_{user_id}_{random.randint(100, 999)}",
        'timestamp': datetime.utcnow().isoformat(),
        'duration_seconds': random.randint(5, 300) if action == 'view' else None
    }
    return event


def on_send_success(record_metadata):
    """Callback for successful message delivery."""
    logger.debug(
        f"Message delivered to {record_metadata.topic} "
        f"partition {record_metadata.partition} "
        f"offset {record_metadata.offset}"
    )


def on_send_error(excp):
    """Callback for failed message delivery."""
    logger.error(f"Message delivery failed: {excp}")


def main():
    """Main producer loop."""
    logger.info("Starting User Engagement Event Producer")
    logger.info(f"Publishing to topic: {TOPIC}")
    logger.info(f"Rate: {EVENTS_PER_SECOND} events/second")

    producer = create_producer()
    events_sent = 0

    try:
        while True:
            event = generate_event()

            producer.send(
                TOPIC,
                value=event,
                key=event['user_id'].encode('utf-8')
            ).add_callback(on_send_success).add_errback(on_send_error)

            events_sent += 1

            if events_sent % 100 == 0:
                logger.info(f"Total events sent: {events_sent}")

            time.sleep(1 / EVENTS_PER_SECOND)

    except KeyboardInterrupt:
        logger.info(f"Producer stopped. Total events sent: {events_sent}")
    finally:
        producer.flush()
        producer.close()
        logger.info("Producer connection closed.")


if __name__ == "__main__":
    main()
