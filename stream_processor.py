"""
Flink Stream Processor
Consumes user engagement events from Kafka,
applies stream processing transformations,
and writes results to ClickHouse.

Processing pipeline:
1. Consume events from Kafka topic: user_events
2. Parse and validate JSON events
3. Apply event-time windowing (1-minute tumbling windows)
4. Aggregate engagement metrics per window
5. Write results to ClickHouse
"""

import json
import logging
from datetime import datetime
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common import WatermarkStrategy, Duration, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.window import TumblingEventTimeWindows, Time
from pyflink.datastream.functions import MapFunction, ReduceFunction, WindowFunction
from clickhouse_driver import Client

# Configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'user_events'
KAFKA_GROUP_ID = 'flink-engagement-processor'
CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 9000
CLICKHOUSE_DB = 'analytics'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ParseEventFunction(MapFunction):
    """Parse raw JSON string events into structured dictionaries."""

    def map(self, value):
        try:
            event = json.loads(value)
            return {
                'event_id': event.get('event_id', ''),
                'user_id': event.get('user_id', ''),
                'content_id': event.get('content_id', ''),
                'action': event.get('action', ''),
                'platform': event.get('platform', ''),
                'region': event.get('region', ''),
                'session_id': event.get('session_id', ''),
                'timestamp': event.get('timestamp', ''),
                'duration_seconds': event.get('duration_seconds')
            }
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse event: {e}")
            return None


class ClickHouseSinkFunction:
    """Sink function to write processed events to ClickHouse."""

    def __init__(self):
        self.client = None

    def open(self):
        self.client = Client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            database=CLICKHOUSE_DB
        )
        logger.info(f"Connected to ClickHouse at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}")

    def write_event(self, event):
        """Write a single event to ClickHouse user_events table."""
        if event is None:
            return

        query = """
            INSERT INTO analytics.user_events
            (event_id, user_id, content_id, action, platform, region,
             session_id, timestamp, duration_seconds)
            VALUES
        """
        data = [(
            event['event_id'],
            event['user_id'],
            event['content_id'],
            event['action'],
            event['platform'],
            event['region'],
            event['session_id'],
            datetime.fromisoformat(event['timestamp']),
            event['duration_seconds']
        )]
        self.client.execute(query, data)

    def close(self):
        if self.client:
            self.client.disconnect()


def create_kafka_source():
    """Create and configure Kafka source connector."""
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_topics(KAFKA_TOPIC)
        .set_group_id(KAFKA_GROUP_ID)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def main():
    """Main Flink streaming job."""
    logger.info("Starting Flink Stream Processor")

    # Set up streaming environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Configure watermark strategy for event-time processing
    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_timestamp_assigner(
            lambda event, timestamp: int(
                datetime.fromisoformat(
                    json.loads(event).get('timestamp', '')
                ).timestamp() * 1000
            )
        )
    )

    # Create Kafka source
    kafka_source = create_kafka_source()

    # Build processing pipeline
    stream = (
        env
        .from_source(kafka_source, watermark_strategy, "Kafka User Events")
    )

    # Parse events
    parsed_stream = stream.map(ParseEventFunction())

    # Filter out null events (failed parsing)
    valid_stream = parsed_stream.filter(lambda x: x is not None)

    # Write to ClickHouse
    sink = ClickHouseSinkFunction()
    sink.open()

    def write_to_clickhouse(event):
        sink.write_event(event)
        return event

    valid_stream.map(write_to_clickhouse)

    logger.info("Flink job configured. Executing...")
    env.execute("Real-Time User Engagement Analytics")


if __name__ == "__main__":
    main()
