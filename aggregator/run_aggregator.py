import datetime
import json
import time
import click
import logging
from aggregator.database import Database
from core.message import Sample
from infra.mqtt_client import MqttClient


logger = logging.getLogger(__name__)


def create_on_message():
    database = Database()

    def on_message(client, userdata, message):
        message = message.payload.decode()
        sample = Sample.parse_raw(message)
        database.store_measurement(sample)

    return on_message


def create_on_connect(mqtt_client: MqttClient, topic: str):
    def on_connect():
        logger.info(f"Subscribing to topic {topic}")
        mqtt_client.subscribe(topic, create_on_message())

    return on_connect


@click.command()
@click.option("--host", default="localhost", help="MQTT host", required=True)
@click.option("--port", default=1883, help="MQTT port", required=True)
@click.option("--topic", default="measurements/#", help="MQTT topic", required=True)
def run_aggregator(host: str, port: int, topic: str):
    logger.info(f"Starting aggregator")

    Database().create_tables()

    logger.info(f"Connecting to {host}:{port} on topic {topic}")

    mqtt_client = MqttClient(host, port)
    mqtt_client.connect()

    mqtt_client.on_connect = create_on_connect(mqtt_client, topic)
    mqtt_client.client.loop_start()

    while True:
        time.sleep(10)

        logger.info(f"Aggregating data")

        database = Database()

        aggregate_timestamp = datetime.datetime.now().replace(second=0, microsecond=0)

        generator = database.get_aggregates_for_minute(aggregate_timestamp)

        for aggregate in generator:
            database.store_aggregate(aggregate_timestamp, aggregate[1])

            aggregate[1]["timestamp"] = aggregate_timestamp.isoformat()

            mqtt_client.publish("aggregates", json.dumps(aggregate[1]))

        logger.info(f"Aggregation complete")

        # now minus 5 minutes
        database.remove_old_measurements(
            datetime.datetime.now() - datetime.timedelta(minutes=5)
        )


if __name__ == "__main__":
    run_aggregator()
