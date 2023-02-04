import datetime
import time
import click
import logging
from core.message import Sample
from infra.mqtt_client import MqttClient
from publisher.device import Device, DeviceConfig


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


@click.option("--host", default="localhost", help="MQTT host", required=True)
@click.option("--port", default=1883, help="MQTT port", required=True)
@click.option("--topic", default="measurements", help="MQTT topic", required=True)
@click.option(
    "--device-config",
    default="./publisher/device_config.json",
    help="Device configuration file",
    required=True,
)
@click.command()
def run_publisher(host: str, port: int, topic: str, device_config: str):
    device_config = DeviceConfig.parse_file(device_config)

    client = MqttClient(host, port)

    client.connect()

    count = 0

    device = Device.from_config(device_config)

    previous_publish_time = datetime.datetime.now()
    # add second to previous publish time
    previous_publish_time += datetime.timedelta(seconds=1)
    while True:
        current_time = datetime.datetime.now()

        for component in device.components:
            for property in component.properties:
                tag = f"{device.name}/{component.name}/{property.name}"
                value = property.generator.generate_gaussian_random_walk(0, 0.2)

                client.publish(
                    topic + "/" + tag,
                    Sample(
                        tag=tag,
                        value=value,
                        timestamp=datetime.datetime.now(),
                    ).json(),
                )

                count += 1

        time.sleep(0.1)

        if current_time - previous_publish_time > datetime.timedelta(seconds=1):
            logger.info(f"Published {count} messages")
            previous_publish_time = current_time
            count = 0
