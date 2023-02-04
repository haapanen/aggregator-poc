import datetime
import click
import logging
from data_generator import DataGenerator, ModelType
from device import Device, DeviceConfig
from message import Sample

from mqtt_client import MqttClient

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


@click.option("--host", default="localhost", help="MQTT host", required=True)
@click.option("--port", default=1883, help="MQTT port", required=True)
@click.option("--topic", default="test", help="MQTT topic", required=True)
@click.option(
    "--device-config",
    default="device_config.json",
    help="Device configuration file",
    required=True,
)
@click.command()
def run(host: str, port: int, topic: str, device_config: str):
    device_config = DeviceConfig.parse_file(device_config)

    client = MqttClient(topic, host, port)

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
                tag = f"{device.name}.{component.name}.{property.name}"
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

        if current_time - previous_publish_time > datetime.timedelta(seconds=1):
            logger.info(f"Published {count} messages")
            previous_publish_time = current_time
            count = 0


if __name__ == "__main__":
    run()

logger.setLevel(logging.DEBUG)
