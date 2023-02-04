from typing import Callable
import paho.mqtt.client as mqtt
import logging

logger = logging.getLogger(__name__)


class MqttClient:
    on_connect: Callable[[], None]

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.client = mqtt.Client()
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect

    def _on_connect(self: "MqttClient", client: mqtt.Client, userdata, flags, rc):
        logger.debug("Connected to MQTT broker")

        if self.on_connect is not None:
            self.on_connect()

    def _on_disconnect(self: "MqttClient", client: mqtt.Client, userdata, rc):
        logger.debug("Disconnected from MQTT broker")

    def connect(self: "MqttClient"):
        self.client.connect(self.host, self.port)

    def publish(self: "MqttClient", topic: str, payload: str):
        self.client.publish(topic, payload)

    def on_message(self: "MqttClient", client: mqtt.Client, userdata, message):
        logger.debug(f"Received message: {message.payload.decode()}")

    def subscribe(
        self: "MqttClient",
        topic: str,
        callback: Callable[[mqtt.Client, any, mqtt.MQTTMessage], None],
    ):
        logger.debug(f"Subscribing to topic {topic}")
        self.subcribed_to = topic
        self.on_message = callback
        self.client.subscribe(topic)
        self.client.on_message = self.on_message
