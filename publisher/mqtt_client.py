from paho.mqtt import client as mqtt
import logging

logger = logging.getLogger(__name__)

class MqttClient:
    def __init__(self, client_id, host, port):
        self.client_id = client_id
        self.host = host
        self.port = port
        self.client = mqtt.Client(client_id=self.client_id)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect

    def on_connect(self: "MqttClient", client: mqtt.Client):
        logger.debug("Connected to MQTT broker")

    def on_disconnect(self: "MqttClient", client: mqtt.Client):
        logger.debug("Disconnected from MQTT broker. Reconnecting...")
        self.connect()

    def connect(self: "MqttClient"):
        self.client.connect(self.host, self.port)

    def publish(self: "MqttClient", topic: str, payload: str):
        self.client.publish(topic, payload)
