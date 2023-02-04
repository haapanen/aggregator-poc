from pydantic import BaseModel

from data_generator import DataGenerator, ModelType


class DeviceComponent(BaseModel):
    name: str
    properties: list[str]


class DeviceConfig(BaseModel):
    name: str
    components: list[DeviceComponent]


class DeviceComponentProperty:
    name: str
    generator: DataGenerator

    def __init__(self, name: str, generator: DataGenerator) -> None:
        self.name = name
        self.generator = generator
        pass


class DeviceComponent:
    name: str
    properties: list[DeviceComponentProperty]

    def __init__(self, name: str, properties: list[DeviceComponentProperty]) -> None:
        self.name = name
        self.properties = properties
        pass


class Device:
    name: str
    components: list[DeviceComponent]

    @staticmethod
    def from_config(config: DeviceConfig):
        device_config = Device()
        device_config.name = config.name
        device_config.components = []
        for component_model in config.components:
            component = DeviceComponent(component_model.name, [])
            component.name = component_model.name
            component.properties = []
            for property_name in component_model.properties:
                component.properties.append(
                    DeviceComponentProperty(
                        property_name,
                        DataGenerator(
                            model_type=ModelType.GaussianRandomWalk, start_value=0
                        ),
                    )
                )
            device_config.components.append(component)
        return device_config
