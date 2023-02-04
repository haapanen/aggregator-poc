from enum import Enum
import numpy as np

class ModelType(Enum):
    GaussianRandomWalk = 1

class DataGenerator():
    def __init__(self, model_type: ModelType, start_value: float):
        self.model_type = model_type
        self.value = start_value

    def generate(self):
        if self.model_type == ModelType.GaussianRandomWalk:
            return self.generate_gaussian_random_walk()

    def generate_gaussian_random_walk(self, mean: float = 0, std: float = 1):
        self.value += np.random.normal(mean, std)
        return self.value
        