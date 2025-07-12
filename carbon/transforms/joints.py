from abc import ABC, abstractmethod

from carbon.common_data_types import Position
from carbon.core import Module, ModuleReference, consumer, producer
from carbon.data import Autofill, StampedData


class Transform(StampedData):
    """
    Represents a transformation in 3D space, including translation and rotation.
    This class is used to encapsulate the transformation information for joints in a robotic system.
    """

    translation: tuple[float, float, float]  # x, y, z translation
    rotation: tuple[float, float, float, float]  # quaternion (x, y, z, w)


class Joint(Module, ABC):
    def __init__(self, parent: ModuleReference, child: ModuleReference):
        super().__init__()
        self.parent = parent
        self.child = child

        self._transform = Transform(
            header=Autofill(),
            translation=(0.0, 0.0, 0.0),  # Placeholder for translation
            rotation=(
                0.0,
                0.0,
                0.0,
                1.0,
            ),  # Placeholder for rotation (identity quaternion)
        )

        self.create_connection(
            Transform,
            self,
            self.child.module,
            sync=True,
        )

    @abstractmethod
    @consumer(Position)
    @producer(Transform)
    def update_state(self, state) -> Transform:
        """
        Update the joint state based on the provided state.
        This method should be implemented by subclasses.
        """
        pass


class ContinuousJoint(Joint):
    def __init__(self, parent: ModuleReference, child: ModuleReference):
        super().__init__(parent, child)

    @consumer(Position)
    @producer(Transform)
    def update_state(self, state: Position) -> Transform:
        """
        Update the joint state and return the corresponding transform.
        """
        print("Updating child links")
        self._transform.translation = (
            state.position.x,  # Assuming position is a float representing the joint's position
            0.0,  # Placeholder for y translation
            0.0,  # Placeholder for z translation
        )
        self._transform.rotation = (
            0.0,  # Placeholder for x rotation
            0.0,  # Placeholder for y rotation
            0.0,  # Placeholder for z rotation
            1.0,  # w component of the quaternion (identity)
        )
        return self._transform
