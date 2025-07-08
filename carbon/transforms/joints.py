from abc import ABC, abstractmethod

from carbon.core import Module, ModuleReference, sink, source
from carbon.data import Autofill, StampedData


class JointState(StampedData):
    """
    Represents the state of a joint, including position and velocity.
    This class is used to encapsulate the state information for joints in a robotic system.
    """

    position: float  # Position of the joint
    velocity: float  # Velocity of the joint


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
            self,
            self.child.module,
            Transform,
            sync=True,
        )

    @abstractmethod
    @sink(JointState)
    @source(Transform)
    def update_state(self, state) -> Transform:
        """
        Update the joint state based on the provided state.
        This method should be implemented by subclasses.
        """
        pass


class ContinuousJoint(Joint):
    def __init__(self, parent: ModuleReference, child: ModuleReference):
        super().__init__(parent, child)

    @sink(JointState)
    @source(Transform)
    def update_state(self, state: JointState) -> Transform:
        """
        Update the joint state and return the corresponding transform.
        """
        self._transform.translation = (
            state.position,  # Assuming position is a float representing the joint's position
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
