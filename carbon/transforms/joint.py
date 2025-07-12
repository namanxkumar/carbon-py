from carbon.common_data_types import Position
from carbon.core import Module, ModuleReference, Sink, Source, consumer, producer
from carbon.data import Autofill
from carbon.transforms.link import Link, Transform


class ContinuousJoint(Module):
    joint_state = Sink(Position)
    child_link_transform = Source(Transform)

    def __init__(self, child: ModuleReference[Link]):
        super().__init__()

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
            self.child_link_transform,
            child.module.transform_update,
            sync=True,
        )

    @consumer(joint_state)
    @producer(child_link_transform)
    def update_state(self, state: Position) -> Transform:
        """
        Update the joint state and return the corresponding transform.
        """
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
