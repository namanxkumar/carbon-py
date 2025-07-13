from typing import Optional, Tuple

from carbon import (
    ConfiguredType,
    Module,
    consumer,
    producer,
    safe_print,
)
from carbon.common_data_types import Position, Twist, Vector3, Velocity


class DifferentialDriveController(Module):
    def __init__(self, open_loop: bool = False):
        super().__init__()

        self._previous_timestamp: Optional[float] = None
        self._previous_positions: Optional[Tuple[Vector3, Vector3]] = None
        self._previous_heading: Optional[float] = None

        if open_loop:
            self.add_method(
                self.update_joint_positions,
                consumes=(Velocity, Velocity),
                produces=(Position, Position),
            )
            self.create_connection((Velocity, Velocity), self, self)

    @consumer(ConfiguredType(Twist, sticky=False, queue_size=4))
    @producer(Velocity, Velocity)
    def convert_motor_commands(self, twist_command: Twist) -> Tuple[Velocity, Velocity]:
        safe_print(f"Creating motor commands from teleop command: {twist_command}")
        # Convert the Twist command to velocity commands for the motors
        left_velocity = twist_command.linear.x - twist_command.angular.z
        right_velocity = twist_command.linear.x + twist_command.angular.z

        return (
            Velocity(velocity=Vector3(x=left_velocity, y=0.0, z=0.0)),
            Velocity(velocity=Vector3(x=right_velocity, y=0.0, z=0.0)),
        )

    def update_joint_positions(
        self, left_velocity: Velocity, right_velocity: Velocity
    ) -> Tuple[Position, Position]:
        print("Updating joint positions in open loop")
        if self._previous_timestamp is None:
            return (
                Position(position=Vector3(x=0.0, y=0.0, z=0.0)),
                Position(position=Vector3(x=0.0, y=0.0, z=0.0)),
            )
        assert self._previous_positions is not None, "Previous positions must be set."

        current_timestamp = left_velocity.header.stamp  # type: ignore[union-attr]
        left_position = self._previous_positions[0].x + left_velocity.velocity.x * (
            current_timestamp - self._previous_timestamp  # type: ignore[union-attr]
        )
        right_position = self._previous_positions[1].x + right_velocity.velocity.x * (
            current_timestamp - self._previous_timestamp  # type: ignore[union-attr]
        )

        self._previous_timestamp = current_timestamp  # type: ignore[union-attr]
        self._previous_positions = (
            Vector3(x=left_position, y=0.0, z=0.0),
            Vector3(x=right_position, y=0.0, z=0.0),
        )

        return (
            Position(position=self._previous_positions[0]),
            Position(position=self._previous_positions[1]),
        )
