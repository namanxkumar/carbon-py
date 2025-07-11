from typing import Optional, Tuple

from carbon import Autofill, ConfigurableSink, Module, safe_print, sink, source
from carbon.common_data_types import Position, Twist, Vector3, Velocity


class DifferentialDriveController(Module):
    """
    A controller for a differential drive robot that converts twist commands
    into motor commands for the left and right motors.
    It can operate in both open-loop and closed-loop modes.
    In open-loop mode, it generates positions based on the velocities.
    In closed-loop mode, it receives positions from the motors.
    """

    def __init__(
        self,
        create_joint_positions: bool = True,
        wheel_separation: float = 0.5,
        wheel_radius: float = 0.1,
    ):
        super().__init__()

        if create_joint_positions:
            self.create_connection((Velocity, Velocity), self, self, sync=True)

        self.wheel_separation = wheel_separation
        self.wheel_radius = wheel_radius

        self._previous_timestamp: Optional[float] = None
        self._previous_positions: Optional[Tuple[Vector3, Vector3]] = None
        self._previous_heading: Optional[float] = None

    @sink(ConfigurableSink(Twist, sticky=False, queue_size=4))
    @source(Velocity, Velocity)
    def convert_motor_commands(self, twist_command: Twist) -> Tuple[Velocity, Velocity]:
        safe_print(f"Creating motor commands from teleop command: {twist_command}")
        # Convert the Twist command to velocity commands for the motors
        left_velocity = twist_command.linear.x - twist_command.angular.z
        right_velocity = twist_command.linear.x + twist_command.angular.z

        return (
            Velocity(
                header=Autofill(),
                velocity=Vector3(x=left_velocity, y=0.0, z=0.0),
            ),
            Velocity(
                header=Autofill(),
                velocity=Vector3(x=right_velocity, y=0.0, z=0.0),
            ),
        )

    @sink(Velocity, Velocity)
    @source(Position, Position)
    def update_joint_positions(
        self, left_velocity: Velocity, right_velocity: Velocity
    ) -> Tuple[Position, Position]:
        if self._previous_timestamp is None:
            return (
                Position(header=Autofill(), position=Vector3(x=0.0, y=0.0, z=0.0)),
                Position(header=Autofill(), position=Vector3(x=0.0, y=0.0, z=0.0)),
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
            Position(
                header=Autofill(),
                position=self._previous_positions[0],
            ),
            Position(
                header=Autofill(),
                position=self._previous_positions[1],
            ),
        )
