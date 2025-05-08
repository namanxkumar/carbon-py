from dataclasses import dataclass
from typing import Tuple

from .joint import JointState
from .module import Module, ModuleReference, sink, source


@dataclass
class TeleopCommand:
    left: float
    right: float


class DifferentialDriveController(Module):
    def __init__(
        self,
        left_motor: ModuleReference,
        right_motor: ModuleReference,
        update_motor_states: bool = False,
    ):
        super().__init__()

        if update_motor_states:
            self.create_one_to_many_connection(
                self.create_motor_commands,
                (left_motor.module, right_motor.module),
                (JointState, JointState),
            )

    @sink(TeleopCommand)
    @source(JointState, JointState)
    def create_motor_commands(
        self, command: TeleopCommand
    ) -> Tuple[JointState, JointState]:
        # Send drive command
        # return MotorCommand(), MotorCommand()
        return (
            JointState(position=0.0, velocity=command.left),
            JointState(position=0.0, velocity=command.right),
        )
