from dataclasses import dataclass
from typing import Tuple

from .joint import JointState
from .module import Module, sink, source


@dataclass
class TeleopCommand:
    left: float
    right: float


class DifferentialDriveController(Module):
    def __init__(self, left_motor: Module, right_motor: Module):
        super().__init__()

        self.create_one_to_many_connection(
            self.create_motor_commands,
            (left_motor, right_motor),
            (JointState, JointState),
        )

    @sink(TeleopCommand)
    @source(JointState, JointState)
    def create_motor_commands(
        self, command: TeleopCommand
    ) -> Tuple[JointState, JointState]:
        # Send drive command
        # return MotorCommand(), MotorCommand()
        pass
