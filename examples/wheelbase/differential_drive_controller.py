from typing import Tuple

from carbon import (
    Autofill,
    ConfiguredType,
    Module,
    ModuleReference,
    consumer,
    producer,
    safe_print,
)
from carbon.transforms import JointState
from examples.wheelbase.teleop import TeleopCommand


class DifferentialDriveController(Module):
    def __init__(
        self,
        left_motor: ModuleReference,
        right_motor: ModuleReference,
        update_motor_states: bool = False,
    ):
        super().__init__()

        if update_motor_states:
            self.create_connection(
                (JointState, JointState),
                self,
                (left_motor.module, right_motor.module),
                sync=True,
            )

    @consumer(ConfiguredType(TeleopCommand, sticky=False, queue_size=4))
    @producer(JointState, JointState)
    def create_motor_commands(
        self, command: TeleopCommand
    ) -> Tuple[JointState, JointState]:
        safe_print(f"Creating motor commands from teleop command: {command}")
        return (
            JointState(header=Autofill(), position=0.0, velocity=command.left),
            JointState(header=Autofill(), position=0.0, velocity=command.right),
        )
