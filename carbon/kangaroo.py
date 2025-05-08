from dataclasses import dataclass
from typing import Tuple

from .joint import JointState
from .module import Module, ModuleReference, sink, source


@dataclass
class KangarooCommand:
    left: float
    right: float


class KangarooDriver(Module):
    def __init__(self, left_actuator: ModuleReference, right_actuator: ModuleReference):
        super().__init__()

        self.create_one_to_many_connection(
            self.receive_motor_feedback,
            (left_actuator.module, right_actuator.module),
            (JointState, JointState),
        )

    @sink(JointState, JointState)
    def send_drive_commands(self, command: Tuple[JointState, JointState]):
        # Executes drive command
        pass

    @source(JointState, JointState)
    def receive_motor_feedback(self):
        pass
