from dataclasses import dataclass
from typing import Tuple

from .joint import JointState
from .module import Module, sink


@dataclass
class KangarooCommand:
    left: float
    right: float


class KangarooDriver(Module):
    def __init__(self):
        super().__init__()

        # self.create_many_to_one_connection(
        #     (left_actuator, right_actuator),
        #     self.send_drive_commands,
        #     (JointState, JointState),
        # )

    @sink(JointState, JointState)
    def send_drive_commands(self, command: Tuple[JointState, JointState]):
        # Executes drive command
        pass

    def receive_motor_feedback(self):
        pass
