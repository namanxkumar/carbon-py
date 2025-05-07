from dataclasses import dataclass
from typing import Tuple

from .joint import JointState
from .module import Module, sink


@dataclass
class KangarooCommand:
    left: float
    right: float


class KangarooDriver(Module):
    def __init__(self, left_actuator: Module, right_actuator: Module):
        super().__init__()

    @sink(JointState, JointState)
    def send_drive_commands(self, command: Tuple[JointState, JointState]):
        # Executes drive command
        pass

    def receive_motor_feedback(self):
        pass
