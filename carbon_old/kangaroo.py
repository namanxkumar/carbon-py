from dataclasses import dataclass
from typing import Tuple

from .joint import JointState
from .module import Module, ModuleReference, consumer, producer


@dataclass
class KangarooCommand:
    left: float
    right: float


class KangarooDriver(Module):
    def __init__(
        self,
        left_actuator: ModuleReference,
        right_actuator: ModuleReference,
        use_encoder: bool = True,
    ):
        super().__init__()

        if use_encoder:
            self.create_one_to_many_connection(
                self.receive_motor_feedback,
                (left_actuator.module, right_actuator.module),
                (JointState, JointState),
            )

    @consumer(JointState, JointState)
    def send_drive_commands(self, command: Tuple[JointState, JointState]):
        # Executes drive command
        print("Executing drive command", command)

    @producer(JointState, JointState)
    def receive_motor_feedback(self):
        return (
            JointState(position=0.0, velocity=0.0),
            JointState(position=0.0, velocity=0.0),
        )
