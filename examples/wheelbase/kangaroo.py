from typing import Tuple

from carbon.common_data_types import Position, Vector3, Velocity
from carbon.core import Module, consumer, producer, safe_print
from carbon.data import Autofill


class KangarooDriver(Module):
    def __init__(self):
        super().__init__()

        self.encoder_is_available = False  # Simulate encoder availability

    @consumer(Velocity, Velocity)
    def drive_motors(self, left_motor: Velocity, right_motor: Velocity):
        safe_print(f"Driving motors with commands: {left_motor}, {right_motor}")
        # Here you would implement the logic to send the motor commands to the hardware
        pass

    @producer(Position, Position)
    def get_encoder_feedback(self) -> Tuple[Position, Position]:
        if not self.encoder_is_available:
            raise RuntimeError("Encoders are not available.")

        # This method would typically read from encoders to get the current position of the wheels
        # For this example, we will return dummy positions
        left_position = Position(
            header=Autofill(), position=Vector3(x=0.0, y=0.0, z=0.0)
        )
        right_position = Position(
            header=Autofill(), position=Vector3(x=0.0, y=0.0, z=0.0)
        )
        # safe_print(f"Getting encoder feedback: {left_position}, {right_position}")
        return left_position, right_position
