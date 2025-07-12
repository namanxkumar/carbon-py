from carbon.common_data_types import Velocity
from carbon.core import ConfiguredSink, Module, Sink, consumer, safe_print


class KangarooDriver(Module):
    motor_commands = Sink(ConfiguredSink(Velocity), Velocity)

    def __init__(self):
        super().__init__()

        self.encoder_is_available = True  # Simulate encoder availability

    @consumer(motor_commands)
    def drive_motors(self, left_motor: Velocity, right_motor: Velocity):
        safe_print(f"Driving motors with commands: {left_motor}, {right_motor}")
        # Here you would implement the logic to send the motor commands to the hardware
        pass

    # def get_encoder_feedback(self) -> Tuple[Position, Position]:
    #     # This method would typically read from encoders to get the current position of the wheels
    #     # For this example, we will return dummy positions
    #     left_position = Position(
    #         header=Autofill(), position=Vector3(x=0.0, y=0.0, z=0.0)
    #     )
    #     right_position = Position(
    #         header=Autofill(), position=Vector3(x=0.0, y=0.0, z=0.0)
    #     )
    #     safe_print(f"Getting encoder feedback: {left_position}, {right_position}")
    #     return left_position, right_position
