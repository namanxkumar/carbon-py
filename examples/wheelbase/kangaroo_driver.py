from typing import (
    Annotated,
    Any,
    Callable,
    Generic,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    overload,
)

from carbon import Autofill, Module, safe_print
from carbon.common_data_types import Position, Vector3, Velocity
from carbon.data import Data

# class KangarooEncoder(Module):
#     def __init__(self):
#         super().__init__()

#     @source(Position, Position)
#     def get_encoder_feedback(self) -> Tuple[Position, Position]:
#         # This method would typically read from encoders to get the current position of the wheels
#         # For this example, we will return dummy positions
#         left_position = Position(
#             header=Autofill(), position=Vector3(x=0.0, y=0.0, z=0.0)
#         )
#         right_position = Position(
#             header=Autofill(), position=Vector3(x=0.0, y=0.0, z=0.0)
#         )
#         safe_print(f"Getting encoder feedback: {left_position}, {right_position}")
#         return left_position, right_position


# class KangarooDriver(Module):
#     def __init__(self):
#         super().__init__()

#         self.encoder_is_available: bool = False  # Simulate encoder availability

#         if self.encoder_is_available:
#             self.encoder = KangarooEncoder()

#     @sink(Velocity, Velocity)
#     def drive_motors(self, left_motor: Velocity, right_motor: Velocity) -> None:
#         safe_print(f"Driving motors with commands: {left_motor}, {right_motor}")
#         # Here you would implement the logic to send the motor commands to the hardware
#         # For example, using a motor controller library or API
#         pass


class SinkConfiguration:
    def __init__(self, queue_size: int = 4, sticky: bool = False):
        self.queue_size = queue_size
        self.sticky = sticky


D = TypeVar("D", bound=Union[Data, Tuple[Data, ...]])


class Sink(Generic[D]):
    def __init__(
        self,
        type: Type[D],
        consumer: Optional[Callable[[D], Any]] = None,
    ):
        self.type = type

        # Get configuration from the annotations if available

        self.method = consumer


class Source(Generic[D]):
    def __init__(
        self,
        type: Union[Type[D], Tuple[Type[D]]],
        producer: Optional[Callable[..., D]] = None,
    ):
        self.type = type
        self.method = producer


def produces(*args: Source[D]):
    def decorator(func: Callable[..., D]):
        func.produces = args
        return func

    return decorator


def consumes(sink: Sink[D]):
    @overload
    def decorator(func: Callable[[Any, D], Any]): ...
    @overload
    def decorator(func: Callable[[D], Any]): ...
    def decorator(func):
        func.consumes = sink
        return func

    return decorator


class KangarooDriver(Module):
    motor_commands = Sink(Tuple[Velocity, Velocity])

    def __init__(self):
        super().__init__()

        self.encoder_is_available = True  # Simulate encoder availability

        if self.encoder_is_available:
            self.position_feedback = Source(
                Tuple[Position, Position], self.get_encoder_feedback
            )

    @consumes(motor_commands)
    def drive_motors(self, left_motor: Tuple[Velocity, Velocity]):
        safe_print(f"Driving motors with commands: {left_motor}")
        # Here you would implement the logic to send the motor commands to the hardware
        pass

    def get_encoder_feedback(self) -> Tuple[Position, Position]:
        # This method would typically read from encoders to get the current position of the wheels
        # For this example, we will return dummy positions
        left_position = Position(
            header=Autofill(), position=Vector3(x=0.0, y=0.0, z=0.0)
        )
        right_position = Position(
            header=Autofill(), position=Vector3(x=0.0, y=0.0, z=0.0)
        )
        safe_print(f"Getting encoder feedback: {left_position}, {right_position}")
        return left_position, right_position


kangaroo = KangarooDriver()
