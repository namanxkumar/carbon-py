from dataclasses import dataclass
from typing import Tuple

from carbon.data import Data
from carbon.execution import ExecutionGraph
from carbon.module import Module, ModuleReference, sink, source


@dataclass
class TeleopCommand(Data):
    left: float
    right: float


@dataclass
class JointState(Data):
    position: float
    velocity: float


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
                self,
                (left_motor.module, right_motor.module),
                (JointState, JointState),
                blocking=True,
            )

    @sink(TeleopCommand)
    @source(JointState, JointState)
    def create_motor_commands(
        self, command: TeleopCommand
    ) -> Tuple[JointState, JointState]:
        return (
            JointState(position=0.0, velocity=command.left),
            JointState(position=0.0, velocity=command.right),
        )


class ContinuousJoint(Module):
    def __init__(self):
        super().__init__()

    @sink(JointState)
    def update_state(self, state: JointState):
        # Update the state of the joint based on the received state
        print(f"Updating joint state: {state}")


class WheelBase(Module):
    def __init__(self):
        super().__init__()
        self.left_motor = ContinuousJoint()

        self.right_motor = ContinuousJoint()

        self.controller = DifferentialDriveController(
            left_motor=self.left_motor.as_reference(),
            right_motor=self.right_motor.as_reference(),
            update_motor_states=True,
        )


class Teleop(Module):
    def __init__(self):
        super().__init__()

    @source(TeleopCommand)
    def teleop_command(self) -> TeleopCommand:
        print("running teleop_command")
        return TeleopCommand(left=0.0, right=0.0)


class Robot(Module):
    def __init__(self):
        super().__init__()
        self.wheelbase = WheelBase()
        self.teleop = Teleop()

        self.create_connection(self.teleop, self.wheelbase.controller, TeleopCommand)


teleop = Robot()
# print(
#     teleop._modules,
#     teleop._connections,
#     teleop._blocked_connections,
#     teleop._sources,
#     teleop._sinks,
# )
# print()
execution_graph = ExecutionGraph(teleop)
print(len(execution_graph.nodes))
print(len(execution_graph.layers))
print(len(execution_graph.process_groups))
