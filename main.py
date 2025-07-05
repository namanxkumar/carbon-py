import sys
import termios
import tty
from typing import Tuple

from carbon import ConfigurableType, Data, Module, ModuleReference, sink, source
from carbon.execution import ExecutionGraph


class TeleopCommand(Data):
    left: float
    right: float


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
                sync=True,
            )

    @sink(ConfigurableType(TeleopCommand, sticky=True))
    @source(JointState, JointState)
    def create_motor_commands(
        self, command: TeleopCommand
    ) -> Tuple[JointState, JointState]:
        safe_print(f"Creating motor commands from teleop command: {command}")
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
        safe_print(f"Updating joint state: {state}")


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
        print("Running Teleop (press 'q' to exit, use wasd for control)")

        def read_wasd_key():
            fd = sys.stdin.fileno()
            old_settings = termios.tcgetattr(fd)
            try:
                # Set raw mode and turn off echo
                tty.setraw(fd)
                new_settings = termios.tcgetattr(fd)
                new_settings[3] = new_settings[3] & ~termios.ECHO  # lflags
                termios.tcsetattr(fd, termios.TCSADRAIN, new_settings)
                ch = sys.stdin.read(1)
                print("\r\033[K", end="", flush=True)
                if ch == "w":
                    return TeleopCommand(left=1.0, right=1.0)
                elif ch == "s":
                    return TeleopCommand(left=-1.0, right=-1.0)
                elif ch == "d":
                    return TeleopCommand(left=0.5, right=1.0)
                elif ch == "a":
                    return TeleopCommand(left=1.0, right=0.5)
                elif ch == "q":
                    print("Exiting...")
                    sys.exit(0)
                return TeleopCommand(left=0.0, right=0.0)
            finally:
                termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)

        return read_wasd_key()


class Robot(Module):
    def __init__(self):
        super().__init__()
        self.wheelbase = WheelBase()
        self.teleop = Teleop()

        self.create_connection(self.teleop, self.wheelbase.controller, TeleopCommand)


# Helper function to always print at the start of a clean line
def safe_print(*args, **kwargs):
    print("\r\033[K", end="", flush=True)  # Move to start and clear line
    print(*args, **kwargs)


robot = Robot()
print(robot)
execution_graph = ExecutionGraph(robot.as_reference())
print("\nExecution Graph Layers:")
print(execution_graph.layers)
print("\nProcess Groups:")
print(execution_graph.processes)
print("\nConnections:")
for connection in robot.get_connections():
    print(connection)
print("\nMethods:")
for method in robot.get_methods():
    print(method.name)
    print("  Depends on:", method.dependents_to_splits.keys())
    print("  Produces for:", method.dependencies_to_merges.keys())
print()
execution_graph.execute()
print("\nExecution completed.")
