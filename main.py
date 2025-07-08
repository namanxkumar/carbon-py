import sys
import termios
import tty
from typing import Tuple

from carbon import (
    Autofill,
    ConfigurableSink,
    Data,
    # ExecutionGraph,
    Module,
    ModuleReference,
    safe_print,
    sink,
    source,
)
from carbon.core.execution2 import ExecutionGraph
from carbon.transforms import (
    ContinuousJoint,
    CylindricalGeometry,
    JointState,
    Link,
)


class TeleopCommand(Data):
    left: float
    right: float


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

    @sink(ConfigurableSink(TeleopCommand, sticky=False, queue_size=4))
    @source(JointState, JointState)
    def create_motor_commands(
        self, command: TeleopCommand
    ) -> Tuple[JointState, JointState]:
        safe_print(f"Creating motor commands from teleop command: {command}")
        return (
            JointState(header=Autofill(), position=0.0, velocity=command.left),
            JointState(header=Autofill(), position=0.0, velocity=command.right),
        )


class Motor(ContinuousJoint):
    def __init__(self, parent: ModuleReference, child: ModuleReference):
        super().__init__(parent, child)


class Wheel(Link):
    def __init__(self):
        super().__init__(
            CylindricalGeometry(
                mass=1.0,  # Example mass for the wheel
                radius=0.1,  # Example radius for the wheel
                height=0.05,  # Example height for the wheel
            )
        )


class WheelBase(Module):
    def __init__(self):
        super().__init__()

        self.left_wheel = Link(
            CylindricalGeometry(
                mass=1.0,  # Example mass for the wheel
                radius=0.1,  # Example radius for the wheel
                height=0.05,  # Example height for the wheel
            )
        )
        self.left_motor = Motor(
            parent=self.as_reference(), child=self.left_wheel.as_reference()
        )

        self.right_wheel = Link(
            CylindricalGeometry(
                mass=1.0,  # Example mass for the wheel
                radius=0.1,  # Example radius for the wheel
                height=0.05,  # Example height for the wheel
            )
        )
        self.right_motor = Motor(
            parent=self.as_reference(), child=self.right_wheel.as_reference()
        )

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
                # Set cbreak mode and turn off echo
                tty.setcbreak(fd)
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


robot = Robot()
print(robot)
execution_graph = ExecutionGraph(robot)
# print("\nExecution Graph Layers:")
# print(execution_graph.layers)
print("\nProcess Groups:")
for index, group in execution_graph.processes.items():
    print(index)
    for process in group:
        print(process)
# print(execution_graph.processes)
# print(execution_graph.process_mapping)
# print(execution_graph.in_process_layer_mapping)
# print("\nConnections:")
# for connection in robot.get_connections():
#     print(connection)
# print("\nMethods:")
# for method in robot.get_methods():
#     print(method.name)
#     print("  Depends on:", method.dependents)
#     print("  Produces for:", method.dependencies)
# print()
# execution_graph.execute()
# print("\nExecution completed.")
