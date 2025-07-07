import sys
import termios
import tty
from typing import Tuple

from carbon import (
    ConfigurableSink,
    Data,
    ExecutionGraph,
    Module,
    ModuleReference,
    sink,
    source,
)


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
                blocking=True,
            )

    @sink(ConfigurableSink(TeleopCommand, sticky=False))
    @source(JointState, JointState)
    def create_motor_commands(
        self, command: TeleopCommand
    ) -> Tuple[JointState, JointState]:
        safe_print(f"Creating motor commands from teleop command: {command}")
        return (
            JointState(position=0.0, velocity=command.left),
            JointState(position=0.0, velocity=command.right),
        )


class Transform(Data):
    translation: Tuple[float, float, float]  # x, y, z
    rotation: Tuple[float, float, float, float]  # quaternion (x, y, z, w)


class ContinuousJoint(Module):
    def __init__(self, parent: ModuleReference, child: ModuleReference):
        super().__init__()
        self.parent = parent
        self.child = child

        self.create_connection(
            self,
            child.module,
            Transform,
            blocking=True,
        )

    @sink(JointState)
    @source(Transform)
    def update_state(self, state: JointState) -> Transform:
        return Transform(
            translation=(0.0, 0.0, 0.0),  # Placeholder for translation
            rotation=(
                0.0,
                0.0,
                0.0,
                1.0,
            ),  # Placeholder for rotation (identity quaternion)
        )


class Motor(ContinuousJoint):
    def __init__(self, parent: ModuleReference, child: ModuleReference):
        super().__init__(parent, child)


class Pose(Data):
    position: Tuple[float, float, float]  # x, y, z
    orientation: Tuple[float, float, float, float]  # quaternion (x, y, z, w)


class Wheel(Module):
    def __init__(self):
        super().__init__()
        self.pose = Pose(
            position=(0.0, 0.0, 0.0),  # Placeholder for position
            orientation=(
                0.0,
                0.0,
                0.0,
                1.0,
            ),  # Placeholder for orientation (identity quaternion)
        )

    @sink(Transform)
    def transform(self, transform: Transform):
        safe_print(f"Transforming wheel with: {transform}")
        # Here you would apply the transform to the wheel's pose
        self.pose.position = transform.translation
        self.pose.orientation = transform.rotation


class WheelBase(Module):
    def __init__(self):
        super().__init__()

        self.left_wheel = Wheel()
        self.left_motor = Motor(
            parent=self.as_reference(), child=self.left_wheel.as_reference()
        )

        self.right_wheel = Wheel()
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


# Helper function to always print at the start of a clean line
def safe_print(*args, **kwargs):
    print("\r\033[K", end="", flush=True)  # Move to start and clear line
    print(*args, **kwargs)


robot = Robot()
print(robot)
execution_graph = ExecutionGraph(robot)
print("\nExecution Graph Layers:")
print(execution_graph.layers)
print("\nProcess Groups:")
print(execution_graph.processes)
print(execution_graph.process_layer_mapping)
print(execution_graph.in_process_layer_mapping)
print("\nConnections:")
for connection in robot.get_connections():
    print(connection)
print("\nMethods:")
for method in robot.get_methods():
    print(method.name)
    print("  Depends on:", method.dependents)
    print("  Produces for:", method.dependencies)
print()
execution_graph.execute()
print("\nExecution completed.")
