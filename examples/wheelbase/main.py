from carbon import (
    ExecutionGraph,
    Module,
)
from carbon.common_data_types import Position, Twist
from carbon.transforms import (
    ContinuousJoint,
    CylindricalGeometry,
    Link,
)
from examples.wheelbase.differential_drive_controller import DifferentialDriveController
from examples.wheelbase.kangaroo_driver import KangarooDriver
from examples.wheelbase.teleop import Teleop


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
        self.left_motor = ContinuousJoint(
            parent=self.as_reference(), child=self.left_wheel.as_reference()
        )

        self.right_wheel = Link(
            CylindricalGeometry(
                mass=1.0,  # Example mass for the wheel
                radius=0.1,  # Example radius for the wheel
                height=0.05,  # Example height for the wheel
            )
        )
        self.right_motor = ContinuousJoint(
            parent=self.as_reference(), child=self.right_wheel.as_reference()
        )

        self.driver = KangarooDriver()
        self.controller = DifferentialDriveController()

        if self.driver.encoder_is_available:
            self.create_connection(
                (Position, Position),
                self.driver,
                (self.left_motor, self.right_motor),
                sync=True,
            )
        else:
            self.create_connection(
                (Position, Position),
                self.controller,
                (self.left_motor, self.right_motor),
                sync=True,
            )


class Robot(Module):
    def __init__(self):
        super().__init__()
        self.wheelbase = WheelBase()
        self.teleop = Teleop()

        self.create_connection(Twist, self.teleop, self.wheelbase.controller)


robot = Robot()
print(robot)
execution_graph = ExecutionGraph(robot)
print("\nExecution Graph Layers:")
print(execution_graph.layers)
print("\nProcess Groups:")
for process_index, process in execution_graph.processes.items():
    print(f"Process {process_index}:")
    for method in process:
        print(
            f"  {method.name} (depends on: {method.dependencies}, produces: {method.dependents})"
        )
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
