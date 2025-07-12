from carbon.core import (
    ExecutionGraph,
    Module,
)
from carbon.transforms import ContinuousJoint, CylindricalGeometry, Link
from differential_drive_controller import DifferentialDriveController
from kangaroo_driver import KangarooDriver
from teleop import Teleop


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
        self.left_motor = ContinuousJoint(child=self.left_wheel.as_reference())

        self.right_wheel = Link(
            CylindricalGeometry(
                mass=1.0,  # Example mass for the wheel
                radius=0.1,  # Example radius for the wheel
                height=0.05,  # Example height for the wheel
            )
        )
        self.right_motor = ContinuousJoint(child=self.right_wheel.as_reference())

        self.driver = KangarooDriver()
        self.controller = DifferentialDriveController()

        self.create_connection(
            source=self.controller.motor_commands,
            sink=self.driver.motor_commands,
            sync=True,
        )

        self.create_connection(
            self.controller.joint_state,
            (self.left_motor.joint_state, self.right_motor.joint_state),
            sync=True,
        )


class Robot(Module):
    def __init__(self):
        super().__init__()
        self.wheelbase = WheelBase()
        self.teleop = Teleop()

        self.create_connection(
            self.teleop.teleop_command, self.wheelbase.controller.drive_command
        )


robot = Robot()
print(robot)
print(robot.get_methods())
print("\nConnections:")
for connection in robot.get_connections():
    print(connection)
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
print("\nMethods:")
for method in robot.get_methods():
    print(method.name)
    print("  Depends on:", method.dependents)
    print("  Produces for:", method.dependencies)
print()
execution_graph.execute()
print("\nExecution completed.")
