from carbon import (
    ExecutionGraph,
    Module,
)
from carbon.common_data_types import Twist
from examples.wheelbase.teleop import Teleop
from examples.wheelbase.wheelbase import WheelBase


class Robot(Module):
    def __init__(self):
        super().__init__()
        self.wheelbase = WheelBase()
        self.teleop = Teleop()

        self.create_connection(Twist, self.teleop, self.wheelbase.controller)


robot = Robot()
execution_graph = ExecutionGraph(robot)


print(robot)
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
print("\nExecution completed.")

execution_graph.execute()
