from carbon.differential_drive_controller import (
    DifferentialDriveController,
    TeleopCommand,
)
from carbon.joint import ContinuousJoint, JointState
from carbon.kangaroo import KangarooDriver
from carbon.module import Module, source


class Wheel(Module):
    def __init__(self):
        super().__init__()
        self.box = RectangularLink()


class RectangularLink(Module):
    def __init__(self):
        super().__init__()


class WheelBase(Module):
    def __init__(self):
        super().__init__()

        self.left_wheel = Wheel()
        self.left_motor = ContinuousJoint(
            parent=self.to_reference(), child=self.left_wheel.to_reference()
        )

        self.right_wheel = Wheel()
        self.right_motor = ContinuousJoint(
            parent=self.to_reference(), child=self.right_wheel.to_reference()
        )

        self.driver = KangarooDriver()
        self.controller = DifferentialDriveController(
            left_motor=self.left_motor.to_reference(),
            right_motor=self.right_motor.to_reference(),
        )

        self.create_one_to_one_connection(
            self.controller, self.driver, (JointState, JointState)
        )


class Teleop(Module):
    @source(TeleopCommand)
    def teleop_command(self) -> TeleopCommand:
        # return TeleopCommand(left=0.0, right=0.0)
        pass


class Robot(Module):
    def __init__(self):
        super().__init__()
        self.wheelbase = WheelBase()
        self.teleop = Teleop()

        self.create_one_to_one_connection(
            self.teleop, self.wheelbase.controller, TeleopCommand
        )


robot = Robot()


def pretty_print_ordered_dict(od, indent=0):
    for key, value in od.items():
        if isinstance(value, dict):
            print(f"{key}:")
            pretty_print_ordered_dict(value, indent + 2)
        else:
            print(" " * indent + f"{key}: {value}")


print(robot)
print("\nSources:")
pretty_print_ordered_dict(robot.get_sources(recursive=True))
print("\nSinks:")
pretty_print_ordered_dict(robot.get_sinks(recursive=True))
print("\nConnections:")
for i in robot.get_connections():
    print(i)
