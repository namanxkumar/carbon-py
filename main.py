from carbon.differential_drive_controller import (
    DifferentialDriveController,
    TeleopCommand,
)
from carbon.joint import ContinuousJoint
from carbon.kangaroo import KangarooDriver
from carbon.module import Module, source


class Wheel(Module):
    def __init__(self):
        super().__init__()
        self.box = RectangularLink()


class RectangularLink(Module):
    def __init__(self):
        super().__init__()

    @source(int)
    def test(self):
        # This is a test function
        pass


class WheelBase(Module):
    def __init__(self):
        super().__init__()

        self.chassis = RectangularLink()
        self.left_wheel = Wheel()
        self.left_motor = ContinuousJoint(name="left_motor")
        self.right_wheel = Wheel()
        self.right_motor = ContinuousJoint(name="right_motor")

        self.create_joint(
            joint=self.right_motor, parent=self.chassis, child=self.right_wheel
        )
        self.create_joint(
            joint=self.left_motor, parent=self.chassis, child=self.left_wheel
        )

        self.driver = KangarooDriver(
            left_actuator=self.left_motor, right_actuator=self.right_motor
        )
        self.controller = DifferentialDriveController(
            left_motor=self.left_motor, right_motor=self.right_motor
        )


class Teleop(Module):
    @source(TeleopCommand)
    def teleop_command(self) -> TeleopCommand:
        # return TeleopCommand(left=0.0, right=0.0)
        pass


wheelbase = WheelBase()
teleop = Teleop()


def pretty_print_ordered_dict(od, indent=0):
    for key, value in od.items():
        if isinstance(value, dict):
            print(f"{key}:")
            pretty_print_ordered_dict(value, indent + 2)
        else:
            print(" " * indent + f"{key}: {value}")


print("Sources:")
pretty_print_ordered_dict(wheelbase.get_sources())
print("\nSinks:")
pretty_print_ordered_dict(wheelbase.get_sinks())
print("\nConnections:")
for i in wheelbase._connections:
    print(i)
