from carbon import Module
from carbon.common_data_types import Position
from carbon.transforms import ContinuousJoint, CylindricalGeometry, Link
from examples.wheelbase.differential_drive_controller import DifferentialDriveController


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

        # self.block_connection(producer=None, consumer=None, data=Transform)

        self.controller = DifferentialDriveController(open_loop=True)

        self.create_connection(
            (Position, Position), self.controller, (self.left_motor, self.right_motor)
        )
