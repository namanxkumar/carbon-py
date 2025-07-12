from carbon import Module
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

        # self.block_connection(source=None, sink=None, data=Transform)

        self.controller = DifferentialDriveController(
            left_motor=self.left_motor.as_reference(),
            right_motor=self.right_motor.as_reference(),
            update_motor_states=True,
        )
