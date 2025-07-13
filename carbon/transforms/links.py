from carbon.core import Module, consumer, safe_print
from carbon.data import Data, StampedData
from carbon.transforms.joints import Transform


class Pose(StampedData):
    """
    Represents a pose in 3D space, including position and orientation.
    This class is used to encapsulate the pose information for joints in a robotic system.
    """

    position: tuple[float, float, float]  # x, y, z
    orientation: tuple[float, float, float, float]  # quaternion (x, y, z, w)


class Geometry(Data):
    """
    Represents the geometry of a link in a robotic system.
    This class is used to encapsulate the geometric information for links.
    """

    mass: float  # Mass of the link


class RectangularGeometry(Geometry):
    """
    Represents a rectangular geometry for a link in a robotic system.
    This class is used to encapsulate the rectangular geometric information for links.
    """

    length: float  # Length of the rectangle
    width: float  # Width of the rectangle
    height: float  # Height of the rectangle


class CylindricalGeometry(Geometry):
    """
    Represents a cylindrical geometry for a link in a robotic system.
    This class is used to encapsulate the cylindrical geometric information for links.
    """

    radius: float  # Radius of the cylinder
    height: float  # Height of the cylinder


class Link(Module):
    """
    Represents a link in a robotic system, which connects two joints.
    This class is used to encapsulate the link information between joints.
    """

    def __init__(self, geometry: Geometry):
        super().__init__()

        self.geometry = geometry  # Geometry of the link

        self.pose = Pose(
            position=(0.0, 0.0, 0.0),  # Placeholder for position
            orientation=(
                0.0,
                0.0,
                0.0,
                1.0,
            ),  # Placeholder for orientation (identity quaternion)
        )

    @consumer(Transform)
    def transform(self, transform: Transform):
        """
        Apply a transformation to the link's pose.
        This method updates the link's pose based on the provided transform.
        """
        # Here you would apply the transform to the link's pose
        self.pose.position = transform.translation
        self.pose.orientation = transform.rotation
        safe_print(
            f"Link {self} transformed to position {self.pose.position} and orientation {self.pose.orientation}"
        )
