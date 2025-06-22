from dataclasses import dataclass

from carbon.data import Data
from carbon.module import Module, ModuleReference, sink, source
from complexdata import Image, IMUData, LaserScan, OccupancyGrid, Odometry, Pose, Twist


def startup(func):
    """Decorator to mark a method as a startup method."""
    setattr(func, "_startup", True)
    return func


class LIDAR(Module):
    def __init__(self):
        super().__init__()

    @source(LaserScan)
    def scan(self) -> LaserScan:
        # Simulate a LIDAR scan returning some data
        return LaserScan()


class IMU(Module):
    def __init__(self):
        super().__init__()

    @source(IMUData)
    def read_imu(self) -> IMUData:
        # Simulate reading IMU data
        return IMUData(
            orientation=None
        )  # Replace with actual Quaternion data if needed


class Camera(Module):
    def __init__(self):
        super().__init__()

    @source(Image)
    def capture_image(self) -> Image:
        # Simulate capturing an image
        return Image(data=b"")  # Replace with actual image data if needed


class DifferentialDrive(Module):
    def __init__(self):
        super().__init__()

    @source(Odometry)
    def get_odometry(self) -> Odometry:
        # Simulate getting odometry data
        return Odometry(
            child_frame_id="base_link",
            pose=None,  # Replace with actual PoseWithCovariance data if needed
            twist=None,  # Replace with actual TwistWithCovariance data if needed
        )

    @sink(Twist)
    def drive(self, twist: Twist):
        # Simulate driving the robot with the given twist
        print(f"Driving with twist: {twist}")


class Mapping(Module):
    @dataclass
    class MapRequest(Data):
        parameters: dict = None

    def __init__(self):
        super().__init__()

    @sink(MapRequest)
    @source(OccupancyGrid)
    def get_map_service(self) -> OccupancyGrid:
        # Simulate a service that returns an occupancy grid map
        print("Getting map service")
        return OccupancyGrid()

    @sink(LaserScan, Odometry, IMUData)
    @source(OccupancyGrid)
    def map_environment(
        self, scan: LaserScan, odometry: Odometry, imu: IMUData
    ) -> OccupancyGrid:
        # Simulate mapping logic
        print(f"Mapping with scan: {scan}, odometry: {odometry}, imu: {imu}")
        return OccupancyGrid()  # Replace with actual mapping data if needed


class Localization(Module):
    def __init__(self, mapping_module: ModuleReference):
        super().__init__()

        self.create_connection(
            self,
            mapping_module.module,
            Mapping.MapRequest,
            sync=True,
        )

    @startup
    @source(Mapping.MapRequest)
    def request_map(self) -> Mapping.MapRequest:
        # Simulate requesting a map
        print("Requesting map")
        return Mapping.MapRequest(parameters={"resolution": 0.05})

    @sink(LaserScan, Odometry, Pose, OccupancyGrid)
    @source(Pose)
    def localize(
        self,
        scan: LaserScan,
        odometry: Odometry,
        pose: Pose,
        map_data: OccupancyGrid,
    ):
        # Simulate localization logic
        print(
            f"Localizing with scan: {scan}, odometry: {odometry}, pose: {pose}, map_data: {map_data}"
        )
        return Pose()
