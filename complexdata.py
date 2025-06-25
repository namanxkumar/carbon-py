from dataclasses import dataclass

from carbon.data import Data, StampedData


@dataclass
class Quaternion(StampedData):
    x: float
    y: float
    z: float
    w: float


@dataclass
class Vector3(Data):
    x: float
    y: float
    z: float


@dataclass
class Pose(Data):
    position: Vector3
    orientation: Quaternion


@dataclass
class PoseWithCovariance(Data):
    pose: Pose
    covariance: list[float]


@dataclass
class Twist(Data):
    linear: Vector3
    angular: Vector3


@dataclass
class TwistWithCovariance(Data):
    twist: Twist
    covariance: list[float]


@dataclass
class Odometry(Data):
    child_frame_id: str
    pose: PoseWithCovariance
    twist: TwistWithCovariance


@dataclass
class LaserScan(Data):
    ranges: list[float]
    intensities: list[float]


@dataclass
class IMUData(Data):
    orientation: Quaternion
    orientation_covariance: list[float]
    angular_velocity: Vector3
    angular_velocity_covariance: list[float]
    linear_acceleration: Vector3
    linear_acceleration_covariance: list[float]


@dataclass
class Image(Data):
    width: int
    height: int
    encoding: str
    data: bytes


@dataclass
class MapMetaData(Data):
    map_load_time: float
    resolution: float
    width: int
    height: int
    origin: Pose


@dataclass
class OccupancyGrid(Data):
    info: dict
    data: list[int]
