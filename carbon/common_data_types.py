from carbon.data import Data, StampedData


class Vector3(Data):
    x: float
    y: float
    z: float


class Quaternion(Data):
    x: float
    y: float
    z: float
    w: float


class Twist(StampedData):
    linear: Vector3
    angular: Vector3


class Position(StampedData):
    position: Vector3


class Velocity(StampedData):
    velocity: Vector3
