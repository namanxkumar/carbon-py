from carbon.data import Data, StampedData


class Trigger(Data):
    """
    A simple trigger data type that can be used to signal events.
    It does not carry any additional information.
    """

    trigger: bool = True


class Vector3(Data):
    x: float
    y: float
    z: float


class Quaternion(Data):
    x: float
    y: float
    z: float
    w: float


class Twist(Data):
    linear: Vector3
    angular: Vector3


class Position(StampedData):
    position: Vector3


class Velocity(StampedData):
    velocity: Vector3
