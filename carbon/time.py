"""
Define the Time Module that provides a time source for the system.
This module can be used to synchronize other modules based on time.
It is automatically added to the system root module.
"""

from core.data import Data
from core.module import Module, source


class TimeData(Data):
    """
    Data class representing time data.
    Contains a timestamp in seconds.
    """

    timestamp: float


class Time(Module):
    """
    Time module that provides a time source for the system.
    Automatically added to the system root module.
    """

    def __init__(self):
        super().__init__()
        self._time = 0.0  # Initialize time to zero

    @source(TimeData)
    def get_time(self) -> TimeData:
        """
        Source method that returns the current time as TimeData.
        """
        return TimeData(timestamp=self._time)

    def update_time(self, delta: float):
        """
        Update the internal time by a delta value.
        This method can be called periodically to simulate time passing.
        """
        self._time += delta
