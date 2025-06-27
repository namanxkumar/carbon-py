from typing import Callable, Dict, List, Optional, Tuple, Type, cast

from .connection import Connection
from .data import Data


class DataMethod:
    def __init__(self, method: Callable):
        self.method = method
        self.name: str = method.__name__

        self.sources: Tuple[Type["Data"], ...] = cast(
            Tuple[Type["Data"], ...], getattr(method, "_sources", tuple())
        )
        self.sinks: Tuple[Type["Data"], ...] = cast(
            Tuple[Type["Data"], ...], getattr(method, "_sinks", tuple())
        )
        self.source_configuration: Dict[Type["Data"], Dict[str, int | bool]] = cast(
            Dict[Type["Data"], Dict[str, int | bool]],
            getattr(method, "_source_configuration", {}),
        )
        self.sink_configuration: Dict[Type["Data"], Dict[str, int | bool]] = cast(
            Dict[Type["Data"], Dict[str, int | bool]],
            getattr(method, "_sink_configuration", {}),
        )

        self.dependencies: Dict["DataMethod", "Connection"] = {}
        self.dependents: Dict["DataMethod", "Connection"] = {}

        self.dependent_splits: Dict[
            "DataMethod", Optional[int]
        ] = {}  # Split index for each dependent

        self.input_queue: Dict[Type["Data"], List["Data"]] = {
            sink: [] for sink in self.sinks
        }  # Queues for each sink type
        self.input_queue_size: Dict[Type["Data"], int] = {
            sink: cast(int, self.sink_configuration.get(sink, {}).get("queue_size", 1))
            for sink in self.sinks
        }
        self.input_is_sticky: Dict[Type["Data"], bool] = {
            sink: cast(bool, self.sink_configuration.get(sink, {}).get("sticky", False))
            for sink in self.sinks
        }
        # TODO: Add support for sticky queues
        # TODO: Add a method to update and fetch source queue
        # TODO: Add a message cache and a message cache size for logging and transforms (historical transforms)

    def __call__(self, *args, **kwargs):
        return self.method(*args, **kwargs)

    def __eq__(self, value):
        if isinstance(value, DataMethod):
            return self.method == value.method
        return self.method == value

    def __repr__(self):
        return f"{self.name}"

    def __hash__(self):
        return hash(self.method)
