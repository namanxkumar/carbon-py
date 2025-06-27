from enum import Enum
from typing import TYPE_CHECKING, Sequence, Tuple, Type

if TYPE_CHECKING:
    from .data import Data
    from .datamethod import DataMethod
    from .module import Module


class ConnectionType(Enum):
    MERGE = "merge"
    SPLIT = "split"
    DIRECT = "direct"


class Connection:
    def __init__(
        self,
        source: "Module" | Sequence["Module"],
        sink: "Module" | Sequence["Module"],
        data: Type["Data"] | Sequence[Type["Data"]],
        blocking: bool = False,
        sticky_queue: bool = False,
        queue_size: int = 1,
    ):
        assert not (
            (isinstance(source, Sequence) and len(source) > 1)
            and (isinstance(sink, Sequence) and len(sink) > 1)
        ), (
            "Cannot connect multiple sources to multiple sinks directly. "
            "Use a single source or sink, or create a connection for each pair."
        )

        self.source = tuple(source) if isinstance(source, Sequence) else (source,)
        self.sink = tuple(sink) if isinstance(sink, Sequence) else (sink,)
        self.data: Tuple[Type["Data"], ...] | Tuple[Type["Data"]] = (
            tuple(data) if isinstance(data, Sequence) else (data,)
        )
        self.blocking = blocking
        self.sticky_queue = sticky_queue
        self.queue_size = queue_size
        self.type: ConnectionType = ConnectionType.DIRECT  # Default type
        self.source_methods: Tuple["DataMethod", ...]
        self.sink_methods: Tuple["DataMethod", ...]

        if self.queue_size < 1:
            raise ValueError("Queue size must be at least 1.")
        if self.blocking:
            assert not self.sticky_queue, (
                "Sticky queues are not allowed for blocking connections."
            )
            assert self.queue_size == 1, (
                "Queue size must be 1 for blocking connections."
            )

        if len(self.source) > 1 and len(self.source) != len(self.data):
            raise ValueError(
                "If multiple sources are provided, data must also be a sequence of the same length."
            )
        elif len(self.sink) > 1 and len(self.sink) != len(self.data):
            raise ValueError(
                "If multiple sinks are provided, data must also be a sequence of the same length."
            )

        if len(self.source) > 1:
            for src, dat in zip(self.source, self.data):
                assert (dat,) in src._sources, (
                    f"Source {src} must have data type {dat} defined in its sources."
                )
            self.source_methods = tuple(
                [src._sources[(dat,)] for src, dat in zip(self.source, self.data)]
            )
            self.type = ConnectionType.MERGE
        elif len(self.source) == 1:
            assert self.data in self.source[0]._sources, (
                f"Source {self.source[0]} must have data type {self.data[0]} defined in its sources."
            )
            self.source_methods = tuple([self.source[0]._sources[self.data]])

        if len(self.sink) > 1:
            for snk, dat in zip(self.sink, self.data):
                assert (dat,) in snk._sinks, (
                    f"Sink {snk} must have data type {dat} defined in its sinks."
                )
            self.sink_methods = tuple(
                [snk._sinks[(dat,)] for snk, dat in zip(self.sink, self.data)]
            )
            self.type = ConnectionType.SPLIT
        elif len(self.sink) == 1:
            assert self.data in self.sink[0]._sinks, (
                f"Sink {self.sink[0]} must have data type {self.data[0]} defined in its sinks."
            )
            self.sink_methods = tuple([self.sink[0]._sinks[self.data]])

        for source_method in self.source_methods:
            for index, sink_method in enumerate(self.sink_methods):
                source_method.dependents[sink_method] = self
                source_method.dependent_splits[sink_method] = (
                    None if self.type is ConnectionType.DIRECT else index
                )
                sink_method.dependencies[source_method] = self

    def __hash__(self):
        return hash((self.source, self.sink, self.data))

    def __eq__(self, other):
        return (
            isinstance(other, Connection)
            and self.source == other.source
            and self.sink == other.sink
            and self.data == other.data
        )

    def __repr__(self):
        return (
            f"Connection(source={self.source}, sink={self.sink}, "
            f"data={self.data}, blocking={self.blocking}, queue_size={self.queue_size}, sticky_queue={self.sticky_queue})"
        )


class AsyncConnection(Connection):
    def __init__(
        self,
        source: "Module" | Sequence["Module"],
        sink: "Module" | Sequence["Module"],
        data: Type["Data"] | Sequence[Type["Data"]],
        sticky_queue: bool = False,
        queue_size: int = 1,
    ):
        super().__init__(
            source=source,
            sink=sink,
            data=data,
            sticky_queue=sticky_queue,
            queue_size=queue_size,
            blocking=False,
        )

    def __repr__(self):
        return (
            f"AsyncConnection(source={self.source}, sink={self.sink}, "
            f"data={self.data}, queue_size={self.queue_size}, sticky_queue={self.sticky_queue})"
        )


class SyncConnection(Connection):
    def __init__(
        self,
        source: "Module" | Sequence["Module"],
        sink: "Module" | Sequence["Module"],
        data: Type["Data"] | Sequence[Type["Data"]],
    ):
        super().__init__(
            source=source,
            sink=sink,
            data=data,
            blocking=True,
        )

    def __repr__(self):
        return (
            f"SyncConnection(source={self.source}, sink={self.sink}, data={self.data})"
        )
