from enum import Enum
from typing import TYPE_CHECKING, Sequence

if TYPE_CHECKING:
    from .data import Data
    from .module import Module


class ConnectionType(Enum):
    BLOCKING = "blocking"
    NON_BLOCKING = "non-blocking"


class Connection:
    def __init__(
        self,
        source: "Module" | Sequence["Module"],
        sink: "Module" | Sequence["Module"],
        data: "Data" | Sequence["Data"],
        blocking: bool = False,
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
        self.data = tuple(data) if isinstance(data, Sequence) else (data,)
        self.type = ConnectionType.BLOCKING if blocking else ConnectionType.NON_BLOCKING
        self.queue_size = queue_size

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
                [src._sources.get((dat,)) for src, dat in zip(self.source, self.data)]
            )
        else:
            assert self.data in self.source[0]._sources, (
                f"Source {self.source[0]} must have data type {self.data[0]} defined in its sources."
            )
            self.source_methods = tuple([self.source[0]._sources.get(self.data)])

        if len(self.sink) > 1:
            for snk, dat in zip(self.sink, self.data):
                assert (dat,) in snk._sinks, (
                    f"Sink {snk} must have data type {dat} defined in its sinks."
                )
            self.sink_methods = tuple(
                [snk._sinks.get((dat,)) for snk, dat in zip(self.sink, self.data)]
            )
        else:
            assert self.data in self.sink[0]._sinks, (
                f"Sink {self.sink[0]} must have data type {self.data[0]} defined in its sinks."
            )
            self.sink_methods = tuple([self.sink[0]._sinks.get(self.data)])

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
            f"data={self.data}, type={self.type}, queue_size={self.queue_size})"
        )
