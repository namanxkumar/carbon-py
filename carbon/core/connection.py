from enum import Enum
from typing import Dict, Protocol, Sequence, Tuple, Type, Union

from carbon.core.datamethod import (
    DataMethod,
    DependencyConfiguration,
    DependentConfiguration,
)
from carbon.core.utilities import ensure_tuple_format
from carbon.data import Data


class ConnectionType(Enum):
    MERGE = "merge"
    SPLIT = "split"
    DIRECT = "direct"


class ModuleLike(Protocol):
    _sources: Dict[Tuple[Type["Data"], ...], "DataMethod"]
    _sinks: Dict[Tuple[Type["Data"], ...], "DataMethod"]


class Connection:
    def __init__(
        self,
        source: Union["ModuleLike", Sequence["ModuleLike"]],
        sink: Union["ModuleLike", Sequence["ModuleLike"]],
        data: Union[Type["Data"], Sequence[Type["Data"]]],
        sync: bool = False,
    ):
        assert not (
            (isinstance(source, Sequence) and len(source) > 1)
            and (isinstance(sink, Sequence) and len(sink) > 1)
        ), (
            "Cannot connect multiple sources to multiple sinks directly. "
            "Use a single source or sink, or create a connection for each pair."
        )

        self.source = ensure_tuple_format(source)
        self.sink = ensure_tuple_format(sink)
        self.data = ensure_tuple_format(data)
        self.sync = sync
        self.blocked = False
        self.type: ConnectionType = ConnectionType.DIRECT  # Default type
        self.source_methods: Tuple["DataMethod", ...]
        self.sink_methods: Tuple["DataMethod", ...]

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
                assert ensure_tuple_format(dat) in src._sources, (
                    f"Source {src} must have data type {dat} defined in its sources."
                )
            self.source_methods = tuple(
                [
                    src._sources[ensure_tuple_format(dat)]
                    for src, dat in zip(self.source, self.data)
                ]
            )
            self.type = ConnectionType.MERGE
        elif len(self.source) == 1:
            assert self.data in self.source[0]._sources, (
                f"Source {self.source[0]} must have data type {self.data[0]} defined in its sources."
            )
            self.source_methods = tuple([self.source[0]._sources[self.data]])

        if len(self.sink) > 1:
            for snk, dat in zip(self.sink, self.data):
                assert ensure_tuple_format(dat) in snk._sinks, (
                    f"Sink {snk} must have data type {dat} defined in its sinks."
                )
            self.sink_methods = tuple(
                [
                    snk._sinks[ensure_tuple_format(dat)]
                    for snk, dat in zip(self.sink, self.data)
                ]
            )
            self.type = ConnectionType.SPLIT
        elif len(self.sink) == 1:
            assert self.data in self.sink[0]._sinks, (
                f"Sink {self.sink[0]} must have data type {self.data[0]} defined in its sinks."
            )
            self.sink_methods = tuple([self.sink[0]._sinks[self.data]])

        for source_index, source_method in enumerate(self.source_methods):
            for sink_index, sink_method in enumerate(self.sink_methods):
                sink_method.add_dependency(
                    source_method,
                    DependencyConfiguration(
                        merge_sink_index=(
                            None if self.type is ConnectionType.DIRECT else source_index
                        ),
                        blocking=self.sync,
                    ),
                )
                source_method.add_dependent(
                    sink_method,
                    DependentConfiguration(
                        split_source_index=(
                            None if self.type is ConnectionType.DIRECT else sink_index
                        ),
                        blocking=self.sync,
                    ),
                )

    def block(self):
        """Block the connection, preventing data from being sent."""
        self.blocked = True
        for source_method in self.source_methods:
            for sink_method in self.sink_methods:
                sink_method.dependency_to_configuration[source_method].active = False
                source_method.dependent_to_configuration[sink_method].active = False

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
            f"data={self.data}, blocking={self.sync}"
        )
