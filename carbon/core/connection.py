from enum import Enum
from typing import Sequence, Type, Union

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


class Connection:
    def __init__(
        self,
        source: Union[DataMethod, Sequence[DataMethod]],
        sink: Union[DataMethod, Sequence[DataMethod]],
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
        self.active = True
        self.type: ConnectionType = ConnectionType.DIRECT  # Default type

        if len(self.source) > 1 and len(self.source) != len(self.data):
            raise ValueError(
                "If multiple sources are provided, data must also be a sequence of the same length."
            )
        elif len(self.sink) > 1 and len(self.sink) != len(self.data):
            raise ValueError(
                "If multiple sinks are provided, data must also be a sequence of the same length."
            )

        if len(self.source) > 1:
            self.type = ConnectionType.MERGE
        if len(self.sink) > 1:
            self.type = ConnectionType.SPLIT

        for source_index, source_method in enumerate(self.source):
            for sink_index, sink_method in enumerate(self.sink):
                sink_method.add_dependency(
                    source_method,
                    DependencyConfiguration(
                        merge_sink_index=(
                            None if self.type is ConnectionType.DIRECT else source_index
                        ),
                        sync=self.sync,
                    ),
                )
                source_method.add_dependent(
                    sink_method,
                    DependentConfiguration(
                        split_source_index=(
                            None if self.type is ConnectionType.DIRECT else sink_index
                        ),
                        sync=self.sync,
                    ),
                )

    def block(self):
        """Block the connection, preventing data from being sent."""
        self.active = False
        for source_method in self.source:
            for sink_method in self.sink:
                sink_method.block_dependency(source_method)
                source_method.block_dependent(sink_method)

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
            f"data={self.data}, sync={self.sync}"
        )
