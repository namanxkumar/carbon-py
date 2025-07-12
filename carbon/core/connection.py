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
    _producers: Dict[Tuple[Type["Data"], ...], "DataMethod"]
    _consumers: Dict[Tuple[Type["Data"], ...], "DataMethod"]


class Connection:
    def __init__(
        self,
        producer: Union["ModuleLike", Sequence["ModuleLike"]],
        consumer: Union["ModuleLike", Sequence["ModuleLike"]],
        data: Union[Type["Data"], Sequence[Type["Data"]]],
        sync: bool = False,
    ):
        assert not (
            (isinstance(producer, Sequence) and len(producer) > 1)
            and (isinstance(consumer, Sequence) and len(consumer) > 1)
        ), (
            "Cannot connect multiple producers to multiple consumers directly. "
            "Use a single producer or consumer, or create a connection for each pair."
        )

        self.producer = ensure_tuple_format(producer)
        self.consumer = ensure_tuple_format(consumer)
        self.data = ensure_tuple_format(data)
        self.sync = sync
        self.active = True
        self.type: ConnectionType = ConnectionType.DIRECT  # Default type
        self.producer_methods: Tuple["DataMethod", ...]
        self.consumer_methods: Tuple["DataMethod", ...]

        if len(self.producer) > 1 and len(self.producer) != len(self.data):
            raise ValueError(
                "If multiple producers are provided, data must also be a sequence of the same length."
            )
        elif len(self.consumer) > 1 and len(self.consumer) != len(self.data):
            raise ValueError(
                "If multiple consumers are provided, data must also be a sequence of the same length."
            )

        if len(self.producer) > 1:
            for src, dat in zip(self.producer, self.data):
                assert ensure_tuple_format(dat) in src._producers, (
                    f"Producer {src} must have data type {dat} defined in its producers."
                )
            self.producer_methods = tuple(
                [
                    src._producers[ensure_tuple_format(dat)]
                    for src, dat in zip(self.producer, self.data)
                ]
            )
            self.type = ConnectionType.MERGE
        elif len(self.producer) == 1:
            assert self.data in self.producer[0]._producers, (
                f"Producer {self.producer[0]} must have data type {self.data[0]} defined in its producers."
            )
            self.producer_methods = tuple([self.producer[0]._producers[self.data]])

        if len(self.consumer) > 1:
            for snk, dat in zip(self.consumer, self.data):
                assert ensure_tuple_format(dat) in snk._consumers, (
                    f"Consumer {snk} must have data type {dat} defined in its consumers."
                )
            self.consumer_methods = tuple(
                [
                    snk._consumers[ensure_tuple_format(dat)]
                    for snk, dat in zip(self.consumer, self.data)
                ]
            )
            self.type = ConnectionType.SPLIT
        elif len(self.consumer) == 1:
            assert self.data in self.consumer[0]._consumers, (
                f"Consumer {self.consumer[0]} must have data type {self.data[0]} defined in its consumers."
            )
            self.consumer_methods = tuple([self.consumer[0]._consumers[self.data]])

        for producer_index, producer_method in enumerate(self.producer_methods):
            for consumer_index, consumer_method in enumerate(self.consumer_methods):
                consumer_method.add_dependency(
                    producer_method,
                    DependencyConfiguration(
                        merge_consumer_index=(
                            None
                            if self.type is ConnectionType.DIRECT
                            else producer_index
                        ),
                        sync=self.sync,
                    ),
                )
                producer_method.add_dependent(
                    consumer_method,
                    DependentConfiguration(
                        split_producer_index=(
                            None
                            if self.type is ConnectionType.DIRECT
                            else consumer_index
                        ),
                        sync=self.sync,
                    ),
                )

    def block(self):
        """Block the connection, preventing data from being sent."""
        self.active = False
        for producer_method in self.producer_methods:
            for consumer_method in self.consumer_methods:
                consumer_method.block_dependency(producer_method)
                producer_method.block_dependent(consumer_method)

    def __hash__(self):
        return hash((self.producer, self.consumer, self.data))

    def __eq__(self, other):
        return (
            isinstance(other, Connection)
            and self.producer == other.producer
            and self.consumer == other.consumer
            and self.data == other.data
        )

    def __repr__(self):
        return (
            f"Connection(producer={self.producer}, consumer={self.consumer}, "
            f"data={self.data}, sync={self.sync}"
        )
