from enum import Enum
from typing import Dict, List, Optional, Protocol, Sequence, Set, Tuple, Type, Union

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

    def get_produced_data_types_mapping(
        self, recursive: bool = True, _memo: Optional[Set] = None
    ) -> Dict[Tuple[Type["Data"], ...], Tuple["ModuleLike", ...]]: ...

    def get_consumed_data_types_mapping(
        self, recursive: bool = True, _memo: Optional[Set] = None
    ) -> Dict[Tuple[Type["Data"], ...], Tuple["ModuleLike", ...]]: ...


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

        self.producers = ensure_tuple_format(producer)
        self.consumers = ensure_tuple_format(consumer)
        self.data = ensure_tuple_format(data)
        self.sync = sync
        self.active = True
        self.type: ConnectionType = ConnectionType.DIRECT  # Default type
        self.producer_methods: Tuple["DataMethod", ...]
        self.consumer_methods: Tuple["DataMethod", ...]

        if len(self.producers) > 1 and len(self.producers) != len(self.data):
            raise ValueError(
                "If multiple producers are provided, data must also be a sequence of the same length."
            )
        elif len(self.consumers) > 1 and len(self.consumers) != len(self.data):
            raise ValueError(
                "If multiple consumers are provided, data must also be a sequence of the same length."
            )

        if len(self.producers) > 1:
            producer_modules: List["ModuleLike"] = []
            for prd, dat in zip(self.producers, self.data):
                producer_modules.append(
                    self.retrieve_producer_module(ensure_tuple_format(dat), prd)
                )
            self.producers = tuple(producer_modules)
            self.producer_methods = tuple(
                [
                    prd._producers[ensure_tuple_format(dat)]
                    for prd, dat in zip(self.producers, self.data)
                ]
            )
            self.type = ConnectionType.MERGE
        elif len(self.producers) == 1:
            self.producers = ensure_tuple_format(
                self.retrieve_producer_module(self.data, self.producers[0])
            )
            self.producer_methods = tuple([self.producers[0]._producers[self.data]])

        if len(self.consumers) > 1:
            consumer_modules: List["ModuleLike"] = []
            for csm, dat in zip(self.consumers, self.data):
                consumer_modules.append(
                    self.retrieve_consumer_module(ensure_tuple_format(dat), csm)
                )
            self.consumers = tuple(consumer_modules)
            self.consumer_methods = tuple(
                [
                    csm._consumers[ensure_tuple_format(dat)]
                    for csm, dat in zip(self.consumers, self.data)
                ]
            )
            self.type = ConnectionType.SPLIT
        elif len(self.consumers) == 1:
            self.consumers = ensure_tuple_format(
                self.retrieve_consumer_module(self.data, self.consumers[0])
            )
            self.consumer_methods = tuple([self.consumers[0]._consumers[self.data]])

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

    def retrieve_producer_module(self, dat: Tuple[Type[Data], ...], prd: "ModuleLike"):
        src_producers = prd.get_produced_data_types_mapping(recursive=True)
        producer_module = src_producers.get(ensure_tuple_format(dat), None)
        if producer_module is None or len(producer_module) == 0:
            raise ValueError(f"Producer {prd} does not produce data type {dat}.")
        if len(producer_module) > 1:
            raise ValueError(
                f"Producer {prd} produces data type {dat} from multiple modules: {producer_module}."
                "Please specify a single producer module."
            )
        return producer_module[0]

    def retrieve_consumer_module(self, dat: Tuple[Type[Data], ...], csm: "ModuleLike"):
        snk_consumers = csm.get_consumed_data_types_mapping(recursive=True)
        consumer_module = snk_consumers.get(dat, None)
        if consumer_module is None or len(consumer_module) == 0:
            raise ValueError(f"Consumer {csm} does not consume data type {dat}.")
        if len(consumer_module) > 1:
            raise ValueError(
                f"Consumer {csm} consumes data type {dat} from multiple modules: {consumer_module}."
                "Please specify a single consumer module."
            )
        return consumer_module[0]

    def block(self):
        """Block the connection, preventing data from being sent."""
        self.active = False
        for producer_method in self.producer_methods:
            for consumer_method in self.consumer_methods:
                consumer_method.block_dependency(producer_method)
                producer_method.block_dependent(consumer_method)

    def __hash__(self):
        return hash((self.producers, self.consumers, self.data))

    def __eq__(self, other):
        return (
            isinstance(other, Connection)
            and self.producers == other.producers
            and self.consumers == other.consumers
            and self.data == other.data
        )

    def __repr__(self):
        return (
            f"Connection(producer={self.producers}, consumer={self.consumers}, "
            f"data={self.data}, sync={self.sync}"
        )
