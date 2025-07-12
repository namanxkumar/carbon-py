from typing import (
    Generic,
    List,
    Optional,
    Tuple,
    Type,
    get_args,
    get_origin,
)

from typing_extensions import TypeVarTuple, Unpack

from carbon.core.datamethod import ConfiguredSink, DataMethod, SinkConfiguration
from carbon.core.utilities import ensure_tuple_format
from carbon.data import Data

D = TypeVarTuple("D")


class Sink(Generic[Unpack[D]]):
    def __init__(
        self,
        *data_type: Unpack[D],
    ):
        data_types: List[Type[Data]] = []
        data_configurations: List[SinkConfiguration] = []
        for dt in data_type:
            if isinstance(dt, ConfiguredSink):
                data_types.append(dt.data_type)
                data_configurations.append(dt.configuration)
            elif isinstance(dt, Type) and issubclass(dt, Data):
                data_types.append(dt)
                data_configurations.append(SinkConfiguration())
            else:
                raise ValueError(f"Type {dt} is not a valid Data type.")
        self.data_types = tuple(data_types)
        self.data_configurations = tuple(data_configurations)
        self.data_method: Optional[DataMethod] = None

    def __hash__(self) -> int:
        return hash(self.data_types)

    def __eq__(self, value: object) -> bool:
        origin = get_origin(value)
        if isinstance(value, Sink):
            return self.data_types == value.data_types
        elif origin is not None and issubclass(origin, Tuple):
            args = get_args(value)
            if all(isinstance(arg, Type) for arg in args):
                return self.data_types == ensure_tuple_format(args)
        elif isinstance(value, Type):
            return self.data_types == ensure_tuple_format(value)
        return super().__eq__(value)

    def __repr__(self) -> str:
        return f"Sink({self.data_types})"


class Source(Generic[Unpack[D]]):
    def __init__(self, *data_type: Unpack[D]):
        data_types: List[Type[Data]] = []
        for dt in data_type:
            # Assert dt is a Type[Data]
            if not isinstance(dt, Type) or not issubclass(dt, Data):
                raise ValueError(f"Type {dt} is not a valid Data type.")
            data_types.append(dt)
        self.data_types = tuple(data_types)
        self.data_method: Optional[DataMethod] = None

    def __hash__(self) -> int:
        return hash(self.data_types)

    def __eq__(self, value: object) -> bool:
        origin = get_origin(value)
        if isinstance(value, Source):
            return self.data_types == value.data_types
        elif origin is not None and issubclass(origin, Tuple):
            args = get_args(value)
            if all(isinstance(arg, Type) for arg in args):
                return self.data_types == ensure_tuple_format(args)
        elif isinstance(value, Type):
            return self.data_types == ensure_tuple_format(value)
        return super().__eq__(value)

    def __repr__(self) -> str:
        return f"Source({self.data_types})"
