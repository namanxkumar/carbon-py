from typing import (
    Annotated,
    Generic,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
    get_args,
    get_origin,
)

from carbon.core.datamethod import DataMethod, SinkConfiguration
from carbon.core.utilities import ensure_tuple_format
from carbon.data import Data

D = TypeVar("D", bound=Union[Data, Tuple[Data, ...]])


class Sink(Generic[D]):
    def __init__(self, data_type: Type[D]):
        origin = get_origin(data_type)
        if origin is not None and issubclass(origin, Tuple):
            potentially_annotated_data_types = get_args(data_type)
            data_types: List[Type[Data]] = []
            data_configurations: List[SinkConfiguration] = []

            for potentially_annotated_data_type in potentially_annotated_data_types:
                parsed_data_type, configuration = (
                    self._parse_potentially_annotated_type(
                        cast(Type[Data], potentially_annotated_data_type)
                    )
                )
                data_types.append(parsed_data_type)
                data_configurations.append(configuration)
            self.data_types = tuple(data_types)
            self.data_configurations = tuple(data_configurations)
        else:
            parsed_data_type, configuration = self._parse_potentially_annotated_type(
                cast(Type[Data], data_type)
            )
            self.data_types = ensure_tuple_format(parsed_data_type)
            self.data_configurations = ensure_tuple_format(configuration)
        self.data_method: Optional[DataMethod] = None

    def _parse_potentially_annotated_type(
        self, potentially_annotated_type: Type[Data]
    ) -> Tuple[Type[Data], SinkConfiguration]:
        """
        Parse an annotated type to extract the data type and its configuration.
        """
        if get_origin(potentially_annotated_type) is Annotated:
            args = get_args(potentially_annotated_type)
            data_type = args[0]
            assert issubclass(data_type, Data), "Annotated type must be a Data subclass"
            if len(args) > 1 and isinstance(args[1], SinkConfiguration):
                return data_type, args[1]
            return data_type, SinkConfiguration()
        else:
            assert issubclass(potentially_annotated_type, Data), (
                "Type must be a Data subclass"
            )
            return potentially_annotated_type, SinkConfiguration()

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


class Source(Generic[D]):
    def __init__(self, data_type: Type[D]):
        assertion_error = (
            "data_type must be a Data subclass or a tuple of Data subclasses"
        )
        origin = get_origin(data_type)
        if origin is not None and issubclass(origin, Tuple):
            data_types: List[Type[Data]] = []
            for dt in get_args(data_type):
                assert issubclass(dt, Data), assertion_error
                data_types.append(dt)
            self.data_types = tuple(data_types)
        else:
            # Assert that D is Data
            assert issubclass(data_type, Data), assertion_error
            self.data_types = ensure_tuple_format(data_type)
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
