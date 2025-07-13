from typing import (
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    Union,
    cast,
)

from carbon.core.connection import Connection
from carbon.core.datamethod import ConsumerConfiguration, DataMethod
from carbon.core.utilities import ensure_tuple_format, is_equal_with_singleton
from carbon.data import Data


class ConfiguredType:
    def __init__(self, type_: Type["Data"], queue_size: int = 1, sticky: bool = False):
        self.type = type_
        self.configuration = ConsumerConfiguration(queue_size=queue_size, sticky=sticky)


def producer(*producers: Type["Data"]):
    def decorator(
        func: Callable[..., Union["Data", Tuple["Data", ...], None]],
    ):
        setattr(func, "_producers", producers)
        return func

    return decorator


def consumer(*consumers: Union[Type["Data"], ConfiguredType]):
    def decorator(func: Callable[..., Union["Data", Tuple["Data", ...], None]]):
        setattr(
            func,
            "_consumers",
            tuple(
                consumer.type if isinstance(consumer, ConfiguredType) else consumer
                for consumer in consumers
            ),
        )
        setattr(
            func,
            "_consumer_configuration",
            tuple(
                consumer.configuration
                if isinstance(consumer, ConfiguredType)
                else ConsumerConfiguration()
                for consumer in consumers
            ),
        )
        return func

    return decorator


def _addindent(s_: str, numSpaces: int):
    s = s_.split("\n")
    # don't do anything for single-line stuff
    if len(s) == 1:
        return s_
    first = s.pop(0)
    s = [(numSpaces * " ") + line for line in s]
    s = "\n".join(s)
    s = first + "\n" + s
    return s


class ModuleReference:
    def __init__(self, module: "Module"):
        self.module = module


class Module:
    def __init__(self):
        self._modules: List["Module"] = []
        self._consumers: Dict[Tuple[Type["Data"], ...], "DataMethod"] = {}
        self._producers: Dict[Tuple[Type["Data"], ...], "DataMethod"] = {}
        self._methods: Set["DataMethod"] = set()
        self._connections: Set["Connection"] = set()

        # Collect producers and consumers
        for attribute_name in dir(self):
            attribute = getattr(self, attribute_name)

            if callable(attribute) and (
                hasattr(attribute, "_producers") or hasattr(attribute, "_consumers")
            ):
                data_method = DataMethod(attribute)
            else:
                continue

            if hasattr(attribute, "_producers"):
                datatype: Tuple[Type["Data"], ...] = getattr(attribute, "_producers")
                if self._producers.get(datatype) is None:
                    self._producers[datatype] = data_method
                    self._methods.add(data_method)
                else:
                    raise ValueError(
                        f"Multiple producers defined for data type {datatype}"
                    )
            if hasattr(attribute, "_consumers"):
                datatype: Tuple[Type["Data"], ...] = getattr(attribute, "_consumers")
                if self._consumers.get(datatype) is None:
                    self._consumers[datatype] = data_method
                    self._methods.add(data_method)
                else:
                    raise ValueError(
                        f"Multiple consumers defined for data type {datatype}"
                    )

    def __setattr__(self, name, value):
        if isinstance(value, Module):
            self.add_modules([value])
            super().__setattr__(name, value)
        else:
            super().__setattr__(name, value)

    def _ensure_unique_connection(
        self,
        connection: "Connection",
    ):
        existing_connections = self.get_connections()
        for existing_connection in existing_connections:
            if existing_connection == connection:
                raise ValueError(
                    f"Connection already exists between {connection.producers} and {connection.consumers} for data {connection.data}"
                )

    def __repr__(self, memo=None, detail: bool = False) -> str:
        if memo is None:
            memo = set()

        # Avoid infinite recursion by checking if the module is already visited
        if self in memo:
            return f"<{self.__class__.__name__} (circular reference)>"

        memo.add(self)
        child_lines = []

        for module in self._modules:
            # Get the string representation of the module
            module_string = module.__repr__(memo, detail=detail)
            module_string = _addindent(module_string, 2)
            child_lines.append(module_string)

        main_str = self.__class__.__name__ + "("
        if child_lines:
            main_str += "\n  " + "\n  ".join(child_lines) + "\n"
        if self._consumers and detail:
            consumers_str = "\n    ".join(
                f"{key[0] if len(key) == 1 else key}" for key in self._consumers.keys()
            )
            consumers_str = f"\n    {consumers_str}"
            main_str += f"\n  consumers: {consumers_str}\n"
        if self._producers and detail:
            producers_str = "\n    ".join(
                f"{key[0] if len(key) == 1 else key}" for key in self._producers.keys()
            )
            producers_str = f"\n    {producers_str}"
            main_str += f"\n  producers: {producers_str}\n"
        main_str += ")"
        return main_str

    def get_description(self) -> str:
        return self.__repr__(detail=True)

    def as_reference(self) -> ModuleReference:
        """
        Return a reference to this module.
        """
        return ModuleReference(self)

    def add_modules(
        self,
        module: Sequence["Module"],
    ):
        """
        Add a module or a collection of modules to this module.
        Ensures that connections are unique.
        """
        for mod in module:
            module_connections = mod.get_connections()
            for connection in module_connections:
                self._ensure_unique_connection(connection)
            if mod not in self._modules:
                self._modules.append(mod)

        return self

    def get_connections(
        self,
        recursive: bool = True,
        active_only: bool = True,
        _memo: Optional[Set] = None,
    ):
        if not recursive:
            if active_only:
                return {conn for conn in self._connections if conn.active}
            return self._connections

        if _memo is None:
            _memo = set()

        if active_only:
            connections = {conn for conn in self._connections if conn.active}
        else:
            connections = self._connections.copy()

        for module in self._modules:
            if module not in _memo:
                _memo.add(module)
                connections.update(
                    module.get_connections(recursive, active_only, _memo)
                )

        return connections

    def get_methods(
        self,
        recursive: bool = True,
        active_only: bool = True,
        _memo: Optional[Set] = None,
    ):
        if not recursive:
            if active_only:
                return {method for method in self._methods if method.active}
            return self._methods

        if _memo is None:
            _memo = set()

        if active_only:
            methods = {method for method in self._methods if method.active}
        else:
            methods = self._methods.copy()

        for module in self._modules:
            if module not in _memo:
                _memo.add(module)
                methods.update(module.get_methods(recursive, active_only, _memo))

        return methods

    def get_produced_data_types(
        self, recursive: bool = True, _memo: Optional[Set] = None
    ):
        """
        Get all data types produced by this module and its submodules.
        """
        if not recursive:
            return set(self._producers.keys())

        if _memo is None:
            _memo = set()

        produced_data_types = set(self._producers.keys())

        for module in self._modules:
            if module not in _memo:
                _memo.add(module)
                produced_data_types.update(
                    module.get_produced_data_types(recursive, _memo)
                )

        return produced_data_types

    def get_produced_data_types_mapping(
        self, recursive: bool = True, _memo: Optional[Set] = None
    ) -> Dict[Tuple[Type["Data"], ...], Tuple["Module", ...]]:
        """
        Get a mapping of data types to the modules that produce them.
        """
        if not recursive:
            return {data_type: (self,) for data_type in self._producers.keys()}

        if _memo is None:
            _memo = set()

        produced_data_types_mapping: Dict[
            Tuple[Type["Data"], ...], Tuple["Module", ...]
        ] = {data_type: (self,) for data_type in self._producers.keys()}

        for module in self._modules:
            if module not in _memo:
                _memo.add(module)
                submodule_mapping = module.get_produced_data_types_mapping(
                    recursive, _memo
                )
                for data_type, submodule in submodule_mapping.items():
                    if data_type not in produced_data_types_mapping:
                        produced_data_types_mapping[data_type] = submodule
                    else:
                        # If the data type is already mapped, we merge them as a tuple
                        existing_module = produced_data_types_mapping[data_type]
                        produced_data_types_mapping[data_type] = (
                            existing_module + submodule
                        )

        return produced_data_types_mapping

    def get_consumed_data_types_mapping(
        self, recursive: bool = True, _memo: Optional[Set] = None
    ) -> Dict[Tuple[Type["Data"], ...], Tuple["Module", ...]]:
        """
        Get a mapping of data types to the modules that consume them.
        """
        if not recursive:
            return {data_type: (self,) for data_type in self._consumers.keys()}

        if _memo is None:
            _memo = set()

        consumed_data_types_mapping: Dict[
            Tuple[Type["Data"], ...], Tuple["Module", ...]
        ] = {data_type: (self,) for data_type in self._consumers.keys()}

        for module in self._modules:
            if module not in _memo:
                _memo.add(module)
                submodule_mapping = module.get_consumed_data_types_mapping(
                    recursive, _memo
                )
                for data_type, submodule in submodule_mapping.items():
                    if data_type not in consumed_data_types_mapping:
                        consumed_data_types_mapping[data_type] = submodule
                    else:
                        # If the data type is already mapped, we merge them as a tuple
                        existing_module = consumed_data_types_mapping[data_type]
                        consumed_data_types_mapping[data_type] = (
                            existing_module + submodule
                        )

        return consumed_data_types_mapping

    def get_consumed_data_types(
        self, recursive: bool = True, _memo: Optional[Set] = None
    ):
        """
        Get all data types consumed by this module and its submodules.
        """
        if not recursive:
            return set(self._consumers.keys())

        if _memo is None:
            _memo = set()

        consumed_data_types = set(self._consumers.keys())

        for module in self._modules:
            if module not in _memo:
                _memo.add(module)
                consumed_data_types.update(
                    module.get_consumed_data_types(recursive, _memo)
                )

        return consumed_data_types

    def add_method(
        self,
        method: Callable,
        consumes: Optional[Union[Type["Data"], Tuple[Type["Data"], ...]]] = None,
        produces: Optional[Union[Type["Data"], Tuple[Type["Data"], ...]]] = None,
    ):
        """
        Add a producer for the specified data type.
        """
        assert callable(method), "Method must be callable"
        assert produces or consumes, (
            "At least one of produces or consumes must be specified"
        )
        assert not hasattr(method, "_producers") and not hasattr(
            method, "_consumers"
        ), "Method already has producers or consumers defined"
        if produces is not None:
            produces = ensure_tuple_format(produces)
        else:
            produces = tuple()

        if consumes is not None:
            consumes = ensure_tuple_format(consumes)
            consumes = tuple(
                consumer.type if isinstance(consumer, ConfiguredType) else consumer
                for consumer in consumes
            )
            consumer_configuration = tuple(
                consumer.configuration
                if isinstance(consumer, ConfiguredType)
                else ConsumerConfiguration()
                for consumer in consumes
            )
        else:
            consumes = tuple()
            consumer_configuration = tuple()

        data_method = DataMethod(
            method,
            produces=produces,
            consumes=consumes,
            consumer_configuration=cast(
                Tuple[ConsumerConfiguration, ...], consumer_configuration
            ),
        )

        if produces:
            if self._producers.get(produces) is None:
                self._producers[produces] = data_method
                self._methods.add(data_method)
            else:
                raise ValueError(f"Multiple producers defined for data type {produces}")

        if consumes:
            if self._consumers.get(consumes) is None:
                self._consumers[consumes] = data_method
                self._methods.add(data_method)
            else:
                raise ValueError(f"Multiple consumers defined for data type {consumes}")

        return self

    def create_connection(
        self,
        data: Union[Type["Data"], Sequence[Type["Data"]]],
        producer: Union["Module", Sequence["Module"]],
        consumer: Union["Module", Sequence["Module"]],
        sync: bool = False,
    ):
        """
        Create a connection between producer and consumer modules for the specified data type.
        """
        connection = Connection(producer, consumer, data, sync=sync)  # type: ignore
        self._ensure_unique_connection(connection)
        self._connections.add(connection)

        return self

    def block_connection(
        self,
        data: Union[Type["Data"], Sequence[Type["Data"]]],
        producer: Union["Module", Sequence["Module"], None] = None,
        consumer: Union["Module", Sequence["Module"], None] = None,
    ):
        """
        Block a connection between producer and consumer modules for the specified data type.
        """
        for existing_connection in self.get_connections():
            removal = False

            if (
                producer is None
                and consumer is None
                and is_equal_with_singleton(existing_connection.data, data)
            ):
                removal = True
            elif (
                producer is None
                and is_equal_with_singleton(existing_connection.consumers, consumer)
                and is_equal_with_singleton(existing_connection.data, data)
            ):
                removal = True
            elif (
                is_equal_with_singleton(existing_connection.producers, producer)
                and consumer is None
                and is_equal_with_singleton(existing_connection.data, data)
            ):
                removal = True
            elif (
                is_equal_with_singleton(existing_connection.producers, producer)
                and is_equal_with_singleton(existing_connection.consumers, consumer)
                and is_equal_with_singleton(existing_connection.data, data)
            ):
                removal = True
            if removal:
                existing_connection.block()
        return self
