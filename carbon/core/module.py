from dataclasses import dataclass, field
from enum import Enum
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

from carbon.core.datamethod import DataMethod, SinkConfiguration
from carbon.core.utilities import ensure_tuple_format, is_equal_with_singleton
from carbon.data import Data


class ConfigurableSink:
    def __init__(self, type_: Type["Data"], queue_size: int = 1, sticky: bool = False):
        self.type = type_
        self.configuration = SinkConfiguration(queue_size=queue_size, sticky=sticky)


def source(*sources: Type["Data"]):
    def decorator(
        func: Callable[..., Union["Data", Tuple["Data", ...], None]],
    ):
        setattr(func, "_sources", sources)
        return func

    return decorator


def sink(*sinks: Union[Type["Data"], ConfigurableSink]):
    def decorator(func: Callable[..., Union["Data", Tuple["Data", ...], None]]):
        setattr(
            func,
            "_sinks",
            tuple(
                sink.type if isinstance(sink, ConfigurableSink) else sink
                for sink in sinks
            ),
        )
        setattr(
            func,
            "_sink_configuration",
            {
                sink_index: sink.configuration
                if isinstance(sink, ConfigurableSink)
                else SinkConfiguration()
                for sink_index, sink in enumerate(sinks)
            },
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


@dataclass
class Connected:
    """
    Configuration for a connected module.
    - connection_index: Index to merge or split data (None if direct connection).
    - blocking: Whether the dependency is blocking or not.
    """

    module: "Module"
    data: Tuple[Type["Data"], ...]  # Data types this connection handles
    connection_index: Optional[
        int
    ]  # Index to merge or split data (None if direct connection)
    sync: bool


@dataclass
class Dependency:
    module: "Module"
    data: Tuple[Type["Data"], ...]
    merge_sink_index: Optional[int] = None
    sync: bool = False


@dataclass
class Dependent:
    module: "Module"
    data: Tuple[Type["Data"], ...]
    split_source_index: Optional[int] = None
    sync: bool = False


@dataclass
class Interface:
    method: "DataMethod"
    connected: List["Connected"] = field(default_factory=list)


class Module:
    def __init__(self):
        self._modules: List["Module"] = []
        self._sinked_to_interface: Dict[Tuple[Type["Data"], ...], Interface] = {}
        self._sourced_to_interface: Dict[Tuple[Type["Data"], ...], Interface] = {}
        self._sinks: Dict[Tuple[Type["Data"], ...], "DataMethod"] = {}  # TBD
        self._sources: Dict[Tuple[Type["Data"], ...], "DataMethod"] = {}  # TBD
        self._methods: Set["DataMethod"] = set()  # TBD
        self._connections: Set["Connection"] = set()
        self._blocked_connections: Set["Connection"] = set()

        # Collect sources and sinks
        for attribute_name in dir(self):
            attribute = getattr(self, attribute_name)

            if callable(attribute) and (
                hasattr(attribute, "_sources") or hasattr(attribute, "_sinks")
            ):
                data_method = DataMethod(attribute)
            else:
                continue

            if hasattr(attribute, "_sources"):
                data_type: Tuple[Type["Data"], ...] = getattr(attribute, "_sources")
                if self._sourced_to_interface.get(data_type) is None:
                    self._sourced_to_interface[data_type] = Interface(
                        method=data_method
                    )
                    self._methods.add(data_method)
                else:
                    raise ValueError(
                        f"Multiple sources defined for data type {data_type}"
                    )
                if self._sources.get(data_type) is None:
                    self._sources[data_type] = data_method
                    self._methods.add(data_method)
                else:
                    raise ValueError(
                        f"Multiple sources defined for data type {data_type}"
                    )
            if hasattr(attribute, "_sinks"):
                data_type: Tuple[Type["Data"], ...] = getattr(attribute, "_sinks")
                if self._sinked_to_interface.get(data_type) is None:
                    self._sinked_to_interface[data_type] = Interface(method=data_method)
                    self._methods.add(data_method)
                else:
                    raise ValueError(
                        f"Multiple sinks defined for data type {data_type}"
                    )
                if self._sinks.get(data_type) is None:
                    self._sinks[data_type] = data_method
                    self._methods.add(data_method)
                else:
                    raise ValueError(
                        f"Multiple sinks defined for data type {data_type}"
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
                    f"Connection already exists between {connection.source} and {connection.sink} for data {connection.data}"
                )

    def __repr__(self, memo=None):
        if memo is None:
            memo = set()

        # Avoid infinite recursion by checking if the module is already visited
        if self in memo:
            return f"<{self.__class__.__name__} (circular reference)>"

        memo.add(self)
        child_lines = []

        for module in self._modules:
            # Get the string representation of the module
            module_string = module.__repr__(memo)
            module_string = _addindent(module_string, 2)
            child_lines.append(module_string)

        main_str = self.__class__.__name__ + "("
        if child_lines:
            main_str += "\n  " + "\n  ".join(child_lines) + "\n"

        main_str += ")"
        return main_str

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

    def get_dependencies(self):
        """
        Get all dependencies of this module.
        """
        dependencies: Set[Dependency] = set()

        for data_type, interface in self._sinked_to_interface.items():
            for connected in interface.connected:
                dependencies.add(
                    Dependency(
                        module=connected.module,
                        data=data_type,
                        merge_sink_index=connected.connection_index,
                        sync=connected.sync,
                    )
                )

        return dependencies

    def get_dependents(self):
        """
        Get all dependents of this module.
        """
        dependents: Set[Dependent] = set()

        for data_type, interface in self._sourced_to_interface.items():
            for connected in interface.connected:
                dependents.add(
                    Dependent(
                        module=connected.module,
                        data=data_type,
                        split_source_index=connected.connection_index,
                        sync=connected.sync,
                    )
                )

        return dependents

    def get_dependencies_as_generator(self):
        """
        Get all dependencies of this module as a generator.
        """
        for data_type, interface in self._sinked_to_interface.items():
            for connected in interface.connected:
                yield Dependency(
                    module=connected.module,
                    data=data_type,
                    merge_sink_index=connected.connection_index,
                    sync=connected.sync,
                )

    def get_dependents_as_generator(self):
        """
        Get all dependents of this module as a generator.
        """
        for data_type, interface in self._sourced_to_interface.items():
            for connected in interface.connected:
                yield Dependent(
                    module=connected.module,
                    data=data_type,
                    split_source_index=connected.connection_index,
                    sync=connected.sync,
                )

    def get_connections(self, recursive: bool = True, _memo: Optional[Set] = None):
        if not recursive:
            return self._connections - self._blocked_connections

        if _memo is None:
            _memo = set()

        connections = self._connections.copy()

        for module in self._modules:
            if module not in _memo:
                _memo.add(module)
                connections.update(module.get_connections(recursive, _memo))

        return connections - self._blocked_connections

    def get_methods(self, recursive: bool = True, _memo: Optional[Set] = None):
        if not recursive:
            return self._methods

        if _memo is None:
            _memo = set()

        methods = self._methods.copy()

        for module in self._modules:
            if module not in _memo:
                _memo.add(module)
                methods.update(module.get_methods(recursive, _memo))

        return methods

    def get_modules(self, recursive: bool = True, _memo: Optional[Set] = None):
        """
        Get all modules contained within this module, optionally recursively.
        """
        if not recursive:
            return self._modules

        if _memo is None:
            _memo = set()

        modules = self._modules.copy()

        for module in self._modules:
            if module not in _memo:
                _memo.add(module)
                modules.extend(module.get_modules(recursive, _memo))

        return modules

    def create_connection(
        self,
        source: Union["Module", Sequence["Module"]],
        sink: Union["Module", Sequence["Module"]],
        data: Union[Type["Data"], Sequence[Type["Data"]]],
        sync: bool = False,
    ):
        """
        Create a connection between source and sink modules for the specified data type.
        """
        connection = Connection(source, sink, data, sync=sync)  # type: ignore
        self._ensure_unique_connection(connection)
        self._connections.add(connection)

        return self

    def block_connection(
        self,
        source: Union["Module", Sequence["Module"], None],
        sink: Union["Module", Sequence["Module"], None],
        data: Union[Type["Data"], Sequence[Type["Data"]]],
    ):
        """
        Block a connection between source and sink modules for the specified data type.
        """
        for existing_connection in self.get_connections():
            removal = False

            if (
                source is None
                and sink is None
                and is_equal_with_singleton(existing_connection.data, data)
            ):
                removal = True
            elif (
                source is None
                and is_equal_with_singleton(existing_connection.sink, sink)
                and is_equal_with_singleton(existing_connection.data, data)
            ):
                removal = True
            elif (
                is_equal_with_singleton(existing_connection.source, source)
                and sink is None
                and is_equal_with_singleton(existing_connection.data, data)
            ):
                removal = True
            elif (
                is_equal_with_singleton(existing_connection.source, source)
                and is_equal_with_singleton(existing_connection.sink, sink)
                and is_equal_with_singleton(existing_connection.data, data)
            ):
                removal = True
            if removal:
                existing_connection.block()
                self._blocked_connections.add(existing_connection)

        return self


class ConnectionType(Enum):
    MERGE = "merge"
    SPLIT = "split"
    DIRECT = "direct"


class Connection:
    def __init__(
        self,
        source: Union["Module", Sequence["Module"]],
        sink: Union["Module", Sequence["Module"]],
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

            for src_index, (src, dat) in enumerate(zip(self.source, self.data)):
                assert ensure_tuple_format(dat) in src._sources, (
                    f"Source {src} must have data type {dat} defined in its sources."
                )
                src._sourced_to_interface[ensure_tuple_format(dat)].connected.append(
                    Connected(
                        cast(Tuple[Module], self.sink)[0],
                        self.data,
                        src_index,
                        self.sync,
                    )
                )
        if len(self.sink) > 1:
            self.type = ConnectionType.SPLIT

            for snk_index, (snk, dat) in enumerate(zip(self.sink, self.data)):
                assert ensure_tuple_format(dat) in snk._sinks, (
                    f"Sink {snk} must have data type {dat} defined in its sinks."
                )
                snk._sinked_to_interface[ensure_tuple_format(dat)].connected.append(
                    Connected(
                        cast(Tuple[Module], self.source)[0],
                        self.data,
                        snk_index,
                        self.sync,
                    )
                )

        if len(self.source) == 1:
            assert self.data in self.source[0]._sources, (
                f"Source {self.source[0]} must have data type {self.data[0]} defined in its sources."
            )

            if self.type is ConnectionType.SPLIT:
                self.source[0]._sourced_to_interface[
                    ensure_tuple_format(self.data)
                ].connected += [
                    Connected(sink, self.data, sink_index, self.sync)
                    for sink_index, sink in enumerate(self.sink)
                ]
            else:
                # DIRECT type
                self.source[0]._sourced_to_interface[
                    ensure_tuple_format(self.data)
                ].connected.append(
                    Connected(
                        cast(Tuple[Module], self.sink)[0], self.data, None, self.sync
                    )
                )

        if len(self.sink) == 1:
            assert self.data in self.sink[0]._sinks, (
                f"Sink {self.sink[0]} must have data type {self.data[0]} defined in its sinks."
            )

            if self.type is ConnectionType.MERGE:
                self.sink[0]._sinked_to_interface[
                    ensure_tuple_format(self.data)
                ].connected += [
                    Connected(source, self.data, source_index, self.sync)
                    for source_index, source in enumerate(self.source)
                ]
            else:
                # DIRECT type
                self.sink[0]._sinked_to_interface[
                    ensure_tuple_format(self.data)
                ].connected.append(
                    Connected(
                        cast(Tuple[Module], self.source)[0], self.data, None, self.sync
                    )
                )

    def block(self):
        """Block the connection, preventing data from being sent."""
        self.blocked = True

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
