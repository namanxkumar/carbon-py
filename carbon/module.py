from enum import Enum
from typing import Callable, Dict, List, Sequence, Set

from .data import Data


def source(*sources):
    def decorator(func):
        setattr(func, "_sources", sources if len(sources) > 1 else sources[0])
        return func

    return decorator


def sink(*sinks):
    def decorator(func):
        setattr(func, "_sinks", sinks if len(sinks) > 1 else sinks[0])
        return func

    return decorator


class ConnectionType(Enum):
    BLOCKING = "blocking"
    NON_BLOCKING = "non-blocking"


class Connection:
    def __init__(
        self,
        source: "Module" | Sequence["Module"],
        sink: "Module" | Sequence["Module"],
        data: Data | Sequence[Data],
        blocking: bool = False,
        queue_size: int = 1,
    ):
        assert not (isinstance(source, Sequence) and isinstance(sink, Sequence)), (
            "Cannot connect multiple sources to multiple sinks directly. "
            "Use a single source or sink, or create a connection for each pair."
        )

        self.source = source
        self.sink = sink
        self.data = data
        self.type = ConnectionType.BLOCKING if blocking else ConnectionType.NON_BLOCKING
        self.queue_size = queue_size

        if isinstance(self.source, Sequence):
            assert isinstance(data, Sequence), (
                "If source is a list, data must also be a list."
            )
            assert len(self.source) == len(self.data), (
                "data must have the same length as sources."
            )
        elif isinstance(self.sink, Sequence):
            assert isinstance(data, Sequence), (
                "If sink is a list, data must also be a list."
            )
            assert len(self.sink) == len(self.data), (
                "data must have the same length as sinks."
            )

        # Verify that source modules contain the correct data type
        if isinstance(self.source, Sequence):
            for src, dat in zip(self.source, data):
                src: "Module"
                assert dat in src._sources, (
                    f"Source {src} must have data type {dat} defined in its sources."
                )
        else:
            assert self.data in self.source._sources, (
                f"Source {source} must have data type {data} defined in its sources."
            )

        # Verify that sink modules contain the correct data type
        if isinstance(self.sink, Sequence):
            for snk, dat in zip(self.sink, data):
                snk: "Module"
                assert dat in snk._sinks, (
                    f"Sink {snk} must have data type {dat} defined in its sinks."
                )
        else:
            assert self.data in self.sink._sinks, (
                f"Sink {sink} must have data type {data} defined in its sinks."
            )

    def __hash__(self):
        return hash((self.source, self.sink, self.data))

    def __eq__(self, other):
        return (
            isinstance(other, Connection)
            and self.source == other.source
            and self.sink == other.sink
            and self.data == other.data
        )


class ModuleReference:
    def __init__(self, module: "Module"):
        self.module = module


class Module:
    def __init__(self):
        self._modules: Dict[str, Module] = {}
        self._sinks: Dict[Data | List[Data], Callable] = {}
        self._sources: Dict[Data | List[Data], Callable] = {}
        self._connections: Set[Connection] = set()
        self._blocked_connections: Set[Connection] = set()

        # Collect sources and sinks
        for attribute_name in dir(self):
            attribute = getattr(self, attribute_name)
            if callable(attribute) and hasattr(attribute, "_sources"):
                datatype = getattr(attribute, "_sources")
                if self._sources.get(datatype) is None:
                    self._sources[datatype] = attribute
                else:
                    raise ValueError(
                        f"Multiple sources defined for data type {datatype}"
                    )
            if callable(attribute) and hasattr(attribute, "_sinks"):
                datatype = getattr(attribute, "_sinks")
                if self._sinks.get(datatype) is None:
                    self._sinks[datatype] = attribute
                else:
                    raise ValueError(f"Multiple sinks defined for data type {datatype}")

    def as_reference(self) -> ModuleReference:
        """
        Return a reference to this module.
        """
        return ModuleReference(self)

    def __setattr__(self, name, value):
        if isinstance(value, Module):
            module_connections = value._get_connections()
            for connection in module_connections:
                self._ensure_unique_connection(connection)
            self._modules[name] = value
            super().__setattr__(name, value)
        else:
            super().__setattr__(name, value)

    def _ensure_unique_connection(
        self,
        connection: Connection,
    ):
        existing_connections = self._get_connections()
        for existing_connection in existing_connections:
            if existing_connection == connection:
                raise ValueError(
                    f"Connection already exists between {connection.source} and {connection.sink} for data {connection.data}"
                )

    def _get_connections(self, recursive: bool = True, memo: Set = None):
        if not recursive:
            return self._connections - self._blocked_connections

        if memo is None:
            memo = set()

        connections = self._connections.copy()

        for module in self._modules.values():
            if module not in memo:
                memo.add(module)
                connections.update(module._get_connections(recursive, memo))

        return connections - self._blocked_connections

    def create_connection(
        self,
        source: "Module" | List["Module"],
        sink: "Module" | List["Module"],
        data: Data | List[Data],
        blocking: bool = False,
        queue_size: int = 1,
    ):
        """
        Create a connection between source and sink modules for the specified data type.
        """
        connection = Connection(source, sink, data, blocking, queue_size)
        self._ensure_unique_connection(connection)
        self._connections.add(connection)
        return connection

    def add_connection(
        self,
        connection: Connection,
    ):
        """
        Add an existing connection to the module.
        """
        self._ensure_unique_connection(connection)
        self._connections.add(connection)

    def block_connection(
        self,
        source: "Module" | List["Module"] | None,
        sink: "Module" | List["Module"] | None,
        data: Data | List[Data],
    ):
        """
        Block a connection between source and sink modules for the specified data type.
        """
        for existing_connection in self._get_connections():
            removal = False
            if source is None and sink is None and existing_connection.data == data:
                removal = True
            elif (
                source is None
                and existing_connection.sink == sink
                and existing_connection.data == data
            ):
                removal = True
            elif (
                existing_connection.source == source
                and sink is None
                and existing_connection.data == data
            ):
                removal = True
            elif (
                existing_connection.source == source
                and existing_connection.sink == sink
                and existing_connection.data == data
            ):
                removal = True
            if removal:
                self._blocked_connections.add(existing_connection)


if __name__ == "__main__":
    from dataclasses import dataclass

    @dataclass
    class WheelBaseCommand(Data):
        left: float
        right: float

    class WheelBase(Module):
        def __init__(self):
            super().__init__()

            self.a = 0

    test = WheelBase.Command(left=0.0, right=0.0)
    print(test)
    print(WheelBase.__dict__)
