from typing import Callable, Dict, List, Sequence, Set, Tuple

from .connection import Connection
from .data import Data


def source(*sources):
    def decorator(func):
        setattr(func, "_sources", tuple(sources))
        return func

    return decorator


def sink(*sinks):
    def decorator(func):
        setattr(func, "_sinks", tuple(sinks))
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


class DataMethod:
    def __init__(self, method: Callable):
        self.method = method
        self.name: str = method.__name__
        self.sources: Tuple[Data] = getattr(method, "_sources", [])
        self.sinks: Tuple[Data] = getattr(method, "_sinks", [])
        self.dependencies: Set["DataMethod"] = (
            set()
        )  # Methods that depend on this method
        self.dependents: Set["DataMethod"] = (
            set()
        )  # Methods that this method depends on

    def __call__(self, *args, **kwargs):
        return self.method(*args, **kwargs)

    def __eq__(self, value):
        if isinstance(value, DataMethod):
            return self.method == value.method
        return self.method == value

    def __repr__(self):
        return f"{self.name}"

    def __hash__(self):
        return hash(self.method)


class ModuleReference:
    def __init__(self, module: "Module"):
        self.module = module


class Module:
    def __init__(self):
        self._modules: List["Module"] = []
        self._sinks: Dict[Tuple[Data], DataMethod] = {}
        self._sources: Dict[Tuple[Data], DataMethod] = {}
        self._methods: Set[DataMethod] = set()
        self._connections: Set[Connection] = set()
        self._blocked_connections: Set[Connection] = set()

        # Collect sources and sinks
        for attribute_name in dir(self):
            attribute = getattr(self, attribute_name)
            if callable(attribute) and hasattr(attribute, "_sources"):
                datatype: Tuple[Data] = getattr(attribute, "_sources")
                if self._sources.get(datatype) is None:
                    data_method = DataMethod(attribute)
                    self._sources[datatype] = data_method
                    self._methods.add(data_method)
                else:
                    raise ValueError(
                        f"Multiple sources defined for data type {datatype}"
                    )
            if callable(attribute) and hasattr(attribute, "_sinks"):
                datatype: Tuple[Data] = getattr(attribute, "_sinks")
                if self._sinks.get(datatype) is None:
                    data_method = DataMethod(attribute)
                    self._sinks[datatype] = data_method
                    self._methods.add(data_method)
                else:
                    raise ValueError(f"Multiple sinks defined for data type {datatype}")

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

    def __setattr__(self, name, value):
        if isinstance(value, Module):
            self.add_modules([value])
            super().__setattr__(name, value)
        else:
            super().__setattr__(name, value)

    def _ensure_unique_connection(
        self,
        connection: Connection,
    ):
        existing_connections = self.get_connections()
        for existing_connection in existing_connections:
            if existing_connection == connection:
                raise ValueError(
                    f"Connection already exists between {connection.source} and {connection.sink} for data {connection.data}"
                )

    def get_connections(self, recursive: bool = True, _memo: Set = None):
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

    def get_methods(self, recursive: bool = True, _memo: Set = None):
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

    def create_connection(
        self,
        source: "Module" | Sequence["Module"],
        sink: "Module" | Sequence["Module"],
        data: Data | Sequence[Data],
        blocking: bool = False,
        queue_size: int = 1,
    ):
        """
        Create a connection between source and sink modules for the specified data type.
        """
        connection = Connection(source, sink, data, blocking, queue_size)
        self._ensure_unique_connection(connection)
        self._connections.add(connection)
        for source_method in connection.source_methods:
            for sink_method in connection.sink_methods:
                source_method.dependents.add(sink_method)
                sink_method.dependencies.add(source_method)
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
        for source_method in connection.source_methods:
            for sink_method in connection.sink_methods:
                source_method.dependents.add(sink_method)
                sink_method.dependencies.add(source_method)

    def block_connection(
        self,
        source: "Module" | Sequence["Module"] | None,
        sink: "Module" | Sequence["Module"] | None,
        data: Data | Sequence[Data],
    ):
        """
        Block a connection between source and sink modules for the specified data type.
        """
        for existing_connection in self.get_connections():
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
            child_lines.append("(" + module.__class__.__name__ + "): " + module_string)

        main_str = self.__class__.__name__ + "("
        if child_lines:
            main_str += "\n  " + "\n  ".join(child_lines) + "\n"

        main_str += ")"
        return main_str
