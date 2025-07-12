from typing import (
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    Sequence,
    Set,
    Type,
    TypeVar,
    Union,
    cast,
    overload,
)

from carbon.core.connection import Connection
from carbon.core.datamethod import DataMethod
from carbon.core.sources_sinks import Sink, Source
from carbon.core.utilities import ensure_tuple_format, is_equal_with_singleton
from carbon.data import Data


def producer(source: Source):
    def decorator(func: Callable):
        func.produces = source
        return func

    return decorator


def consumer(sink: Sink):
    def decorator(func: Callable):
        func.consumes = sink
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


T = TypeVar("T", bound="Module")


class ModuleReference(Generic[T]):
    def __init__(self, module: T):
        self.module = module


class Module:
    def __init__(self):
        self._modules: List["Module"] = []
        self._sinks: Dict[Sink, DataMethod] = {}
        self._sources: Dict[Source, DataMethod] = {}
        self._methods: Set[DataMethod] = set()
        self._connections: Set[Connection] = set()

        # Collect sources, sinks, and data methods defined in this module
        sources_to_add = set()
        sinks_to_add = set()
        methods_to_add = set()
        for attribute_name in dir(self):
            attribute = getattr(self, attribute_name)
            if isinstance(attribute, Source):
                if attribute in sources_to_add:
                    raise ValueError(
                        f"Multiple sources defined for data type {attribute.data_types}"
                    )
                sources_to_add.add(attribute)
            elif isinstance(attribute, Sink):
                if attribute in sinks_to_add:
                    raise ValueError(
                        f"Multiple sinks defined for data type {attribute.data_types}"
                    )

                sinks_to_add.add(attribute)
            elif isinstance(attribute, Callable) and (
                hasattr(attribute, "produces") or hasattr(attribute, "consumes")
            ):
                methods_to_add.add(attribute)

        for attribute in methods_to_add:
            source = None
            sink = None

            if hasattr(attribute, "produces"):
                source = cast(Source, attribute.produces)
                if source not in sources_to_add:
                    raise ValueError(
                        f"Source {source} is not defined in the module {self.__class__.__name__}"
                    )

            if hasattr(attribute, "consumes"):
                sink = cast(Sink, attribute.consumes)
                if sink not in sinks_to_add:
                    raise ValueError(
                        f"Sink {sink} is not defined in the module {self.__class__.__name__}"
                    )

            data_method = DataMethod(
                attribute,
                produces=source.data_types if source else None,
                consumes=sink.data_types if sink else None,
                sink_configuration=sink.data_configurations if sink else None,
            )
            if source:
                source.data_method = data_method
                self._sources[source] = data_method
            if sink:
                sink.data_method = data_method
                self._sinks[sink] = data_method
            self._methods.add(data_method)

        if sources_to_add.difference(set(self._sources.keys())):
            print(sources_to_add, self._sources.keys())
            raise ValueError(
                f"Sources {sources_to_add.union(set(self._sources.keys()))} do not have methods defined in the module {self.__class__.__name__}"
            )
        if sinks_to_add.difference(set(self._sinks.keys())):
            print(sinks_to_add, self._sinks.keys())
            raise ValueError(
                f"Sinks {sinks_to_add.union(set(self._sinks.keys()))} do not have methods defined in the module {self.__class__.__name__}"
            )

        print(
            f"Module {self.__class__.__name__} initialized with sources: {self._sources} and sinks: {self._sinks}"
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

    def as_reference(self):
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

    # @overload
    # def create_connection(
    #     self,
    #     source: Source[Unpack[T]],
    #     sink: Sink[Unpack[T]],
    #     sync: bool = False,
    # ): ...
    @overload
    def create_connection(
        self,
        source: Source,
        sink: Sink,
        sync: bool = False,
    ): ...
    @overload
    def create_connection(
        self,
        source: Sequence[Source],
        sink: Sink,
        sync: bool = False,
    ): ...
    @overload
    def create_connection(
        self,
        source: Source,
        sink: Sequence[Sink],
        sync: bool = False,
    ): ...
    def create_connection(
        self,
        source,
        sink,
        sync=False,
    ):
        """
        Create a connection between source and sink modules for the specified data type.
        """
        if isinstance(source, Source):
            source_types = ensure_tuple_format(source.data_types)
        else:
            source_types = ensure_tuple_format(
                [src.data_types[0] for src in cast(Sequence[Source], source)]
            )
        if isinstance(sink, Sink):
            sink_types = ensure_tuple_format(sink.data_types)
        else:
            sink_types = ensure_tuple_format(
                [snk.data_types[0] for snk in cast(Sequence[Sink], sink)]
            )
        if not is_equal_with_singleton(source_types, sink_types):
            raise ValueError(
                f"Source data types {source_types} do not match sink data types {sink_types}"
            )
        source_data_methods = (
            tuple(
                [
                    cast(DataMethod, src.data_method)
                    for src in cast(Sequence[Source], source)
                ]
            )
            if isinstance(source, Sequence)
            else ensure_tuple_format(cast(DataMethod, source.data_method))
        )
        sink_data_methods = (
            tuple(
                [
                    cast(DataMethod, snk.data_method)
                    for snk in cast(Sequence[Sink], sink)
                ]
            )
            if isinstance(sink, Sequence)
            else ensure_tuple_format(cast(DataMethod, sink.data_method))
        )
        connection = Connection(
            source_data_methods,
            sink_data_methods,
            source_types,  # type: ignore
            sync=sync,
        )
        self._ensure_unique_connection(connection)
        self._connections.add(connection)

        return self

    def block_connection(
        self,
        data: Union[Type["Data"], Sequence[Type["Data"]]],
        source: Union["Module", Sequence["Module"], None] = None,
        sink: Union["Module", Sequence["Module"], None] = None,
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
        return self
