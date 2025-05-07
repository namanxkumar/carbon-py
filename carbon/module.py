from collections import OrderedDict
from typing import Callable, List, Set, Tuple, Type, overload


class ModuleReference:
    """A reference to a module in the tree."""

    def __init__(self, module: "Module"):
        self._module = module

    @property
    def module(self):
        return self._module


class Tree:
    def __init__(self):
        self._root = None

    @property
    def root(self):
        return self._root

    def update(self, links: List, joints: List):
        pass


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


class Module:
    def __init__(self):
        self._tree = Tree()
        self._modules: OrderedDict[str, Module] = OrderedDict()
        self._sinks: OrderedDict[Callable, Tuple[Type, ...]] = OrderedDict()
        self._sources: OrderedDict[Callable, Tuple[Type, ...]] = OrderedDict()
        self._connections: Set[Tuple] = set()

        # Collect sources and sinks
        for attribute_name in dir(self):
            attribute = getattr(self, attribute_name)
            if callable(attribute) and hasattr(attribute, "_sources"):
                self._sources[attribute] = getattr(attribute, "_sources")
            if callable(attribute) and hasattr(attribute, "_sinks"):
                self._sinks[attribute] = getattr(attribute, "_sinks")

    def to_reference(self) -> ModuleReference:
        """Convert the module to a reference."""
        return ModuleReference(self)

    def __getattr__(self, name):
        if name in self._modules:
            return self._modules[name]
        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{name}'"
        )

    def __repr__(self, memo=None):
        if memo is None:
            memo = set()

        # Avoid infinite recursion by checking if the module is already visited
        if self in memo:
            return f"<{self.__class__.__name__} (circular reference)>"

        memo.add(self)
        child_lines = []

        for key, module in self._modules.items():
            # Get the string representation of the module
            module_string = module.__repr__(memo)
            module_string = _addindent(module_string, 2)
            child_lines.append("(" + key + "): " + module_string)

        main_str = self.__class__.__name__ + "("
        if child_lines:
            main_str += "\n  " + "\n  ".join(child_lines) + "\n"

        main_str += ")"
        return main_str

    def get_sources(self, recursive: bool = False, memo: Set = None):
        """Get sources of the module and its immediate children if recursive is False,
        or all sources in the tree if recursive is True."""
        if not recursive:
            return self._sources

        if memo is None:
            memo = set()
        sources = self._sources.copy()
        for module in self._modules.values():
            # Avoid infinite recursion
            if module in memo:
                continue
            memo.add(module)
            # Get sources from the module
            module_sources = module.get_sources(recursive=True, memo=memo)
            sources.update(module_sources)
        return sources

    def get_sinks(self, recursive: bool = False, memo: Set = None):
        """Get sinks of the module and its immediate children if recursive is False,
        or all sinks in the tree if recursive is True."""
        if not recursive:
            return self._sinks

        if memo is None:
            memo = set()
        sinks = self._sinks.copy()
        for module in self._modules.values():
            # Avoid infinite recursion
            if module in memo:
                continue
            memo.add(module)
            # Get sinks from the module
            module_sinks = module.get_sinks(recursive=True, memo=memo)
            sinks.update(module_sinks)
        return sinks

    def get_connections(self, recursive: bool = True, memo: Set = None):
        """Get connections of the module if recursive is False,
        or all connections in the tree if recursive is True."""

        if not recursive:
            return self._connections

        if memo is None:
            memo = set()
        connections = self._connections.copy()
        for module in self._modules.values():
            # Avoid infinite recursion
            if module in memo:
                continue
            memo.add(module)
            # Get connections from the module
            module_connections = module.get_connections(recursive=True, memo=memo)
            connections.update(module_connections)
        return connections

    def __setattr__(self, name, value):
        if isinstance(value, Module):
            # # Create connections before adding the module
            # module_sources = value._sources
            # module_sinks = value._sinks

            # for source_path, source_type in module_sources.items():
            #     self._create_connections_from_source(source_path, source_type)

            # for sink_path, sink_type in module_sinks.items():
            #     self._create_connections_from_sink(sink_path, sink_type)

            self._modules[name] = value
            value._tree._root = self
            super().__setattr__(name, value)
        else:
            super().__setattr__(name, value)

    @property
    def tree(self):
        return self._tree

    # def _create_connections_from_sink(self, sink: Callable, sink_type: Type):
    #     """Create connections from a sink to all sources of the module that produce the sink type."""
    #     for source, source_type in self.get_sources().items():
    #         if source_type == sink_type:
    #             self._connections.add((source, sink))

    # def _create_connections_from_source(self, source: Callable, source_type: Type):
    #     """Create connections from a source to all sinks of the module that consume the source type."""
    #     for sink, sink_type in self.get_sinks().items():
    #         if sink_type == source_type:
    #             self._connections.add((source, sink))

    # def expose_sink(self, sink: Callable | "Module"):
    #     """Expose a child sink to the module tree."""
    #     if isinstance(sink, Callable):
    #         assert hasattr(sink, "_sinks"), "Provided method is not a sink"

    #         sink_type = getattr(sink, "_sinks")
    #         self._create_connections_from_sink(sink, sink_type)
    #         self._sinks[sink] = sink_type
    #     elif isinstance(sink, Module):
    #         # If sink is a module, expose all its sinks
    #         for sink_path, sink_type in sink._sinks.items():
    #             self._create_connections_from_sink(sink_path, sink_type)
    #             self._sinks[sink_path] = sink_type
    #     else:
    #         raise TypeError("Provided sink is neither a callable nor a module")

    # def expose_source(self, source: Callable | "Module"):
    #     """Expose a child source to the module tree."""
    #     if isinstance(source, Callable):
    #         assert hasattr(source, "_sources"), "Provided method is not a source"

    #         source_type = getattr(source, "_sources")
    #         self._create_connections_from_source(source, source_type)
    #         self._sources[source] = source_type
    #     elif isinstance(source, Module):
    #         # If source is a module, expose all its sources
    #         for source_path, source_type in source._sources.items():
    #             self._create_connections_from_source(source_path, source_type)
    #             self._sources[source_path] = source_type
    #     else:
    #         raise TypeError("Provided source is neither a callable nor a module")

    @overload
    def _get_source_path_from_type(
        self, source: "Module", message_type: Type
    ) -> Callable: ...
    @overload
    def _get_source_path_from_type(
        self, source: Callable, message_type: Type
    ) -> Callable: ...
    def _get_source_path_from_type(
        self, source: "Module" | Callable, message_type: Type
    ) -> Callable:
        """Get the source path from a source and its type."""
        if type(message_type) is not tuple:
            message_type = (message_type,)
        if isinstance(source, Module):
            sources = source.get_sources()

            # Filter sources to match the type
            sources = {k: v for k, v in sources.items() if v == message_type}

            if not sources:
                raise ValueError(
                    f"No matching sources found for the given type {message_type} in module {source.__class__.__name__}"
                )

            if len(sources) > 1:
                raise ValueError(
                    f"Multiple sources found for the given type {message_type} in module {source.__class__.__name__}"
                )

            return list(sources.keys())[0]
        elif isinstance(source, Callable):
            if not hasattr(source, "_sources"):
                raise ValueError(f"Provided function {source.__name__} is not a source")
            source_type = getattr(source, "_sources")
            if source_type != message_type:
                raise ValueError(
                    f"Provided source type {message_type} does not match the source function {source.__name__}"
                )
            return source
        else:
            raise TypeError("Provided source is neither a callable nor a module")

    @overload
    def _get_sink_path_from_type(
        self, sink: "Module", message_type: Type
    ) -> Callable: ...
    @overload
    def _get_sink_path_from_type(
        self, sink: Callable, message_type: Type
    ) -> Callable: ...
    def _get_sink_path_from_type(
        self, sink: "Module" | Callable, message_type: Type
    ) -> Callable:
        """Get the sink path from a sink and its type."""
        if type(message_type) is not tuple:
            message_type = (message_type,)
        if isinstance(sink, Module):
            sinks = sink.get_sinks()

            # Filter sinks to match the type
            sinks = {k: v for k, v in sinks.items() if v == message_type}

            if not sinks:
                raise ValueError(
                    f"No matching sinks found for the given type {message_type} in module {sink.__class__.__name__}"
                )

            if len(sinks) > 1:
                raise ValueError(
                    f"Multiple sinks found for the given type {message_type} in module {sink.__class__.__name__}"
                )

            return list(sinks.keys())[0]
        elif isinstance(sink, Callable):
            if not hasattr(sink, "_sinks"):
                raise ValueError(f"Provided function {sink.__name__} is not a sink")
            sink_type = getattr(sink, "_sinks")
            if sink_type != message_type:
                raise ValueError(
                    f"Provided sink type {message_type} does not match the sink function {sink.__name__}"
                )
            return sink
        else:
            raise TypeError("Provided sink is neither a callable nor a module")

    def create_one_to_one_connection(
        self,
        source: "Module" | Callable,
        sink: "Module" | Callable,
        message_type: Type | Tuple[Type, ...],
    ) -> None:
        """Create a one-to-one connection between a source and a sink."""

        source_path = self._get_source_path_from_type(source, message_type)

        sink_path = self._get_sink_path_from_type(sink, message_type)

        # Create the connection
        self._connections.add((source_path, sink_path))

    def create_one_to_many_connection(
        self,
        source: "Module" | Callable,
        sink: Tuple["Module", ...] | Tuple[Callable, ...],
        message_type: Tuple[Type, ...],
    ):
        """Create a one-to-many connection between a source and multiple sinks."""
        if len(sink) != len(message_type):
            raise ValueError("Number of sinks must match the number of types")

        source_path = self._get_source_path_from_type(source, message_type)

        if isinstance(sink, (list, tuple)):
            sink_paths = []
            for s, t in zip(sink, message_type):
                sink_path = self._get_sink_path_from_type(s, t)
                sink_paths.append(sink_path)
        else:
            raise TypeError("Provided sinks must be a list or tuple")

        # Create the connection
        self._connections.add((source_path, tuple(sink_paths)))

    def create_many_to_one_connection(
        self,
        source: Tuple["Module", ...] | Tuple[Callable, ...],
        sink: "Module" | Callable,
        message_type: Tuple[Type, ...],
    ) -> None:
        """Create a many-to-one connection between multiple sources and a sink."""
        if len(source) != len(message_type):
            raise ValueError("Number of sources must match the number of types")

        sink_path = self._get_sink_path_from_type(sink, message_type)

        if isinstance(source, (list, tuple)):
            source_paths = []
            for s, t in zip(source, message_type):
                source_path = self._get_source_path_from_type(s, t)
                source_paths.append(source_path)
        else:
            raise TypeError("Provided sources must be a list or tuple")

        # Create the connection
        self._connections.add((tuple(source_paths), sink_path))

    def create_joint(self, *args, **kwargs):
        pass

    def localize_messages(self, func):
        # Localize messages to this module only
        pass

    def rebroadcast_messages(self, func):
        # Rebroadcast messages to parent modules
        pass


def source(*sources):
    def decorator(func):
        setattr(func, "_sources", sources)
        return func

    return decorator


def sink(*sinks):
    def decorator(func):
        setattr(func, "_sinks", sinks)
        return func

    return decorator
