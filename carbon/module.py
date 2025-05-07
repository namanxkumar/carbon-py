from collections import OrderedDict
from typing import Any, Callable, List, Set, Tuple


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
        self._sinks: OrderedDict[Tuple[Module | Callable], Any] = OrderedDict()
        self._sources: OrderedDict[Tuple[Module | Callable], Any] = OrderedDict()
        self._connections: Set[Tuple] = set()

        # Collect sources and sinks
        for attribute_name in dir(self):
            attribute = getattr(self, attribute_name)
            if callable(attribute) and hasattr(attribute, "_sources"):
                self._sources[attribute] = getattr(attribute, "_sources")
            if callable(attribute) and hasattr(attribute, "_sinks"):
                self._sinks[attribute] = getattr(attribute, "_sinks")

    def __repr__(self):
        child_lines = []
        for key, module in self._modules.items():
            module_string = repr(module)
            module_string = _addindent(module_string, 2)
            child_lines.append("(" + key + "): " + module_string)

        main_str = self.__class__.__name__ + "("
        if child_lines:
            main_str += "\n  " + "\n  ".join(child_lines) + "\n"

        main_str += ")"
        return main_str

    def get_sources(self, recursive: bool = True):
        if not recursive:
            return self._sources

        # Recursively get sources from child modules
        sources = self._sources.copy()
        for module in self._modules.values():
            module_sources = module.get_sources(recursive=False)
            sources.update(module_sources)
            # for source in module_sources:
            #     sources[(module,) + source] = module_sources[source]
        return sources

    def get_sinks(self, recursive: bool = True):
        if not recursive:
            return self._sinks

        # Recursively get sinks from child modules
        sinks = self._sinks.copy()
        for module in self._modules.values():
            module_sinks = module.get_sinks(recursive=False)
            sinks.update(module_sinks)
            # for sink in module_sinks:
            #     sinks[(module,) + sink] = module_sinks[sink]
        return sinks

    def __setattr__(self, name, value):
        if isinstance(value, Module):
            current_sinks = self.get_sinks()
            current_sources = self.get_sources()

            self._modules[name] = value
            value._tree._root = self

            # Create connections
            module_sources = value.get_sources(recursive=False)
            module_sinks = value.get_sinks(recursive=False)

            for source_path, source_type in module_sources.items():
                for sink, sink_type in current_sinks.items():
                    if sink_type == source_type:
                        self._connections.add((source_path, sink))

            for sink_path, sink_type in module_sinks.items():
                for source, source_type in current_sources.items():
                    if sink_type == source_type:
                        self._connections.add((source, sink_path))

            super().__setattr__(name, value)
        else:
            super().__setattr__(name, value)

    @property
    def tree(self):
        return self._tree

    def create_connection(self, *args, **kwargs):
        pass

    def create_joint(self, *args, **kwargs):
        pass

    def localize_messages(self, func):
        # Localize messages to this module only
        pass

    def rebroadcast_messages(self, func):
        # Rebroadcast messages to parent modules
        pass


def source(sources):
    def decorator(func):
        setattr(func, "_sources", sources)
        return func

    return decorator


def sink(sinks):
    def decorator(func):
        setattr(func, "_sinks", sinks)
        return func

    return decorator


class Robot(Module):
    def __init__(self):
        super().__init__()
