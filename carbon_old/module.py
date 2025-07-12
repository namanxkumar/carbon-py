from collections import OrderedDict
from typing import Callable, List, Set, Tuple, Type


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
        self._modules: OrderedDict[str, Module] = OrderedDict()
        self._consumers: OrderedDict[Callable, Tuple[Type, ...]] = OrderedDict()
        self._producers: OrderedDict[Callable, Tuple[Type, ...]] = OrderedDict()
        self._connections: Set[
            Tuple[
                Callable | Tuple[Callable, ...],
                Callable | Tuple[Callable, ...],
                Tuple[Type, ...],
            ]
        ] = set()
        self._blocked_connections: Set[
            Tuple[
                Callable | Tuple[Callable, ...],
                Callable | Tuple[Callable, ...],
                Tuple[Type, ...],
            ]
        ] = set()

        # Collect producers and consumers
        for attribute_name in dir(self):
            attribute = getattr(self, attribute_name)
            if callable(attribute) and hasattr(attribute, "_producers"):
                self._producers[attribute] = getattr(attribute, "_producers")
            if callable(attribute) and hasattr(attribute, "_consumers"):
                self._consumers[attribute] = getattr(attribute, "_consumers")

    def as_reference(self) -> ModuleReference:
        """Convert the module to a reference."""
        return ModuleReference(self)

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

    def get_producers(
        self, recursive: bool = False, memo: Set = None
    ) -> OrderedDict[Callable, Tuple[Type, ...]]:
        """Get producers of the module and its immediate children if recursive is False,
        or all producers in the tree if recursive is True."""
        if not recursive:
            return self._producers

        if memo is None:
            memo = set()
        producers = self._producers.copy()
        for module in self._modules.values():
            # Avoid infinite recursion
            if module in memo:
                continue
            memo.add(module)
            # Get producers from the module
            module_producers = module.get_producers(recursive=True, memo=memo)
            producers.update(module_producers)
        return producers

    def get_consumers(
        self, recursive: bool = False, memo: Set = None
    ) -> OrderedDict[Callable, Tuple[Type, ...]]:
        """Get consumers of the module and its immediate children if recursive is False,
        or all consumers in the tree if recursive is True."""
        if not recursive:
            return self._consumers

        if memo is None:
            memo = set()
        consumers = self._consumers.copy()
        for module in self._modules.values():
            # Avoid infinite recursion
            if module in memo:
                continue
            memo.add(module)
            # Get consumers from the module
            module_consumers = module.get_consumers(recursive=True, memo=memo)
            consumers.update(module_consumers)
        return consumers

    def get_connections(
        self, recursive: bool = True, memo: Set = None
    ) -> Set[
        Tuple[
            Callable | Tuple[Callable, ...],
            Callable | Tuple[Callable, ...],
            Tuple[Type, ...],
        ]
    ]:
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
        return connections - self._blocked_connections

    def __setattr__(self, name, value):
        if isinstance(value, Module):
            module_connections = value.get_connections(recursive=True)
            for connection in module_connections:
                try:
                    self._validate_connection(connection)
                except Exception as e:
                    raise ValueError(
                        f"Invalid connection {connection} in module {self.__class__.__name__}"
                    ) from e
            self._modules[name] = value
            super().__setattr__(name, value)
        else:
            super().__setattr__(name, value)

    def _get_producer_path_from_type(
        self, producer: "Module" | Callable, message_type: Type
    ) -> Callable:
        """Get the producer path from a producer and its type."""
        if type(message_type) is not tuple:
            message_type = (message_type,)
        if isinstance(producer, Module):
            producers = producer.get_producers()

            # Filter producers to match the type
            producers = {k: v for k, v in producers.items() if v == message_type}

            if not producers:
                raise ValueError(
                    f"No matching producers found for the given type {message_type} in module {producer.__class__.__name__}"
                )

            if len(producers) > 1:
                raise ValueError(
                    f"Multiple producers found for the given type {message_type} in module {producer.__class__.__name__}"
                )

            return list(producers.keys())[0]
        elif isinstance(producer, Callable):
            if not hasattr(producer, "_producers"):
                raise ValueError(
                    f"Provided function {producer.__name__} is not a producer"
                )
            producer_type = getattr(producer, "_producers")
            if producer_type != message_type:
                raise ValueError(
                    f"Provided producer type {message_type} does not match the producer function {producer.__name__}"
                )
            return producer
        else:
            raise TypeError("Provided producer is neither a callable nor a module")

    def _get_consumer_path_from_type(
        self, consumer: "Module" | Callable, message_type: Type
    ) -> Callable:
        """Get the consumer path from a consumer and its type."""
        if type(message_type) is not tuple:
            message_type = (message_type,)
        if isinstance(consumer, Module):
            consumers = consumer.get_consumers()

            # Filter consumers to match the type
            consumers = {k: v for k, v in consumers.items() if v == message_type}

            if not consumers:
                raise ValueError(
                    f"No matching consumers found for the given type {message_type} in module {consumer.__class__.__name__}"
                )

            if len(consumers) > 1:
                raise ValueError(
                    f"Multiple consumers found for the given type {message_type} in module {consumer.__class__.__name__}"
                )

            return list(consumers.keys())[0]
        elif isinstance(consumer, Callable):
            if not hasattr(consumer, "_consumers"):
                raise ValueError(
                    f"Provided function {consumer.__name__} is not a consumer"
                )
            consumer_type = getattr(consumer, "_consumers")
            if consumer_type != message_type:
                raise ValueError(
                    f"Provided consumer type {message_type} does not match the consumer function {consumer.__name__}"
                )
            return consumer
        else:
            raise TypeError("Provided consumer is neither a callable nor a module")

    def _validate_connection(
        self,
        connection: Tuple[
            Callable | Tuple[Callable, ...],
            Callable | Tuple[Callable, ...],
            Tuple[Type, ...],
        ],
    ):
        producer_path, consumer_path, _ = connection
        existing_connections = self.get_connections(recursive=True)
        for existing_connection in existing_connections:
            existing_producer, existing_consumer, _ = existing_connection
            if consumer_path == existing_consumer:
                raise ValueError(
                    f"Consumer {consumer_path} is already connected to another producer {existing_producer}"
                )
            if isinstance(existing_consumer, tuple):
                if consumer_path in existing_consumer:
                    raise ValueError(
                        f"Consumer {consumer_path} is already connected to another producer {existing_producer}"
                    )
            if existing_connection == connection:
                raise ValueError(
                    f"Connection already exists between {producer_path} and {consumer_path}"
                )

    def create_one_to_one_connection(
        self,
        producer: "Module" | Callable,
        consumer: "Module" | Callable,
        message_type: Type | Tuple[Type, ...],
    ) -> None:
        """Create a one-to-one connection between a producer and a consumer."""

        producer_path = self._get_producer_path_from_type(producer, message_type)

        consumer_path = self._get_consumer_path_from_type(consumer, message_type)

        if type(message_type) is not tuple:
            message_type = (message_type,)

        # Create the connection
        connection = (producer_path, consumer_path, message_type)
        self._validate_connection(connection)
        self._connections.add(connection)

    def create_one_to_many_connection(
        self,
        producer: "Module" | Callable,
        consumer: Tuple["Module", ...] | Tuple[Callable, ...],
        message_type: Tuple[Type, ...],
    ):
        """Create a one-to-many connection between a producer and multiple consumers."""
        if len(consumer) != len(message_type):
            raise ValueError("Number of consumers must match the number of types")

        producer_path = self._get_producer_path_from_type(producer, message_type)

        if isinstance(consumer, tuple):
            consumer_paths = []
            for s, t in zip(consumer, message_type):
                consumer_path = self._get_consumer_path_from_type(s, t)
                consumer_paths.append(consumer_path)
        else:
            raise TypeError("Provided consumers must be a tuple")

        if type(message_type) is not tuple:
            message_type = (message_type,)

        # Create the connection
        connection = (producer_path, tuple(consumer_paths), message_type)
        self._validate_connection(connection)
        self._connections.add(connection)

    def create_many_to_one_connection(
        self,
        producer: Tuple["Module", ...] | Tuple[Callable, ...],
        consumer: "Module" | Callable,
        message_type: Tuple[Type, ...],
    ) -> None:
        """Create a many-to-one connection between multiple producers and a consumer."""
        if len(producer) != len(message_type):
            raise ValueError("Number of producers must match the number of types")

        consumer_path = self._get_consumer_path_from_type(consumer, message_type)

        if isinstance(producer, tuple):
            producer_paths = []
            for s, t in zip(producer, message_type):
                producer_path = self._get_producer_path_from_type(s, t)
                producer_paths.append(producer_path)
        else:
            raise TypeError("Provided producers must be a tuple")

        if type(message_type) is not tuple:
            message_type = (message_type,)

        # Create the connection
        connection = (tuple(producer_paths), consumer_path, message_type)
        self._validate_connection(connection)
        self._connections.add(connection)

    def block_connection(
        self,
        producer: "Module"
        | Callable
        | Tuple[Callable, ...]
        | Tuple["Module", ...]
        | None,
        consumer: "Module"
        | Callable
        | Tuple[Callable, ...]
        | Tuple["Module", ...]
        | None,
        message_type: Type | Tuple[Type, ...],
    ):
        """Block a connection between a producer and a consumer."""
        # If connection is many to many
        if isinstance(producer, tuple) and isinstance(consumer, tuple):
            raise ValueError("Producer and Consumer cannot be both tuples.")

        if producer is None:
            producer_path = None
        elif isinstance(producer, tuple):
            producer_paths = []
            for s, t in zip(producer, message_type):
                producer_path = self._get_producer_path_from_type(s, t)
                producer_paths.append(producer_path)
            producer_path = tuple(producer_paths)
        else:
            producer_path = self._get_producer_path_from_type(producer, message_type)

        if consumer is None:
            consumer_path = None
        elif isinstance(consumer, tuple):
            consumer_paths = []
            for s, t in zip(consumer, message_type):
                consumer_path = self._get_consumer_path_from_type(s, t)
                consumer_paths.append(consumer_path)
            consumer_path = tuple(consumer_paths)
        else:
            consumer_path = self._get_consumer_path_from_type(consumer, message_type)

        if type(message_type) is not tuple:
            message_type = (message_type,)

        print(producer_path, consumer_path, message_type)

        # Remove the connection
        for existing_connection in self.get_connections(recursive=True):
            existing_producer, existing_consumer, existing_type = existing_connection
            removal = False
            if (
                producer_path is None
                and producer_path is None
                and message_type == existing_type
            ):
                removal = True

            if (
                producer_path is None
                and existing_consumer == consumer_path
                and message_type == existing_type
            ):
                removal = True

            if (
                consumer_path is None
                and existing_producer == producer_path
                and message_type == existing_type
            ):
                removal = True

            if (
                producer_path == existing_producer
                and consumer_path == existing_consumer
                and message_type == existing_type
            ):
                removal = True

            if removal:
                self._blocked_connections.add(existing_connection)

    def create_joint(self, *args, **kwargs):
        pass

    def localize_messages(self, func):
        # Localize messages to this module only
        pass

    def rebroadcast_messages(self, func):
        # Rebroadcast messages to parent modules
        pass


def producer(*producers):
    def decorator(func):
        setattr(func, "_producers", producers)
        return func

    return decorator


def consumer(*consumers):
    def decorator(func):
        setattr(func, "_consumers", consumers)
        return func

    return decorator
