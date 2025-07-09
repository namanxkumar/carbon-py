from dataclasses import dataclass
from typing import (
    Callable,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
    cast,
)

from carbon.core.utilities import ensure_tuple_format
from carbon.data import Data, DataQueue, QueueItem


@dataclass
class DependencyConfiguration:
    """
    Configuration for a dependency.
    - merge_sink_index: Index of the sink to merge data into (None if all sinks are received).
    - sync: Whether the dependency is sync or not.
    """

    merge_sink_index: Optional[int]
    sync: bool
    active: bool = True  # Whether the dependency is active or not


@dataclass
class DependentConfiguration:
    """
    Configuration for a dependent.
    - split_source_index: Index of the source to split data from (None if all sources are sent).
    - sync: Whether the dependent is sync or not.
    """

    split_source_index: Optional[int]
    sync: bool
    active: bool = True  # Whether the dependent is active or not


@dataclass
class SinkConfiguration:
    """
    Configuration for a sink.
    - queue_size: Size of the queue for the sink.
    - sticky: Whether the sink is sticky or not.
    """

    queue_size: int = 1
    sticky: bool = False


class DataMethod:
    def __init__(self, method: Callable):
        self.method = method

        self.sources = cast(
            Tuple[Type["Data"], ...], getattr(method, "_sources", tuple())
        )
        self.source_indices: Tuple[int, ...] = tuple(range(len(self.sources)))

        self.sinks = cast(Tuple[Type["Data"], ...], getattr(method, "_sinks", tuple()))
        self.sink_indices: Tuple[int, ...] = tuple(range(len(self.sinks)))

        sink_configuration = cast(
            Dict[int, SinkConfiguration],
            getattr(method, "_sink_configuration", {}),
        )

        self.input_queue: Dict[int, DataQueue] = {
            sink_index: DataQueue(
                self.sinks[sink_index],
                size=sink_configuration[sink_index].queue_size,
                sticky=sink_configuration[sink_index].sticky,
            )
            for sink_index in self.sink_indices
        }

        self.remaining_for_execution: Set[int] = set(
            self.sink_indices
        )  # Indices of sinks that are not ready for execution

        self._dependency_to_configuration: Dict[
            "DataMethod", DependencyConfiguration
        ] = {}  # Dependencies of this method

        self._dependent_to_configuration: Dict[
            "DataMethod", DependentConfiguration
        ] = {}  # Dependents of this method

        # TODO: Add a message cache and a message cache size for logging and transforms (historical transforms)

    @property
    def name(self) -> str:
        """Get the name of the method."""
        return self.method.__name__

    @property
    def active(self) -> bool:
        """
        Check if the method is active.
        A method is considered active if it doesn't require any data to be executed
        or if it has at least one active dependency or dependent.
        """
        if self.sinks and not self.active_dependencies:
            return False  # If there are no active dependencies but there are sinks, the method is not active
        return True

    def add_dependency(
        self,
        dependency: "DataMethod",
        dependency_configuration: DependencyConfiguration,
    ) -> None:
        """Add a dependency to the method."""
        self._dependency_to_configuration[dependency] = dependency_configuration

    def add_dependent(
        self,
        dependent: "DataMethod",
        dependency_configuration: DependentConfiguration,
    ) -> None:
        """Add a dependent to the method."""
        self._dependent_to_configuration[dependent] = dependency_configuration

    def block_dependency(self, dependency: "DataMethod") -> None:
        """Block a dependency."""
        self._dependency_to_configuration[dependency].active = False

    def block_dependent(self, dependent: "DataMethod") -> None:
        """Block a dependent."""
        self._dependent_to_configuration[dependent].active = False

    @property
    def dependencies(self) -> Set["DataMethod"]:
        """Get the dependencies of the method."""
        return set(self._dependency_to_configuration.keys())

    @property
    def active_dependencies(self) -> Set["DataMethod"]:
        """Get the dependencies of the method."""
        return set(
            dependency
            for dependency, configuration in self._dependency_to_configuration.items()
            if configuration.active
        )

    def active_dependency_generator(self):
        """Generator for active dependencies."""
        for dependency, configuration in self._dependency_to_configuration.items():
            if configuration.active:
                yield dependency

    @property
    def dependents(self) -> Set["DataMethod"]:
        """Get the dependents of the method."""
        return set(self._dependent_to_configuration.keys())

    @property
    def active_dependents(self) -> Set["DataMethod"]:
        """Get the dependents of the method."""
        return set(
            dependent
            for dependent, configuration in self._dependent_to_configuration.items()
            if configuration.active
        )

    def active_dependent_generator(self):
        """Generator for active dependents."""
        for dependent, configuration in self._dependent_to_configuration.items():
            if configuration.active:
                yield dependent

    def get_dependent_configuration(
        self, dependent: "DataMethod"
    ) -> DependentConfiguration:
        """Get the configuration for a dependent."""
        return self._dependent_to_configuration[dependent]

    def get_dependency_configuration(
        self, dependency: "DataMethod"
    ) -> DependencyConfiguration:
        """Get the configuration for a dependency."""
        return self._dependency_to_configuration[dependency]

    @property
    def is_ready_for_execution(self) -> bool:
        """Check if the method is ready for execution."""
        return not self.remaining_for_execution

    def pop_data_for_execution(self) -> List["Data"]:
        """Pop data from the input queue for execution."""
        assert self.is_ready_for_execution, (
            "Method is not ready for execution. Ensure all required data is available."
        )

        data = []
        for sink_index in self.sink_indices:
            data.append(self.input_queue[sink_index].pop())  # Memory is allocated

            if self.input_queue[sink_index].is_empty():
                self.remaining_for_execution.add(sink_index)
        return data

    def receive_data(
        self,
        dependency: "DataMethod",
        data: Union["QueueItem", Tuple["QueueItem", ...]],
    ) -> None:
        configuration = self.get_dependency_configuration(dependency)

        if configuration.merge_sink_index is None:
            assert isinstance(data, tuple), "Expected data to be a tuple"

            # If the dependency is a direct connection, add all data to the input queue
            for sink_index, item in zip(self.sink_indices, data):
                self.input_queue[sink_index].append(item, sync=configuration.sync)
                self.remaining_for_execution.discard(sink_index)
        else:
            assert not isinstance(data, (list, tuple)) or len(data) == 1, (
                "Expected data to be a singleton tuple or a Data instance"
            )

            self.input_queue[configuration.merge_sink_index].append(
                data[0] if isinstance(data, tuple) else data, sync=configuration.sync
            )
            self.remaining_for_execution.discard(configuration.merge_sink_index)

    def execute(self) -> Optional[Tuple[QueueItem, ...]]:
        """Execute the method and return the output."""
        output = self.__call__(*self.pop_data_for_execution())
        return (
            tuple(item.export_to_queue_format() for item in output)
            if output is not None
            else None
        )

    def __call__(self, *args, **kwargs) -> Optional[Tuple["Data", ...]]:
        output = self.method(*args, **kwargs)

        if output is None:
            return output

        output = ensure_tuple_format(output)

        assert len(output) == len(self.sources), (
            f"Method {self.name} must return {len(self.sources)} items, but got {len(output)}."
        )

        return output

    def __eq__(self, value):
        if isinstance(value, DataMethod):
            return self.method == value.method
        return self.method == value

    def __repr__(self):
        return f"{self.name}"

    def __hash__(self):
        return hash(self.method)
