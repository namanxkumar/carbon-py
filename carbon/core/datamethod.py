from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
    cast,
    overload,
)

if TYPE_CHECKING:
    import pyarrow as pa

    from carbon.core.data import Data

from typing import NamedTuple

from carbon.core.data import DataQueue
from carbon.core.utilities import ensure_tuple_format


class DependencyConfiguration(NamedTuple):
    """
    Configuration for a dependency.
    - merge_sink_index: Index of the sink to merge data into (None if all sinks are received).
    - blocking: Whether the dependency is blocking or not.
    """

    merge_sink_index: Optional[int]
    blocking: bool


class DependentConfiguration(NamedTuple):
    """
    Configuration for a dependent.
    - split_source_index: Index of the source to split data from (None if all sources are sent).
    - blocking: Whether the dependent is blocking or not.
    """

    split_source_index: Optional[int]
    blocking: bool


class DataMethod:
    def __init__(self, method: Callable):
        self.method = method
        self.name: str = method.__name__

        self.sources = cast(
            Tuple[Type["Data"], ...], getattr(method, "_sources", tuple())
        )
        self.source_indices: Tuple[int, ...] = tuple(range(len(self.sources)))

        self.sinks = cast(Tuple[Type["Data"], ...], getattr(method, "_sinks", tuple()))
        self.sink_indices: Tuple[int, ...] = tuple(range(len(self.sinks)))

        self.source_configuration = cast(
            Dict[int, Dict[str, int | bool]],
            getattr(method, "_source_configuration", {}),
        )
        self.sink_configuration = cast(
            Dict[int, Dict[str, int | bool]],
            getattr(method, "_sink_configuration", {}),
        )

        self._input_queue: Dict[int, DataQueue] = {
            sink_index: DataQueue(
                self.sinks[sink_index],
                size=cast(
                    int,
                    self.sink_configuration.get(sink_index, {}).get("queue_size", 1),
                ),
                sticky=cast(
                    bool,
                    self.sink_configuration.get(sink_index, {}).get("sticky", False),
                ),
            )
            for sink_index in self.sink_indices
        }

        self._remaining_for_execution: Set[int] = set(
            self.sink_indices
        )  # Indices of sinks that are not ready for execution

        self.dependency_to_configuration: Dict[
            "DataMethod", DependencyConfiguration
        ] = {}  # Dependencies of this method

        self.dependent_to_configuration: Dict[
            "DataMethod", DependentConfiguration
        ] = {}  # Dependents of this method

        # TODO: Add a message cache and a message cache size for logging and transforms (historical transforms)

    def add_dependency(
        self,
        dependency: "DataMethod",
        merge_sink_index: Optional[int],
        blocking: bool,
    ) -> None:
        """Add a dependency to the method."""
        self.dependency_to_configuration[dependency] = DependencyConfiguration(
            merge_sink_index=merge_sink_index,
            blocking=blocking,
        )

    def add_dependent(
        self,
        dependent: "DataMethod",
        split_source_index: Optional[int],
        blocking: bool,
    ) -> None:
        """Add a dependent to the method."""
        self.dependent_to_configuration[dependent] = DependentConfiguration(
            split_source_index=split_source_index,
            blocking=blocking,
        )

    @property
    def dependencies(self) -> Set["DataMethod"]:
        """Get the dependencies of the method."""
        return set(self.dependency_to_configuration.keys())

    @property
    def dependents(self) -> Set["DataMethod"]:
        """Get the dependents of the method."""
        return set(self.dependent_to_configuration.keys())

    @property
    def is_ready_for_execution(self) -> bool:
        """Check if the method is ready for execution."""
        return not self._remaining_for_execution

    def pop_data_for_execution(self) -> List["Data"]:
        """Pop data from the input queue for execution."""
        assert self.is_ready_for_execution, (
            "Method is not ready for execution. Ensure all required data is available."
        )

        data = []
        for sink_index in self.sink_indices:
            data.append(self._input_queue[sink_index].pop())  # Memory is allocated

            if self._input_queue[sink_index].is_empty():
                self._remaining_for_execution.add(sink_index)
        return data

    @overload
    def receive_data(
        self, dependency: "DataMethod", data: Union["Data", Tuple["Data", ...]]
    ) -> None:
        """Receive Data from a dependency and add it to the input queue."""
        ...

    @overload
    def receive_data(
        self, dependency: "DataMethod", data: "pa.Table" | Tuple["pa.Table", ...]
    ) -> None:
        """Receive Arrow Table from a dependency and add it to the input queue (Zero Copy)."""
        ...

    def receive_data(self, dependency, data):
        merge_sink_index = self.dependency_to_configuration[dependency].merge_sink_index
        if merge_sink_index is None:
            assert isinstance(data, tuple), "Expected data to be a tuple"

            # If the dependency is a direct connection, add all data to the input queue
            for sink_index, item in zip(self.sink_indices, data):
                self._input_queue[sink_index].append(
                    item
                )  # No memory allocation (ZERO COPY)
                self._remaining_for_execution.discard(sink_index)
        else:
            assert not isinstance(data, (list, tuple)) or len(data) == 1, (
                "Expected data to be a singleton tuple or a Data instance"
            )

            self._input_queue[merge_sink_index].append(
                data[0] if isinstance(data, tuple) else data
            )
            self._remaining_for_execution.discard(merge_sink_index)

    def execute(self) -> Optional[Tuple["Data", ...]]:
        """Execute the method and return the output."""
        return self.__call__(*self.pop_data_for_execution())

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
