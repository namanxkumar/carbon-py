from typing import Callable, Dict, List, Optional, Set, Tuple, Type, cast

import pyarrow as pa

from carbon.core.data import Data


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

        print(self.sinks[sink_index].schema for sink_index in self.sink_indices)
        self._input_queue: Dict[int, pa.Table] = {
            sink_index: pa.Table.from_pylist(
                [], schema=self.sinks[sink_index].get_schema()
            )
            for sink_index in self.sink_indices
        }  # Queues for each sink type

        self._remaining_for_execution: Set[int] = set(
            self.sink_indices
        )  # Indices of sinks that are not ready for execution

        self._input_queue_size: Dict[int, int] = {
            sink_index: cast(
                int, self.sink_configuration.get(sink_index, {}).get("queue_size", 1)
            )
            for sink_index in self.sink_indices
        }
        self._input_is_sticky: Dict[int, bool] = {
            sink_index: cast(
                bool, self.sink_configuration.get(sink_index, {}).get("sticky", False)
            )
            for sink_index in self.sink_indices
        }

        self.dependencies_to_merges: Dict[
            "DataMethod", Optional[int]
        ] = {}  # Sink index for each dependency (None if all sinks are received from the dependency (direct connection), int indicating sink index of the data if only a part of the sinks is received)
        self.dependencies_to_blocking: Dict[
            "DataMethod", bool
        ] = {}  # Whether the dependency is blocking or not

        self.dependents_to_splits: Dict[
            "DataMethod", Optional[int]
        ] = {}  # Source index for each dependent (None if all sources are sent to the dependent (direct connection), int indicating source index of the data if only a part of the sources is sent)
        self.dependents_to_blocking: Dict[
            "DataMethod", bool
        ] = {}  # Whether the dependent is blocking or not

        # TODO: Add a message cache and a message cache size for logging and transforms (historical transforms)

    def add_dependency(
        self,
        dependency: "DataMethod",
        merge_sink_index: Optional[int],
        blocking: bool,
    ) -> None:
        """Add a dependency to the method."""
        self.dependencies_to_merges[dependency] = merge_sink_index
        self.dependencies_to_blocking[dependency] = blocking

    def add_dependent(
        self,
        dependent: "DataMethod",
        split_source_index: Optional[int],
        blocking: bool,
    ) -> None:
        """Add a dependent to the method."""
        self.dependents_to_splits[dependent] = split_source_index
        self.dependents_to_blocking[dependent] = blocking

    @property
    def dependencies(self) -> Set["DataMethod"]:
        """Get the dependencies of the method."""
        return set(self.dependencies_to_merges.keys())

    @property
    def dependents(self) -> Set["DataMethod"]:
        """Get the dependents of the method."""
        return set(self.dependents_to_splits.keys())

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
            data.append(
                self.sinks[sink_index].from_arrow(
                    self._input_queue[sink_index].take([0])
                )
            )
            if not (
                self._input_is_sticky[sink_index]
                and self._input_queue[sink_index].num_rows == 1
            ):
                self._input_queue[sink_index] = self._input_queue[sink_index].slice(1)
                if self._input_queue[sink_index].num_rows == 0:
                    self._remaining_for_execution.add(sink_index)
        return data

    def receive_data(
        self, dependency: "DataMethod", data: Data | Tuple["Data", ...]
    ) -> None:
        """Receive data from a dependency and add it to the input queue."""
        merge_sink_index = self.dependencies_to_merges[dependency]
        if merge_sink_index is None:
            assert isinstance(data, tuple), "Expected data to be a tuple"

            # If the dependency is a direct connection, add all data to the input queue
            for sink_index, item in zip(self.sink_indices, data):
                if (
                    self._input_queue[sink_index].num_rows
                    >= self._input_queue_size[sink_index]
                ):
                    # If the queue is full, remove the oldest item
                    table_to_merge = self._input_queue[sink_index].slice(1)
                else:
                    table_to_merge = self._input_queue[sink_index]

                self._input_queue[sink_index] = pa.concat_tables(
                    [
                        table_to_merge,
                        item.to_arrow_table(),
                    ]
                )

                self._remaining_for_execution.discard(sink_index)
        else:
            assert isinstance(data, Data) or len(data) == 1, (
                "Expected data to be a singleton tuple or a Data instance"
            )

            if (
                self._input_queue[merge_sink_index].num_rows
                >= self._input_queue_size[merge_sink_index]
            ):
                # If the queue is full, remove the oldest item
                table_to_merge = self._input_queue[merge_sink_index].slice(1)
            else:
                table_to_merge = self._input_queue[merge_sink_index]

            self._input_queue[merge_sink_index] = pa.concat_tables(
                [
                    table_to_merge,
                    data[0].to_arrow_table()
                    if isinstance(data, tuple)
                    else data.to_arrow_table(),
                ]
            )

            self._remaining_for_execution.discard(merge_sink_index)

    def execute(self) -> Optional[Tuple["Data", ...]]:
        """Execute the method and return the output."""
        return self.__call__(*self.pop_data_for_execution())

    def __call__(self, *args, **kwargs) -> Optional[Tuple["Data", ...]]:
        output = self.method(*args, **kwargs)

        if output is None:
            return output

        # Ensure the output is same as the expected source type
        if not isinstance(output, (tuple, list)):
            output = (output,)
        elif isinstance(output, list):
            output = tuple(output)

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
