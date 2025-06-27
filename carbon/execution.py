from threading import Thread
from typing import Dict, List, Set

from .core.datamethod import DataMethod
from .core.module import Module, ModuleReference


class ExecutionGraph(Module):
    def __init__(self, root_module: ModuleReference, max_threads: int = 1):
        super().__init__()

        methods = root_module.module.get_methods()

        self.processes, self.process_mapping = self._build_processes(methods)
        self.layers, self.layer_mapping, self.process_layer_mapping = (
            self._build_layers(methods)
        )

    def _build_layers(self, methods: Set["DataMethod"]):
        """Compute a topological ordering of the functions.
        This does not handle cycles. Ordering is done as follows:
        Given a graph as follows:
        A -> B -> C --------v
             | -> D -> E -> G
                  | -> F

        The execution flow is then data-based, and divided into layers, based on dependencies.
        The execution order will be:
        1. A
        2. B
        3. C, D
        4. E, F
        5. G

        The execution order is stored in the `execution_order` attribute,
        with each layer as a list of nodes.
        """

        remaining_dependencies = {
            method: len(method.dependencies) for method in methods
        }

        layers: List[Set["DataMethod"]] = []
        layer_mapping: Dict["DataMethod", int] = {}
        process_layer_mapping: Dict[int, List[Set["DataMethod"]]] = {
            index: [] for index in self.processes.keys()
        }

        remaining_methods = methods.copy()

        level_index = 0
        while remaining_methods:
            current_level = [
                method
                for method in remaining_methods
                if remaining_dependencies[method] == 0
            ]

            if not current_level:
                raise ValueError("Cycle detected in function dependencies")

            layers.append(set(current_level))

            for method in current_level:
                layer_mapping[method] = level_index

                process_index = self.process_mapping[method]
                if len(process_layer_mapping[process_index]) <= level_index:
                    process_layer_mapping[process_index].append({method})
                else:
                    process_layer_mapping[process_index][level_index].add(method)

                remaining_methods.remove(method)
                for dependent_method in method.dependents.keys():
                    remaining_dependencies[dependent_method] -= 1

            level_index += 1

        return layers, layer_mapping, process_layer_mapping

    def _build_processes(self, methods: Set["DataMethod"]):
        """Build processes based on connection type of the nodes."""

        groups = {index: {method} for index, method in enumerate(methods)}
        group_mapping = {method: index for index, method in enumerate(methods)}

        for method in methods:
            for dependency, connection in method.dependencies.items():
                if connection.blocking:
                    node_group = group_mapping[method]
                    dependency_group = group_mapping[dependency]
                    # Check if the dependency is in a different group
                    if node_group != dependency_group:
                        # If it is, we need to merge groups
                        merged_group = groups[dependency_group].union(
                            groups[node_group]
                        )

                        for member in merged_group:
                            group_mapping[member] = dependency_group

                        groups[dependency_group] = merged_group
                        del groups[node_group]

        return groups, group_mapping

    def _execute_process_group(self, process_index: int):
        """Execute the methods in the given process group."""
        process_layers = self.process_layer_mapping[process_index]

        while True:
            # Check if there are any methods to execute in the current layer
            if not process_layers:
                break

            for layer_index, layer in enumerate(process_layers):
                remaining_methods_and_remaining_data = {
                    method: list(method.sinks) for method in layer
                }

                while remaining_methods_and_remaining_data:
                    # print([method.input_queue for method in layer])
                    method, remaining_data = (
                        remaining_methods_and_remaining_data.popitem()
                    )  # Relies on dict order
                    new_remaining_data = []
                    for data_type in remaining_data:
                        if not method.input_queue[data_type]:
                            new_remaining_data.append(data_type)

                    if new_remaining_data:
                        # If there are no data to process, skip this method and add it back to the remaining methods
                        remaining_methods_and_remaining_data[method] = (
                            new_remaining_data
                        )
                        continue

                    # If there are data to process, execute the method
                    method_output = method(
                        *[
                            method.input_queue[data_type].pop(0)
                            if (
                                not method.input_is_sticky[data_type]
                                or len(method.input_queue[data_type]) > 1
                            )
                            else method.input_queue[data_type][0]
                            for data_type in method.sinks
                        ]
                    )

                    # Add the output to the input queue of the dependent methods
                    for (
                        dependent_method,
                        dependent_connection,
                    ) in method.dependents.items():
                        split_index = method.dependent_splits.get(
                            dependent_method, None
                        )
                        if split_index is not None:
                            queue_key = method.sources[split_index]
                            queue_addition = method_output[split_index]
                        else:
                            queue_key = method.sources[0]
                            queue_addition = method_output

                        # Check queue size
                        if (
                            len(dependent_method.input_queue[queue_key])
                            >= dependent_method.input_queue_size[queue_key]
                        ):
                            # If the queue is full, remove the oldest item and add the new one
                            dependent_method.input_queue[queue_key].pop(0)

                        dependent_method.input_queue[queue_key].append(queue_addition)

    def execute(self):
        """Execute the methods in the execution order defined by the layers."""
        threads: Dict[int, Thread] = {}

        # Create a new process for each process group since shared memory is required for the current implementation
        # TODO: Replace this with zero copy pyarrow shared memory between processes
        for process in self.processes:
            # Create a new thread for each process group
            threads[process] = Thread(
                target=self._execute_process_group, args=(process,)
            )
            threads[process].start()

        # Wait for all processes to finish
        for thread in threads.values():
            thread.join()
