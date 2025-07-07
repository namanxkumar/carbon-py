from threading import Event, Thread
from typing import Dict, List, Set

from carbon.core.datamethod import DataMethod
from carbon.core.module import Module


class ExecutionGraph:
    def __init__(self, root_module: "Module"):
        methods = root_module.get_methods()

        self.processes, self.process_mapping = self._build_processes(methods)
        self.layers, self.layer_mapping, self.process_layer_mapping = (
            self._build_layers(methods)
        )
        self.threads: Dict[int, Thread] = {}
        self.stop_event = Event()  # Event to signal threads to stop

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
                for dependent_method in method.dependents:
                    remaining_dependencies[dependent_method] -= 1

            level_index += 1

        return layers, layer_mapping, process_layer_mapping

    def _build_processes(self, methods: Set["DataMethod"]):
        """Build processes based on connection type of the nodes."""

        groups = {index: {method} for index, method in enumerate(methods)}
        group_mapping = {method: index for index, method in enumerate(methods)}

        for method in methods:
            for dependency, configuration in method.dependency_to_configuration.items():
                if configuration.blocking:
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

        while not self.stop_event.is_set():
            # Check if there are any methods to execute in the current layer
            if not process_layers:
                break

            for layer_index, layer in enumerate(process_layers):
                remaining_methods = list(layer)

                while remaining_methods and not self.stop_event.is_set():
                    method = remaining_methods.pop(0)

                    if not method.is_ready_for_execution:
                        # If there are no data to process, skip this method and add it back to the remaining methods
                        remaining_methods.append(method)
                        continue

                    # If there are data to process, execute the method
                    method_output = method.execute()

                    # Add the output to the input queue of the dependent methods
                    for (
                        dependent_method,
                        configuration,
                    ) in method.dependent_to_configuration.items():
                        assert method_output is not None, (
                            f"Method {method.name} returned None, but it should return a valid output for its dependents."
                        )
                        dependent_method.receive_data(
                            method,
                            method_output
                            if configuration.split_source_index is None
                            else method_output[configuration.split_source_index],
                        )

    def execute(self, graceful_timeout: float = 5.0):
        """Execute the methods in the execution order defined by the layers. Gracefully handle Ctrl+C."""
        try:
            for process in self.processes:
                # Create a new thread for each process group
                self.threads[process] = Thread(
                    target=self._execute_process_group, args=(process,)
                )
                self.threads[process].start()

            # Wait for all processes to finish
            for thread in self.threads.values():
                thread.join()
        except KeyboardInterrupt:
            print("\nReceived Ctrl+C, attempting graceful shutdown...")
            self.stop_event.set()
            for thread in self.threads.values():
                thread.join(timeout=graceful_timeout)
            print("\nAll threads terminated (or timed out). Exiting.")
