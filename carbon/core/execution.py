from threading import Event, Thread
from typing import Dict, List, Set

from carbon.core.datamethod import DataMethod
from carbon.core.module import Module


class ExecutionGraph:
    def __init__(self, root_module: "Module"):
        methods = root_module.get_methods()

        self.processes, self.process_mapping = self._build_processes(methods)
        (
            self.layers,
            self.layer_mapping,
            self.process_layer_mapping,
            self.in_process_layer_mapping,
        ) = self._build_layers(methods)
        self.process_readiness = {
            process_index: any(
                method.is_ready_for_execution
                for method in self.process_layer_mapping[process_index][0]
            )
            for process_index in self.processes.keys()
        }
        self.process_exited_manually = {
            process_index: False for process_index in self.processes.keys()
        }  # Track if a process exited manually
        self.threads: Dict[int, Thread] = {}
        self.reactive_threads = True  # Flag to indicate if reactive threads are enabled
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
            method: len(method.active_dependencies) for method in methods
        }

        layers: List[Set["DataMethod"]] = []
        layer_mapping: Dict["DataMethod", int] = {}
        process_layer_mapping: Dict[int, List[Set["DataMethod"]]] = {
            index: [] for index in self.processes.keys()
        }  # Process index to layers mapping
        process_previous_level_index: Dict[int, int] = {
            index: -1 for index in self.processes.keys()
        }  # Starting level for each process
        in_process_layer_mapping: Dict["DataMethod", int] = {}  # Level within process

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
                if process_previous_level_index[process_index] < level_index:
                    # If the process layer mapping does not have this level, add it
                    process_layer_mapping[process_index].append({method})
                    process_previous_level_index[process_index] = level_index
                    in_process_layer_mapping[method] = (
                        len(process_layer_mapping[process_index]) - 1
                    )
                else:
                    # If the process layer mapping has this level, add the method to it
                    current_index = len(process_layer_mapping[process_index]) - 1
                    process_layer_mapping[process_index][current_index].add(method)
                    in_process_layer_mapping[method] = current_index
                remaining_methods.remove(method)
                for dependent_method in method.active_dependent_generator():
                    remaining_dependencies[dependent_method] -= 1

            level_index += 1

        return layers, layer_mapping, process_layer_mapping, in_process_layer_mapping

    def _build_processes(self, methods: Set["DataMethod"]):
        """Build processes based on connection type of the nodes."""

        groups = {index: {method} for index, method in enumerate(methods)}
        group_mapping = {method: index for index, method in enumerate(methods)}

        for method in methods:
            for dependency in method.active_dependency_generator():
                if method.get_dependency_configuration(dependency).sync:
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
        self.process_exited_manually[process_index] = True

        while not self.stop_event.is_set():
            # Check if there are any methods to execute in the current layer
            if not process_layers:
                break

            for layer_index, layer in enumerate(process_layers):
                remaining_methods = list(layer)
                remaining_method_index = 0

                while remaining_methods and not self.stop_event.is_set():
                    method = remaining_methods.pop(0)

                    if not method.is_ready_for_execution:
                        # If there are no data to process, skip this method and add it back to the remaining methods
                        remaining_methods.append(method)
                        remaining_method_index += 1
                        if (
                            remaining_method_index >= len(remaining_methods)
                            and layer_index == 0
                            and self.reactive_threads
                        ):
                            self.process_exited_manually[process_index] = False
                            return
                        continue

                    # If there are data to process, execute the method
                    method_output = method.execute()

                    # Add the output to the input queue of the dependent methods
                    for dependent_method in method.active_dependent_generator():
                        assert method_output is not None, (
                            f"Method {method.name} returned None, but it should return a valid output for its dependents."
                        )
                        split_producer_index = method.get_dependent_configuration(
                            dependent_method
                        ).split_producer_index
                        dependent_method.receive_data(
                            method,
                            method_output
                            if split_producer_index is None
                            else method_output[split_producer_index],
                        )

                        # If the dependent method is in the first layer and its process is not ready, mark it as ready
                        if (
                            self.in_process_layer_mapping[dependent_method] == 0
                            and not self.process_readiness[
                                self.process_mapping[dependent_method]
                            ]
                        ):
                            self.process_readiness[
                                self.process_mapping[dependent_method]
                            ] = True

                    if method.is_ready_for_execution and method.consumers:
                        # If the method is ready for execution and has consumers, it can be executed again
                        remaining_methods.append(method)
            # break

    def _monitor_processes(self):
        """Monitor the processes and execute them in the correct order."""
        while not self.stop_event.is_set():
            # Check if there are any processes that are ready to execute
            ready_processes = [
                process_index
                for process_index, is_ready in self.process_readiness.items()
                if is_ready
            ]

            if not ready_processes:
                break

            for process_index in ready_processes:
                if (
                    self.threads[process_index].is_alive()
                    or self.process_exited_manually[process_index]
                ):
                    # If the thread is still alive, skip to the next process
                    continue
                self.threads[process_index] = Thread(
                    target=self._execute_process_group, args=(process_index,)
                )
                self.threads[process_index].start()
                self.process_readiness[process_index] = False  # Reset readiness

    def execute(self, graceful_timeout: float = 5.0):
        """Execute the methods in the execution order defined by the layers. Gracefully handle Ctrl+C."""
        try:
            for process in self.processes:
                # Create a new thread for each process group
                self.threads[process] = Thread(
                    target=self._execute_process_group, args=(process,)
                )
                if self.process_readiness[process] or not self.reactive_threads:
                    # If the process is ready or reactive threads is off, start the thread immediately
                    self.threads[process].start()

            # Start monitoring the processes
            if self.reactive_threads:
                # If reactive threads are enabled, start a monitoring thread
                monitor_thread = Thread(target=self._monitor_processes)
                monitor_thread.start()
                # Wait for all processes to finish
                monitor_thread.join()
            else:
                # If reactive threads are disabled, wait for all threads to finish
                for thread in self.threads.values():
                    thread.join()
        except KeyboardInterrupt:
            print("\nReceived Ctrl+C, attempting graceful shutdown...")
            self.stop_event.set()
            for thread in self.threads.values():
                if thread.is_alive():
                    print(f"Waiting for thread {thread.name} to finish...")
                    thread.join(timeout=graceful_timeout)
            print("\nAll threads terminated (or timed out). Exiting.")
