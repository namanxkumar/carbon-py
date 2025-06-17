from typing import List, Set

from .connection import ConnectionType
from .module import DataMethod, Module


class ExecutionGraph:
    def __init__(self, root_module: Module):
        self.root_module = root_module

        methods = self.root_module.get_methods()

        self.layers: List[List["DataMethod"]] = self._build_layers(methods)

        self.process_groups: List[List["DataMethod"]] = self._build_processes(methods)

    def _build_layers(self, methods: Set["DataMethod"]) -> List[List["DataMethod"]]:
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

        layers: List[List["DataMethod"]] = []
        remaining_methods = methods.copy()

        while remaining_methods:
            current_level = [
                method
                for method in remaining_methods
                if remaining_dependencies[method] == 0
            ]

            if not current_level:
                raise ValueError("Cycle detected in function dependencies")

            layers.append(current_level)

            for method in current_level:
                remaining_methods.remove(method)
                for dependent_method in method.dependents.keys():
                    remaining_dependencies[dependent_method] -= 1

        return layers

    def _build_processes(self, methods: Set["DataMethod"]) -> List[List["DataMethod"]]:
        """Build processes based on connection type of the nodes."""

        groups = {method: {method} for method in methods}
        group_mapping = {method: method for method in methods}

        methods_beyond_first_layer = [
            method for layer in self.layers[1:] for method in layer
        ]

        for method in methods_beyond_first_layer:
            for dependency, type in method.dependencies.items():
                if type is ConnectionType.BLOCKING:
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

        return list(groups.values())
