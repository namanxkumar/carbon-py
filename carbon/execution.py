from typing import Callable, Dict, List, Sequence

from .connection import Connection, ConnectionType
from .module import Module


class ExecutionNode:
    """Represents a node in the execution graph."""

    def __init__(self, function: Callable):
        self.name = function.__name__
        self.function = function
        self.dependencies: Dict[
            ExecutionNode, ConnectionType
        ] = {}  # Nodes that feed into this one
        self.dependents: Dict[
            ExecutionNode, ConnectionType
        ] = {}  # Nodes that this one feeds into
        self.result = None  # Stores the last execution result

    def __repr__(self):
        representation = f"ExecutionNode({self.name})"
        representation += f"\n <- {[dep.name for dep in self.dependencies.keys()]}"
        representation += f"\n -> {[dep.name for dep in self.dependents.keys()]}"
        return representation

    def execute(self, inputs=None):
        """Execute the module with the given inputs."""
        # Execute the module and store the result
        self.result = self.function(inputs) if inputs else self.function()
        return self.result

    def __eq__(self, value):
        """Check equality based on function name."""
        return isinstance(value, ExecutionNode) and self.function == value.function

    def __hash__(self):
        """Hash based on function name."""
        return hash(self.function)


class ExecutionGraph:
    def __init__(self, root_module: Module):
        self.root_module = root_module

        self.nodes: Dict[Callable, ExecutionNode] = {}

        for connection in self.root_module.get_connections():
            self._build_connection(connection)

        self.layers: List[List[Callable]] = self._build_layers()

        self.process_groups: List[List[Callable]] = self._build_processes()

    def _create_node(self, function: Callable) -> ExecutionNode:
        """Add a node to the execution graph."""
        if function not in self.nodes:
            self.nodes[function] = ExecutionNode(function)
        return self.nodes[function]

    def _build_connection(self, connection: Connection):
        """Build a connection between source and sink nodes."""
        source_nodes: List[ExecutionNode] = []

        if isinstance(connection.source, Sequence):
            for source_index, source in enumerate(connection.source):
                source_method = source._sources.get(connection.data[source_index])
                source_nodes.append(self._create_node(source_method))
        else:
            source_method = connection.source._sources.get(connection.data)
            source_nodes.append(self._create_node(source_method))

        sink_nodes: List[ExecutionNode] = []

        if isinstance(connection.sink, Sequence):
            for sink_index, sink in enumerate(connection.sink):
                sink_method = sink._sinks.get(connection.data[sink_index])
                sink_nodes.append(self._create_node(sink_method))
        else:
            sink_method = connection.sink._sinks.get(connection.data)
            sink_nodes.append(self._create_node(sink_method))

        for source_node in source_nodes:
            for sink_node in sink_nodes:
                source_node.dependents[sink_node] = connection.type
                sink_node.dependencies[source_node] = connection.type

    def _build_layers(self):
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

        remaining_dependencies = {node_id: 0 for node_id in self.nodes.keys()}

        for node_id, node in self.nodes.items():
            remaining_dependencies[node_id] = len(node.dependencies)

        layers = []
        remaining_nodes = set(self.nodes.keys())

        while remaining_nodes:
            current_level = [
                node_id
                for node_id in remaining_nodes
                if remaining_dependencies[node_id] == 0
            ]

            if not current_level:
                raise ValueError("Cycle detected in function dependencies")

            layers.append(current_level)

            for node_id in current_level:
                remaining_nodes.remove(node_id)
                for dependent in self.nodes[node_id].dependents.keys():
                    remaining_dependencies[dependent.function] -= 1

        return layers

    def _build_processes(self):
        """Build processes based on connection type of the nodes."""

        groups = {node_id: {node_id} for node_id in self.nodes.keys()}
        group_mapping = {node_id: node_id for node_id in self.nodes.keys()}

        nodes_beyond_first_layer = [
            node_id for layer in self.layers[1:] for node_id in layer
        ]

        for node_id in nodes_beyond_first_layer:
            for dependency, type in self.nodes[node_id].dependencies.items():
                if type is ConnectionType.BLOCKING:
                    node_group = group_mapping[node_id]
                    dependency_group = group_mapping[dependency.function]
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
