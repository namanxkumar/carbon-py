from collections import OrderedDict
from typing import Callable, List, Tuple


class FunctionNode:
    """Represents a function node in our execution graph."""

    def __init__(self, function: Callable):
        self.name = function.__name__
        self.function = function
        self.dependencies: List[FunctionNode] = []  # Functions that feed into this one
        self.dependents: List[FunctionNode] = []  # Functions that this one feeds into
        self.result = None  # Stores the last execution result
        self.node_level = None  # Level in the execution order

    def __repr__(self):
        representation = f"FunctionNode({self.name})"
        representation += f"\n <- {[dep.name for dep in self.dependencies]}"
        representation += f"\n -> {[dep.name for dep in self.dependents]}"
        representation += f"\n Execution Level: {self.node_level}"
        return representation


class FunctionFlow:
    def __init__(self):
        self.nodes: OrderedDict[int, FunctionNode] = OrderedDict()
        self.execution_order: List[Tuple] = []

    def _add_function(self, function: Callable):
        """Add a function to the flow."""
        # function_id = id(function)
        if function not in self.nodes:
            self.nodes[function] = FunctionNode(function)
        return self.nodes[function]

    def _connect(
        self,
        source: Callable | Tuple[Callable, ...],
        sink: Callable | Tuple[Callable, ...],
    ):
        """Connect source function to sink function."""
        source_nodes: List[FunctionNode] = []
        if isinstance(source, tuple):
            for function in source:
                source_nodes.append(self._add_function(function))
        elif callable(source):
            source_nodes.append(self._add_function(source))
        else:
            raise TypeError("Source must be a callable or a tuple of callables")

        sink_nodes: List[FunctionNode] = []
        if isinstance(sink, tuple):
            for function in sink:
                sink_nodes.append(self._add_function(function))
        elif callable(sink):
            sink_nodes.append(self._add_function(sink))
        else:
            raise TypeError("Target must be a callable or a tuple of callables")

        for source_node in source_nodes:
            for sink_node in sink_nodes:
                source_node.dependents.append(sink_node)
                sink_node.dependencies.append(source_node)

    def build_from_tuples(
        self,
        connections: List[
            Tuple[Callable | Tuple[Callable, ...], Callable | Tuple[Callable, ...]]
        ],
    ):
        """Build the function flow from a list of tuples."""
        for connection in connections:
            source, sink = connection
            self._connect(source, sink)

        # After building all connections, compute execution order
        self._compute_execution_order()

    def _compute_execution_order(self):
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
        with each layer as a tuple.
        """

        remaining_dependencies = {node_id: 0 for node_id in self.nodes.keys()}

        for node_id, node in self.nodes.items():
            remaining_dependencies[node_id] = len(node.dependencies)

        levels = []
        remaining_nodes = set(self.nodes.keys())

        while remaining_nodes:
            current_level = [
                node_id
                for node_id in remaining_nodes
                if remaining_dependencies[node_id] == 0
            ]

            if not current_level:
                raise ValueError("Cycle detected in function dependencies")

            levels.append(tuple(current_level))

            level_number = len(levels) - 1
            for node_id in current_level:
                self.nodes[node_id].node_level = level_number
                remaining_nodes.remove(node_id)

                for dependent in self.nodes[node_id].dependents:
                    remaining_dependencies[dependent.function] -= 1

        self.execution_order = levels


if __name__ == "__main__":
    # Example usage
    def func_a():
        return "A executed"

    def func_b():
        return "B executed"

    def func_c():
        return "C executed"

    def func_d():
        return "D executed"

    def func_e():
        return "E executed"

    def func_f():
        return "F executed"

    def func_g():
        return "G executed"

    flow = FunctionFlow()
    flow.build_from_tuples(
        [
            (func_a, func_b),
            (func_b, (func_c, func_d)),
            (func_d, (func_e, func_f)),
            ((func_c, func_e), func_g),
        ]
    )

    for value in flow.nodes.values():
        print(value)

    print("\nExecution Order:")
    for index, layer in enumerate(flow.execution_order):
        print(index + 1, list(flow.nodes[node_id].name for node_id in layer))
