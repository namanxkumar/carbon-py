import multiprocessing
from collections import OrderedDict
from typing import Callable, List, Tuple


class FunctionNode:
    """Represents a function node in our execution graph."""

    def __init__(self, function: Callable):
        self.name = function.__name__
        self.function = function
        self.consumer_type = None  # Type of the consumer
        self.producer_type = None
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

    def execute(self, inputs=None):
        """Execute the function with the given inputs."""
        # Execute the function and store the result
        self.result = self.function(inputs) if inputs else self.function()
        return self.result


def run_function(
    args: Tuple[FunctionNode, List],
):
    """Run a function with the given inputs."""
    return args[0].execute(args[1])


class FunctionFlow:
    def __init__(self, max_workers: int = 5):
        self.max_workers = max_workers
        self.nodes: OrderedDict[Callable, FunctionNode] = OrderedDict()
        self.execution_order: List[Tuple[Callable, ...]] = []

    def _add_function(self, function: Callable):
        """Add a function to the flow."""
        # function_id = id(function)
        if function not in self.nodes:
            self.nodes[function] = FunctionNode(function)
        return self.nodes[function]

    def _connect(
        self,
        producer: Callable | Tuple[Callable, ...],
        consumer: Callable | Tuple[Callable, ...],
    ):
        """Connect producer function to consumer function."""
        producer_nodes: List[FunctionNode] = []
        if isinstance(producer, tuple):
            for function in producer:
                producer_nodes.append(self._add_function(function))
        elif callable(producer):
            producer_nodes.append(self._add_function(producer))
        else:
            raise TypeError("Producer must be a callable or a tuple of callables")

        consumer_nodes: List[FunctionNode] = []
        if isinstance(consumer, tuple):
            for function in consumer:
                consumer_nodes.append(self._add_function(function))
        elif callable(consumer):
            consumer_nodes.append(self._add_function(consumer))
        else:
            raise TypeError("Target must be a callable or a tuple of callables")

        for producer_node in producer_nodes:
            for consumer_node in consumer_nodes:
                producer_node.dependents.append(consumer_node)
                consumer_node.dependencies.append(producer_node)

    def build_from_tuples(
        self,
        connections: List[
            Tuple[Callable | Tuple[Callable, ...], Callable | Tuple[Callable, ...]]
        ],
    ):
        """Build the function flow from a list of tuples."""
        for connection in connections:
            producer, consumer, _ = connection
            self._connect(producer, consumer)

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

    def _execute_layer(self, layer: Tuple[Callable, ...], inputs: List = None):
        """Execute a layer of functions parallelly when possible."""
        if inputs is None:
            inputs = [None] * len(layer)

        nodes = [self.nodes[node_id] for node_id in layer]
        # Create a pool of workers and pass nth input to nth function
        with multiprocessing.Pool(processes=self.max_workers) as pool:
            results = pool.map(run_function, list(zip(nodes, inputs)))

        return results

    def execute(self):
        """Execute the function flow in the computed order."""
        next_inputs = None
        for layer in self.execution_order:
            print(f"\nExecuting layer: {layer}")
            next_inputs = self._execute_layer(layer, next_inputs)
            print(f"Results: {next_inputs}")
        # Return the results of the last layer
        return next_inputs


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
