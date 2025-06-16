import asyncio
import queue
import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional


class ConnectionType(Enum):
    SYNC = "sync"
    ASYNC = "async"


@dataclass
class Connection:
    from_node: str
    to_node: str
    conn_type: ConnectionType


@dataclass
class Node:
    name: str
    func: Callable
    inputs: List[str] = None
    outputs: List[str] = None

    def __post_init__(self):
        if self.inputs is None:
            self.inputs = []
        if self.outputs is None:
            self.outputs = []


class SharedState:
    """Thread-safe shared state for async communication"""

    def __init__(self):
        self._data = {}
        self._locks = {}

    def set(self, key: str, value: Any):
        if key not in self._locks:
            self._locks[key] = threading.Lock()

        with self._locks[key]:
            self._data[key] = value

    def get(self, key: str, default=None):
        if key not in self._locks:
            return default

        with self._locks[key]:
            return self._data.get(key, default)


class ExecutionProcess:
    """Represents a single execution process containing sync-connected nodes"""

    def __init__(self, process_id: int, nodes: List[Node], shared_state: SharedState):
        self.process_id = process_id
        self.nodes = nodes
        self.shared_state = shared_state
        self.execution_order = []
        self._should_stop = threading.Event()

    def _topological_sort(self, connections: List[Connection]):
        """Sort nodes within this process based on sync dependencies"""
        # Build dependency graph for nodes in this process
        node_names = {node.name for node in self.nodes}
        in_degree = {node.name: 0 for node in self.nodes}
        graph = {node.name: [] for node in self.nodes}

        for conn in connections:
            if (
                conn.from_node in node_names
                and conn.to_node in node_names
                and conn.conn_type == ConnectionType.SYNC
            ):
                graph[conn.from_node].append(conn.to_node)
                in_degree[conn.to_node] += 1

        # Topological sort
        queue_nodes = [node for node in node_names if in_degree[node] == 0]
        result = []

        while queue_nodes:
            current = queue_nodes.pop(0)
            result.append(current)

            for neighbor in graph[current]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue_nodes.append(neighbor)

        self.execution_order = result

    def run(self, connections: List[Connection]):
        """Run this process continuously"""
        self._topological_sort(connections)

        print(f"Process {self.process_id} starting with nodes: {self.execution_order}")

        # Create node lookup
        node_lookup = {node.name: node for node in self.nodes}

        while not self._should_stop.is_set():
            # Execute nodes in topological order
            local_data = {}

            for node_name in self.execution_order:
                node = node_lookup[node_name]

                # Gather inputs
                inputs = {}
                for input_name in node.inputs:
                    # Check if input comes from async connection
                    async_input = False
                    for conn in connections:
                        if (
                            conn.to_node == node_name
                            and conn.conn_type == ConnectionType.ASYNC
                        ):
                            # Get from shared state
                            inputs[input_name] = self.shared_state.get(input_name)
                            async_input = True
                            break

                    if not async_input:
                        # Get from local execution data
                        inputs[input_name] = local_data.get(input_name)

                # Execute function
                try:
                    if inputs:
                        result = node.func(**inputs)
                    else:
                        result = node.func()

                    # Store outputs
                    if isinstance(result, dict):
                        for output_name in node.outputs:
                            if output_name in result:
                                local_data[output_name] = result[output_name]

                                # Check if this output goes to async connection
                                for conn in connections:
                                    if (
                                        conn.from_node == node_name
                                        and conn.conn_type == ConnectionType.ASYNC
                                    ):
                                        self.shared_state.set(
                                            output_name, result[output_name]
                                        )
                    else:
                        # Single output
                        if node.outputs:
                            output_name = node.outputs[0]
                            local_data[output_name] = result

                            # Check if this output goes to async connection
                            for conn in connections:
                                if (
                                    conn.from_node == node_name
                                    and conn.conn_type == ConnectionType.ASYNC
                                ):
                                    self.shared_state.set(output_name, result)

                except Exception as e:
                    print(f"Error in node {node_name}: {e}")

            # Small delay to prevent overwhelming CPU
            time.sleep(0.01)

    def stop(self):
        self._should_stop.set()


class AsyncExecutionFramework:
    """Main framework that splits nodes into processes based on async connections"""

    def __init__(self):
        self.nodes: Dict[str, Node] = {}
        self.connections: List[Connection] = []
        self.shared_state = SharedState()
        self.processes: List[ExecutionProcess] = []
        self.threads: List[threading.Thread] = []

    def add_node(self, node: Node):
        """Add a node to the framework"""
        self.nodes[node.name] = node

    def add_connection(self, from_node: str, to_node: str, conn_type: ConnectionType):
        """Add a connection between nodes"""
        self.connections.append(Connection(from_node, to_node, conn_type))

    def _find_process_groups(self):
        """Split nodes into process groups based on async boundaries"""
        # Start with each node in its own group
        groups = {name: {name} for name in self.nodes.keys()}
        group_mapping = {name: name for name in self.nodes.keys()}

        # Merge groups connected by sync connections
        for conn in self.connections:
            if conn.conn_type == ConnectionType.SYNC:
                from_group = group_mapping[conn.from_node]
                to_group = group_mapping[conn.to_node]

                if from_group != to_group:
                    # Merge groups
                    merged_group = groups[from_group] | groups[to_group]

                    # Update group mapping for all nodes in merged groups
                    for node in merged_group:
                        group_mapping[node] = from_group

                    groups[from_group] = merged_group
                    del groups[to_group]

        return list(groups.values())

    def start(self):
        """Start the execution framework"""
        # Find process groups
        process_groups = self._find_process_groups()

        print(f"Found {len(process_groups)} process groups:")
        for i, group in enumerate(process_groups):
            print(f"  Process {i}: {list(group)}")

        # Create and start processes
        for i, group in enumerate(process_groups):
            group_nodes = [self.nodes[name] for name in group]
            process = ExecutionProcess(i, group_nodes, self.shared_state)
            self.processes.append(process)

            # Start process in separate thread
            thread = threading.Thread(target=process.run, args=(self.connections,))
            thread.daemon = True
            thread.start()
            self.threads.append(thread)

    def stop(self):
        """Stop all processes"""
        for process in self.processes:
            process.stop()


# Example usage - Wheelbase system
def teleop_func():
    """Continuously read keyboard input (simulated)"""
    import random

    return {"cmd_vel": random.choice(["forward", "backward", "left", "right", "stop"])}


def diff_drive_controller(cmd_vel=None):
    """Convert velocity commands to wheel speeds"""
    if cmd_vel is None:
        return {"left_speed": 0, "right_speed": 0}

    speed_map = {
        "forward": {"left_speed": 1.0, "right_speed": 1.0},
        "backward": {"left_speed": -1.0, "right_speed": -1.0},
        "left": {"left_speed": -0.5, "right_speed": 0.5},
        "right": {"left_speed": 0.5, "right_speed": -0.5},
        "stop": {"left_speed": 0, "right_speed": 0},
    }
    return speed_map.get(cmd_vel, {"left_speed": 0, "right_speed": 0})


def motor_driver(left_speed=None, right_speed=None):
    """Drive the motors"""
    print(f"Motors: Left={left_speed}, Right={right_speed}")
    return True


# Example from your diagram
def process_a():
    print("Process A executing")
    return {"data_a": f"A_{time.time():.2f}"}


def process_b():
    print("Process B executing")
    return {"data_b": f"B_{time.time():.2f}"}


def process_c(data_a=None):
    print(f"Process C executing with data_a={data_a}")
    return {"data_c": f"C_{time.time():.2f}"}


def process_c_red(data_b=None):
    print(f"Process C (red) executing with data_b={data_b}")
    return {"data_c_red": f"C_red_{time.time():.2f}"}


def process_d(data_c=None):
    print(f"Process D executing with data_c={data_c}")
    return {"data_d": f"D_{time.time():.2f}"}


def process_e(data_c=None):
    print(f"Process E executing with data_c={data_c}")
    return {"data_e": f"E_{time.time():.2f}"}


if __name__ == "__main__":
    # Example 1: Wheelbase system
    print("=== Wheelbase Example ===")
    framework1 = AsyncExecutionFramework()

    # Add nodes
    framework1.add_node(Node("teleop", teleop_func, outputs=["cmd_vel"]))
    framework1.add_node(
        Node(
            "controller",
            diff_drive_controller,
            inputs=["cmd_vel"],
            outputs=["left_speed", "right_speed"],
        )
    )
    framework1.add_node(
        Node("driver", motor_driver, inputs=["left_speed", "right_speed"])
    )

    # Add connections (T -> C is async, C -> D is sync)
    framework1.add_connection("teleop", "controller", ConnectionType.ASYNC)
    framework1.add_connection("controller", "driver", ConnectionType.SYNC)

    framework1.start()

    # Let it run for a few seconds
    time.sleep(3)
    framework1.stop()

    print("\n=== Diagram Example ===")
    # Example 2: Your diagram
    framework2 = AsyncExecutionFramework()

    # Add nodes
    framework2.add_node(Node("proc_a", process_a, outputs=["data_a"]))
    framework2.add_node(Node("proc_b", process_b, outputs=["data_b"]))
    framework2.add_node(
        Node("proc_c", process_c, inputs=["data_a"], outputs=["data_c"])
    )
    framework2.add_node(
        Node("proc_c_red", process_c_red, inputs=["data_b"], outputs=["data_c_red"])
    )
    framework2.add_node(
        Node("proc_d", process_d, inputs=["data_c"], outputs=["data_d"])
    )
    framework2.add_node(
        Node("proc_e", process_e, inputs=["data_c"], outputs=["data_e"])
    )

    # Add connections based on your description
    framework2.add_connection(
        "proc_a", "proc_c", ConnectionType.SYNC
    )  # Green stays together
    framework2.add_connection(
        "proc_b", "proc_c", ConnectionType.ASYNC
    )  # Green stays together
    framework2.add_connection(
        "proc_b", "proc_c_red", ConnectionType.SYNC
    )  # Red process B -> Red process C
    framework2.add_connection(
        "proc_c", "proc_d", ConnectionType.SYNC
    )  # Green C -> Green D (but will split due to async to E)
    framework2.add_connection(
        "proc_c", "proc_e", ConnectionType.ASYNC
    )  # Green C -> Yellow E (causes split)

    framework2.start()

    # Let it run for a few seconds
    time.sleep(3)
    framework2.stop()

    print("Done!")
