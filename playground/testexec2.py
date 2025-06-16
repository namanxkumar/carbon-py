import queue
import sys
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


class OutputManager:
    """Manages columnar output for different processes"""

    def __init__(self, num_processes: int, column_width: int = 30):
        self.num_processes = num_processes
        self.column_width = column_width
        self.lock = threading.Lock()
        self.process_outputs = {i: [] for i in range(num_processes)}
        self.max_lines = 0

    def add_output(self, process_id: int, message: str):
        """Add output for a specific process"""
        with self.lock:
            self.process_outputs[process_id].append(message)
            self.max_lines = max(self.max_lines, len(self.process_outputs[process_id]))

    def print_header(self):
        """Print column headers"""
        header = ""
        for i in range(self.num_processes):
            header += f"Process {i}".ljust(self.column_width)
        print(header)
        print("=" * (self.column_width * self.num_processes))

    def print_current_state(self):
        """Print current state of all processes in columns"""
        with self.lock:
            # Clear screen and print header
            print("\033[2J\033[H", end="")  # Clear screen and move cursor to top
            self.print_header()

            # Print each line
            for line_num in range(self.max_lines):
                line = ""
                for process_id in range(self.num_processes):
                    if line_num < len(self.process_outputs[process_id]):
                        content = self.process_outputs[process_id][line_num]
                    else:
                        content = ""
                    line += content.ljust(self.column_width)
                print(line)

            # Keep only last 10 lines per process to avoid memory issues
            for process_id in range(self.num_processes):
                if len(self.process_outputs[process_id]) > 10:
                    self.process_outputs[process_id] = self.process_outputs[process_id][
                        -10:
                    ]

            self.max_lines = min(self.max_lines, 10)


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

    def __init__(
        self,
        process_id: int,
        nodes: List[Node],
        shared_state: SharedState,
        output_manager: OutputManager = None,
    ):
        self.process_id = process_id
        self.nodes = nodes
        self.shared_state = shared_state
        self.output_manager = output_manager
        self.execution_order = []
        self._should_stop = threading.Event()

    def log(self, message: str):
        """Log message to output manager if available, otherwise print normally"""
        if self.output_manager:
            self.output_manager.add_output(self.process_id, message)
        else:
            print(f"Process {self.process_id}: {message}")

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

        self.log(f"Starting with: {self.execution_order}")

        # Create node lookup
        node_lookup = {node.name: node for node in self.nodes}
        iteration = 0

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

                    # Log execution
                    self.log(f"{node_name}: {result}")

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
                    self.log(f"ERROR in {node_name}: {e}")

            iteration += 1
            # Small delay to prevent overwhelming CPU
            time.sleep(0.5)  # Increased delay for better readability

    def stop(self):
        self._should_stop.set()


class AsyncExecutionFramework:
    """Main framework that splits nodes into processes based on async connections"""

    def __init__(self, enable_columnar_output: bool = True):
        self.nodes: Dict[str, Node] = {}
        self.connections: List[Connection] = []
        self.shared_state = SharedState()
        self.processes: List[ExecutionProcess] = []
        self.threads: List[threading.Thread] = []
        self.enable_columnar_output = enable_columnar_output
        self.output_manager: Optional[OutputManager] = None
        self.display_thread: Optional[threading.Thread] = None
        self._should_stop_display = threading.Event()

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

    def _display_loop(self):
        """Continuously update the display"""
        while not self._should_stop_display.is_set():
            if self.output_manager:
                self.output_manager.print_current_state()
            time.sleep(0.1)  # Update display 10 times per second

    def start(self):
        """Start the execution framework"""
        # Find process groups
        process_groups = self._find_process_groups()

        if self.enable_columnar_output:
            self.output_manager = OutputManager(len(process_groups))

            # Start display thread
            self.display_thread = threading.Thread(target=self._display_loop)
            self.display_thread.daemon = True
            self.display_thread.start()
        else:
            print(f"Found {len(process_groups)} process groups:")
            for i, group in enumerate(process_groups):
                print(f"  Process {i}: {list(group)}")

        # Create and start processes
        for i, group in enumerate(process_groups):
            group_nodes = [self.nodes[name] for name in group]
            process = ExecutionProcess(
                i, group_nodes, self.shared_state, self.output_manager
            )
            self.processes.append(process)

            # Start process in separate thread
            thread = threading.Thread(target=process.run, args=(self.connections,))
            thread.daemon = True
            thread.start()
            self.threads.append(thread)

    def stop(self):
        """Stop all processes"""
        self._should_stop_display.set()
        for process in self.processes:
            process.stop()


# Example usage - Wheelbase system
def teleop_func():
    """Continuously read keyboard input (simulated)"""
    import random

    cmd = random.choice(["forward", "backward", "left", "right", "stop"])
    return {"cmd_vel": cmd}


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
    return f"L:{left_speed} R:{right_speed}"


# Example from your diagram
def process_a():
    import random

    val = random.randint(1, 100)
    return {"data_a": f"A_{val}"}


def process_b():
    import random

    val = random.randint(1, 100)
    return {"data_b": f"B_{val}"}


def process_c(data_a=None):
    return {"data_c": f"C({data_a})"}


def process_c_red(data_b=None):
    return {"data_c_red": f"C_red({data_b})"}


def process_d(data_c=None):
    return {"data_d": f"D({data_c})"}


def process_e(data_c=None):
    return {"data_e": f"E({data_c})"}


if __name__ == "__main__":
    print("Choose example:")
    print("1. Wheelbase system")
    print("2. Diagram example")
    print("3. Both (non-columnar)")
    choice = input("Enter choice (1-3): ").strip()

    if choice == "1":
        print("=== Wheelbase Example (Columnar Output) ===")
        framework = AsyncExecutionFramework(enable_columnar_output=True)

        # Add nodes
        framework.add_node(Node("teleop", teleop_func, outputs=["cmd_vel"]))
        framework.add_node(
            Node(
                "controller",
                diff_drive_controller,
                inputs=["cmd_vel"],
                outputs=["left_speed", "right_speed"],
            )
        )
        framework.add_node(
            Node("driver", motor_driver, inputs=["left_speed", "right_speed"])
        )

        # Add connections (T -> C is async, C -> D is sync)
        framework.add_connection("teleop", "controller", ConnectionType.ASYNC)
        framework.add_connection("controller", "driver", ConnectionType.SYNC)

        framework.start()

        # Let it run for a while
        try:
            time.sleep(10)
        except KeyboardInterrupt:
            pass
        framework.stop()

    elif choice == "2":
        print("=== Diagram Example (Columnar Output) ===")
        framework = AsyncExecutionFramework(enable_columnar_output=True)

        # Add nodes
        framework.add_node(Node("proc_a", process_a, outputs=["data_a"]))
        framework.add_node(Node("proc_b", process_b, outputs=["data_b"]))
        framework.add_node(
            Node("proc_c", process_c, inputs=["data_a"], outputs=["data_c"])
        )
        framework.add_node(
            Node("proc_c_red", process_c_red, inputs=["data_b"], outputs=["data_c_red"])
        )
        framework.add_node(
            Node("proc_d", process_d, inputs=["data_c"], outputs=["data_d"])
        )
        framework.add_node(
            Node("proc_e", process_e, inputs=["data_c"], outputs=["data_e"])
        )

        # Add connections based on your description
        framework.add_connection(
            "proc_a", "proc_c", ConnectionType.SYNC
        )  # Green stays together
        framework.add_connection(
            "proc_b", "proc_c_red", ConnectionType.SYNC
        )  # Red B -> Red C (sync within red)
        framework.add_connection(
            "proc_c", "proc_d", ConnectionType.SYNC
        )  # Green C -> Green D
        framework.add_connection(
            "proc_c", "proc_e", ConnectionType.ASYNC
        )  # Green C -> Yellow E (async boundary)

        framework.start()

        try:
            time.sleep(10)
        except KeyboardInterrupt:
            pass
        framework.stop()

    else:
        # Run both examples without columnar output
        print("=== Wheelbase Example ===")
        framework1 = AsyncExecutionFramework(enable_columnar_output=False)

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

        framework1.add_connection("teleop", "controller", ConnectionType.ASYNC)
        framework1.add_connection("controller", "driver", ConnectionType.SYNC)

        framework1.start()
        time.sleep(3)
        framework1.stop()

        print("\n=== Diagram Example ===")
        framework2 = AsyncExecutionFramework(enable_columnar_output=False)

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

        framework2.add_connection(
            "proc_a", "proc_c", ConnectionType.SYNC
        )  # Green stays together
        framework2.add_connection(
            "proc_b", "proc_c_red", ConnectionType.ASYNC
        )  # Red process B -> Red process C
        framework2.add_connection(
            "proc_c", "proc_d", ConnectionType.SYNC
        )  # Green C -> Green D (but will split due to async to E)
        framework2.add_connection(
            "proc_c", "proc_e", ConnectionType.ASYNC
        )  # Green C -> Yellow E (causes split)

        framework2.start()
        time.sleep(3)
        framework2.stop()
