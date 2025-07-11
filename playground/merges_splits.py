from typing import Tuple

from carbon.core import ConfigurableSink, ExecutionGraph, Module, sink, source
from carbon.data import Data


class DataA(Data):
    a: int = 1


class ModuleA(Module):
    def __init__(self):
        super().__init__()
        self.a = 0

    @source(DataA)
    def method_a(self) -> DataA:
        self.a += 1
        return DataA(a=self.a)


class ModuleA2(Module):
    def __init__(self):
        super().__init__()
        self.a = 0

    @source(DataA)
    def method_a2(self) -> DataA:
        self.a -= 1
        return DataA(a=self.a)


class DataB(Data):
    a: int = 1


class ModuleB(Module):
    def __init__(self):
        super().__init__()

    @source(DataB)
    def method_b(self) -> DataB:
        return DataB()


class DataC(Data):
    a: int = 1


class ModuleC(Module):
    def __init__(self):
        super().__init__()

    @source(DataC)
    def method_a(self) -> DataC:
        return DataC()


class DataD(Data):
    a: int = 1


class ModuleD(Module):
    def __init__(self):
        super().__init__()

    @source(DataD, DataD)
    def method_a(self) -> Tuple[DataD, DataD]:
        return DataD(), DataD()


class ModuleE(Module):
    def __init__(self):
        super().__init__()

    @sink(ConfigurableSink(DataA), ConfigurableSink(DataB))
    def method_e(self, data: DataA, data2: DataB) -> None:
        print(f"ModuleE received DataA: {data.a} and DataB: {data2.a}")

    @sink(DataC)
    def method_b(self, data: DataC) -> None:
        print(f"ModuleE received DataC: {data.a}")

    @sink(DataD)
    def method_c(self, data1: DataD) -> None:
        print(f"ModuleE received DataD: {data1.a}")


class Orchestration(Module):
    def __init__(self):
        super().__init__()
        self.module_a = ModuleA()
        self.module_a2 = ModuleA2()
        self.module_b = ModuleB()
        self.module_b2 = ModuleB()
        # self.module_c = ModuleC()
        # self.module_d = ModuleD()
        self.module_e = ModuleE()
        # self.module_e2 = ModuleE()

        self.create_connection(
            (DataA, DataB), (self.module_a, self.module_b), self.module_e, sync=True
        )
        self.create_connection(
            (DataA, DataB), (self.module_a2, self.module_b2), self.module_e, sync=True
        )
        # self.create_connection(DataC, self.module_c, self.module_e, sync=True)
        # self.create_connection(
        #     (DataD, DataD), self.module_d, (self.module_e, self.module_e2), sync=True
        # )


orchestration = Orchestration()
execution_graph = ExecutionGraph(root_module=orchestration)
print("\nExecution Graph Layers:")
print(execution_graph.layers)
print("\nProcess Groups:")
for process_index, process in execution_graph.processes.items():
    print(f"Process {process_index}:")
    for method in process:
        print(
            f"  {method} (depends on: {method.dependencies}, produces: {method.dependents})"
        )
print(execution_graph.process_layer_mapping)
print("\nConnections:")
for connection in orchestration.get_connections():
    print(connection)
execution_graph.execute()
print("\nExecution completed.")
