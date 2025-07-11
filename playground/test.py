from carbon.common_data_types import Position, Vector3, Velocity
from carbon.core.execution import ExecutionGraph
from carbon.core.module import Module, consumer, producer
from carbon.core.sources_sinks import Sink, Source
from carbon.data import Autofill, Data


class TestClass(Module):
    test_source = Source(Position)

    def __init__(self):
        super().__init__()

    @producer(test_source)
    def test_producer(self) -> Position:
        print("Producing position data")
        return Position(Autofill(), position=Vector3(1, 2, 3))


class TestSink(Module):
    test_sink = Sink(Position)

    def __init__(self):
        super().__init__()

    @consumer(test_sink)
    def test_consumer(self, velocity: Position):
        print(f"Consumed velocity: {velocity}")


class Root(Module):
    def __init__(self):
        super().__init__()
        self.test_class = TestClass()
        self.test_sink = TestSink()

        self.create_connection(
            self.test_class.test_source, self.test_sink.test_sink, sync=True
        )


root = Root()
execution = ExecutionGraph(root)
print(execution.processes, execution.process_layer_mapping)
execution.execute()
