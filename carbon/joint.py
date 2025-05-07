from dataclasses import dataclass

from .module import Module, sink


@dataclass
class JointState:
    position: float
    velocity: float


class ContinuousJoint(Module):
    def __init__(self, parent: Module = None, child: Module = None, name=None):
        super().__init__()
        self.parent = parent
        self.child = child
        self.name = name

    @sink(JointState)
    def update_state(self):
        print(self.name)

    def _update_child(self):
        pass
