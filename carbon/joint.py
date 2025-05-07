from dataclasses import dataclass

from .module import Module, ModuleReference, sink


@dataclass
class JointState:
    position: float
    velocity: float


class ContinuousJoint(Module):
    def __init__(self, parent: ModuleReference, child: ModuleReference):
        super().__init__()
        self.parent = parent
        self.child = child

    @sink(JointState)
    def update_state(self):
        pass

    def _update_child(self):
        pass
