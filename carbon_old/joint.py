from dataclasses import dataclass

from .module import Module, ModuleReference, consumer


@dataclass
class JointState:
    position: float
    velocity: float


class ContinuousJoint(Module):
    def __init__(self, parent: ModuleReference, child: ModuleReference):
        super().__init__()
        self.parent = parent
        self.child = child

    @consumer(JointState)
    def update_state(self, state: JointState):
        # Update the state of the joint based on the received state
        print(f"Updating joint state: {state}")

    def _update_child(self):
        pass
