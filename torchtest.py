import torch.nn as nn


class ChildModule(nn.Module):
    def __init__(self):
        super().__init__()
        # This automatically registers LinearLayer as a child module
        self.linear = nn.Linear(10, 5)


class ParentModule(nn.Module):
    def __init__(self):
        super().__init__()
        # This automatically registers LinearLayer as a child module
        self.linear = nn.Linear(10, 5)
        self.child = ChildModule()


module = ParentModule()

print(module)
print(module._modules)
