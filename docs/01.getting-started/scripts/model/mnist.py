import torch
import torch.functional as F


class MyMnistModel(torch.nn.Module):
    def __init__(self):
        super().__init__()
        self.layers = torch.nn.Sequential(
            torch.nn.Conv2d(1, 16, kernel_size=3, padding=1),  # 1x28x28 -> 16x28x28
            torch.nn.ReLU(),
            torch.nn.Conv2d(16, 32, kernel_size=3, padding=1),  # 16x28x28 -> 32x28x28
            torch.nn.ReLU(),
            torch.nn.Flatten(),
            torch.nn.Linear(32 * 28 * 28, 1024),
            torch.nn.ReLU(),
            torch.nn.Linear(1024, 10),
        )

    def forward(self, x):
        logit = self.layers(x)
        return logit
