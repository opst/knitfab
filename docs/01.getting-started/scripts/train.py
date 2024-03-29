import argparse
import gzip
import os
import pathlib
import random

import numpy as np
import torch
from torch.utils.data import DataLoader, TensorDataset

from model.mnist import MyMnistModel

# Set fixed seeds for reproducibility
np.random.seed(0)
torch.manual_seed(0)
random.seed(0)

# hyper parameters
BATCH_SIZE = 64
EPOCHES = 3

# parse args
parser = argparse.ArgumentParser()
parser.add_argument("--dataset", type=pathlib.Path)
parser.add_argument("--save-to", type=pathlib.Path)
args = parser.parse_args()

os.makedirs(args.save_to, exist_ok=True)

# Load Data
with gzip.open(args.dataset / "images.gz", "rb") as _img:
    img = (
        np.frombuffer(_img.read(), dtype=np.uint8, offset=16).reshape(-1, 28, 28).copy()
    )

with gzip.open(args.dataset / "labels.gz", "rb") as _label:
    bigendianInt32 = np.dtype(np.int32).newbyteorder(">")
    label = (
        np.frombuffer(_label.read(), dtype=bigendianInt32, offset=12)
        .reshape(-1, 8)
        .copy()
    )
    label = label.astype(np.int32)[:, 0].astype(np.uint8)

print(f"data shape:{img.shape}, type: {img.dtype}")
print(f"label shape:{label.shape}, type: {label.dtype}")

# split data into train and validation randomly
indice = np.asarray(range(0, 60000))
np.random.shuffle(indice)
trainIdx, validationIdx = indice[:50000], indice[50000:]

trainset = DataLoader(
    TensorDataset(
        torch.asarray(img[trainIdx]),
        torch.asarray(label[trainIdx]),
    ),
    batch_size=BATCH_SIZE,
    shuffle=True,
)
validationset = DataLoader(
    TensorDataset(
        torch.asarray(img[validationIdx]),
        torch.asarray(label[validationIdx]),
    ),
    batch_size=BATCH_SIZE,
    shuffle=False,
)

# Load Model
model = MyMnistModel()
optimizer = torch.optim.SGD(model.parameters(), lr=0.01)

best_loss = float("inf")
for epoch in range(1, EPOCHES + 1):
    # Train Model
    total = 0
    correct = 0
    total_loss = 0
    print(f"**TRAINING START** Epoch: #{epoch}")

    model.train()
    for batch, (data, label) in enumerate(trainset):
        data = data.reshape(-1, 1, 28, 28)

        pred = model(data.float() / 255.0)

        l = torch.eye(10)[label.tolist()]
        loss = torch.nn.functional.cross_entropy(pred, l)
        loss.backward()
        optimizer.step()
        optimizer.zero_grad()

        p = pred.argmax(dim=1)
        correct += (p == label).sum().item()
        total += len(label)
        total_loss += loss.item()

        if batch % 100 == 0:
            print(
                f"Epoch: #{epoch}, Batch: #{batch} -- Loss: {loss.item()}, Accuracy: {correct/total}"
            )

    print(
        f"**TRAINING RESULT** Epoch: #{epoch} -- total Loss: {total_loss}, Accuracy: {correct/total}"
    )

    print(f"**VALIDATION START** Epoch: #{epoch}")
    # Validate Model
    total = 0
    correct = 0
    total_loss = 0
    model.eval()
    with torch.no_grad():
        for batch, (data, label) in enumerate(validationset):
            data = data.reshape(-1, 1, 28, 28)
            pred = model(data.float() / 255.0)
            p = pred.argmax(dim=1)
            correct += (p == label).sum().item()
            l = torch.eye(10)[label.tolist()]
            loss = torch.nn.functional.cross_entropy(pred, l)
            total += len(label)
            total_loss += loss.item()

    val_acc = correct / total
    print(
        f"**VALIDATION RESULT** Epoch: #{epoch} -- total Loss: {total_loss}, Accuracy: {correct/total}"
    )

    # Save Model If It Is Best
    if total_loss < best_loss:
        print(f"**SAVING MODEL** at Epoch: #{epoch}")
        best_loss = total_loss
        torch.save(model.state_dict(), args.save_to / "model.pth")
