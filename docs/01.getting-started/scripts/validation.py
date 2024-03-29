import argparse
import gzip
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
BATCH_SIZE = 250
EPOCHES = 3

# parse args
parser = argparse.ArgumentParser()
parser.add_argument("--dataset", type=pathlib.Path)
parser.add_argument("--model", type=pathlib.Path)
parser.add_argument("--id", type=int)
args = parser.parse_args()

# Load Data
with gzip.open(args.dataset / "images.gz", "rb") as _img:
    img = torch.asarray(
        np.frombuffer(_img.read(), dtype=np.uint8, offset=16).reshape(-1, 28, 28).copy()
    )

with gzip.open(args.dataset / "labels.gz", "rb") as _label:
    bigendianInt32 = np.dtype(np.int32).newbyteorder(">")
    label = (
        np.frombuffer(_label.read(), dtype=bigendianInt32, offset=12)
        .reshape(-1, 8)
        .copy()
    )
    label = torch.asarray(label.astype(np.int32)[:, 0].astype(np.uint8))

model = MyMnistModel()
model.load_state_dict(torch.load(args.model))
model.eval()

# validation
if args.id is None:
    loader = DataLoader(
        TensorDataset(img, label),
        batch_size=BATCH_SIZE,
    )
    ok = 0
    total = 0
    for d, l in loader:
        total += len(l)
        output = model(d.reshape(-1, 1, 28, 28).float() / 255.0)
        pred = torch.argmax(output, dim=1)
        ok += torch.sum(pred == l).item()
        if total % 10000 == 0:
            print(f"Accuracy (at {total} images): {ok/total}")

    print()
    print("=== Validation Result ===")
    print(f"Accuracy: {ok/total}")
else:
    print("img shape", img.shape)
    print("label shape", label.shape)
    data = img[args.id].reshape(1, 1, 28, 28)
    l = label[args.id]
    output = model(data.float() / 255.0)
    pred = torch.argmax(output, dim=1)

    print(f"=== image ===")

    for row in data[0, 0]:
        print("".join("#" if 128 < x.item() else " " for x in row))

    print(f"=== ===== ===")
    print(f"Prediction: {pred}, Ground Truth: {l}")
