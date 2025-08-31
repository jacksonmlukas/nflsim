# Lightweight dataset registry (skeleton)
from dataclasses import dataclass

@dataclass
class Dataset:
    name: str
    version: str
    path: str
