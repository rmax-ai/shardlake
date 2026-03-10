#!/usr/bin/env python3
"""Generate random JSONL vector fixtures."""
import argparse
import json
import random

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--dims", type=int, default=8)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    random.seed(args.seed)
    for i in range(args.count):
        vec = [random.gauss(0, 1) for _ in range(args.dims)]
        print(json.dumps({"id": i + 1, "vector": vec, "metadata": {"idx": i}}))

if __name__ == "__main__":
    main()
