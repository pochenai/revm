#!/usr/bin/env python3

import sys

def process_file(path):
    results = []
    last_exec_line = None

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")

            if line.startswith("executing block:"):
                last_exec_line = line
            elif line == "KZG Point Evaluation Precompile":
                # End of a section: keep the last executing-block line
                if last_exec_line is not None:
                    results.append(last_exec_line)
                    last_exec_line = None

    # No need to process trailing lines (your logic says only before KZG)

    return results


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python trim_kzg.py <input_file>")
        sys.exit(1)

    for line in process_file(sys.argv[1]):
        print(line)