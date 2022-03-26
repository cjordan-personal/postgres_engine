import os
import pathlib

with open(str(pathlib.Path(__file__).parent.resolve()) + "/creds.txt", "r") as f:
    for line in f.readlines():
        a_pairs = line.rstrip().split("=")
        os.environ[a_pairs[0]] = a_pairs[1]
f.close()