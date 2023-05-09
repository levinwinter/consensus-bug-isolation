import random

buggy = random.getrandbits(1)

if (buggy and random.getrandbits(1)):
    exit(1)

if (buggy and random.getrandbits(1)):
    exit(1)
