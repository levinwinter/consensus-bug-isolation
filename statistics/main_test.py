import os
import sys
import tempfile
import subprocess

from isolate import isolate, Report
from instrument import instrument

if len(sys.argv) < 3:
    print("iterations and/or path to instrumented test-script is missing")
    print("expected:", sys.argv[0], "#iterations", "path/to/script")
    exit(1)

iterations = int(sys.argv[1])
script = sys.argv[2]

reports = []

temp = tempfile.NamedTemporaryFile()
temp.write(str.encode(instrument(script)))
temp.flush()

# print(instrument(script))

for i in range(iterations):
    cmd = subprocess.run(["python3", temp.name], capture_output = True)
    output = cmd.stdout.decode().splitlines()

    # print(cmd.stderr.decode())

    observations = {}
    for line in output:
        observation = line.split(" ")[0] == "True"
        id = " ".join(line.split(" ")[1:])
        if id not in observations:
            observations[id] = observation
        else:
            observations[id] = observation or observations[id]

    reports.append(Report(cmd.returncode == 0, observations))

isolate(reports)
