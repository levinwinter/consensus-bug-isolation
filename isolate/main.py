import os

from isolate import isolate, Report

from runs import runs as all_runs

reports = []

for (dirpath, _, filenames) in os.walk('data'):
    if (len(filenames) == 0):
        continue
    if int(dirpath.split('/')[-1]) not in all_runs:
        continue
    print(dirpath)
    with open(os.path.join(dirpath, 'results.txt'), 'r') as f:
        correct = 'reason: all committed' in f.read()
    all_observations = []
    observations = {}
    for i in range(0, 7):
        with open(os.path.join(dirpath, f'validator_{i}.txt'), 'r') as f:
            for line in f.readlines():
                if not line.startswith('PRED'):
                    continue
                line = line.strip()[5:]
                id = " ".join(line.split(" ")[:-1])
                if 'PRED' in id:
                    continue
                observation = line.split(" ")[-1] == "1"
                if id not in observations:
                    observations[id + ' is true'] = observation
                    observations[id + ' is false'] = not observation
                else:
                    observations[id + ' is true'] = observation or observations[id + ' is true']
                    observations[id + ' is false'] = (not observation) or observations[id + ' is false']
        all_observations.append(observations)
    # merged_observations = {}
    # merged_observations2 = {}
    # for observations in all_observations[:5]:
    #     for id, value in observations.items():
    #         if id not in merged_observations:
    #             merged_observations[id] = (1 if value else 0, 1 if not value else 0)
    #         else:
    #             merged_observations[id] = (merged_observations[id][0] + (1 if value else 0), merged_observations[id][1] + (1 if not value else 0))
    # for observations in all_observations[2:]:
    #     for id, value in observations.items():
    #         if id not in merged_observations2:
    #             merged_observations2[id] = (1 if value else 0, 1 if not value else 0)
    #         else:
    #             merged_observations2[id] = (merged_observations2[id][0] + (1 if value else 0), merged_observations2[id][1] + (1 if not value else 0))
    # observations = {}
    # for value, (t, f) in merged_observations.items():
    #     # if 'LedgerTrie.h' in value:
    #     #     continue
    #     for i in range(0, 5):
    #         observations[f'{value} for >{i}'] = t > i
    # for value, (t, f) in merged_observations2.items():
    #     # if 'LedgerTrie.h' in value:
    #     #     continue
    #     for i in range(2, 7):
    #         if f'{value} - > {i}' in observations:
    #             observations[f'{value} for >{i}'] = observations[f'{value} for >{i}'] or t > i
    #         else:
    #             observations[f'{value} for >{i}'] = t > i
    reports.append(Report(correct, observations, dirpath.split('/')[-1]))

# for _ in range(int(sys.argv[1])):  # iterations
#     cmd = subprocess.run(["python3", temp.name], capture_output=True)
#     output = cmd.stdout.decode().splitlines()

#     observations = {}
#     for line in output:
#         observation = line.split(" ")[0] == "True"
#         id = " ".join(line.split(" ")[1:])
#         if id not in observations:
#             observations[id] = observation
#         else:
#             observations[id] = observation or observations[id]

#     reports.append(Report(cmd.returncode == 0, observations))

isolate(reports)
