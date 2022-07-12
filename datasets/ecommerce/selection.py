

files = open("/COOL/datasets/ecommerce/update-sample.csv")

contest = files.readlines()

time_range = {}

# group by products ID with time, and then count distinct product ids
i = -1
for line in contest:
    if i < 0:
        i += 1
        continue

    res = line.split(",")
    time = res[-2]
    pid = res[-1]

    key = "-".join(time.split("-")[:2])

    if key not in time_range:
        time_range[key] = []
        time_range[key].append(pid)
    else:
        time_range[key].append(pid)

for k, v in time_range.items():
    time_range[k] = list(set(v))

    print(str(k)+" : " + str(len(time_range[k])))

files.close()

