import os

import pandas as pd

cwd = os.getcwd()
data_dir = os.path.join(cwd, "datasets", "olap-tpch")
data_path = os.path.join(data_dir, "scripts", "data.csv")
query_path = os.path.join(data_dir, "query.json")


def get_res():
    df = pd.read_csv(data_path)
    rows = df[
        (df["O_ORDERPRIORITY"] == "2-HIGH") &
        (df["R_NAME"] == "EUROPE") &
        (df["O_ORDERDATE"] >= "1993-01-01") &
        (df["O_ORDERDATE"] < "1994-01-02")
        ]
    print(rows.indexs)


if __name__ == "__main__":
    get_res()
