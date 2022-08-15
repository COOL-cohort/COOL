import numpy as np
import pandas as pd
import os
import datetime
from tqdm import tqdm
import json
# import argparse

# parser = argparse.ArgumentParser(description= "Add the input Data Path")
# parser.add_argument("data_path", type=str, )
'''
Cohort Query Target
BirthEvent: select two "Disease-A" in field "prescribe"
AgeSelector: from 0->7
Cohort:1950-1960, 1960-1970, 1970-1980, 1980-1990, 1990-2000
'''

cwd = os.getcwd()
data_dir = os.path.join(cwd, "datasets", "health_raw")
data_path = os.path.join(data_dir, "data.csv")
ret_path = os.path.join(data_dir,"sample_query_count","query_result.json")


def generateCohort(year: int) -> str:
    if year < 1950 or year > 2000:
        return None
    n_interval = (year - 1950) // 10
    left_margin = 1950 + n_interval * 10
    
    right_margin = left_margin + 10 if left_margin + 10 <= 2000 else 2001
    
    return str(left_margin) + "-" + str(right_margin)


def generateAge(birth_date: datetime.date, action_date: datetime.date) -> int:
    dtime = action_date - birth_date
    if dtime.days > 7 or dtime.days < 0:
        return -1
    return dtime.days


if __name__ == '__main__':
    data = pd.read_csv(data_path, parse_dates=['time'])
    # check the birthday of every user
    data_medicine_a = data[data["prescribe"] == "Medicine-A"]
    userId2time = {}
    userId2medicineACount = {}
    for i in range(len(data_medicine_a)):
        item = data_medicine_a.iloc[i]
        user_id = item["id"]
        if user_id in userId2time:
            continue
        if user_id not in userId2medicineACount:
            userId2medicineACount[user_id] = 0
        oldCount = userId2medicineACount[user_id]
        if oldCount + 1 == 2:
            userId2time[user_id] = item["time"]
            continue
        userId2medicineACount[user_id] += 1

    result = {}
    # key : cohort , value : dict
    # value -> k: age , v: Set {user_key}
    # print("Selected Users :" + str(userId2time.keys()))
    for user_key, birth_time in tqdm(userId2time.items()):
        user_sub_data = data[(data["id"] == user_key) &
                             (data["labtest"] == "Labtest-C")]
        for i in range(len(user_sub_data)):
            item = user_sub_data.iloc[i]
            age = generateAge(birth_time.date(), item["time"].date())
            if age == -1:
                continue
            cohort = generateCohort(int(item["birthyear"]))
            if cohort == None:
                continue

            # check value
            v = int(item["value"])
            if v < 131 and v >= 45:
                continue

            # update result
            if cohort not in result:
                result[cohort] = {}
            if age not in result[cohort]:
                result[cohort][age] = list()
            if user_key in result[cohort][age]:
                print("duplicate user_key", user_key)
            result[cohort][age].append(user_key)

    # summary count, fill None Value
    cohortRet = {}
    for y in range(1950, 2001, 10):
        cohort = generateCohort(y)
        if cohort not in cohortRet:
            cohortRet[cohort] = [0] * 8

    for cohort, age_dict in result.items():
        for age, user_set in age_dict.items():
            cohortRet[cohort][age] = len(user_set)

    # Save cohortRet into json file
    '''CohortRet
    {
        "CohortName": []
    }
    '''

    with open(ret_path, 'w', encoding='utf-8') as f:
        json.dump(cohortRet, f, ensure_ascii=False, indent=4)
