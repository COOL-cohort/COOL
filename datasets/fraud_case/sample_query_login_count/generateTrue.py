import pandas as pd
import os
from copy import deepcopy
import numpy as np
from datetime import datetime
from tqdm import tqdm
import json

'''
Cohort Query Target
BirthEvent: select "update_phone" or 'update_pwd' in the same day using teh same clientIP
AgeSelector: from 0->14
Cohort: suspicious actions_date_clientIP
'''


cwd = os.getcwd()
data_dir = os.path.join(cwd, "datasets", "fraud_case")
data_path = os.path.join(data_dir, "data.csv")
query_result_path = os.path.join(data_dir, "sample_query_login_count", "query_result.json")

def getAge(eventday:str, birthday:str):
    
    eventday = datetime.strptime(eventday, '%Y-%m-%d').date()
    birthday = datetime.strptime(birthday, '%Y-%m-%d').date()
    delta_days = eventday - birthday
    
    return delta_days.days

if __name__ == '__main__':
    data = pd.read_csv(data_path)

    user_birthdate = {}
    cohortRet = {}
    for i in tqdm(range(len(data))):
        item = data.iloc[i]
        user_id = item['userid']
        is_birth = item['isBirth']
        if is_birth == 1:
            user_birthdate[user_id] = item['date']
            continue
        if user_id not in user_birthdate:
            continue

        if item['actions'] != 'bG9naW4=':
            continue

        birthdate = user_birthdate[user_id]
        cohort_name = item['cohort']
        age = getAge(item['date'], birthdate)
        if age < 0 or age > 14:
            continue

        if cohort_name not in cohortRet:
            cohortRet[cohort_name] = [0] * 15
        cohortRet[cohort_name][age] += 1

    with open(query_result_path, 'w', encoding= 'utf-8') as f:
        json.dump(cohortRet, f, ensure_ascii=False, indent=4)