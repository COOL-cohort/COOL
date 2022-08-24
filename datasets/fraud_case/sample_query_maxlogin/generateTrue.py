import numpy as np
import math
import time
import pickle
import os
import pandas as pd
import random
import json
from datetime import datetime

'''
Cohort Query Target
BirthEvent: select "update_phone" or 'update_pwd' in the same day using teh same clientIP
AgeSelector: from 0->7
Cohort: suspicious actions_date_clientIP
'''


cwd = os.getcwd()
data_dir = os.path.join(cwd, "datasets", "fraud_case")
data_path = os.path.join(cwd, "data.csv")
ret_path = os.path.join(cwd, "sample_query_maxlogin", "query_result.json")

def getAge(eventday, birthday):
    
    eventday = datetime.strptime(date, '%Y-%m-%d').date()
    birthday = datetime.strptime(birthday, '%Y-%m-%d').date()
    delta_days = eventday - birthday
    
    return delta_days.days

if __name__ == '__main__':
    sample_data = pd.read_csv(data_path)
    #print()
    suspicious_actions = [
        'dXBkYXRlX3Bob25l',
        'bW9kaWZ5X3Bhc3N3b3Jk']

    sample_data_ATO = sample_data[sample_data['actions'].isin(suspicious_actions)]
    cohort_candidates = sample_data_ATO.groupby(['clientIP', 'date', 'actions'])['userid'].apply(list)
    cohort_candidates = cohort_candidates[cohort_candidates.str.len() > 2]

    cohort_logincount = {}
    print(len(cohort_candidates))
    for i in range(len(cohort_candidates)):
        print(i)
        cohort_name = '_'.join(cohort_candidates.index[i])
        birthday = cohort_candidates.index[i][1]
        login_count = [0] * 8
        max_login = 0
        max_login_userid = 0
        for userid in cohort_candidates.values[i]:
            new_login_count = [0] * 8
            login_dates = sample_data[sample_data['userid'] == userid][sample_data['actions'] == 'bG9naW4=']['date']
            #print(login_dates)
            valid_dates = []
            for date in login_dates:

                delta_days = getAge(date, birthday)

                if delta_days < 0 or delta_days > 7:
                    continue
                valid_dates.append(date)
                new_login_count[delta_days] += 1
                
            if len(valid_dates) > max_login:
                max_login = len(valid_dates)
                max_login_userid = userid
                login_count = new_login_count
                
        #login_count = [i/len(cohort_candidates.values[i]) for i in login_count]
        cohort_logincount[cohort_name] = login_count

    with open(ret_path, 'w', encoding='utf-8') as f:
        json.dump(cohort_logincount, f, ensure_ascii=False, indent=4)
