import numpy as np
import math
import time
import pickle
import itertools
import pandas as pd
import random
from datetime import datetime

sample_data = pd.read_csv('fraud_samples_small.csv')
susp_action = 'bW9kaWZ5X3Bhc3N3b3Jk'

date = [datetime.utcfromtimestamp(t).strftime('%Y-%m-%d') for t in sample_data['timestamp']]
sample_data['date'] = date

sample_data1 = sample_data[sample_data['actions'] == susp_action]
cohort_candidates = sample_data1.groupby(['clientIP', 'date'])['users'].apply(list)

cohort = {}
idx = 0
for x in cohort_candidates.values:
    if len(x) > 1:
        cohort[idx] = x
        idx += 1
        #print(x)
        
cohort = pd.Series(cohort)
#print(cohort)
cohort.to_csv('cohort.csv')





