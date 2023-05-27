from calendar import month
from numpy import product
import pandas as pd
import os
import json


'''
Cohort Query Target
BirthEvent: no filter, for one Product_ID, any event can be consider as its birthDate
AgeSelector: Month from 0 -> 12
Cohort:1950
'''
cwd = os.getcwd()
ecommerce_data_path = os.path.join(
    cwd, "datasets", "ecommerce_query", "data.csv")
ecommerce_data_output_path = os.path.join(
    cwd, "datasets", "ecommerce_query", "sample_query", "query_result.json")

if __name__ == '__main__':
    data = pd.read_csv(ecommerce_data_path, parse_dates=[
                       "Reported_Date", "First_Reported_Date"])
    cohortRet = {}
    have_birth_products = set()
    for i in range(len(data)):
        item = data.iloc[i]
        product_id = item['Product_ID']
        if product_id not in have_birth_products:
            have_birth_products.add(product_id)

        # now have birth day, start to calculate
        cohort = str(item['First_Reported_Date'].to_pydatetime().date())

        if cohort not in cohortRet:
            cohortRet[cohort] = {}
        birth_date = item["First_Reported_Date"].to_pydatetime()
        action_date = item["Reported_Date"].to_pydatetime()

        duration = action_date - birth_date
        age = duration.days // 30

        if age not in cohortRet[cohort]:
            cohortRet[cohort][age] = {}

        if product_id not in cohortRet[cohort][age]:
            cohortRet[cohort][age][product_id] = 0

    cohortList = [str(i.to_pydatetime().date())
                  for i in set(data['First_Reported_Date'].tolist())]

    Ret = {}
    for cohort in cohortList:
        if cohort not in Ret:
            Ret[cohort] = [0] * 13
        age2set = cohortRet[cohort]
        for i in range(0, 13):
            if i in age2set:
                Ret[cohort][i] = len(age2set[i])
    # save
    with open(ecommerce_data_output_path, 'w', encoding='utf-8') as f:
        json.dump(Ret, f, ensure_ascii=False, indent=4)
