



import pandas as pd

# this will order the update-sample_ori.csv by Reported_date and then write back to another file
df = pd.read_csv('/COOL/datasets/ecommerce/update-sample_ori.csv.csv')
df.sort_values("Reported_Date", inplace=True)
df.to_csv('/COOL/datasets/ecommerce/update-sample.csv', index=False)