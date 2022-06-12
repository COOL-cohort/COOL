



import pandas as pd

df = pd.read_csv('/Users/kevin/project_java/COOL/datasets/ecommerce/test4Cool/cool-ecommerce.csv')

df['Str_Reported_Date'] = df['Reported_Date'].map(lambda x: "-".join(x.split("-")[:2]))


df.to_csv('/Users/kevin/project_java/COOL/datasets/ecommerce/test4Cool/cool-ecommerce_dup1.csv', index=False)