import pandas as pd

df = pd.read_csv('data/rating_complete.csv')
df = df.head(300000)
df.to_csv('data/recommendation_data.csv', index = False)