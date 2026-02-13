import pandas as pd

df=pd.read_csv("C:\\Belgeler\\sales-etl-pipeline\\data\\Walmart_Sales.csv")

print("Shape (satır,sütün):",df.shape)
print("\nColumns:",df.columns.tolist())
print("\nInfoÇ",df.info())
print("\nMissingValues:",df.isnull().sum().sort_values(ascending=False).head(20))
print("\nFirst 5 rows:",df.head())

