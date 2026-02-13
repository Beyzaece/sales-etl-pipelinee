import pandas as pd
def extract_data():
    df=pd.read_csv("C:\\Belgeler\\sales-etl-pipeline\\data\Walmart_Sales.csv")
    return df


if __name__=="__main__":
    df=extract_data()
    print(df.head())