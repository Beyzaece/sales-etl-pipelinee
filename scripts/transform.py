import pandas as pd
from logger import get_logger
logger=get_logger("transform")

def transform_data(df : pd.DataFrame)-> pd.DataFrame:
    df=df.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    df["date"]=pd.to_datetime(df["date"],dayfirst=True,errors="coerce")

    expected_int_cols=["store","holiday_flag"]
    for col in expected_int_cols:
        df[col]=pd.to_numeric(df[col],errors="coerce").astype("Int64")


    df.loc[df["weekly_sales"]<0,"weekly_sales"]=pd.NA

    numeric_cols=["weekly_sales", "temperature", "fuel_price", "cpi", "unemployment"]
    for col in numeric_cols:
        df[col]=pd.to_numeric(df[col],errors="coerce")

    for col in numeric_cols:
        df[col]=df[col].fillna(df[col].median())
    
    df=df.drop_duplicates()
    df=df.sort_values(["store","date"]).reset_index(drop=True)

    df["pipeline_loaded_at"]=pd.Timestamp.utcnow()
    
    return df
if __name__=="__main__":
    raw_df=pd.read_csv("C:\\Belgeler\\sales-etl-pipeline\\data\\Walmart_Sales.csv")
    clean=transform_data(raw_df)

    logger.info("RAW:",raw_df.shape)
    logger.info("Clean:",clean.shape)
    logger.info(clean.dtypes)
    logger.info("Nulls:",clean.isnull().sum().sum())

    clean.to_csv("data/clean_walmart_sales.csv", index=False)
    logger.info("Saved->data/clean_walmart_sales.csv")
