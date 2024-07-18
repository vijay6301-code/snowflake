df = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/manibigdata5@gmail.com/Superstore.csv")
from pyspark.sql.functions import col,to_date,regexp_replace,when
data_types={
    "row_id":"int",
    "order_id":"string",
    "Order_date":"Date",
    "ship_date":"date",
    "ship_mode":"string",
    "Customer_id":"string",
    "Segment":"string",
    "Country":"String",
    "city":"string",
    "State":"string",
    "postal_code":"int",
    "Region":"String",
    "product_id":"string",
    "category":"string",
    "sub_category":"string",
    "product_name":"String",
    "sales":"float",
    "quantity":"int",
    "discount":"float",
    "profit":"float"
}
df = df.withColumn("Ship_Date",
                   when(col("Ship_Date").contains("/"),\
                        to_date(col("Ship_Date"), "M/d/yyyy"))\
                   .otherwise(to_date(col("Ship_Date"), "MM-dd-yyyy")))\
        .withColumn("order_Date",
                   when(col("order_Date").contains("/"),\
                        to_date(col("order_Date"), "M/d/yyyy"))\
                   .otherwise(to_date(col("order_Date"), "MM-dd-yyyy")))
def cast_columns(df, data_types):
    for column, new_type in data_types.items():
        df = df.withColumn(column, col(column).cast(new_type))
    return df

super_store_df = cast_columns(df,data_types)

super_store_df.select ("order_date","ship_date").show(10)