import ast
from pyspark.sql import functions as f

def get_max_revenue(spark_dataframe):

    result = spark_dataframe.withColumn(
        "total_price",f.col("price") * f.col("books_count")
        ).select("total_price").groupBy().sum().collect()[0][0]
    return {"sum": f"{result:.2f}"}

def get_books_within_range(spark_dataframe, min_val, max_val):
    results = spark_dataframe.filter(
        (spark_dataframe["price"] >= min_val) & (spark_dataframe["price"] < max_val)
        ).toJSON().collect()
    
    return {"books": [ast.literal_eval(r) for r in results]}