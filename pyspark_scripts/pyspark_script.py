from pyspark.sql import functions as f

def get_average_rating(spark_dataframe) -> float:

    average_rating = spark_dataframe.select(f.avg("average_rating")).collect()[0][0]

    return average_rating