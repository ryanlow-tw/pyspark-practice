import ast
from pyspark.sql import functions as f


def get_average_rating(spark_dataframe) -> float:
    average_rating = spark_dataframe.select(f.avg("average_rating")).collect()[0][0]

    return round(average_rating, 2)

def get_highly_rated_books(spark_dataframe) -> dict:
    average_rating = get_average_rating(spark_dataframe)

    result = spark_dataframe.filter(
        spark_dataframe["average_rating"] >= average_rating
        ).toJSON().collect()

    return {"highly_rated": [ast.literal_eval(r) for r in result]}

def get_less_rated_books(spark_dataframe) -> dict:
    average_rating = get_average_rating(spark_dataframe)

    result = spark_dataframe.filter(
        spark_dataframe["average_rating"] < average_rating
        ).toJSON().collect()

    return {"less_rated": [ast.literal_eval(r) for r in result]}

def get_formatted_average_rating(spark_dataframe):
    average_rating = get_average_rating(spark_dataframe)

    return {"mean": f"{average_rating:.2f}"}