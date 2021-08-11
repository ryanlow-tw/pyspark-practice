import logging

from pyspark.sql import SparkSession

from pyspark_scripts.aggregate_authors import get_aggregated_authors
from pyspark_scripts.aggregate_years import get_aggregated_years
from pyspark_scripts.price import get_max_revenue, get_books_within_range
from pyspark_scripts.ratings import get_formatted_average_rating
from pyspark_scripts.ratings import get_less_rated_books, get_highly_rated_books

def run(spark: SparkSession, input_path: str) -> None:
    logging.info("Reading text file from %sinput_path", input_path)
    input_df = spark.read.option("inferSchema", "true") \
        .option("header", "true") \
        .csv(input_path)

    print("=======================================================")
    print(get_aggregated_authors(input_df))
    print("=======================================================")
    print(get_aggregated_authors(input_df, author="Tom Clancy"))
    print("=======================================================")
    print(get_aggregated_years(input_df))
    print("=======================================================")
    print(get_aggregated_years(input_df, 1980))
    print("=======================================================")
    print(get_max_revenue(input_df))
    print("=======================================================")
    print(get_books_within_range(input_df, min_val=1000, max_val=2000))
    print("=======================================================")
    print(get_formatted_average_rating(input_df))
    print("=======================================================")
    print(get_less_rated_books(input_df))
    print("=======================================================")
    print(get_highly_rated_books(input_df))
    print("=======================================================")
