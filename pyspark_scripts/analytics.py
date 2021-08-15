import logging

from pyspark.sql import SparkSession

from pyspark_scripts.aggregate import get_aggregated_data
from pyspark_scripts.price import get_max_revenue, get_books_within_range
from pyspark_scripts.ratings import get_less_rated_books, get_highly_rated_books

def run(spark: SparkSession, input_path: str) -> None:
    logging.info("Reading text file from %s", input_path)
    input_df = spark.read.option("inferSchema", "true") \
        .option("header", "true") \
        .csv(input_path)

    logging.info("Logging get_max_revenue...")
    logging.info(get_max_revenue(input_df))
    logging.info("Logging get_books_within_range...")
    logging.info(get_books_within_range(input_df, min_val=1000, max_val=2000))
    logging.info("Logging get_less_rated_books...")
    logging.info(get_less_rated_books(input_df))
    logging.info("Logging get_highly_rated_books...")
    logging.info(get_highly_rated_books(input_df))
    logging.info("Logging get_aggregated_authors")
    logging.info(get_aggregated_data(input_df, column='author'))
    logging.info("Logging get_aggregated_authors Tom Clancy...")
    logging.info(get_aggregated_data(input_df, column='author', value="Tom Clancy"))
    logging.info("Logging get_aggregated_years...")
    logging.info(get_aggregated_data(input_df, column='original_publication_year'))
    logging.info("Logging get_aggregated_years... 1980")
    logging.info(get_aggregated_data(input_df, column='original_publication_year', value=1980))
    
