import sys
import logging

from pyspark.sql import SparkSession
from pyspark_scripts.analytics import run

APP_NAME = "BooksAnalytics"
LOG_FILENAME = 'project.log'
if __name__ == '__main__':
    logging.basicConfig(filename=LOG_FILENAME, level=logging.info)
    logging.info(sys.argv)

    if len(sys.argv) < 2:
        logging.warning("Input .csv file is required")
        sys.exit(1)

    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    sc = spark.sparkContext
    app_name = sc.appName

    logging.info("Application Initialized: %s", app_name)
    input_path = sys.argv[1]

    run(spark, input_path)
    logging.info("Application Done: %s", spark.sparkContext.appName)
    spark.stop()





