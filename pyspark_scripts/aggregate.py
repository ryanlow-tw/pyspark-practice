import ast
import pyspark.sql.functions as f

def get_aggregated_data(spark_dataframe, column, value=None) -> dict:

    if value is not None:
        filtered_dataframe = spark_dataframe.filter(
            spark_dataframe[column] == value)

        result = _get_book_details_by_data(filtered_dataframe, column)
        return [ast.literal_eval(r) for r in result]

    results = _get_book_details_by_data(spark_dataframe, column)

    return [ast.literal_eval(r) for r in results]


def _get_book_details_by_data(spark_dataframe, column):
    return spark_dataframe.groupBy(column).agg(

        f.collect_list(f.struct(spark_dataframe.columns)).alias("book_details")

        ).toJSON().collect()
