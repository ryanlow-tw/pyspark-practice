import ast
import pyspark.sql.functions as f


def get_aggregated_authors(spark_dataframe, author=None) -> dict:
    if author is not None:
        result = spark_dataframe.filter(spark_dataframe.author == author).groupBy("author").agg(
            f.collect_list(f.struct(spark_dataframe.columns)).alias("book_details")
            ).toJSON().collect()
        return [ast.literal_eval(r) for r in result]

    results = spark_dataframe.groupBy("author").agg(
        f.collect_list(f.struct(spark_dataframe.columns)).alias("book_details")
        ).toJSON().collect()

    return [ast.literal_eval(r) for r in results]