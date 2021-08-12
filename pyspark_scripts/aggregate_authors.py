import ast
import pyspark.sql.functions as f


def get_aggregated_authors(spark_dataframe, author=None) -> dict:
    if author is not None:
        filtered_dataframe = spark_dataframe.filter(spark_dataframe.author == author)
        result = _get_book_details_by_authors(filtered_dataframe)
        return [ast.literal_eval(r) for r in result]

    results = _get_book_details_by_authors(spark_dataframe)

    return [ast.literal_eval(r) for r in results]

def _get_book_details_by_authors(spark_dataframe):
    return spark_dataframe.groupBy("author").agg(
        
        f.collect_list(f.struct(spark_dataframe.columns)).alias("book_details")
        
        ).toJSON().collect()