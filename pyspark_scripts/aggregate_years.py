import ast
import pyspark.sql.functions as f

def get_aggregated_years(spark_dataframe, year=None) -> dict:

    if year is not None:
        filtered_dataframe = spark_dataframe.filter(spark_dataframe.original_publication_year == year)
        result = _get_book_details_by_years(filtered_dataframe)
        return [ast.literal_eval(r) for r in result]    
    
    results = _get_book_details_by_years(spark_dataframe)
    
    return [ast.literal_eval(r) for r in results]


def _get_book_details_by_years(spark_dataframe):
    return spark_dataframe.groupBy("original_publication_year").agg(

        f.collect_list(f.struct(spark_dataframe.columns)).alias("book_details")
        
        ).toJSON().collect()