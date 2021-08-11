import ast

def get_aggregated_years(spark_dataframe, year=None):
    if year is not None:
        return ""
    
    unique_years = get_unique_years(spark_dataframe)
    
    results = {}

    for unique_year in unique_years:

        books = spark_dataframe.filter(
            spark_dataframe["original_publication_year"] == int(unique_year)
                ).toJSON().collect()
    
        results[unique_year] = [ast.literal_eval(book) for book in books]

    return results

def get_unique_years(spark_dataframe) -> list :

    num_unique = len(spark_dataframe.select(
        "original_publication_year"
            ).distinct().orderBy("original_publication_year").collect())

    results = []

    for i in range(num_unique):
        unique_year = str(spark_dataframe.select(
            "original_publication_year"
            ).distinct().orderBy("original_publication_year").collect()[i][0])
        results.append(unique_year)
    
    return results

