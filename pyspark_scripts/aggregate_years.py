import ast

def get_aggregated_years(spark_dataframe, year=None) -> dict:

    if year is not None:
        books = get_books_by_year(spark_dataframe, year)
        result = {str(year): [ast.literal_eval(b) for b in books]}

        return result
    
    unique_years = get_unique_years(spark_dataframe)
    
    results = {}

    for unique_year in unique_years:
        books = get_books_by_year(spark_dataframe, unique_year)
        results[unique_year] = [ast.literal_eval(book) for book in books]

    return results

def get_unique_years(spark_dataframe) -> list:

    unique_years = spark_dataframe.select(
        "original_publication_year"
            ).distinct().orderBy("original_publication_year").collect()

    num_unique = len(unique_years)

    return [str(unique_years[i][0]) for i in range(num_unique)]
    

def get_books_by_year(spark_dataframe, year) -> list:
    books = spark_dataframe.filter(
            spark_dataframe["original_publication_year"] == year
                ).toJSON().collect()
    return books