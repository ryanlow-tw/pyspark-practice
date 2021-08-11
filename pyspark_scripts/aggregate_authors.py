import ast

def get_aggregated_authors(spark_dataframe, author=None) -> dict:

    if author is not None:
        # books = get_books_by_year(spark_dataframe, year)
        # result = {str(year): [ast.literal_eval(b) for b in books]}

        return ""
    
    unique_authors = get_unique_authors(spark_dataframe)
    
    results = {}

    for unique_author in unique_authors:
        books = get_books_by_author(spark_dataframe, unique_author)
        results[unique_author] = [ast.literal_eval(book) for book in books]

    return results

def get_unique_authors(spark_dataframe) -> list:

    num_unique = len(spark_dataframe.select(
        "author"
            ).distinct().orderBy("author").collect())

    results = []

    for i in range(num_unique):
        unique_year = str(spark_dataframe.select(
            "author"
            ).distinct().orderBy("author").collect()[i][0])
        results.append(unique_year)
    
    return results

def get_books_by_author(spark_dataframe, author) -> list:
    books = spark_dataframe.filter(
            spark_dataframe["author"] == author
                ).toJSON().collect()
    return books