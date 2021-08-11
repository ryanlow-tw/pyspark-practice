import ast


def get_aggregated_authors(spark_dataframe, author=None) -> dict:

    if author is not None:
        books = get_books_by_author(spark_dataframe, author)
        result = {author: [ast.literal_eval(b) for b in books]}

        return result
    
    unique_authors = get_unique_authors(spark_dataframe)
    
    results = {}

    for unique_author in unique_authors:
        books = get_books_by_author(spark_dataframe, unique_author)
        results[unique_author] = [ast.literal_eval(book) for book in books]

    return results

def get_unique_authors(spark_dataframe) -> list:


    unique_authors = spark_dataframe.select(
            "author"
            ).distinct().orderBy("author").collect()

    num_unique = len(unique_authors)

    return [unique_authors[i][0] for i in range(num_unique)]

def get_books_by_author(spark_dataframe, author) -> list:
    books = spark_dataframe.filter(
            spark_dataframe["author"] == author
                ).toJSON().collect()
    return books