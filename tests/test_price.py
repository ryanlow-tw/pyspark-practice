import pytest
from tests import SPARK
from pyspark_scripts.price import get_max_revenue

@pytest.fixture(name="df_columns")
def fixture_columns():
    columns = [
        'id',
        'author',
        'title',
        'image_url',
        'small_image_url',
        'price',
        'books_count',
        'isbn',
        'isbn13',
        'original_publication_year',
        'original_title',
        'language_code',
        'average_rating'
    ]
    return columns

def test_should_return_max_revenue(df_columns) -> None:

    test_data = [
        [51,"author1","book1","test_url","test_url",1000,100,1416914285,9.78142E+12,2007,"City of Bones","eng",2.00],
        [53,"author2","book2","test_url","test_url",1000,100,1416914285,9.78142E+12,2007,"City of Bones","eng",3.00],
        [54,"author3","book3","test_url","test_url",1000,100,1416914285,9.78142E+12,2007,"City of Bones","eng",4.00]
            ]

    test_dataframe = SPARK.createDataFrame(test_data, df_columns)

    actual = get_max_revenue(test_dataframe)
    expected = {"sum": "300000.00"}

    assert actual == expected

def test_should_return_max_revenue_formatted(df_columns) -> None:

    test_data = [
        [51,"author1","book1","test_url","test_url",1.2345,100,1416914285,9.78142E+12,2007,"City of Bones","eng",2.00]
            ]

    test_dataframe = SPARK.createDataFrame(test_data, df_columns)

    actual = get_max_revenue(test_dataframe)
    expected = {"sum": "123.45"}

    assert actual == expected