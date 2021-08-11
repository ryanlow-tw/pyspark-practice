import pytest
from tests import SPARK
from pyspark_scripts.price import get_max_revenue, get_books_within_range

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

def test_should_return_list_of_books_within_price_range(df_columns) -> None:

    test_data = [
        [51,"author1","book1","test_url","test_url",1000,100,1416914285,9.78142E+12,2007,"City of Bones","eng",2.00],
        [53,"author2","book2","test_url","test_url",1999,100,1416914285,9.78142E+12,2007,"City of Bones","eng",3.00],
        [54,"author3","book3","test_url","test_url",2000,100,1416914285,9.78142E+12,2007,"City of Bones","eng",4.00]
            ]

    test_dataframe = SPARK.createDataFrame(test_data, df_columns)

    result1 = {}
    result2 = {}
    expected_data1 = test_data[0]
    expected_data2 = test_data[1]

    for i, col in enumerate(df_columns):
        result1[col] = expected_data1[i]
        result2[col] = expected_data2[i]

    actual = get_books_within_range(test_dataframe, min_val=1000, max_val=2000)

    expected = {"books": [result1, result2]}

    assert actual == expected